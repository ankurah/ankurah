use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    property::backend::backend_from_string,
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::{Attestation, AttestationSet, Attested, EntityState, EventId, OperationSet, State, StateBuffers};

use futures_util::{pin_mut, TryStreamExt};

pub mod sql_builder;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, CollectionId, EntityId, Event};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql};
use tracing::{debug, error, info, warn};

/// Default connection pool size for `Postgres::open()`.
/// Production applications should configure their own pool via `Postgres::new()`.
pub const DEFAULT_POOL_SIZE: u32 = 15;

/// Default connection timeout in seconds
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

pub struct Postgres {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

impl Postgres {
    pub fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> { Ok(Self { pool }) }

    pub async fn open(uri: &str) -> anyhow::Result<Self> {
        let manager = PostgresConnectionManager::new_from_stringlike(uri, NoTls)?;
        let pool = bb8::Pool::builder()
            .max_size(DEFAULT_POOL_SIZE)
            .connection_timeout(Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS))
            .build(manager)
            .await?;
        Self::new(pool)
    }

    // TODO: newtype this to `BucketName(&str)` with a constructor that
    // only accepts a subset of characters.
    pub fn sane_name(collection: &str) -> bool {
        for char in collection.chars() {
            match char {
                char if char.is_alphanumeric() => {}
                char if char.is_numeric() => {}
                '_' | '.' | ':' => {}
                _ => return false,
            }
        }

        true
    }
}

/// Compute advisory lock key from a string identifier
fn advisory_lock_key(identifier: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    identifier.hash(&mut hasher);
    hasher.finish() as i64
}

/// Acquire a PostgreSQL advisory lock for DDL operations on a collection
async fn acquire_ddl_lock(client: &tokio_postgres::Client, collection_id: &str) -> Result<i64, StateError> {
    let lock_key = advisory_lock_key(&format!("ankurah_ddl:{}", collection_id));
    debug!("Acquiring advisory lock {} for collection {}", lock_key, collection_id);
    client.execute("SELECT pg_advisory_lock($1)", &[&lock_key]).await.map_err(|err| {
        error!("Failed to acquire advisory lock for {}: {:?}", collection_id, err);
        StateError::DDLError(Box::new(err))
    })?;
    Ok(lock_key)
}

/// Release a PostgreSQL advisory lock
async fn release_ddl_lock(client: &tokio_postgres::Client, lock_key: i64) -> Result<(), StateError> {
    debug!("Releasing advisory lock {}", lock_key);
    client.execute("SELECT pg_advisory_unlock($1)", &[&lock_key]).await.map_err(|err| {
        error!("Failed to release advisory lock {}: {:?}", lock_key, err);
        StateError::DDLError(Box::new(err))
    })?;
    Ok(())
}

#[async_trait]
impl StorageEngine for Postgres {
    type Value = PGValue;

    async fn collection(&self, collection_id: &CollectionId) -> Result<std::sync::Arc<dyn StorageCollection>, RetrievalError> {
        if !Postgres::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let mut client = self.pool.get().await.map_err(RetrievalError::storage)?;

        // get the current schema from the database
        let schema = client.query_one("SELECT current_database()", &[]).await.map_err(RetrievalError::storage)?;
        let schema = schema.get("current_database");

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            schema,
            collection_id: collection_id.clone(),
            columns: Arc::new(RwLock::new(Vec::new())),
            #[cfg(debug_assertions)]
            last_spilled_predicate: Arc::new(RwLock::new(None)),
        };

        // Acquire advisory lock to serialize DDL operations for this collection
        let lock_key = acquire_ddl_lock(&client, collection_id.as_str()).await?;

        // Create tables if they don't exist (protected by advisory lock)
        let result = async {
            bucket.create_state_table(&mut client).await?;
            bucket.create_event_table(&mut client).await?;
            bucket.rebuild_columns_cache(&mut client).await?;
            Ok::<_, StateError>(())
        }
        .await;

        // Always release the lock, even if DDL failed
        release_ddl_lock(&client, lock_key).await?;

        result?;
        Ok(Arc::new(bucket))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut client = self.pool.get().await.map_err(|err| MutationError::General(Box::new(err)))?;

        // Get all tables in the public schema
        let query = r#"
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        "#;

        let rows = client.query(query, &[]).await.map_err(|err| MutationError::General(Box::new(err)))?;
        if rows.is_empty() {
            return Ok(false);
        }

        // Start a transaction to drop all tables atomically
        let transaction = client.transaction().await.map_err(|err| MutationError::General(Box::new(err)))?;

        // Drop each table
        for row in rows {
            let table_name: String = row.get("table_name");
            let drop_query = format!(r#"DROP TABLE IF EXISTS "{}""#, table_name);
            transaction.execute(&drop_query, &[]).await.map_err(|err| MutationError::General(Box::new(err)))?;
        }

        // Commit the transaction
        transaction.commit().await.map_err(|err| MutationError::General(Box::new(err)))?;

        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct PostgresColumn {
    pub name: String,
    pub is_nullable: bool,
    pub data_type: String,
}

pub struct PostgresBucket {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    collection_id: CollectionId,
    schema: String,
    columns: Arc<RwLock<Vec<PostgresColumn>>>,
    /// Tracks the last predicate that spilled to post-filtering (debug builds only)
    #[cfg(debug_assertions)]
    last_spilled_predicate: Arc<RwLock<Option<ankql::ast::Predicate>>>,
}

impl PostgresBucket {
    fn state_table(&self) -> String { self.collection_id.as_str().to_string() }

    pub fn event_table(&self) -> String { format!("{}_event", self.collection_id.as_str()) }

    /// Returns the last predicate that spilled to post-filtering (debug builds only).
    ///
    /// Use this in tests to verify queries are fully pushed down to PostgreSQL:
    /// ```rust,ignore
    /// let spilled = bucket.last_spilled_predicate();
    /// assert!(spilled.is_none(), "Expected full pushdown, but got spill: {:?}", spilled);
    /// ```
    #[cfg(debug_assertions)]
    pub fn last_spilled_predicate(&self) -> Option<ankql::ast::Predicate> { self.last_spilled_predicate.read().unwrap().clone() }

    /// Rebuild the cache of columns in the table.
    pub async fn rebuild_columns_cache(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        debug!("PostgresBucket({}).rebuild_columns_cache", self.collection_id);
        let column_query =
            r#"SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_catalog = $1 AND table_name = $2;"#
                .to_string();
        let mut new_columns = Vec::new();
        debug!("Querying existing columns: {:?}, [{:?}, {:?}]", column_query, &self.schema, &self.collection_id.as_str());
        let rows = client
            .query(&column_query, &[&self.schema, &self.collection_id.as_str()])
            .await
            .map_err(|err| StateError::DDLError(Box::new(err)))?;
        for row in rows {
            let is_nullable: String = row.get("is_nullable");
            new_columns.push(PostgresColumn {
                name: row.get("column_name"),
                is_nullable: is_nullable.eq("YES"),
                data_type: row.get("data_type"),
            })
        }

        let mut columns = self.columns.write().unwrap();
        *columns = new_columns;
        drop(columns);

        Ok(())
    }

    pub fn existing_columns(&self) -> Vec<String> {
        let columns = self.columns.read().unwrap();
        columns.iter().map(|column| column.name.clone()).collect()
    }

    pub fn column(&self, column_name: &String) -> Option<PostgresColumn> {
        let columns = self.columns.read().unwrap();
        columns.iter().find(|column| column.name == *column_name).cloned()
    }

    pub fn has_column(&self, column_name: &String) -> bool { self.column(column_name).is_some() }

    pub async fn create_event_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(43) PRIMARY KEY,
                "entity_id" character(22),
                "operations" bytea,
                "parent" character(43)[],
                "attestations" bytea
            )"#,
            self.event_table()
        );

        debug!("{create_query}");
        client.execute(&create_query, &[]).await.map_err(|e| StateError::DDLError(Box::new(e)))?;
        Ok(())
    }

    pub async fn create_state_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(22) PRIMARY KEY,
                "state_buffer" BYTEA,
                "head" character(43)[],
                "attestations" BYTEA[]
            )"#,
            self.state_table()
        );

        debug!("{create_query}");
        match client.execute(&create_query, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // Log full error details for debugging
                if let Some(db_err) = err.as_db_error() {
                    error!("PostgresBucket({}).create_state_table error: {} (code: {:?})", self.collection_id, db_err, db_err.code());
                } else {
                    error!("PostgresBucket({}).create_state_table error: {:?}", self.collection_id, err);
                }
                Err(StateError::DDLError(Box::new(err)))
            }
        }
    }

    pub async fn add_missing_columns(
        &self,
        client: &mut tokio_postgres::Client,
        missing: Vec<(String, &'static str)>, // column name, datatype
    ) -> Result<(), StateError> {
        if missing.is_empty() {
            return Ok(());
        }

        // Acquire advisory lock to serialize DDL operations for this collection
        let lock_key = acquire_ddl_lock(client, self.collection_id.as_str()).await?;

        let result = async {
            // Re-check columns after acquiring lock (another session may have added them)
            self.rebuild_columns_cache(client).await?;

            for (column, datatype) in missing {
                if Postgres::sane_name(&column) && !self.has_column(&column) {
                    let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, self.state_table(), column, datatype);
                    info!("PostgresBucket({}).add_missing_columns: {}", self.collection_id, alter_query);
                    match client.execute(&alter_query, &[]).await {
                        Ok(_) => {}
                        Err(err) => {
                            // Log full error details for debugging
                            if let Some(db_err) = err.as_db_error() {
                                warn!(
                                    "Error adding column {} to table {}: {} (code: {:?})",
                                    column,
                                    self.state_table(),
                                    db_err,
                                    db_err.code()
                                );
                            } else {
                                warn!("Error adding column {} to table {}: {:?}", column, self.state_table(), err);
                            }
                            self.rebuild_columns_cache(client).await?;
                            return Err(StateError::DDLError(Box::new(err)));
                        }
                    }
                }
            }

            self.rebuild_columns_cache(client).await?;
            Ok(())
        }
        .await;

        // Always release the lock
        release_ddl_lock(client, lock_key).await?;

        result
    }
}

#[async_trait]
impl StorageCollection for PostgresBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let state_buffers = bincode::serialize(&state.payload.state.state_buffers)?;
        let attestations: Vec<Vec<u8>> = state.attestations.iter().map(bincode::serialize).collect::<Result<Vec<_>, _>>()?;
        let id = state.payload.entity_id;

        // Ensure head is not empty for new records
        if state.payload.state.head.is_empty() {
            warn!("Warning: Empty head detected for entity {}", id);
        }

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;

        let mut columns: Vec<String> = vec!["id".to_owned(), "state_buffer".to_owned(), "head".to_owned(), "attestations".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        params.push(&id);
        params.push(&state_buffers);
        params.push(&state.payload.state.head);
        params.push(&attestations);

        let mut materialized: Vec<(String, Option<PGValue>)> = Vec::new();
        let mut seen_properties = std::collections::HashSet::new();

        // Process property values directly from state buffers
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (column, value) in backend.property_values() {
                if !seen_properties.insert(column.clone()) {
                    // Skip if property already seen in another backend
                    // TODO: this should cause all (or subsequent?) fields with the same name
                    // to be suffixed with the property id when we have property ids
                    // requires some thought (and field metadata) on how to do this right
                    continue;
                }

                let pg_value: Option<PGValue> = value.map(|value| value.into());
                if !self.has_column(&column) {
                    // We don't have the column yet and we know the type.
                    if let Some(ref pg_value) = pg_value {
                        self.add_missing_columns(&mut client, vec![(column.clone(), pg_value.postgres_type())]).await?;
                    } else {
                        // The column doesn't exist yet and we don't have a value.
                        // This means the entire column is already null/none so we
                        // don't need to set anything.
                        continue;
                    }
                }

                materialized.push((column.clone(), pg_value));
            }
        }

        for (name, parameter) in &materialized {
            columns.push(name.clone());

            match &parameter {
                Some(value) => match value {
                    PGValue::CharacterVarying(string) => params.push(string),
                    PGValue::SmallInt(number) => params.push(number),
                    PGValue::Integer(number) => params.push(number),
                    PGValue::BigInt(number) => params.push(number),
                    PGValue::DoublePrecision(float) => params.push(float),
                    PGValue::Bytea(bytes) => params.push(bytes),
                    PGValue::Boolean(bool) => params.push(bool),
                    PGValue::Jsonb(json_val) => params.push(json_val),
                },
                None => params.push(&UntypedNull),
            }
        }
        let columns_str = columns.iter().map(|name| format!("\"{}\"", name)).collect::<Vec<String>>().join(", ");
        let values_str = params.iter().enumerate().map(|(index, _)| format!("${}", index + 1)).collect::<Vec<String>>().join(", ");
        let columns_update_str = columns
            .iter()
            .enumerate()
            .skip(1) // Skip "id"
            .map(|(index, name)| format!("\"{}\" = ${}", name, index + 1))
            .collect::<Vec<String>>()
            .join(", ");

        // be careful with sql injection via bucket name
        let query = format!(
            r#"WITH old_state AS (
                SELECT "head" FROM "{0}" WHERE "id" = $1
            )
            INSERT INTO "{0}"({1}) VALUES({2})
            ON CONFLICT("id") DO UPDATE SET {3}
            RETURNING (SELECT "head" FROM old_state) as old_head"#,
            self.state_table(),
            columns_str,
            values_str,
            columns_update_str
        );

        debug!("PostgresBucket({}).set_state: {}", self.collection_id, query);
        let mut created_table = false;
        let row = loop {
            match client.query_one(&query, params.as_slice()).await {
                Ok(row) => break row,
                Err(err) => {
                    let kind = error_kind(&err);
                    if let ErrorKind::UndefinedTable { table } = kind {
                        if table == self.state_table() && !created_table {
                            self.create_state_table(&mut client).await?;
                            created_table = true;
                            continue; // retry exactly once
                        }
                    }
                    return Err(StateError::DDLError(Box::new(err)).into());
                }
            }
        };

        // If this is a new entity (no old_head), or if the heads are different, return true
        let old_head: Option<Clock> = row.get("old_head");
        let changed = match old_head {
            None => true, // New entity
            Some(old_head) => old_head != state.payload.state.head,
        };

        debug!("PostgresBucket({}).set_state: Changed: {}", self.collection_id, changed);
        Ok(changed)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        // be careful with sql injection via bucket name
        let query = format!(r#"SELECT "id", "state_buffer", "head", "attestations" FROM "{}" WHERE "id" = $1"#, self.state_table());

        let mut client = match self.pool.get().await {
            Ok(client) => client,
            Err(err) => {
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        debug!("PostgresBucket({}).get_state: {}", self.collection_id, query);
        let rows = match client.query(&query, &[&id]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.state_table() {
                        self.create_state_table(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
                        return Err(RetrievalError::EntityNotFound(id));
                    }
                }
                return Err(RetrievalError::StorageError(err.into()));
            }
        };

        let row = match rows.into_iter().next() {
            Some(row) => row,
            None => return Err(RetrievalError::EntityNotFound(id)),
        };

        debug!("PostgresBucket({}).get_state: Row: {:?}", self.collection_id, row);
        let row_id: EntityId = row.try_get("id").map_err(RetrievalError::storage)?;
        assert_eq!(row_id, id);

        let serialized_buffers: Vec<u8> = row.try_get("state_buffer").map_err(RetrievalError::storage)?;
        let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&serialized_buffers).map_err(RetrievalError::storage)?;
        let head: Clock = row.try_get("head").map_err(RetrievalError::storage)?;
        let attestation_bytes: Vec<Vec<u8>> = row.try_get("attestations").map_err(RetrievalError::storage)?;
        let attestations = attestation_bytes
            .into_iter()
            .map(|bytes| bincode::deserialize(&bytes))
            .collect::<Result<Vec<Attestation>, _>>()
            .map_err(RetrievalError::storage)?;

        Ok(Attested {
            payload: EntityState {
                entity_id: id,
                collection: self.collection_id.clone(),
                state: State { state_buffers: StateBuffers(state_buffers), head },
            },
            attestations: AttestationSet(attestations),
        })
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("fetch_states: {:?}", selection);
        let mut client = self.pool.get().await.map_err(|err| RetrievalError::StorageError(Box::new(err)))?;

        // Pre-filter selection based on cached schema to avoid undefined column errors.
        // If we see columns not in our cache, refresh it first (they might have been added).
        // TODO: Once property metadata is in the system catalog, we can create missing columns
        // on-demand here instead of refreshing the cache each time we see unknown columns.
        let referenced = selection.referenced_columns();
        let cached = self.existing_columns();
        let unknown_to_cache: Vec<&String> = referenced.iter().filter(|col| !cached.contains(col)).collect();

        // Refresh cache if we see columns we haven't seen before
        if !unknown_to_cache.is_empty() {
            debug!("PostgresBucket({}).fetch_states: Unknown columns {:?}, refreshing schema cache", self.collection_id, unknown_to_cache);
            self.rebuild_columns_cache(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
        }

        // Now check with (possibly refreshed) cache - columns still missing truly don't exist
        let existing = self.existing_columns();
        let missing: Vec<String> = referenced.into_iter().filter(|col| !existing.contains(col)).collect();

        let effective_selection = if missing.is_empty() {
            selection.clone()
        } else {
            debug!("PostgresBucket({}).fetch_states: Columns {:?} don't exist, treating as NULL", self.collection_id, missing);
            selection.assume_null(&missing)
        };

        // Split predicate into parts we can pushdown to PostgreSQL vs post-filter in Rust
        let split = sql_builder::split_predicate_for_postgres(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate; // Cache before moving sql_predicate
        debug!(
            "PostgresBucket({}).fetch_states: SQL predicate: {:?}, remaining: {:?}, needs_post_filter: {}",
            self.collection_id, split.sql_predicate, remaining_predicate, needs_post_filter
        );

        // Track spilled predicate for test assertions (debug builds only)
        #[cfg(debug_assertions)]
        {
            let mut spilled = self.last_spilled_predicate.write().unwrap();
            *spilled = if needs_post_filter { Some(remaining_predicate.clone()) } else { None };
        }

        // Track spilled predicate for test assertions (debug builds only)
        #[cfg(debug_assertions)]
        {
            let spilled = if needs_post_filter { Some(remaining_predicate.clone()) } else { None };
            *self.last_spilled_predicate.write().unwrap() = spilled;
        }

        // Build SQL with only the pushdown-capable predicate
        let sql_selection = ankql::ast::Selection {
            predicate: split.sql_predicate,
            order_by: effective_selection.order_by.clone(),
            limit: if needs_post_filter {
                None // Can't limit in SQL if we need to post-filter (would drop valid results)
            } else {
                effective_selection.limit
            },
        };

        let mut results = Vec::new();
        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer", "head", "attestations"]);
        builder.table_name(self.state_table());
        builder.selection(&sql_selection)?;

        let (sql, args) = builder.build()?;
        debug!("PostgresBucket({}).fetch_states: SQL: {} with args: {:?}", self.collection_id, sql, args);

        let stream = match client.query_raw(&sql, args).await {
            Ok(stream) => stream,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.state_table() {
                        // Table doesn't exist yet, return empty results
                        return Ok(Vec::new());
                    }
                }
                return Err(RetrievalError::StorageError(err.into()));
            }
        };
        pin_mut!(stream);

        while let Some(row) = stream.try_next().await.map_err(RetrievalError::storage)? {
            let id: EntityId = row.try_get(0).map_err(RetrievalError::storage)?;
            let state_buffer: Vec<u8> = row.try_get(1).map_err(RetrievalError::storage)?;
            let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer).map_err(RetrievalError::storage)?;
            let head: Clock = row.try_get("head").map_err(RetrievalError::storage)?;
            let attestation_bytes: Vec<Vec<u8>> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations = attestation_bytes
                .into_iter()
                .map(|bytes| bincode::deserialize(&bytes))
                .collect::<Result<Vec<Attestation>, _>>()
                .map_err(RetrievalError::storage)?;

            results.push(Attested {
                payload: EntityState {
                    entity_id: id,
                    collection: self.collection_id.clone(),
                    state: State { state_buffers: StateBuffers(state_buffers), head },
                },
                attestations: AttestationSet(attestations),
            });
        }

        // Post-filter results if we have remaining predicate that couldn't be pushed down
        let results = if needs_post_filter {
            debug!(
                "PostgresBucket({}).fetch_states: Post-filtering {} results with remaining predicate",
                self.collection_id,
                results.len()
            );
            let filtered = post_filter_states(&results, &remaining_predicate, &self.collection_id);

            // Apply limit after post-filter if needed
            if let Some(limit) = effective_selection.limit {
                filtered.into_iter().take(limit as usize).collect()
            } else {
                filtered
            }
        } else {
            results
        };

        Ok(results)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        let operations = bincode::serialize(&entity_event.payload.operations)?;
        let attestations = bincode::serialize(&entity_event.attestations)?;

        let query = format!(
            r#"INSERT INTO "{0}"("id", "entity_id", "operations", "parent", "attestations") VALUES($1, $2, $3, $4, $5)
               ON CONFLICT ("id") DO NOTHING"#,
            self.event_table(),
        );

        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;
        debug!("PostgresBucket({}).add_event: {}", self.collection_id, query);
        let mut created_table = false;
        let affected = loop {
            match client
                .execute(
                    &query,
                    &[
                        &entity_event.payload.id(),
                        &entity_event.payload.entity_id,
                        &operations,
                        &entity_event.payload.parent,
                        &attestations,
                    ],
                )
                .await
            {
                Ok(affected) => break affected,
                Err(err) => {
                    let kind = error_kind(&err);
                    if let ErrorKind::UndefinedTable { table } = kind {
                        if table == self.event_table() && !created_table {
                            self.create_event_table(&mut client).await?;
                            created_table = true;
                            continue; // retry exactly once
                        }
                    }
                    error!("PostgresBucket({}).add_event: Error: {:?}", self.collection_id, err);
                    return Err(StateError::DMLError(Box::new(err)).into());
                }
            }
        };

        Ok(affected > 0)
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let query = format!(
            r#"SELECT "id", "entity_id", "operations", "parent", "attestations" FROM "{0}" WHERE "id" = ANY($1)"#,
            self.event_table(),
        );

        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let rows = match client.query(&query, &[&event_ids]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                match kind {
                    ErrorKind::UndefinedTable { table } if table == self.event_table() => return Ok(Vec::new()),
                    _ => return Err(RetrievalError::storage(err)),
                }
            }
        };

        let mut events = Vec::new();
        for row in rows {
            let entity_id: EntityId = row.try_get("entity_id").map_err(RetrievalError::storage)?;
            let operations: OperationSet = row.try_get("operations").map_err(RetrievalError::storage)?;
            let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
            let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(RetrievalError::storage)?;

            let event = Attested {
                payload: Event { collection: self.collection_id.clone(), entity_id, operations, parent },
                attestations: AttestationSet(attestations),
            };
            events.push(event);
        }
        Ok(events)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, ankurah_core::error::RetrievalError> {
        let query =
            format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{0}" WHERE "entity_id" = $1"#, self.event_table(),);

        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        debug!("PostgresBucket({}).get_events: {}", self.collection_id, query);
        let rows = match client.query(&query, &[&entity_id]).await {
            Ok(rows) => rows,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.event_table() {
                        return Ok(Vec::new());
                    }
                }

                return Err(RetrievalError::storage(err));
            }
        };

        let mut events = Vec::new();
        for row in rows {
            // let event_id: EventId = row.try_get("id").map_err(|err| RetrievalError::storage(err))?;
            let operations_binary: Vec<u8> = row.try_get("operations").map_err(RetrievalError::storage)?;
            let operations = bincode::deserialize(&operations_binary).map_err(RetrievalError::storage)?;
            let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
            let attestations_binary: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
            let attestations: Vec<Attestation> = bincode::deserialize(&attestations_binary).map_err(RetrievalError::storage)?;

            events.push(Attested {
                payload: Event { collection: self.collection_id.clone(), entity_id, operations, parent },
                attestations: AttestationSet(attestations),
            });
        }

        Ok(events)
    }
}

// Some hacky shit because rust-postgres doesn't let us ask for the error kind
// TODO: remove this when https://github.com/sfackler/rust-postgres/pull/1185
//       gets merged
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    RowCount,
    UndefinedTable { table: String },
    UndefinedColumn { table: Option<String>, column: String },
    Unknown,
    PostgresError(String),
}

pub fn error_kind(err: &tokio_postgres::Error) -> ErrorKind {
    let string = err.as_db_error().map(|e| e.message()).unwrap_or_default().trim().to_owned();
    let _db_error = err.as_db_error();
    let sql_code = err.code().cloned();

    // Check the error's Display string for RowCount errors (client-side, not db error)
    let err_string = err.to_string();
    if err_string.contains("query returned an unexpected number of rows") || string == "query returned an unexpected number of rows" {
        return ErrorKind::RowCount;
    }

    // Useful for adding new errors
    // error!("postgres error: {:?}", err);
    // error!("db_err: {:?}", err.as_db_error());
    // error!("sql_code: {:?}", err.code());
    // error!("err: {:?}", err);
    // error!("err: {:?}", err.to_string());
    debug!("postgres error: {:?}", err);

    let quote_indices = |s: &str| {
        let mut quotes = Vec::new();
        for (index, char) in s.char_indices() {
            if char == '"' {
                quotes.push(index)
            }
        }
        quotes
    };

    match sql_code {
        Some(SqlState::UNDEFINED_TABLE) => {
            // relation "album" does not exist
            let quotes = quote_indices(&string);
            if quotes.len() >= 2 {
                let table = &string[quotes[0] + 1..quotes[1]];
                ErrorKind::UndefinedTable { table: table.to_owned() }
            } else {
                ErrorKind::PostgresError(string.clone())
            }
        }
        Some(SqlState::UNDEFINED_COLUMN) => {
            // Handle both formats:
            // "column "name" of relation "album" does not exist"
            // "column "status" does not exist"
            let quotes = quote_indices(&string);
            if quotes.len() >= 2 {
                let column = string[quotes[0] + 1..quotes[1]].to_owned();

                let table = if quotes.len() >= 4 {
                    // Full format with table name
                    Some(string[quotes[2] + 1..quotes[3]].to_owned())
                } else {
                    // Short format without table name
                    None
                };

                ErrorKind::UndefinedColumn { table, column }
            } else {
                ErrorKind::PostgresError(string.clone())
            }
        }
        _ => ErrorKind::Unknown,
    }
}

#[allow(unused)]
pub struct MissingMaterialized {
    pub name: String,
}

use bytes::BytesMut;
use tokio_postgres::types::{to_sql_checked, IsNull, Type};

use crate::sql_builder::SqlBuilder;

/// Post-filter EntityStates using a predicate that couldn't be pushed to SQL.
///
/// This is the escape hatch for predicates that PostgreSQL can't handle natively,
/// such as complex JSON traversals or future features like Ref traversal.
fn post_filter_states(
    states: &[Attested<EntityState>],
    predicate: &ankql::ast::Predicate,
    collection_id: &CollectionId,
) -> Vec<Attested<EntityState>> {
    use ankurah_core::entity::TemporaryEntity;
    use ankurah_core::selection::filter::evaluate_predicate;

    states
        .iter()
        .filter(|attested| {
            // Create a TemporaryEntity for filtering (implements Filterable)
            match TemporaryEntity::new(attested.payload.entity_id, collection_id.clone(), &attested.payload.state) {
                Ok(temp_entity) => {
                    // Evaluate the predicate
                    match evaluate_predicate(&temp_entity, predicate) {
                        Ok(true) => true,
                        Ok(false) => false,
                        Err(e) => {
                            warn!("Post-filter evaluation error for entity {}: {}", attested.payload.entity_id, e);
                            false // Exclude entities that fail evaluation
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to create TemporaryEntity for post-filtering {}: {}", attested.payload.entity_id, e);
                    false // Exclude entities we can't evaluate
                }
            }
        })
        .cloned()
        .collect()
}

#[derive(Debug)]
struct UntypedNull;

impl ToSql for UntypedNull {
    fn to_sql(&self, _ty: &Type, _out: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> { Ok(IsNull::Yes) }

    fn accepts(_ty: &Type) -> bool {
        true // Accept all types
    }

    to_sql_checked!();
}
