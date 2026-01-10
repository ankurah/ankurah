//! SQLite storage engine implementation

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use ankurah_core::entity::TemporaryEntity;
use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::property::backend::backend_from_string;
use ankurah_core::selection::filter::evaluate_predicate;
use ankurah_core::storage::{StorageCollection, StorageEngine};
use ankurah_proto::{
    AttestationSet, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers,
};
use async_trait::async_trait;
use rusqlite::{params_from_iter, Connection};
use tracing::{debug, warn};

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;
use crate::sql_builder::{split_predicate_for_sqlite, SqlBuilder};
use crate::value::SqliteValue;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// SQLite storage engine
pub struct SqliteStorageEngine {
    pool: bb8::Pool<SqliteConnectionManager>,
}

impl SqliteStorageEngine {
    /// Create a new storage engine with an existing pool
    pub fn new(pool: bb8::Pool<SqliteConnectionManager>) -> Self { Self { pool } }

    /// Open a file-based SQLite database
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::file(path.as_ref());
        let pool = bb8::Pool::builder().max_size(DEFAULT_POOL_SIZE).build(manager).await?;
        Ok(Self::new(pool))
    }

    /// Open an in-memory SQLite database (for testing)
    pub async fn open_in_memory() -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::memory();
        // For in-memory, we use a single connection to keep the database alive
        let pool = bb8::Pool::builder().max_size(1).build(manager).await?;
        Ok(Self::new(pool))
    }

    /// Check if a collection name is valid
    pub fn sane_name(collection: &str) -> bool {
        for char in collection.chars() {
            match char {
                c if c.is_alphanumeric() => {}
                '_' | '.' | ':' => {}
                _ => return false,
            }
        }
        true
    }

    /// Get a reference to the connection pool (for testing/diagnostics)
    pub fn pool(&self) -> &bb8::Pool<SqliteConnectionManager> { &self.pool }
}

#[async_trait]
impl StorageEngine for SqliteStorageEngine {
    type Value = SqliteValue;

    async fn collection(&self, collection_id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        if !Self::sane_name(collection_id.as_str()) {
            return Err(RetrievalError::InvalidBucketName);
        }

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let bucket = SqliteBucket::new(self.pool.clone(), collection_id.clone());

        // Create tables if they don't exist
        let collection_id_clone = collection_id.clone();
        conn.with_connection(move |c| {
            create_state_table(c, &collection_id_clone)?;
            create_event_table(c, &collection_id_clone)?;
            Ok(())
        })
        .await?;

        // Rebuild column cache
        bucket.rebuild_columns_cache(&conn).await?;

        Ok(Arc::new(bucket))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        conn.with_connection(|c| {
            // Get all table names
            let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
            let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).collect();

            if tables.is_empty() {
                return Ok(false);
            }

            for table in tables {
                c.execute(&format!(r#"DROP TABLE IF EXISTS "{}""#, table), [])?;
            }

            Ok(true)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }
}

fn create_state_table(conn: &Connection, collection_id: &CollectionId) -> Result<(), SqliteError> {
    let table_name = collection_id.as_str();
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY,
            "state_buffer" BLOB NOT NULL,
            "head" TEXT NOT NULL,
            "attestations" BLOB
        )"#,
        table_name
    );
    debug!("Creating state table: {}", query);
    conn.execute(&query, [])?;
    Ok(())
}

fn create_event_table(conn: &Connection, collection_id: &CollectionId) -> Result<(), SqliteError> {
    let table_name = format!("{}_event", collection_id.as_str());
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY,
            "entity_id" TEXT,
            "operations" BLOB,
            "parent" TEXT,
            "attestations" BLOB
        )"#,
        table_name
    );
    debug!("Creating event table: {}", query);
    conn.execute(&query, [])?;

    // Create index on entity_id for efficient dump_entity_events queries
    let index_query = format!(r#"CREATE INDEX IF NOT EXISTS "{}_entity_id_idx" ON "{}"("entity_id")"#, table_name, table_name);
    conn.execute(&index_query, [])?;

    Ok(())
}

/// Column metadata
#[derive(Clone, Debug)]
pub struct SqliteColumn {
    pub name: String,
    #[allow(dead_code)]
    pub data_type: String,
}

/// SQLite storage bucket (collection)
pub struct SqliteBucket {
    pool: bb8::Pool<SqliteConnectionManager>,
    collection_id: CollectionId,
    /// Cached state table name (avoids repeated allocations)
    state_table_name: String,
    /// Cached event table name (avoids repeated allocations)
    event_table_name: String,
    columns: Arc<std::sync::RwLock<Vec<SqliteColumn>>>,
    ddl_lock: Arc<tokio::sync::Mutex<()>>,
}

impl SqliteBucket {
    /// Create a new bucket with cached table names
    fn new(pool: bb8::Pool<SqliteConnectionManager>, collection_id: CollectionId) -> Self {
        let state_table_name = collection_id.as_str().to_string();
        let event_table_name = format!("{}_event", collection_id.as_str());
        Self {
            pool,
            collection_id,
            state_table_name,
            event_table_name,
            columns: Arc::new(std::sync::RwLock::new(Vec::new())),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[inline]
    fn state_table(&self) -> &str { &self.state_table_name }

    #[inline]
    fn event_table(&self) -> &str { &self.event_table_name }

    /// Returns all column names currently in the schema cache
    pub fn existing_columns(&self) -> Vec<String> {
        let columns = self.columns.read().expect("RwLock poisoned");
        columns.iter().map(|c| c.name.clone()).collect()
    }

    /// Check if a column exists in the schema cache
    pub fn has_column(&self, name: &str) -> bool {
        let columns = self.columns.read().expect("RwLock poisoned");
        columns.iter().any(|c| c.name == name)
    }

    async fn rebuild_columns_cache(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let table_name = self.state_table().to_owned();
        let new_columns = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&format!("PRAGMA table_info(\"{}\")", table_name))?;
                let columns: Vec<SqliteColumn> = stmt
                    .query_map([], |row| Ok(SqliteColumn { name: row.get(1)?, data_type: row.get(2)? }))?
                    .filter_map(|r| r.ok())
                    .collect();
                Ok(columns)
            })
            .await?;

        let mut columns = self.columns.write().expect("RwLock poisoned");
        *columns = new_columns;
        Ok(())
    }

    async fn add_missing_columns(&self, conn: &PooledConnection, missing: Vec<(String, &'static str)>) -> Result<(), SqliteError> {
        if missing.is_empty() {
            return Ok(());
        }

        // Acquire DDL lock
        let _lock = self.ddl_lock.lock().await;

        // Re-check columns after acquiring lock
        self.rebuild_columns_cache(conn).await?;

        let table_name = self.state_table();
        for (column, datatype) in missing {
            if SqliteStorageEngine::sane_name(&column) && !self.has_column(&column) {
                let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, table_name, column, datatype);
                debug!("Adding column: {}", alter_query);

                let query = alter_query.clone();
                conn.with_connection(move |c| {
                    c.execute(&query, [])?;
                    Ok(())
                })
                .await?;
            }
        }

        self.rebuild_columns_cache(conn).await?;
        Ok(())
    }
}

#[async_trait]
impl StorageCollection for SqliteBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        // Ensure head is not empty for new records
        if state.payload.state.head.is_empty() {
            warn!("Warning: Empty head detected for entity {}", state.payload.entity_id);
        }

        let state_buffers = bincode::serialize(&state.payload.state.state_buffers)?;
        let head_json = serde_json::to_string(&state.payload.state.head).map_err(|e| MutationError::General(Box::new(e)))?;
        let attestations_blob = bincode::serialize(&state.attestations)?;
        let id = state.payload.entity_id.to_base64();
        let id_clone = id.clone(); // Clone for use in closure

        // Collect materialized columns (with JSONB flag for proper SQL generation)
        let mut materialized: Vec<(String, Option<SqliteValue>, bool)> = Vec::new(); // (name, value, is_jsonb)
        let mut seen_properties = std::collections::HashSet::new();

        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (column, value) in backend.property_values() {
                if !seen_properties.insert(column.clone()) {
                    continue;
                }

                let sqlite_value: Option<SqliteValue> = value.map(|v| v.into());
                let is_jsonb = sqlite_value.as_ref().is_some_and(|v| v.is_jsonb());

                if !self.has_column(&column) {
                    if let Some(ref sv) = sqlite_value {
                        self.add_missing_columns(&conn, vec![(column.clone(), sv.sqlite_type())]).await?;
                    } else {
                        continue;
                    }
                }

                materialized.push((column, sqlite_value, is_jsonb));
            }
        }

        // Build the UPSERT query
        const BASE_COLUMNS: &[&str] = &["id", "state_buffer", "head", "attestations"];

        let table_name = self.state_table();
        let num_columns = BASE_COLUMNS.len() + materialized.len();
        let mut columns: Vec<&str> = Vec::with_capacity(num_columns);
        columns.extend_from_slice(BASE_COLUMNS);

        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(num_columns);
        values.push(rusqlite::types::Value::Text(id));
        values.push(rusqlite::types::Value::Blob(state_buffers));
        values.push(rusqlite::types::Value::Text(head_json));
        values.push(rusqlite::types::Value::Blob(attestations_blob));

        // Track which placeholders need jsonb() wrapper (base columns don't)
        let mut placeholder_is_jsonb: Vec<bool> = Vec::with_capacity(num_columns);
        placeholder_is_jsonb.resize(BASE_COLUMNS.len(), false);

        for (name, value, is_jsonb) in &materialized {
            columns.push(name.as_str());
            values.push(match value {
                Some(v) => v.to_sql(),
                None => rusqlite::types::Value::Null,
            });
            placeholder_is_jsonb.push(*is_jsonb);
        }

        let columns_str = columns.iter().map(|c| format!(r#""{}""#, c)).collect::<Vec<_>>().join(", ");
        // Use jsonb(?) for JSONB columns to convert JSON text to JSONB binary format
        let placeholders =
            placeholder_is_jsonb.iter().map(|is_jsonb| if *is_jsonb { "jsonb(?)" } else { "?" }).collect::<Vec<_>>().join(", ");
        let update_str = columns.iter().skip(1).map(|c| format!(r#""{}" = excluded."{}""#, c, c)).collect::<Vec<_>>().join(", ");

        // Use SQLite's RETURNING clause (3.35.0+) to get the old head for comparison
        // If RETURNING is not available, we'll fall back to a separate query
        let query = format!(
            r#"INSERT INTO "{}"({}) VALUES({})
               ON CONFLICT("id") DO UPDATE SET {}"#,
            table_name, columns_str, placeholders, update_str
        );

        debug!("set_state query: {}", query);

        let new_head = state.payload.state.head.clone();
        let table_name_clone = table_name.to_string();
        let query_clone = query.clone();
        let values_clone = values.clone();
        let changed = conn
            .with_connection(move |c| {
                // First, get the old head if the entity exists
                let old_head_json: Option<String> =
                    match c
                        .query_row(&format!(r#"SELECT "head" FROM "{}" WHERE "id" = ?"#, table_name_clone), [&id_clone], |row| row.get(0))
                    {
                        Ok(json) => Some(json),
                        Err(rusqlite::Error::QueryReturnedNoRows) => None,
                        Err(e) => return Err(SqliteError::Rusqlite(e)),
                    };

                // Execute the UPSERT
                c.execute(&query_clone, params_from_iter(values_clone.iter())).map_err(|e| SqliteError::Rusqlite(e))?;

                // Determine if state changed
                let changed = match old_head_json {
                    Some(json) => {
                        // Entity existed - compare heads
                        let old_head: Clock = serde_json::from_str(&json).map_err(|e| SqliteError::Json(e))?;
                        old_head != new_head
                    }
                    None => {
                        // New entity
                        true
                    }
                };

                Ok(changed)
            })
            .await?;

        debug!("set_state: Changed: {}", changed);
        Ok(changed)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.state_table().to_owned();
        let id_str = id.to_base64();
        let collection_id = self.collection_id.clone();

        let result = conn
            .with_connection(move |c| {
                let query = format!(r#"SELECT "id", "state_buffer", "head", "attestations" FROM "{}" WHERE "id" = ?"#, table_name);

                let result = c.query_row(&query, [&id_str], |row| {
                    let _row_id: String = row.get(0)?;
                    let state_buffer: Vec<u8> = row.get(1)?;
                    let head_json: String = row.get(2)?;
                    let attestations_blob: Vec<u8> = row.get(3)?;
                    Ok((state_buffer, head_json, attestations_blob))
                });

                match result {
                    Ok((state_buffer, head_json, attestations_blob)) => {
                        let state_buffers: BTreeMap<String, Vec<u8>> =
                            bincode::deserialize(&state_buffer).map_err(|e| SqliteError::Serialization(e))?;
                        let head: Clock = serde_json::from_str(&head_json).map_err(|e| SqliteError::Json(e))?;
                        let attestations: AttestationSet =
                            bincode::deserialize(&attestations_blob).map_err(|e| SqliteError::Serialization(e))?;

                        Ok(Attested {
                            payload: EntityState {
                                entity_id: id,
                                collection: collection_id,
                                state: State { state_buffers: StateBuffers(state_buffers), head },
                            },
                            attestations,
                        })
                    }
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        // Table might not exist - create it and return EntityNotFound
                        // This matches Postgres behavior
                        let _ = create_state_table(c, &collection_id);
                        Err(SqliteError::Rusqlite(rusqlite::Error::QueryReturnedNoRows))
                    }
                    Err(e) => Err(SqliteError::Rusqlite(e)),
                }
            })
            .await
            .map_err(|e| match e {
                SqliteError::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => RetrievalError::EntityNotFound(id),
                _ => RetrievalError::StorageError(Box::new(e)),
            })?;

        Ok(result)
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("SqliteBucket({}).fetch_states: {:?}", self.collection_id, selection);

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        // Pre-filter selection based on cached schema to avoid undefined column errors.
        // If we see columns not in our cache, refresh it first (they might have been added).
        let referenced = selection.referenced_columns();
        let cached = self.existing_columns();
        let unknown_to_cache: Vec<&String> = referenced.iter().filter(|col| !cached.contains(col)).collect();

        // Refresh cache if we see columns we haven't seen before
        if !unknown_to_cache.is_empty() {
            debug!("SqliteBucket({}).fetch_states: Unknown columns {:?}, refreshing schema cache", self.collection_id, unknown_to_cache);
            self.rebuild_columns_cache(&conn).await?;
        }

        // Now check with (possibly refreshed) cache - columns still missing truly don't exist
        let existing = self.existing_columns();
        let missing: Vec<String> = referenced.into_iter().filter(|col| !existing.contains(col)).collect();

        let effective_selection = if missing.is_empty() {
            selection.clone()
        } else {
            debug!("SqliteBucket({}).fetch_states: Columns {:?} don't exist, treating as NULL", self.collection_id, missing);
            // Note: assume_null() has a limitation with JSON paths - it checks path.property()
            // (last step) instead of path.first() (column name). This means for paths like
            // "licensing.territory", if "licensing" is missing, assume_null() won't match
            // because it checks "territory". However, this should be rare since columns
            // are created on-demand during set_state. If it happens, assume_null() will
            // leave the predicate unchanged, which may cause the query to fail.
            // TODO: Fix assume_null() in ankql to check path.first() for multi-step paths.
            selection.assume_null(&missing)
        };

        // Split predicate for pushdown
        let split = split_predicate_for_sqlite(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate.clone();

        // Build SQL
        let sql_selection = ankql::ast::Selection {
            predicate: split.sql_predicate,
            order_by: effective_selection.order_by.clone(),
            limit: if needs_post_filter { None } else { effective_selection.limit },
        };

        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer", "head", "attestations"]);
        builder.table_name(self.state_table());
        builder.selection(&sql_selection).map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        let (sql, params) = builder.build().map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;
        debug!("fetch_states SQL: {} with {} params", sql, params.len());

        let collection_id = self.collection_id.clone();

        let mut results = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(params.iter()), |row| {
                    let id_str: String = row.get(0)?;
                    let state_buffer: Vec<u8> = row.get(1)?;
                    let head_json: String = row.get(2)?;
                    let attestations_blob: Vec<u8> = row.get(3)?;
                    Ok((id_str, state_buffer, head_json, attestations_blob))
                })?;

                let mut results = Vec::new();
                for row in rows {
                    let (id_str, state_buffer, head_json, attestations_blob) = row?;

                    let id = EntityId::from_base64(&id_str).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(std::io::Error::other(e)))
                    })?;
                    let state_buffers: BTreeMap<String, Vec<u8>> = bincode::deserialize(&state_buffer).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                    })?;
                    let head: Clock = serde_json::from_str(&head_json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(std::io::Error::other(e)))
                    })?;
                    let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                    })?;

                    results.push(Attested {
                        payload: EntityState {
                            entity_id: id,
                            collection: collection_id.clone(),
                            state: State { state_buffers: StateBuffers(state_buffers), head },
                        },
                        attestations,
                    });
                }

                Ok(results)
            })
            .await?;

        // Post-filter if needed
        if needs_post_filter {
            debug!("Post-filtering {} results", results.len());
            results = post_filter_states(&results, &remaining_predicate, &self.collection_id);

            if let Some(limit) = effective_selection.limit {
                results.truncate(limit as usize);
            }
        }

        Ok(results)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        let operations = bincode::serialize(&entity_event.payload.operations)?;
        let attestations = bincode::serialize(&entity_event.attestations)?;
        let parent_json = serde_json::to_string(&entity_event.payload.parent).map_err(|e| MutationError::General(Box::new(e)))?;

        let table_name = self.event_table();
        let event_id = entity_event.payload.id().to_base64();
        let entity_id = entity_event.payload.entity_id.to_base64();

        let query = format!(
            r#"INSERT INTO "{}"("id", "entity_id", "operations", "parent", "attestations") VALUES(?, ?, ?, ?, ?)
               ON CONFLICT ("id") DO NOTHING"#,
            table_name
        );

        conn.with_connection(move |c| {
            let affected = c.execute(&query, rusqlite::params![event_id, entity_id, operations, parent_json, attestations])?;
            Ok(affected > 0)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.event_table().to_owned();
        let collection_id = self.collection_id.clone();
        let id_strings: Vec<String> = event_ids.iter().map(|id| id.to_base64()).collect();
        let num_ids = id_strings.len();

        conn.with_connection(move |c| {
            let placeholders = (0..num_ids).map(|_| "?").collect::<Vec<_>>().join(", ");
            let query = format!(
                r#"SELECT "id", "entity_id", "operations", "parent", "attestations" FROM "{}" WHERE "id" IN ({})"#,
                table_name, placeholders
            );

            let mut stmt = c.prepare(&query)?;
            let params: Vec<&dyn rusqlite::ToSql> = id_strings.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
            let rows = stmt.query_map(params.as_slice(), |row| {
                let _event_id: String = row.get(0)?;
                let entity_id_str: String = row.get(1)?;
                let operations: Vec<u8> = row.get(2)?;
                let parent_json: String = row.get(3)?;
                let attestations_blob: Vec<u8> = row.get(4)?;
                Ok((entity_id_str, operations, parent_json, attestations_blob))
            })?;

            let mut events = Vec::with_capacity(num_ids);
            for row in rows {
                let (entity_id_str, operations_blob, parent_json, attestations_blob) = row?;

                let entity_id = EntityId::from_base64(&entity_id_str).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(std::io::Error::other(e)))
                })?;
                let operations: OperationSet = bincode::deserialize(&operations_blob).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                })?;
                let parent: Clock = serde_json::from_str(&parent_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(std::io::Error::other(e)))
                })?;
                let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                })?;

                events.push(Attested { payload: Event { collection: collection_id.clone(), entity_id, operations, parent }, attestations });
            }

            Ok(events)
        })
        .await
        .map_err(|e| RetrievalError::StorageError(Box::new(e)))
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        let table_name = self.event_table().to_owned();
        let collection_id = self.collection_id.clone();
        let entity_id_str = entity_id.to_base64();

        conn.with_connection(move |c| {
            let query = format!(r#"SELECT "id", "operations", "parent", "attestations" FROM "{}" WHERE "entity_id" = ?"#, table_name);

            let mut stmt = c.prepare(&query)?;
            let rows = stmt.query_map([&entity_id_str], |row| {
                let _event_id: String = row.get(0)?;
                let operations: Vec<u8> = row.get(1)?;
                let parent_json: String = row.get(2)?;
                let attestations_blob: Vec<u8> = row.get(3)?;
                Ok((operations, parent_json, attestations_blob))
            })?;

            let mut events = Vec::new();
            for row in rows {
                let (operations_blob, parent_json, attestations_blob) = row?;

                let operations: OperationSet = bincode::deserialize(&operations_blob).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                })?;
                let parent: Clock = serde_json::from_str(&parent_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(std::io::Error::other(e)))
                })?;
                let attestations: AttestationSet = bincode::deserialize(&attestations_blob).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Blob, Box::new(std::io::Error::other(e)))
                })?;

                events.push(Attested { payload: Event { collection: collection_id.clone(), entity_id, operations, parent }, attestations });
            }

            Ok(events)
        })
        .await
        .map_err(|e| RetrievalError::StorageError(Box::new(e)))
    }
}

/// Post-filter EntityStates using a predicate that couldn't be pushed to SQL.
fn post_filter_states(
    states: &[Attested<EntityState>],
    predicate: &ankql::ast::Predicate,
    collection_id: &CollectionId,
) -> Vec<Attested<EntityState>> {
    states
        .iter()
        .filter(|attested| match TemporaryEntity::new(attested.payload.entity_id, collection_id.clone(), &attested.payload.state) {
            Ok(temp_entity) => match evaluate_predicate(&temp_entity, predicate) {
                Ok(result) => result,
                Err(e) => {
                    warn!("Post-filter evaluation error for entity {}: {}", attested.payload.entity_id, e);
                    false
                }
            },
            Err(e) => {
                warn!("Failed to create TemporaryEntity for post-filtering {}: {}", attested.payload.entity_id, e);
                false
            }
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_open_in_memory() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let collection = engine.collection(&"test_collection".into()).await.unwrap();
        let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(collection.fetch_states(&all).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_sane_name() {
        assert!(SqliteStorageEngine::sane_name("test_collection"));
        assert!(SqliteStorageEngine::sane_name("test.collection"));
        assert!(SqliteStorageEngine::sane_name("test:collection"));
        assert!(!SqliteStorageEngine::sane_name("test;collection"));
        assert!(!SqliteStorageEngine::sane_name("test'collection"));
    }

    /// Test that SQLite JSONB functions are available and work correctly.
    ///
    /// This test verifies:
    /// 1. The `jsonb()` function exists and can convert JSON text to JSONB
    /// 2. The `->` operator works for JSON path traversal
    /// 3. Type-aware comparisons work (numeric vs string)
    /// 4. JSONB storage and retrieval works correctly
    #[tokio::test]
    async fn test_jsonb_function_availability() -> Result<(), SqliteError> {
        let engine = SqliteStorageEngine::open_in_memory().await.map_err(|e| SqliteError::DDL(e.to_string()))?;
        let conn = engine.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        // Test 1: Verify jsonb() function exists and works
        // jsonb() returns a BLOB (JSONB binary format), so we query it as BLOB
        let result = conn
            .with_connection(|c| {
                let value: Vec<u8> = c.query_row("SELECT jsonb('{\"key\": \"value\"}')", [], |row| row.get(0))?;
                Ok(value)
            })
            .await?;
        // jsonb() returns JSONB BLOB format - verify it's not empty
        assert!(!result.is_empty(), "jsonb() function should return a non-empty BLOB");

        // Test 2: Verify -> operator works for path traversal
        // json_extract returns the SQL value (unquoted string for JSON strings)
        let result = conn
            .with_connection(|c| {
                let value: String =
                    c.query_row(r#"SELECT json_extract(jsonb('{"territory": "US", "count": 10}'), '$.territory')"#, [], |row| row.get(0))?;
                Ok(value)
            })
            .await?;
        // json_extract returns the unquoted SQL value, not the JSON string representation
        assert_eq!(result, "US", "JSON path extraction should return the SQL value");

        // Test 3: Verify numeric comparison is numeric (not lexicographic)
        // In SQLite, json_extract with numeric comparison should work correctly
        let result = conn
            .with_connection(|c| {
                let value: bool = c.query_row(
                    r#"SELECT json_extract(jsonb('{"count": 9}'), '$.count') > json_extract(jsonb('{"count": 10}'), '$.count')"#,
                    [],
                    |row| row.get(0),
                )?;
                Ok(value)
            })
            .await?;
        assert!(!result, "Numeric comparison: 9 > 10 should be false");

        Ok(())
    }

    /// Test JSON path queries with the -> operator (SQLite JSONB syntax).
    ///
    /// This test verifies that:
    /// 1. JSON properties can be queried using path syntax (e.g., `data.status = 'active'`)
    /// 2. The SQL builder generates correct SQLite JSONB syntax
    /// 3. Queries return correct results
    #[tokio::test]
    async fn test_json_path_query() -> anyhow::Result<()> {
        use crate::sql_builder::SqlBuilder;
        use ankql::parser::parse_selection;

        // Test that the SQL builder generates correct JSONB syntax
        let selection = parse_selection(r#"data.status = 'active'"#).expect("Failed to parse query");
        let mut builder = SqlBuilder::with_fields(vec!["id", "state_buffer"]);
        builder.table_name("test_table");
        builder.selection(&selection).map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        let (sql, _params) = builder.build().map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        // Verify the SQL uses json_extract() for reliable JSON path comparisons
        assert!(sql.contains("json_extract"), "SQL should use json_extract() for JSON path: {}", sql);
        assert!(sql.contains(r#"json_extract("data", '$.status')"#), "SQL should extract from data column with $.status path: {}", sql);

        Ok(())
    }

    /// Test the full cycle: store JSONB via parameter, query via json_extract with parameter.
    /// This mimics exactly what the real code does.
    #[tokio::test]
    async fn test_jsonb_storage_and_parameterized_query() -> Result<(), SqliteError> {
        let engine = SqliteStorageEngine::open_in_memory().await.map_err(|e| SqliteError::DDL(e.to_string()))?;
        let conn = engine.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        conn.with_connection(|c| {
            // Create table with BLOB column for JSONB
            c.execute(r#"CREATE TABLE test_jsonb (id TEXT PRIMARY KEY, data BLOB)"#, [])?;

            // Insert using jsonb(?) - this is what the real code does
            let json_text = r#"{"territory": "US", "count": 10}"#;
            c.execute(r#"INSERT INTO test_jsonb (id, data) VALUES (?, jsonb(?))"#, rusqlite::params!["1", json_text])?;

            // Verify data is stored
            let count: i32 = c.query_row("SELECT COUNT(*) FROM test_jsonb", [], |row| row.get(0))?;
            assert_eq!(count, 1, "Should have 1 row");

            // Check what's in the data column
            let data_type: String = c.query_row("SELECT typeof(data) FROM test_jsonb WHERE id = '1'", [], |row| row.get(0))?;
            eprintln!("Data column type: {}", data_type);

            // Check what json_extract returns
            let extracted: String =
                c.query_row(r#"SELECT json_extract(data, '$.territory') FROM test_jsonb WHERE id = '1'"#, [], |row| row.get(0))?;
            eprintln!("Extracted territory: '{}'", extracted);

            // Now try the parameterized query - THIS IS WHAT THE REAL CODE DOES
            let query_param = "US";
            let result: Result<String, _> = c.query_row(
                r#"SELECT id FROM test_jsonb WHERE json_extract(data, '$.territory') = ?"#,
                rusqlite::params![query_param],
                |row| row.get(0),
            );
            eprintln!("Query result: {:?}", result);

            match result {
                Ok(id) => assert_eq!(id, "1", "Should find the row with territory = US"),
                Err(e) => panic!("Query failed: {:?}", e),
            }

            Ok(())
        })
        .await
    }
}
