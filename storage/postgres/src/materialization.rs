use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    property::backend::backend_from_string,
    schema::CatalogResolver,
};
use ankurah_proto::{Attested, EntityId, EntityState, ModelId, PropertyId};
use ankurah_storage_common::naming;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio_postgres::{types::ToSql, GenericClient};
use tracing::{debug, error, info, warn};

use super::{
    acquire_ddl_lock, error_kind, release_ddl_lock, ErrorKind, Postgres, COLUMN_MAP_TABLE, IDENTIFIER_MAX_BYTES,
};
use crate::value::PGValue;

#[derive(Clone, Debug)]
struct PostgresColumn {
    pub name: String,
    pub is_nullable: bool,
    pub data_type: String,
}

/// Storage handle for one model's durably assigned PostgreSQL materialization.
pub(crate) struct Materialization {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    model_key: Vec<u8>,
    materialization_table_name: String,
    schema: String,
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    columns: Arc<RwLock<Vec<PostgresColumn>>>,
    /// This model's slice of the engine-owned durable property-to-column
    /// map (the `_ankurah_postgres_column_map` table), cached and keyed by
    /// durable [`PropertyId`]. The map -- not the display name -- is what
    /// addresses a property's column once assigned: renames never move columns,
    /// collisions were deduped at assignment. Always carries the
    /// `PropertyId::Id -> "id"` pin so a read of the primary key is a uniform
    /// map hit.
    property_columns: Arc<RwLock<BTreeMap<PropertyId, String>>>,
}

/// A projection whose physical table and columns have already been prepared.
///
/// The remaining write is ordinary transactional DML, so a caller can include
/// it in the same PostgreSQL transaction as the canonical entity and its
/// model associations.
pub(super) struct PreparedMaterialization {
    entity_id: EntityId,
    table: String,
    materialized: Vec<(String, PGValue)>,
}

impl PreparedMaterialization {
    pub(super) async fn write<C>(&self, client: &C) -> Result<(), MutationError>
    where C: GenericClient + Sync {
        let mut columns: Vec<String> = vec!["id".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&self.entity_id];
        for (name, parameter) in &self.materialized {
            columns.push(name.clone());
            match parameter {
                PGValue::CharacterVarying(value) => params.push(value),
                PGValue::SmallInt(value) => params.push(value),
                PGValue::Integer(value) => params.push(value),
                PGValue::BigInt(value) => params.push(value),
                PGValue::DoublePrecision(value) => params.push(value),
                PGValue::Bytea(value) => params.push(value),
                PGValue::Boolean(value) => params.push(value),
                PGValue::Jsonb(value) => params.push(value),
            }
        }
        let columns_str = columns.iter().map(|name| format!("\"{}\"", name)).collect::<Vec<String>>().join(", ");
        let values_str = params.iter().enumerate().map(|(index, _)| format!("${}", index + 1)).collect::<Vec<String>>().join(", ");
        // Replace the whole projection so properties absent from this state become NULL.
        client
            .execute(&format!(r#"DELETE FROM "{}" WHERE "id" = $1"#, self.table), &[&self.entity_id])
            .await
            .map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        let query = format!(r#"INSERT INTO "{}" ({}) VALUES ({})"#, self.table, columns_str, values_str);
        debug!("materialize_state {}: {}", self.table, query);
        client.execute(&query, params.as_slice()).await.map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        Ok(())
    }
}

/// Fixed columns of every materialization table: reserved, never assignable to
/// a projected property.
const BASE_COLUMNS: [&str; 1] = ["id"];

/// The durable, serialized address of a property: the JSON form of its
/// [`PropertyId`], stored as the `property_key` text in
/// `_ankurah_postgres_column_map`. The write side (the `PropertyId` a backend's
/// `property_values()` yields) and the read side (a `PropertyId` straight off
/// the resolved AST) both go through this, so a column assigned on write is
/// found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes to JSON") }

impl Materialization {
    /// Open this model's materialization and restore its persistent column assignments.
    pub(super) async fn open(engine: &Postgres, model_id: &ModelId) -> Result<Self, RetrievalError> {
        let materialization_table_name = engine.table_for_model(model_id).await?;
        let pool = engine.pool.clone();
        let resolver = engine.resolver.clone();
        let mut client = pool.get().await.map_err(RetrievalError::storage)?;
        let model_key = bincode::serialize(model_id).map_err(RetrievalError::storage)?;
        let schema: String =
            client.query_one("SELECT current_database()", &[]).await.map_err(RetrievalError::storage)?.get("current_database");

        let bucket = Materialization {
            pool: pool.clone(),
            schema,
            model_key,
            materialization_table_name,
            resolver: resolver.clone(),
            columns: Arc::new(RwLock::new(Vec::new())),
            property_columns: Arc::new(RwLock::new(BTreeMap::new())),
        };

        let lock_key = acquire_ddl_lock(&client, bucket.table()).await?;
        let result = async {
            bucket.create_materialization_table(&mut client).await?;
            let map_lock = acquire_ddl_lock(&client, COLUMN_MAP_TABLE).await?;
            let map_result = bucket.create_column_map_table(&client).await;
            release_ddl_lock(&client, map_lock).await?;
            map_result?;

            let id_pin_key = property_key_text(&PropertyId::Id);
            client
                .execute(
                    &format!(
                        r#"INSERT INTO "{COLUMN_MAP_TABLE}" ("model_key", "property_key", "column_name")
                           VALUES ($1, $2, 'id')
                           ON CONFLICT ("model_key", "property_key") DO NOTHING"#
                    ),
                    &[&bucket.model_key, &id_pin_key],
                )
                .await
                .map_err(|error| StateError::DDLError(Box::new(error)))?;
            bucket.rebuild_columns_cache(&mut client).await?;
            bucket.load_column_map(&client).await?;
            Ok::<_, StateError>(())
        }
        .await;
        release_ddl_lock(&client, lock_key).await?;
        result.map_err(RetrievalError::storage)?;
        Ok(bucket)
    }

    pub(super) fn table(&self) -> &str { &self.materialization_table_name }

    /// Create the engine-wide durable property-to-column map table. One table
    /// for the whole database; rows are scoped by model, so deduplication is
    /// per materialization. `property_key` is a serialized
    /// [`PropertyId`] (JSON text, see [`property_key_text`]) -- the same durable
    /// address the read side resolves against, so registered AND system
    /// properties alike are addressed by identity, never by a raw name.
    /// `_ankurah_postgres_` is the reserved prefix that shields this internal
    /// table from materialization-name collisions.
    async fn create_column_map_table(&self, client: &tokio_postgres::Client) -> Result<(), StateError> {
        let query = r#"CREATE TABLE IF NOT EXISTS "_ankurah_postgres_column_map" (
            "model_key" bytea NOT NULL,
            "property_key" text NOT NULL,
            "column_name" text NOT NULL,
            PRIMARY KEY ("model_key", "property_key"),
            UNIQUE ("model_key", "column_name")
        )"#;
        client.execute(query, &[]).await.map_err(|err| StateError::DDLError(Box::new(err)))?;
        Ok(())
    }

    /// Load this model's property-to-column assignments into the cache. The
    /// `property_key` column is a serialized [`PropertyId`] (JSON text, see
    /// [`property_key_text`]); a row we cannot parse refuses the materialization:
    /// a hidden assignment would let that property's next cache miss claim a
    /// fresh column and silently split its data, so the map loads whole or
    /// not at all. The `PropertyId::Id -> "id"` pin is seeded last so no row
    /// can remap the primary key.
    async fn load_column_map(&self, client: &tokio_postgres::Client) -> Result<(), StateError> {
        let rows = client
            .query(r#"SELECT "property_key", "column_name" FROM "_ankurah_postgres_column_map" WHERE "model_key" = $1"#, &[&self.model_key])
            .await
            .map_err(|err| StateError::DDLError(Box::new(err)))?;
        let mut map = BTreeMap::new();
        for row in rows {
            let property_key: String = row.get("property_key");
            let column_name: String = row.get("column_name");
            match serde_json::from_str::<PropertyId>(&property_key) {
                Ok(id) => {
                    map.insert(id, column_name);
                }
                Err(e) => {
                    return Err(StateError::SerializationError(
                        format!(
                            "corrupt property_key {:?} in the column map for materialization {}: {}",
                            property_key, self.materialization_table_name, e
                        )
                        .into(),
                    ));
                }
            }
        }
        // The `id` pseudo-property is always the reserved primary-key column.
        map.insert(PropertyId::Id, "id".to_string());
        *self.property_columns.write().unwrap() = map;
        Ok(())
    }

    /// Return the materialized column assigned to a durable [`PropertyId`],
    /// creating and recording an assignment on a cache miss. Registered
    /// properties seed a readable column name from the catalog and append an
    /// identity suffix when needed. System properties use their fixed built-in
    /// names and reject collisions. Unique constraints and read-back make
    /// concurrent callers converge on one assignment.
    async fn column_for_property(&self, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().unwrap().get(property_id) {
            return Ok(column.clone());
        }

        let property_key = property_key_text(property_id);

        let label = match property_id {
            PropertyId::EntityId(_) => {
                let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                match resolver {
                    Some(resolver) => resolver.get_property_label(property_id).await,
                    None => None,
                }
            }
            _ => None,
        };
        // Assignment path. Retry on a column-name uniqueness race: reload the
        // map (fresh taken-set) and re-dedupe.
        for _attempt in 0..3 {
            let column = match property_id {
                // System property: its name is unique and is the column. There
                // is no entity id to suffix-dedupe with, so a collision with a
                // reserved base column or with a column assigned to a different
                // identity is a hard error on first sight (silently absorbing
                // the column is how aliasing arises, and the name may come from
                // an untrusted peer's state buffer).
                PropertyId::System(property) => {
                    let candidate = naming::sanitize(property.as_str());
                    if BASE_COLUMNS.contains(&candidate.as_str()) {
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is a reserved base column",
                                property_key,
                                candidate
                            )
                            .into(),
                        ));
                    }
                    let owner = self
                        .property_columns
                        .read()
                        .unwrap()
                        .iter()
                        .find(|(other, col)| other != &property_id && col.as_str() == candidate)
                        .map(|(other, _)| property_key_text(other));
                    if let Some(owner) = owner {
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to property {}",
                                property_key,
                                candidate,
                                owner
                            )
                            .into(),
                        ));
                    }
                    candidate
                }
                // Registered properties seed their one-time physical
                // assignment from the canonical catalog label. The durable map
                // remains authoritative on every subsequent access.
                PropertyId::EntityId(ulid) => {
                    let id = *ulid;
                    let assigned = self.property_columns.read().unwrap();
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    match label.as_deref() {
                        Some(label) => naming::dedupe_bounded(&naming::sanitize(label), &id, IDENTIFIER_MAX_BYTES, is_taken),
                        None => naming::fallback("p", &id, is_taken),
                    }
                    .map_err(|e| MutationError::UpdateFailed(Box::new(e)))?
                }
                // The `id` pseudo-property is the primary key, never a stored
                // property value, so it never reaches column assignment (it is
                // pinned to the "id" column at table creation).
                PropertyId::Id => {
                    return Err(MutationError::UpdateFailed(
                        anyhow::anyhow!("the id pseudo-property is never materialized as a stored value").into(),
                    ))
                }
            };

            let client = self.pool.get().await.map_err(|error| MutationError::General(error.into()))?;
            let inserted = client
                .execute(
                    r#"INSERT INTO "_ankurah_postgres_column_map" ("model_key", "property_key", "column_name") VALUES ($1, $2, $3)
                       ON CONFLICT ("model_key", "property_key") DO NOTHING"#,
                    &[&self.model_key, &property_key, &column],
                )
                .await;
            match inserted {
                Ok(_) => {
                    // Read back the winner: covers both "we inserted" and "a
                    // concurrent writer beat us on the same property key".
                    let row = client
                        .query_one(
                            r#"SELECT "column_name" FROM "_ankurah_postgres_column_map" WHERE "model_key" = $1 AND "property_key" = $2"#,
                            &[&self.model_key, &property_key],
                        )
                        .await
                        .map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;
                    let winner: String = row.get(0);
                    self.property_columns.write().unwrap().insert(property_id.clone(), winner.clone());
                    return Ok(winner);
                }
                Err(err) if error_kind(&err) == ErrorKind::UniqueViolation => {
                    // A different property owns this column name durably.
                    // Refresh the taken-set either way.
                    self.load_column_map(&client).await.map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
                    if matches!(property_id, PropertyId::System(_)) {
                        // A system property has exactly one candidate name, so
                        // retrying would recompute the same name; refuse now,
                        // naming the identity that owns the column.
                        let owner = self
                            .property_columns
                            .read()
                            .unwrap()
                            .iter()
                            .find(|(other, col)| other != &property_id && col.as_str() == column)
                            .map(|(other, _)| format!("property {}", property_key_text(other)))
                            .unwrap_or_else(|| "another property".to_string());
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to {}",
                                property_key,
                                column,
                                owner
                            )
                            .into(),
                        ));
                    }
                    // Registered property: re-dedupe against the refreshed set.
                    continue;
                }
                Err(err) => return Err(MutationError::UpdateFailed(Box::new(err))),
            }
        }
        Err(MutationError::UpdateFailed(
            anyhow::anyhow!("could not assign a column for property {} after repeated collisions", property_key).into(),
        ))
    }

    /// Rebuild the cache of columns in the table.
    pub async fn rebuild_columns_cache(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        debug!("Materialization({}).rebuild_columns_cache", self.materialization_table_name);
        let column_query =
            r#"SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_catalog = $1 AND table_name = $2;"#
                .to_string();
        let mut new_columns = Vec::new();
        debug!("Querying existing columns: {:?}, [{:?}, {:?}]", column_query, &self.schema, &self.materialization_table_name.as_str());
        let rows = client
            .query(&column_query, &[&self.schema, &self.materialization_table_name.as_str()])
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

    fn column(&self, column_name: &String) -> Option<PostgresColumn> {
        let columns = self.columns.read().unwrap();
        columns.iter().find(|column| column.name == *column_name).cloned()
    }

    pub fn has_column(&self, column_name: &String) -> bool { self.column(column_name).is_some() }

    /// Create this model's projection table. Canonical state is stored only in
    /// [`super::ENTITY_TABLE`]; materializations contain the entity id and projected
    /// property columns used for model-scoped querying.
    pub async fn create_materialization_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(43) PRIMARY KEY
            )"#,
            self.table()
        );

        debug!("{create_query}");
        match client.execute(&create_query, &[]).await {
            Ok(_) => Ok(()),
            Err(err) => {
                // Log full error details for debugging
                if let Some(db_err) = err.as_db_error() {
                    error!(
                        "Materialization({}).create_materialization_table error: {} (code: {:?})",
                        self.materialization_table_name,
                        db_err,
                        db_err.code()
                    );
                } else {
                    error!("Materialization({}).create_materialization_table error: {:?}", self.materialization_table_name, err);
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

        // Serialize DDL operations for this materialization.
        let lock_key = acquire_ddl_lock(client, self.materialization_table_name.as_str()).await?;

        let result = async {
            // Re-check columns after acquiring lock (another session may have added them)
            self.rebuild_columns_cache(client).await?;

            for (column, datatype) in missing {
                if Postgres::sane_name(&column) && !self.has_column(&column) {
                    let alter_query = format!(r#"ALTER TABLE "{}" ADD COLUMN "{}" {}"#, self.table(), column, datatype);
                    info!("Materialization({}).add_missing_columns: {}", self.materialization_table_name, alter_query);
                    match client.execute(&alter_query, &[]).await {
                        Ok(_) => {}
                        Err(err) => {
                            // Log full error details for debugging
                            if let Some(db_err) = err.as_db_error() {
                                warn!("Error adding column {} to table {}: {} (code: {:?})", column, self.table(), db_err, db_err.code());
                            } else {
                                warn!("Error adding column {} to table {}: {:?}", column, self.table(), err);
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

impl Materialization {
    /// Prepare this model's projection without publishing an entity record.
    ///
    /// Durable table/column assignments and empty schema may be created here;
    /// the returned DML remains suitable for the caller's canonical-state
    /// transaction.
    pub(super) async fn prepare_state(&self, state: &Attested<EntityState>) -> Result<PreparedMaterialization, MutationError> {
        let mut values = BTreeMap::new();
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (property_id, value) in backend.property_values() {
                values.entry(property_id).or_insert(value);
            }
        }

        let mut materialized = Vec::new();
        let mut missing = Vec::new();
        for (property_id, value) in values {
            let Some(value) = value else { continue };
            let column = self.column_for_property(&property_id).await?;
            let value = PGValue::from(value);
            if !self.has_column(&column) {
                missing.push((column.clone(), value.postgres_type()));
            }
            materialized.push((column, value));
        }
        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;
        self.add_missing_columns(&mut client, missing).await?;
        Ok(PreparedMaterialization { entity_id: state.payload.entity_id, table: self.table().to_owned(), materialized })
    }

    /// Resolve the query's properties through this table's persistent column assignments.
    pub(super) async fn query_columns(&self, referenced: &[PropertyId]) -> Result<BTreeMap<PropertyId, String>, RetrievalError> {
        let mut client = self.pool.get().await.map_err(RetrievalError::storage)?;

        // Resolve, through this engine's own durable map, the column for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned column
        // -- or whose column was never materialized -- is ABSENT (evaluates
        // NULL, folded below), never re-derived from a raw name. The SQL builder
        // then translates each surviving identity to its column at emit time,
        // and the in-memory post-filter keeps reading by identity, so a rename
        // is harmless end to end.
        let assigned = self.property_columns.read().unwrap().clone();

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle or durable node
        // sharing this database may have assigned the column after this
        // snapshot was loaded. Reloading the engine-owned durable map is part
        // of the shared-database contract.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&client).await.map_err(|e| RetrievalError::storage(e))?;
            self.property_columns.read().unwrap().clone()
        } else {
            assigned
        };

        // Refresh the schema cache if we reference an assigned column we have
        // not yet seen materialized (projection preparation adds columns on
        // demand).
        let cached = self.existing_columns();
        let need_refresh = referenced.iter().any(|p| assigned.get(p).map_or(false, |c| !cached.contains(c)));
        if need_refresh {
            debug!(
                "Materialization({}).query_columns: unseen assigned column referenced, refreshing schema cache",
                self.materialization_table_name
            );
            self.rebuild_columns_cache(&mut client).await.map_err(|e| RetrievalError::storage(e))?;
        }
        let existing = self.existing_columns();

        Ok(assigned.into_iter().filter(|(property, column)| referenced.contains(property) && existing.contains(column)).collect())
    }
}
