//! SQLite storage engine implementation

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use ankurah_core::entity::TemporaryEntity;
use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::property::backend::backend_from_string;
use ankurah_core::schema::CatalogResolver;
use ankurah_core::selection::filter::evaluate_predicate;
use ankurah_core::storage::{
    naming, CommitBatchOutcome, CommittedEntityWrite, PreparedEntityWrite, StorageCommitResult, StorageEngine, StorageWriteBatch,
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, EventId, OperationSet, State, StateBuffers, PROTOCOL_VERSION};
use ankurah_proto::{ModelId, PropertyId, SystemModel, ValueType};
use async_trait::async_trait;
use rusqlite::{params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use tracing::{debug, warn};

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;
use crate::sql_builder::{split_predicate_for_sqlite, SqlBuilder};
use crate::value::SqliteValue;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Engine-level key/value metadata for the store itself (currently just the
/// 'protocol_version' record). Not application data: it survives
/// [`StorageEngine::delete_all`], because wiping the record would make the store
/// read as unversioned and refuse its own reopen.
const META_TABLE: &str = "_ankurah_meta";
const MODEL_MAP_TABLE: &str = "_ankurah_sqlite_model_map";
const COLUMN_MAP_TABLE: &str = "_ankurah_sqlite_column_map";
const ENTITY_TABLE: &str = "_ankurah_entity";
const EVENT_TABLE: &str = "_ankurah_event";
const ENTITY_MODEL_TABLE: &str = "_ankurah_entity_model";
const FIXED_STORAGE_TABLES: &[&str] = &[META_TABLE, MODEL_MAP_TABLE, COLUMN_MAP_TABLE, ENTITY_TABLE, EVENT_TABLE, ENTITY_MODEL_TABLE];

fn quote_identifier(identifier: &str) -> String { format!(r#""{}""#, identifier.replace('"', "\"\"")) }

fn system_label(model: SystemModel) -> &'static str {
    match model {
        SystemModel::System => "_ankurah_system",
        SystemModel::Model => "_ankurah_model",
        SystemModel::Property => "_ankurah_property",
        SystemModel::ModelProperty => "_ankurah_model_property",
    }
}

fn reserved_system_table_names() -> Vec<String> {
    [SystemModel::System, SystemModel::Model, SystemModel::Property, SystemModel::ModelProperty]
        .into_iter()
        .map(|model| naming::sanitize(system_label(model)))
        .collect()
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, SqliteError> {
    Ok(conn.query_row("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [table], |_| Ok(())).optional()?.is_some())
}

/// SQLite storage engine
pub struct SqliteStorageEngine {
    pool: bb8::Pool<SqliteConnectionManager>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Shared with every bucket:
    /// the name SOURCE for the engine-owned durable id-to-column map.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl SqliteStorageEngine {
    async fn registered_table_name(&self, model: &ModelId) -> Result<Option<String>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        let model_key = bincode::serialize(model).map_err(RetrievalError::storage)?;
        conn.with_connection(move |c| {
            if !table_exists(c, MODEL_MAP_TABLE)? {
                return Ok(None);
            }
            c.query_row(
                &format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}" WHERE "model_key" = ?"#),
                rusqlite::params![model_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(SqliteError::from)
        })
        .await
        .map_err(|e| RetrievalError::StorageError(Box::new(e)))
    }

    fn catalog_registered_label(&self, model: &ModelId) -> Result<String, RetrievalError> {
        if let ModelId::System(system) = model {
            return Ok(system_label(*system).to_owned());
        }
        let resolver = self
            .resolver
            .read()
            .expect("RwLock poisoned")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {model}")))?;
        resolver.model_name(model).map_err(|error| RetrievalError::Other(error.to_string()))
    }

    /// Return the immutable physical table registration for `model`, assigning
    /// it on first use. The SQLite-private map is authoritative, so reopening
    /// an existing model does not require a ready catalog resolver.
    async fn get_or_insert_unique_durable_table_registration(&self, model: &ModelId) -> Result<String, RetrievalError> {
        if let Some(name) = self.registered_table_name(model).await? {
            return Ok(name);
        }

        // Resolver access is intentionally after the durable miss.
        let registered_label = self.catalog_registered_label(model)?;
        let desired = naming::sanitize(&registered_label);
        let model_key = bincode::serialize(model).map_err(RetrievalError::storage)?;
        let model = *model;
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        conn.with_connection_mut(move |c| {
            let tx = c.transaction()?;
            tx.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{MODEL_MAP_TABLE}" (
                        "model_key" BLOB PRIMARY KEY,
                        "materialization_table_name" TEXT NOT NULL UNIQUE
                    )"#
                ),
                [],
            )?;
            if let Some(name) = tx
                .query_row(
                    &format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}" WHERE "model_key" = ?"#),
                    rusqlite::params![&model_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                tx.commit()?;
                return Ok(name);
            }

            let mut stmt = tx.prepare(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}""#))?;
            let mut taken = std::collections::HashSet::new();
            for row in stmt.query_map([], |row| row.get::<_, String>(0))? {
                taken.insert(row?);
            }
            drop(stmt);
            let mut stmt = tx.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?;
            for row in stmt.query_map([], |row| row.get::<_, String>(0))? {
                taken.insert(row?);
            }
            drop(stmt);
            taken.extend(FIXED_STORAGE_TABLES.iter().map(|name| (*name).to_owned()));
            let is_taken =
                |candidate: &str| taken.contains(candidate) || reserved_system_table_names().iter().any(|reserved| reserved == candidate);
            let materialization_table_name = match model {
                ModelId::EntityId(id) => {
                    naming::dedupe(&desired, &id, is_taken).map_err(|error| SqliteError::CorruptRecord(error.to_string()))?
                }
                ModelId::System(system) => {
                    let fixed = naming::sanitize(system_label(system));
                    if taken.contains(&fixed) {
                        return Err(SqliteError::CorruptRecord(format!(
                            "reserved system model {model} cannot claim its physical table name {fixed:?}"
                        )));
                    }
                    fixed
                }
            };
            tx.execute(
                &format!(r#"INSERT INTO "{MODEL_MAP_TABLE}" ("model_key", "materialization_table_name") VALUES (?, ?)"#),
                rusqlite::params![&model_key, &materialization_table_name],
            )?;
            tx.commit()?;
            Ok(materialization_table_name)
        })
        .await
        .map_err(RetrievalError::from)
    }

    /// Create a new storage engine with an existing pool.
    ///
    /// Records or checks the store's protocol version (see
    /// `check_protocol_version`) so every construction path verifies
    /// the store it is about to serve.
    pub async fn new(pool: bb8::Pool<SqliteConnectionManager>) -> anyhow::Result<Self> {
        let engine = Self { pool, resolver: Arc::new(std::sync::RwLock::new(None)) };
        engine.check_protocol_version().await?;
        Ok(engine)
    }

    /// Open a file-based SQLite database
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::file(path.as_ref());
        let pool = bb8::Pool::builder().max_size(DEFAULT_POOL_SIZE).build(manager).await?;
        Self::new(pool).await
    }

    /// Open an in-memory SQLite database (for testing)
    pub async fn open_in_memory() -> anyhow::Result<Self> {
        let manager = SqliteConnectionManager::memory();
        // For in-memory, we use a single connection to keep the database alive
        let pool = bb8::Pool::builder().max_size(1).build(manager).await?;
        Self::new(pool).await
    }

    /// Check whether a physical SQLite name uses only the supported characters.
    pub fn sane_name(name: &str) -> bool {
        for char in name.chars() {
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

    async fn ensure_shared_tables(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        conn.with_connection(|c| {
            c.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{ENTITY_TABLE}" (
                        "id" TEXT PRIMARY KEY,
                        "state_buffer" BLOB NOT NULL,
                        "head" TEXT NOT NULL,
                        "attestations" BLOB NOT NULL
                    )"#
                ),
                [],
            )?;
            c.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{EVENT_TABLE}" (
                        "id" TEXT PRIMARY KEY,
                        "entity_id" TEXT NOT NULL,
                        "operations" BLOB NOT NULL,
                        "parent" TEXT NOT NULL,
                        "attestations" BLOB NOT NULL
                    )"#
                ),
                [],
            )?;
            c.execute(
                &format!(
                    r#"CREATE INDEX IF NOT EXISTS "{EVENT_TABLE}_entity_id_idx"
                       ON "{EVENT_TABLE}" ("entity_id")"#
                ),
                [],
            )?;
            c.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{ENTITY_MODEL_TABLE}" (
                        "entity_id" TEXT NOT NULL,
                        "model_key" BLOB NOT NULL,
                        PRIMARY KEY ("entity_id", "model_key")
                    )"#
                ),
                [],
            )?;
            Ok(())
        })
        .await
    }

    async fn materialization(&self, model: &ModelId) -> Result<SqliteBucket, RetrievalError> {
        let table_name = self.get_or_insert_unique_durable_table_registration(model).await?;
        let model_key = bincode::serialize(model).map_err(RetrievalError::storage)?;
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let bucket = SqliteBucket::new(self.pool.clone(), *model, model_key.clone(), table_name.clone(), self.resolver.clone());
        let id_pin_key = property_key_text(&PropertyId::Id);
        conn.with_connection(move |c| {
            create_materialization_table(c, &table_name)?;
            create_column_map_table(c)?;
            c.execute(
                &format!(
                    r#"INSERT OR IGNORE INTO "{COLUMN_MAP_TABLE}"
                       ("model_key", "property_key", "column_name") VALUES (?, ?, 'id')"#
                ),
                rusqlite::params![model_key, id_pin_key],
            )?;
            Ok(())
        })
        .await?;
        bucket.rebuild_columns_cache(&conn).await?;
        bucket.load_column_map(&conn).await?;
        Ok(bucket)
    }

    async fn associated_models(&self, conn: &PooledConnection, entity_id: EntityId) -> Result<Vec<ModelId>, RetrievalError> {
        let entity_id = entity_id.to_base64();
        let model_keys = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ?"#))?;
                let keys = stmt.query_map([entity_id], |row| row.get::<_, Vec<u8>>(0))?.collect::<Result<Vec<_>, _>>()?;
                Ok(keys)
            })
            .await?;
        let mut models = model_keys
            .into_iter()
            .map(|key| bincode::deserialize(&key))
            .collect::<Result<Vec<ModelId>, _>>()
            .map_err(RetrievalError::storage)?;
        models.sort();
        Ok(models)
    }

    /// Record or check the store's protocol version
    /// ([`ankurah_proto::PROTOCOL_VERSION`]), run by every constructor:
    ///
    /// - fresh store (no record, no ankurah tables): write the record, proceed
    /// - record present and equal: proceed
    /// - record present and different: refuse, naming found and expected
    /// - ankurah tables present but no record: refuse as an unversioned store
    ///
    /// Same spirit as the peering handshake's `protocol_compatible` refusal:
    /// serving a store persisted by a different protocol version would
    /// silently misread its shapes, so refuse loudly and advise a
    /// development-database reset.
    async fn check_protocol_version(&self) -> Result<(), SqliteError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        conn.with_connection(|c| {
            let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
            let tables: Vec<String> = stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).collect();
            let has_ankurah_tables = tables.iter().any(|table| FIXED_STORAGE_TABLES.contains(&table.as_str()));

            let recorded: Option<String> = if tables.iter().any(|t| t == META_TABLE) {
                match c.query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), [], |row| row.get(0))
                {
                    Ok(value) => Some(value),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(e) => return Err(SqliteError::Rusqlite(e)),
                }
            } else {
                None
            };

            let expected = PROTOCOL_VERSION.to_string();
            match recorded {
                Some(found) if found == expected => Ok(()),
                Some(found) => Err(SqliteError::ProtocolVersionMismatch { found, expected: PROTOCOL_VERSION }),
                None if has_ankurah_tables => Err(SqliteError::UnversionedStore { expected: PROTOCOL_VERSION }),
                None => {
                    // Fresh store: claim the record.
                    c.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{META_TABLE}" ("key" TEXT PRIMARY KEY, "value" TEXT)"#), [])?;
                    c.execute(
                        &format!(r#"INSERT OR IGNORE INTO "{META_TABLE}" ("key", "value") VALUES ('protocol_version', ?)"#),
                        rusqlite::params![expected],
                    )?;
                    // Re-read: if another process recorded between our scan and
                    // our insert, the store must still match this binary.
                    let reread: String =
                        c.query_row(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), [], |row| {
                            row.get(0)
                        })?;
                    if reread == expected {
                        Ok(())
                    } else {
                        Err(SqliteError::ProtocolVersionMismatch { found: reread, expected: PROTOCOL_VERSION })
                    }
                }
            }
        })
        .await
    }
}

enum SqliteBatchAttempt {
    Committed(StorageCommitResult),
    Conflict(BTreeMap<EntityId, Option<Attested<EntityState>>>),
    MissingSchema(Vec<(EntityId, ModelId)>),
}

#[async_trait]
impl StorageEngine for SqliteStorageEngine {
    type Value = SqliteValue;

    async fn append_events(&self, events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.pool.get().await.map_err(|error| MutationError::General(Box::new(SqliteError::Pool(error.to_string()))))?;
        self.ensure_shared_tables(&conn).await?;
        let events = events.to_vec();
        conn.with_connection_mut(move |c| {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut inserted = Vec::with_capacity(events.len());
            for event in events {
                let affected = tx.execute(
                    &format!(
                        r#"INSERT INTO "{EVENT_TABLE}" ("id", "entity_id", "operations", "parent", "attestations")
                           VALUES (?, ?, ?, ?, ?) ON CONFLICT ("id") DO NOTHING"#
                    ),
                    rusqlite::params![
                        event.payload.id().to_base64(),
                        event.payload.entity_id.to_base64(),
                        bincode::serialize(&event.payload.operations)?,
                        serde_json::to_string(&event.payload.parent)?,
                        bincode::serialize(&event.attestations)?,
                    ],
                )?;
                inserted.push(affected > 0);
            }
            tx.commit()?;
            Ok(inserted)
        })
        .await
        .map_err(MutationError::from)
    }

    async fn commit_batch(&self, batch: StorageWriteBatch) -> Result<CommitBatchOutcome, MutationError> {
        if batch.entities.is_empty() {
            return Ok(CommitBatchOutcome::Committed(StorageCommitResult::default()));
        }
        let mut seen = BTreeSet::new();
        for write in &batch.entities {
            if !seen.insert(write.state.payload.entity_id) {
                return Err(MutationError::General(
                    format!("storage write batch contains duplicate entity {}", write.state.payload.entity_id).into(),
                ));
            }
        }

        let conn = self.pool.get().await.map_err(|error| MutationError::General(Box::new(SqliteError::Pool(error.to_string()))))?;
        self.ensure_shared_tables(&conn).await?;
        let mut models_by_entity = BTreeMap::<EntityId, BTreeSet<ModelId>>::new();
        for write in &batch.entities {
            let entity_id = write.state.payload.entity_id;
            let mut models: BTreeSet<ModelId> = self.associated_models(&conn, entity_id).await?.into_iter().collect();
            models.extend(write.associate_with.iter().copied());
            models_by_entity.insert(entity_id, models);
        }
        // An in-memory engine has a one-connection pool; release it before
        // materialization schema preparation obtains its own handle.
        drop(conn);

        let mut prepared = BTreeMap::<(EntityId, ModelId), PreparedSqliteMaterialization>::new();
        for write in &batch.entities {
            let entity_id = write.state.payload.entity_id;
            for model in models_by_entity.remove(&entity_id).unwrap_or_default() {
                let projection = self
                    .materialization(&model)
                    .await
                    .map_err(|error| MutationError::General(error.to_string().into()))?
                    .prepare_state(&write.state)
                    .await?;
                prepared.insert((entity_id, model), projection);
            }
        }

        loop {
            let conn = self.pool.get().await.map_err(|error| MutationError::General(Box::new(SqliteError::Pool(error.to_string()))))?;
            let attempt_batch = batch.clone();
            let attempt_prepared = prepared.clone();
            let attempt = conn
                .with_connection_mut(move |c| {
                    // BEGIN IMMEDIATE serializes writers before expectations
                    // are read, including concurrent first inserts.
                    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let mut ordered: Vec<(usize, &PreparedEntityWrite)> = attempt_batch.entities.iter().enumerate().collect();
                    ordered.sort_by_key(|(_, write)| write.state.payload.entity_id);

                    let mut observed = BTreeMap::new();
                    let mut conflict = false;
                    for (_, write) in &ordered {
                        let entity_id = write.state.payload.entity_id;
                        let entity_key = entity_id.to_base64();
                        let current = tx
                            .query_row(
                                &format!(
                                    r#"SELECT "id", "state_buffer", "head", "attestations"
                                       FROM "{ENTITY_TABLE}" WHERE "id" = ?"#
                                ),
                                [&entity_key],
                                raw_state_from_sqlite_row,
                            )
                            .optional()?
                            .map(decode_state_row)
                            .transpose()?;
                        let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
                        if current_head != write.expected_head {
                            conflict = true;
                        }
                        observed.insert(entity_id, current);
                    }
                    if conflict {
                        tx.rollback()?;
                        return Ok(SqliteBatchAttempt::Conflict(observed));
                    }

                    let mut models_by_entity = BTreeMap::<EntityId, Vec<ModelId>>::new();
                    let mut added_by_entity = BTreeMap::<EntityId, Vec<ModelId>>::new();
                    let mut missing_schema = Vec::new();
                    for (_, write) in &ordered {
                        let entity_id = write.state.payload.entity_id;
                        let entity_key = entity_id.to_base64();
                        let mut added = Vec::new();
                        for model in &write.associate_with {
                            let affected = tx.execute(
                                &format!(
                                    r#"INSERT OR IGNORE INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key")
                                       VALUES (?, ?)"#
                                ),
                                rusqlite::params![&entity_key, bincode::serialize(model)?],
                            )?;
                            if affected > 0 {
                                added.push(*model);
                            }
                        }
                        let mut stmt = tx.prepare(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ?"#))?;
                        let model_keys = stmt.query_map([&entity_key], |row| row.get::<_, Vec<u8>>(0))?.collect::<Result<Vec<_>, _>>()?;
                        drop(stmt);
                        let mut models =
                            model_keys.into_iter().map(|key| bincode::deserialize(&key)).collect::<Result<Vec<ModelId>, _>>()?;
                        models.sort();
                        for model in &models {
                            if !attempt_prepared.contains_key(&(entity_id, *model)) {
                                missing_schema.push((entity_id, *model));
                            }
                        }
                        models_by_entity.insert(entity_id, models);
                        added_by_entity.insert(entity_id, added);
                    }
                    if !missing_schema.is_empty() {
                        tx.rollback()?;
                        return Ok(SqliteBatchAttempt::MissingSchema(missing_schema));
                    }

                    let mut committed = Vec::with_capacity(attempt_batch.entities.len());
                    for (original_index, write) in ordered {
                        let state = &write.state;
                        let entity_id = state.payload.entity_id;
                        tx.execute(
                            &format!(
                                r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
                                   VALUES (?, ?, ?, ?)
                                   ON CONFLICT ("id") DO UPDATE SET
                                       "state_buffer" = excluded."state_buffer",
                                       "head" = excluded."head",
                                       "attestations" = excluded."attestations""#
                            ),
                            rusqlite::params![
                                entity_id.to_base64(),
                                bincode::serialize(&state.payload.state.state_buffers)?,
                                serde_json::to_string(&state.payload.state.head)?,
                                bincode::serialize(&state.attestations)?,
                            ],
                        )?;
                        let models = models_by_entity.remove(&entity_id).ok_or_else(|| {
                            SqliteError::CorruptRecord(format!("storage batch omitted the model set for entity {entity_id}"))
                        })?;
                        for model in &models {
                            attempt_prepared
                                .get(&(entity_id, *model))
                                .ok_or_else(|| {
                                    SqliteError::CorruptRecord(format!(
                                        "storage batch omitted the prepared materialization for entity {entity_id} under model {model}"
                                    ))
                                })?
                                .write(&tx)?;
                        }
                        committed.push((
                            original_index,
                            CommittedEntityWrite {
                                entity_id,
                                canonical_changed: write.expected_head != state.payload.state.head,
                                associations_added: added_by_entity.remove(&entity_id).unwrap_or_default(),
                                materialized_as: models,
                            },
                        ));
                    }
                    committed.sort_by_key(|(index, _)| *index);
                    let entities = committed.into_iter().map(|(_, result)| result).collect();
                    tx.commit()?;
                    Ok(SqliteBatchAttempt::Committed(StorageCommitResult { entities }))
                })
                .await?;
            drop(conn);

            match attempt {
                SqliteBatchAttempt::Committed(result) => return Ok(CommitBatchOutcome::Committed(result)),
                SqliteBatchAttempt::Conflict(observed) => return Ok(CommitBatchOutcome::Conflict { observed }),
                SqliteBatchAttempt::MissingSchema(missing) => {
                    for (entity_id, model) in missing {
                        let write = batch.entities.iter().find(|write| write.state.payload.entity_id == entity_id).ok_or_else(|| {
                            MutationError::General(format!("schema retry referenced entity {entity_id} outside the storage batch").into())
                        })?;
                        let projection = self
                            .materialization(&model)
                            .await
                            .map_err(|error| MutationError::General(error.to_string().into()))?
                            .prepare_state(&write.state)
                            .await?;
                        prepared.insert((entity_id, model), projection);
                    }
                }
            }
        }
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        load_state(&conn, id).await
    }

    async fn fetch_states(&self, model: &ModelId, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.materialization(model).await?.fetch_states(selection).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let event_ids: Vec<String> = event_ids.into_iter().map(|id| id.to_base64()).collect();
        conn.with_connection(move |c| {
            let placeholders = (0..event_ids.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
            let mut stmt = c.prepare(&format!(
                r#"SELECT "entity_id", "operations", "parent", "attestations"
                   FROM "{EVENT_TABLE}" WHERE "id" IN ({placeholders})"#
            ))?;
            let params: Vec<&dyn rusqlite::ToSql> = event_ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
            let rows = stmt.query_map(params.as_slice(), event_from_sqlite_row)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .map_err(RetrievalError::storage)?
        .into_iter()
        .map(decode_event_row)
        .collect()
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let entity_id = entity_id.to_base64();
        conn.with_connection(move |c| {
            let mut stmt = c.prepare(&format!(
                r#"SELECT "entity_id", "operations", "parent", "attestations"
                   FROM "{EVENT_TABLE}" WHERE "entity_id" = ?"#
            ))?;
            let rows = stmt.query_map([entity_id], event_from_sqlite_row)?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .map_err(RetrievalError::storage)?
        .into_iter()
        .map(decode_event_row)
        .collect()
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) {
        *self.resolver.write().expect("RwLock poisoned") = Some(resolver);
    }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;

        conn.with_connection(|c| {
            // Dynamic materializations are engine-owned only when named by the
            // durable model map. Arbitrary tables may belong to the embedding
            // application and must survive an Ankurah reset.
            let mut stmt = c.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
            let existing: BTreeSet<String> = stmt.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
            drop(stmt);

            let mut owned: BTreeSet<String> = FIXED_STORAGE_TABLES
                .iter()
                .copied()
                .filter(|name| *name != META_TABLE && existing.contains(*name))
                .map(str::to_owned)
                .collect();
            if existing.contains(MODEL_MAP_TABLE) {
                let mut stmt = c.prepare(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}""#))?;
                owned.extend(
                    stmt.query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter(|name| existing.contains(name)),
                );
            }

            if owned.is_empty() {
                return Ok(false);
            }

            for table in owned {
                c.execute(&format!("DROP TABLE IF EXISTS {}", quote_identifier(&table)), [])?;
            }

            Ok(true)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;
        let models = conn
            .with_connection(|c| {
                if !table_exists(c, MODEL_MAP_TABLE)? {
                    return Ok(Vec::new());
                }
                let mut stmt = c.prepare(&format!(r#"SELECT "model_key" FROM "{MODEL_MAP_TABLE}""#))?;
                let models = stmt
                    .query_map([], |row| row.get::<_, Vec<u8>>(0))?
                    .map(|row| row.map_err(SqliteError::from).and_then(|bytes| bincode::deserialize(&bytes).map_err(SqliteError::from)))
                    .collect::<Result<Vec<ModelId>, SqliteError>>()?;
                Ok(models)
            })
            .await?;
        Ok(models)
    }
}

fn create_materialization_table(conn: &Connection, table_name: &str) -> Result<(), SqliteError> {
    let query = format!(
        r#"CREATE TABLE IF NOT EXISTS "{}"(
            "id" TEXT PRIMARY KEY
        )"#,
        table_name
    );
    debug!("Creating materialization table: {}", query);
    conn.execute(&query, [])?;
    Ok(())
}

/// The durable, serialized address of a property: the JSON form of its
/// [`PropertyId`], stored as the `property_key` text in `_ankurah_sqlite_column_map`.
/// The write side (the `PropertyId` a backend's `property_values()` yields) and
/// the read side (a `PropertyId` straight off the resolved AST) both go through
/// this, so a column assigned on write is found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes to JSON") }

/// Create the engine-wide durable property-to-column map table. One table for
/// the whole database; rows are scoped by model (dedup scope is
/// per-materialization, the ratified naming rule). `property_key` is a serialized
/// [`PropertyId`] (JSON TEXT, see [`property_key_text`]) -- the same durable
/// address the read side resolves against, so registered AND system properties
/// alike are addressed by identity, never by a raw name. `_ankurah_sqlite_` is
/// the reserved prefix that shields this internal table from materialization
/// name collisions.
///
/// `CREATE TABLE IF NOT EXISTS` is idempotent, so this needs no DDL lock,
/// exactly like the canonical-table creators above.
fn create_column_map_table(conn: &Connection) -> Result<(), SqliteError> {
    let query = r#"CREATE TABLE IF NOT EXISTS "_ankurah_sqlite_column_map"(
            "model_key" BLOB NOT NULL,
            "property_key" TEXT NOT NULL,
            "column_name" TEXT NOT NULL,
            PRIMARY KEY ("model_key", "property_key"),
            UNIQUE ("model_key", "column_name")
        )"#;
    debug!("Creating property column map table: {}", query);
    conn.execute(query, [])?;
    Ok(())
}

/// Column metadata
#[derive(Clone, Debug)]
pub struct SqliteColumn {
    pub name: String,
    #[allow(dead_code)]
    pub data_type: String,
}

fn sqlite_column_value_type(data_type: &str) -> Option<ValueType> {
    Some(match data_type.to_ascii_uppercase().as_str() {
        "INTEGER" => ValueType::I64,
        "REAL" => ValueType::F64,
        "TEXT" => ValueType::String,
        "BLOB" => ValueType::Binary,
        _ => return None,
    })
}

/// Private handle for one model's SQLite query materialization.
pub struct SqliteBucket {
    pool: bb8::Pool<SqliteConnectionManager>,
    model_id: ModelId,
    model_key: Vec<u8>,
    materialization_table_name: String,
    columns: Arc<std::sync::RwLock<Vec<SqliteColumn>>>,
    ddl_lock: Arc<tokio::sync::Mutex<()>>,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::column_for_key`]. Weak so storage never keeps the node alive.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This model's slice of the engine-owned durable property-to-column
    /// map (the `_ankurah_sqlite_column_map` table), cached and keyed by durable
    /// [`PropertyId`]. The map -- not the display name -- is what addresses a
    /// property's column once assigned: renames never move columns, collisions
    /// were deduped at assignment. Always carries the `PropertyId::Id -> "id"`
    /// pin so a read of the primary key is a uniform map hit.
    property_columns: Arc<std::sync::RwLock<BTreeMap<PropertyId, String>>>,
}

/// A projection whose table and columns already exist.
#[derive(Clone)]
struct PreparedSqliteMaterialization {
    entity_id: String,
    table: String,
    materialized: Vec<(String, rusqlite::types::Value, bool)>,
}

impl PreparedSqliteMaterialization {
    fn write(&self, transaction: &rusqlite::Transaction<'_>) -> Result<(), SqliteError> {
        let mut columns = vec!["id".to_owned()];
        let mut values = vec![rusqlite::types::Value::Text(self.entity_id.clone())];
        let mut placeholder_is_jsonb = vec![false];
        for (name, value, is_jsonb) in &self.materialized {
            columns.push(name.clone());
            values.push(value.clone());
            placeholder_is_jsonb.push(*is_jsonb);
        }
        let columns_str = columns.iter().map(|column| format!(r#""{column}""#)).collect::<Vec<_>>().join(", ");
        let placeholders =
            placeholder_is_jsonb.iter().map(|is_jsonb| if *is_jsonb { "jsonb(?)" } else { "?" }).collect::<Vec<_>>().join(", ");
        let update = if columns.len() == 1 {
            r#""id" = excluded."id""#.to_owned()
        } else {
            columns.iter().skip(1).map(|column| format!(r#""{column}" = excluded."{column}""#)).collect::<Vec<_>>().join(", ")
        };
        let query = format!(
            r#"INSERT INTO "{}"({}) VALUES({})
               ON CONFLICT("id") DO UPDATE SET {}"#,
            self.table, columns_str, placeholders, update
        );
        debug!("materialize_state query: {}", query);
        transaction.execute(&query, params_from_iter(values.iter()))?;
        Ok(())
    }
}

/// Fixed columns of every materialization table.
const BASE_COLUMNS: &[&str] = &["id"];

impl SqliteBucket {
    /// Create a new bucket with cached table names
    fn new(
        pool: bb8::Pool<SqliteConnectionManager>,
        model_id: ModelId,
        model_key: Vec<u8>,
        materialization_table_name: String,
        resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    ) -> Self {
        Self {
            pool,
            model_id,
            model_key,
            materialization_table_name,
            columns: Arc::new(std::sync::RwLock::new(Vec::new())),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
            resolver,
            property_columns: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
        }
    }

    #[inline]
    fn table(&self) -> &str { &self.materialization_table_name }

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

    /// Load this model's property-to-column assignments into the cache. The
    /// `property_key` column is a serialized [`PropertyId`] (JSON TEXT, see
    /// [`property_key_text`]); a row we cannot parse refuses the materialization:
    /// a hidden assignment would let that property's next cache miss claim a
    /// fresh column and silently split its data, so the map loads whole or
    /// not at all. The `PropertyId::Id -> "id"` pin is seeded last so no row
    /// can remap the primary key.
    async fn load_column_map(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let model_key = self.model_key.clone();
        let rows: Vec<(String, String)> = conn
            .with_connection(move |c| {
                let mut stmt =
                    c.prepare(r#"SELECT "property_key", "column_name" FROM "_ankurah_sqlite_column_map" WHERE "model_key" = ?"#)?;
                let rows =
                    stmt.query_map([&model_key], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
                        .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;

        let mut map = BTreeMap::new();
        for (property_key, column_name) in rows {
            match serde_json::from_str::<PropertyId>(&property_key) {
                Ok(id) => {
                    map.insert(id, column_name);
                }
                Err(e) => {
                    return Err(SqliteError::CorruptRecord(format!(
                        "property_key {:?} in the column map for materialization {}: {}",
                        property_key, self.materialization_table_name, e
                    )));
                }
            }
        }
        // The `id` pseudo-property is always the reserved primary-key column.
        map.insert(PropertyId::Id, "id".to_string());
        *self.property_columns.write().expect("RwLock poisoned") = map;
        Ok(())
    }

    /// The materialized column for a property key, addressed by durable
    /// [`PropertyId`]. Both arms route through the map (there is no name
    /// short-circuit), so a system property is addressed by identity on reads
    /// exactly like a registered one.
    ///
    /// A cache miss assigns a column NOW, then records it under the serialized
    /// `PropertyId`:
    /// - a **system** property (`PropertyId::System(SystemProperty)`) has a
    ///   closed, globally unique identity and stable label. Its sanitized label
    ///   is the column directly, recorded as a durable map row so reads resolve
    ///   it by identity. It has no entity id to suffix-dedupe with, so a
    ///   candidate that lands on a reserved base column or on a column assigned
    ///   to a different identity is a hard error, never a silent takeover;
    /// - a **registered** property (`PropertyId::EntityId`) seeds from the catalog resolver's
    ///   display name (sanitized), deduped against the base columns and this
    ///   model's other assignments (`{name}_{trailing id chars}`, the ratified
    ///   collision rule). A missing label is an error rather than an
    ///   identity-derived placeholder.
    ///
    /// The assignment is claimed with `INSERT OR IGNORE` and the winner read
    /// back, so concurrent writers converge on one column. This is DML on the
    /// map table (not DDL on the materialization table), and the
    /// `UNIQUE ("model_key", "column_name")` constraint plus the read-back +
    /// bounded retry below handle concurrent assignment, so it takes no DDL
    /// lock -- mirroring postgres, which relies on its unique constraint rather
    /// than the advisory DDL lock here. The re-dedupe retry applies only to
    /// registered properties: a system property has exactly one candidate name,
    /// so its collision is refused on first sight rather than retried.
    async fn column_for_key(&self, conn: &PooledConnection, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().expect("RwLock poisoned").get(property_id) {
            return Ok(column.clone());
        }

        let property_key = property_key_text(property_id);

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
                        .expect("RwLock poisoned")
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
                // Registered property: seed from the resolver, dedupe by id.
                PropertyId::EntityId(ulid) => {
                    let id = *ulid;
                    let resolver =
                        self.resolver.read().expect("RwLock poisoned").as_ref().and_then(std::sync::Weak::upgrade).ok_or_else(|| {
                            MutationError::UpdateFailed(
                                anyhow::anyhow!("catalog resolver is unavailable for property {property_id}").into(),
                            )
                        })?;
                    let label =
                        resolver.property_name(property_id).map_err(|error| MutationError::UpdateFailed(error.to_string().into()))?;
                    let seeded = naming::sanitize(&label);
                    let assigned = self.property_columns.read().expect("RwLock poisoned");
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    naming::dedupe(&seeded, &id, is_taken).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?
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

            // Claim the column with INSERT OR IGNORE, then read back the winner
            // for our property key. SQLite's INSERT OR IGNORE swallows BOTH the
            // (model_key, property_key) primary-key conflict and the
            // (model_key, column_name) uniqueness conflict, so -- unlike
            // postgres, which targets its ON CONFLICT only at the primary key
            // and catches the uniqueness violation as an error -- we distinguish
            // them by the read-back: a row for our key means we (or a concurrent
            // writer on the SAME property) won, converge on it; NO row means the
            // name we chose was already claimed by a DIFFERENT property, so
            // reload the taken-set and re-dedupe.
            let model_key = self.model_key.clone();
            let property_key = property_key.clone();
            let candidate = column.clone();
            let winner: Option<String> = conn
                .with_connection(move |c| {
                    c.execute(
                        r#"INSERT OR IGNORE INTO "_ankurah_sqlite_column_map" ("model_key", "property_key", "column_name") VALUES (?, ?, ?)"#,
                        rusqlite::params![model_key, property_key, candidate],
                    )?;
                    match c.query_row(
                        r#"SELECT "column_name" FROM "_ankurah_sqlite_column_map" WHERE "model_key" = ? AND "property_key" = ?"#,
                        rusqlite::params![model_key, property_key],
                        |row| row.get::<_, String>(0),
                    ) {
                        Ok(name) => Ok(Some(name)),
                        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                        Err(e) => Err(SqliteError::Rusqlite(e)),
                    }
                })
                .await?;

            match winner {
                Some(winner) => {
                    self.property_columns.write().expect("RwLock poisoned").insert(property_id.clone(), winner.clone());
                    return Ok(winner);
                }
                None => {
                    // Our candidate name is durably claimed under a different
                    // property key. Refresh the taken-set either way.
                    self.load_column_map(conn).await?;
                    if matches!(property_id, PropertyId::System(_)) {
                        // A system property has exactly one candidate name, so
                        // retrying would recompute the same name; refuse now,
                        // naming the identity that owns the column.
                        let owner = self
                            .property_columns
                            .read()
                            .expect("RwLock poisoned")
                            .iter()
                            .find(|(other, col)| other != &property_id && col.as_str() == column)
                            .map(|(other, _)| format!("property {}", property_key_text(other)))
                            .unwrap_or_else(|| "another property".to_string());
                        return Err(MutationError::UpdateFailed(
                            anyhow::anyhow!(
                                "system property {} maps to column {:?}, which is already assigned to {}",
                                property_key_text(property_id),
                                column,
                                owner
                            )
                            .into(),
                        ));
                    }
                    continue;
                }
            }
        }
        Err(MutationError::UpdateFailed(
            anyhow::anyhow!("could not assign a column for property {} after repeated collisions", property_key).into(),
        ))
    }

    async fn rebuild_columns_cache(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
        let table_name = self.table().to_owned();
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

        let table_name = self.table();
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

impl SqliteBucket {
    /// Prepare projection DML after assigning all physical names and columns.
    async fn prepare_state(&self, state: &Attested<EntityState>) -> Result<PreparedSqliteMaterialization, MutationError> {
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteError::Pool(e.to_string()))))?;
        let resolver = self
            .resolver
            .read()
            .expect("RwLock poisoned")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| MutationError::General(format!("catalog resolver is unavailable for model {}", self.model_id).into()))?;
        let projected = resolver.model_properties(&self.model_id).map_err(|error| MutationError::General(error.to_string().into()))?;
        let projected_set: std::collections::HashSet<PropertyId> = projected.iter().cloned().collect();
        let mut values_by_property = BTreeMap::new();
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (property_id, value) in backend.property_values() {
                if projected_set.contains(&property_id) {
                    values_by_property.entry(property_id).or_insert_with(|| value.map(SqliteValue::from));
                }
            }
        }

        let mut materialized = Vec::new();
        let mut missing = Vec::new();
        for property_id in projected {
            let column = self.column_for_key(&conn, &property_id).await?;
            if !self.has_column(&column) {
                let value_type =
                    resolver.property_value_type(&property_id).map_err(|error| MutationError::General(error.to_string().into()))?;
                missing.push((column.clone(), sqlite_type_for_value_type(value_type)));
            }
            let value = values_by_property.remove(&property_id).unwrap_or(None);
            let is_jsonb = value.as_ref().is_some_and(SqliteValue::is_jsonb);
            materialized.push((column, value, is_jsonb));
        }
        self.add_missing_columns(&conn, missing).await?;

        Ok(PreparedSqliteMaterialization {
            entity_id: state.payload.entity_id.to_base64(),
            table: self.table().to_owned(),
            materialized: materialized
                .into_iter()
                .map(|(name, value, is_jsonb)| {
                    let value = match value {
                        Some(v) => v.to_sql(),
                        None => rusqlite::types::Value::Null,
                    };
                    (name, value, is_jsonb)
                })
                .collect(),
        })
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("SqliteBucket({}).fetch_states: {:?}", self.materialization_table_name, selection);
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name.
        selection.check()?;

        let conn = self.pool.get().await.map_err(|e| SqliteError::Pool(e.to_string()))?;

        // Resolve, through this engine's own durable map, the column for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned column
        // -- or whose column was never materialized -- is ABSENT (evaluates
        // NULL, folded below), never re-derived from a raw name. The SQL builder
        // then translates each surviving identity to its column at emit time,
        // and the in-memory post-filter keeps reading by identity, so a rename
        // is harmless end to end.
        let assigned = self.property_columns.read().expect("RwLock poisoned").clone();
        let referenced = selection.referenced_properties();

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle to this
        // materialization may have assigned the column after this snapshot was
        // loaded. Reload the engine-owned durable map so another engine
        // instance using the same database file cannot leave this cache stale.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&conn).await?;
            self.property_columns.read().expect("RwLock poisoned").clone()
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
                "SqliteBucket({}).fetch_states: unseen assigned column referenced, refreshing schema cache",
                self.materialization_table_name
            );
            self.rebuild_columns_cache(&conn).await?;
        }
        let existing = self.existing_columns();

        // A property is absent when it has no assigned column, or its assigned
        // column is not (yet) a physical column in the materialization. Matched
        // by durable identity, so a rename never mis-targets which one is absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|p| assigned.get(p).map_or(true, |c| !existing.contains(c))).collect();
        if !absent.is_empty() {
            debug!("SqliteBucket({}).fetch_states: absent properties {:?}, treating as NULL", self.materialization_table_name, absent);
        }
        let effective_selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent)? };

        // Re-cast immediately before SQLite planning/binding. Prefer the
        // registered logical type (SQLite affinity collapses bool/integer and
        // JSON/binary); fall back to the actual materialized affinity only
        // when no resolver was configured, as in standalone storage tests.
        let physical_types: BTreeMap<String, ValueType> = self
            .columns
            .read()
            .expect("RwLock poisoned")
            .iter()
            .filter_map(|column| sqlite_column_value_type(&column.data_type).map(|ty| (column.name.clone(), ty)))
            .collect();
        let resolver = {
            let resolver = self.resolver.read().expect("RwLock poisoned");
            match resolver.as_ref() {
                None => None,
                Some(weak) => Some(
                    weak.upgrade()
                        .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {}", self.model_id)))?,
                ),
            }
        };
        let mut execution_types = BTreeMap::new();
        for property in effective_selection.referenced_properties() {
            if property == PropertyId::Id {
                execution_types.insert(property, ValueType::EntityId);
                continue;
            }
            let value_type = if let Some(resolver) = resolver.as_deref() {
                Some(resolver.property_value_type(&property).map_err(|error| RetrievalError::Other(error.to_string()))?)
            } else {
                assigned.get(&property).and_then(|column| physical_types.get(column)).copied()
            };
            if let Some(value_type) = value_type {
                execution_types.insert(property, value_type);
            }
        }
        let effective_selection = effective_selection
            .cast_comparison_values(&|path| execution_types.get(&path.id()).copied())
            .map_err(|error| RetrievalError::Other(error.to_string()))?;

        // Split predicate for pushdown
        let split = split_predicate_for_sqlite(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate.clone();

        // Build SQL. The builder translates each resolved identity to its
        // assigned column via `assigned`; every identity that survives the split
        // has one (absent properties were folded to NULL above).
        let sql_selection = ankql::ast::Selection {
            predicate: split.sql_predicate,
            order_by: effective_selection.order_by.clone(),
            limit: if needs_post_filter { None } else { effective_selection.limit },
        };

        let mut builder = SqlBuilder::with_fields(vec!["id"]);
        builder.table_name(self.table());
        builder.column_map(assigned);
        builder.selection(&sql_selection).map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

        let (sql, params) = builder.build().map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;
        debug!("fetch_states SQL: {} with {} params", sql, params.len());

        let ids = conn
            .with_connection(move |c| {
                let mut stmt = c.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(params.iter()), |row| row.get::<_, String>(0))?;
                Ok(rows.collect::<Result<Vec<_>, _>>()?)
            })
            .await?;

        let ids = ids
            .into_iter()
            .map(|id| EntityId::from_base64(&id).map_err(|error| RetrievalError::storage(std::io::Error::other(error))))
            .collect::<Result<Vec<_>, _>>()?;
        let mut results = load_states(&conn, &ids).await?;

        // Post-filter if needed
        if needs_post_filter {
            debug!("Post-filtering {} results", results.len());
            results = post_filter_states(&results, &remaining_predicate, &self.model_id);

            if let Some(limit) = effective_selection.limit {
                results.truncate(limit as usize);
            }
        }

        Ok(results)
    }
}

type RawStateRow = (String, Vec<u8>, String, Vec<u8>);

fn raw_state_from_sqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawStateRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn decode_state_row((id, state_buffer, head, attestations): RawStateRow) -> Result<Attested<EntityState>, SqliteError> {
    let entity_id = EntityId::from_base64(&id).map_err(|error| SqliteError::CorruptRecord(format!("invalid entity id {id:?}: {error}")))?;
    Ok(Attested {
        payload: EntityState {
            entity_id,
            state: State { state_buffers: StateBuffers(bincode::deserialize(&state_buffer)?), head: serde_json::from_str(&head)? },
        },
        attestations: bincode::deserialize(&attestations)?,
    })
}

async fn load_state(conn: &PooledConnection, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
    load_states(conn, &[id]).await?.into_iter().next().ok_or(RetrievalError::EntityNotFound(id))
}

async fn load_states(conn: &PooledConnection, ids: &[EntityId]) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let id_strings: Vec<String> = ids.iter().map(EntityId::to_base64).collect();
    let count = id_strings.len();
    let raw = conn
        .with_connection(move |c| {
            let placeholders = (0..count).map(|_| "?").collect::<Vec<_>>().join(", ");
            let mut stmt = c.prepare(&format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" WHERE "id" IN ({placeholders})"#
            ))?;
            let params: Vec<&dyn rusqlite::ToSql> = id_strings.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
            let rows = stmt.query_map(params.as_slice(), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, String>(2)?, row.get::<_, Vec<u8>>(3)?))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .map_err(RetrievalError::storage)?;

    let mut by_id = BTreeMap::new();
    for (id, state_buffer, head, attestations) in raw {
        let entity_id = EntityId::from_base64(&id).map_err(|error| RetrievalError::storage(std::io::Error::other(error)))?;
        by_id.insert(
            entity_id,
            Attested {
                payload: EntityState {
                    entity_id,
                    state: State {
                        state_buffers: StateBuffers(bincode::deserialize(&state_buffer).map_err(RetrievalError::storage)?),
                        head: serde_json::from_str(&head).map_err(RetrievalError::storage)?,
                    },
                },
                attestations: bincode::deserialize(&attestations).map_err(RetrievalError::storage)?,
            },
        );
    }
    Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
}

type RawEventRow = (String, Vec<u8>, String, Vec<u8>);

fn event_from_sqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawEventRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn decode_event_row((entity_id, operations, parent, attestations): RawEventRow) -> Result<Attested<Event>, RetrievalError> {
    let entity_id = EntityId::from_base64(&entity_id).map_err(|error| RetrievalError::storage(std::io::Error::other(error)))?;
    Ok(Attested {
        payload: Event {
            entity_id,
            operations: bincode::deserialize::<OperationSet>(&operations).map_err(RetrievalError::storage)?,
            parent: serde_json::from_str(&parent).map_err(RetrievalError::storage)?,
        },
        attestations: bincode::deserialize(&attestations).map_err(RetrievalError::storage)?,
    })
}

fn sqlite_type_for_value_type(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::I16 | ValueType::I32 | ValueType::I64 | ValueType::Bool => "INTEGER",
        ValueType::F64 => "REAL",
        ValueType::String | ValueType::EntityId => "TEXT",
        ValueType::Object | ValueType::Binary | ValueType::Json => "BLOB",
    }
}

/// Post-filter EntityStates using a predicate that couldn't be pushed to SQL.
fn post_filter_states(
    states: &[Attested<EntityState>],
    predicate: &ankql::ast::Predicate,
    model_id: &ModelId,
) -> Vec<Attested<EntityState>> {
    states
        .iter()
        .filter(|attested| match TemporaryEntity::new(attested.payload.entity_id, *model_id, &attested.payload.state) {
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
    use ankurah_core::{
        property::backend::{lww::LWWBackend, PropertyBackend},
        value::Value,
    };
    use ankurah_proto::Clock;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestResolver {
        model_names: BTreeMap<ModelId, String>,
        model_properties: BTreeMap<ModelId, Vec<PropertyId>>,
        property_names: BTreeMap<PropertyId, String>,
    }

    impl CatalogResolver for TestResolver {
        fn resolve_model_property(&self, model: &ModelId, property_name: &str) -> anyhow::Result<Option<PropertyId>> {
            Ok(self
                .model_properties
                .get(model)
                .into_iter()
                .flatten()
                .find_map(|property| (self.property_names.get(property).map(String::as_str) == Some(property_name)).then_some(*property)))
        }

        fn model_properties(&self, model: &ModelId) -> anyhow::Result<Vec<PropertyId>> {
            Ok(self.model_properties.get(model).cloned().unwrap_or_default())
        }

        fn model_name(&self, model: &ModelId) -> anyhow::Result<String> {
            self.model_names.get(model).cloned().ok_or_else(|| anyhow::anyhow!("unknown model {model}"))
        }

        fn property_name(&self, property: &PropertyId) -> anyhow::Result<String> {
            self.property_names.get(property).cloned().ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
        }

        fn property_value_type(&self, property: &PropertyId) -> anyhow::Result<ValueType> {
            self.property_names
                .contains_key(property)
                .then_some(ValueType::String)
                .ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
        }
    }

    fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 16]) }

    fn state_with_strings(entity_id: EntityId, event_byte: u8, values: &[(PropertyId, &str)]) -> Attested<EntityState> {
        let backend = LWWBackend::new();
        for (property, value) in values {
            backend.set(*property, Some(Value::String((*value).to_owned())));
        }
        let operations = backend.to_operations().unwrap().expect("state has values");
        let event_id = EventId::from_bytes([event_byte; 32]);
        backend.apply_operations_with_event(&operations, event_id.clone()).unwrap();
        Attested::opt(
            EntityState {
                entity_id,
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                    head: Clock::from(vec![event_id]),
                },
            },
            None,
        )
    }

    async fn commit_state(engine: &SqliteStorageEngine, expected_head: Clock, model: ModelId, state: Attested<EntityState>) {
        let outcome =
            engine.commit_batch(StorageWriteBatch::new(vec![PreparedEntityWrite::new(expected_head, state, [model])])).await.unwrap();
        assert!(matches!(outcome, CommitBatchOutcome::Committed(_)));
    }

    fn install_resolver(engine: &SqliteStorageEngine, resolver: Arc<dyn CatalogResolver>) -> Arc<dyn CatalogResolver> {
        engine.set_catalog_resolver(Arc::downgrade(&resolver));
        resolver
    }

    #[tokio::test]
    async fn test_open_in_memory() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(engine.fetch_states(&ModelId::System(SystemModel::System), &all).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_models_reads_durable_registrations() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        assert!(engine.list_materializations().await.unwrap().is_empty());

        let expected = [SystemModel::System, SystemModel::Model, SystemModel::Property].map(ModelId::System);
        for model in &expected {
            engine.materialization(model).await.unwrap();
        }

        let mut found = engine.list_materializations().await.unwrap();
        found.sort();
        let mut expected = expected.to_vec();
        expected.sort();
        assert_eq!(found, expected);
    }

    /// Human labels only seed first-use physical assignments. Equal labels
    /// remain distinct by durable identity, and every generated identifier is
    /// normalized to lowercase.
    #[tokio::test]
    async fn colliding_model_and_property_labels_get_distinct_lowercase_names() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let conn = engine.pool.get().await.unwrap();
        conn.with_connection(|connection| {
            connection.execute(r#"CREATE TABLE "sales_report" ("application_value" TEXT)"#, [])?;
            Ok(())
        })
        .await
        .unwrap();
        drop(conn);
        let model_a = ModelId::EntityId(entity_id(0x11));
        let model_b = ModelId::EntityId(entity_id(0x22));
        let property_a = PropertyId::EntityId(entity_id(0x33));
        let property_b = PropertyId::EntityId(entity_id(0x44));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            model_names: BTreeMap::from([(model_a, "Sales Report".to_owned()), (model_b, "Sales Report".to_owned())]),
            model_properties: BTreeMap::from([(model_a, vec![property_a, property_b]), (model_b, Vec::new())]),
            property_names: BTreeMap::from([(property_a, "Display Name".to_owned()), (property_b, "Display Name".to_owned())]),
        });
        let _resolver = install_resolver(&engine, resolver);

        let state = state_with_strings(entity_id(0x55), 1, &[(property_a, "alpha"), (property_b, "beta")]);
        commit_state(&engine, Clock::default(), model_a, state).await;
        let first = engine.materialization(&model_a).await.unwrap();
        let second = engine.materialization(&model_b).await.unwrap();

        assert_ne!(first.table(), second.table());
        assert_ne!(first.table(), "sales_report");
        assert_ne!(second.table(), "sales_report");
        assert!(first.table().starts_with("sales_report"));
        assert!(second.table().starts_with("sales_report"));
        assert_eq!(first.table(), first.table().to_ascii_lowercase());
        assert_eq!(second.table(), second.table().to_ascii_lowercase());

        let conn = engine.pool.get().await.unwrap();
        let model_key = bincode::serialize(&model_a).unwrap();
        let columns = conn
            .with_connection(move |connection| {
                let mut statement = connection.prepare(&format!(
                    r#"SELECT "column_name" FROM "{COLUMN_MAP_TABLE}"
                       WHERE "model_key" = ? AND "column_name" != 'id'
                       ORDER BY "column_name""#
                ))?;
                let columns = statement.query_map([model_key], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
                Ok(columns)
            })
            .await
            .unwrap();
        assert_eq!(columns.len(), 2);
        assert_ne!(columns[0], columns[1]);
        assert!(columns.iter().all(|column| column.starts_with("display_name")));
        assert!(columns.iter().all(|column| column == &column.to_ascii_lowercase()));
    }

    /// Once an entity has been used through two models, a canonical write
    /// through either one must refresh both model projections.
    #[tokio::test]
    async fn write_refreshes_every_associated_model_materialization() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let model_a = ModelId::EntityId(entity_id(0x61));
        let model_b = ModelId::EntityId(entity_id(0x62));
        let property_a = PropertyId::EntityId(entity_id(0x71));
        let property_b = PropertyId::EntityId(entity_id(0x72));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
            model_properties: BTreeMap::from([(model_a, vec![property_a]), (model_b, vec![property_b])]),
            property_names: BTreeMap::from([(property_a, "alpha".to_owned()), (property_b, "beta".to_owned())]),
        });
        let resolver = install_resolver(&engine, resolver);
        let entity = entity_id(0x73);

        let initial = state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]);
        commit_state(&engine, Clock::default(), model_a, initial.clone()).await;
        commit_state(&engine, initial.payload.state.head.clone(), model_b, initial.clone()).await;

        // The update arrives under model A but contains newer canonical data
        // for model B's projected property as well.
        let updated = state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]);
        commit_state(&engine, initial.payload.state.head, model_a, updated).await;

        let selection = ankql::parser::parse_selection("beta = 'b2'").unwrap().resolve_names(&model_b, resolver.as_ref()).unwrap();
        let found = engine.fetch_states(&model_b, &selection).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].payload.entity_id, entity);
    }

    #[tokio::test]
    async fn stale_head_rolls_back_the_complete_batch() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let model_a = ModelId::EntityId(entity_id(0x81));
        let model_b = ModelId::EntityId(entity_id(0x82));
        let property = PropertyId::EntityId(entity_id(0x83));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
            model_properties: BTreeMap::from([(model_a, vec![property]), (model_b, vec![property])]),
            property_names: BTreeMap::from([(property, "value".to_owned())]),
        });
        let _resolver = install_resolver(&engine, resolver);
        let first_id = entity_id(0x84);
        let second_id = entity_id(0x85);
        let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
        let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
        commit_state(&engine, Clock::default(), model_a, first.clone()).await;
        commit_state(&engine, Clock::default(), model_a, second.clone()).await;

        let outcome = engine
            .commit_batch(StorageWriteBatch::new(vec![
                PreparedEntityWrite::new(
                    first.payload.state.head.clone(),
                    state_with_strings(first_id, 3, &[(property, "first-new")]),
                    [model_b],
                ),
                PreparedEntityWrite::new(Clock::default(), state_with_strings(second_id, 4, &[(property, "second-new")]), [model_b]),
            ]))
            .await
            .unwrap();
        let CommitBatchOutcome::Conflict { observed } = outcome else {
            panic!("one stale expected head must reject the complete batch");
        };
        assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
        assert_eq!(engine.get_state(first_id).await.unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(engine.get_state(second_id).await.unwrap().payload.state.head, second.payload.state.head);

        let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(
            engine.fetch_states(&model_b, &all).await.unwrap().is_empty(),
            "a rejected batch must not publish associations or projections"
        );
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
