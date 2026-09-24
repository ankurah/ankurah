//! SQLite storage engine implementation

use ankurah_core::util::safemap::SafeMap;

use ankql::ast::Resolved;
use ankurah_storage_common::naming;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::schema::CatalogResolver;
use ankurah_core::storage::{
    CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageEngine, StorageTransaction,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, State, StateBuffers, PROTOCOL_VERSION};
use ankurah_proto::{ModelId, SystemModel};
use async_trait::async_trait;
use rusqlite::{params_from_iter, Connection, OptionalExtension, TransactionBehavior};

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;
use crate::value::SqliteValue;

mod materialization;
mod query;
mod transaction;
pub use transaction::SqliteTransaction;
use materialization::{Materialization, PreparedMaterialization};

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Engine-level key/value metadata for the store itself (currently just the
/// 'protocol_version' record). Not application data: it survives
/// [`StorageEngine::delete_all`], because wiping the record would make the store
/// read as unversioned and refuse its own reopen.
const META_TABLE: &str = "_ankurah_meta";
const MODEL_MAP_TABLE: &str = "_ankurah_sqlite_model_map";
const COLUMN_MAP_TABLE: &str = "_ankurah_sqlite_column_map";
pub(crate) const ENTITY_TABLE: &str = "_ankurah_entity";
pub(crate) const EVENT_TABLE: &str = "_ankurah_event";
pub(crate) const ENTITY_MODEL_TABLE: &str = "_ankurah_entity_model";
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
    /// One shared schema and physical-name map per materialization.
    materializations: SafeMap<ModelId, Arc<tokio::sync::OnceCell<Arc<Materialization>>>>,
    pool: bb8::Pool<SqliteConnectionManager>,
    /// Optional labels for first-use physical name assignment.
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
        .map_err(|e| RetrievalError::storage(e))
    }

    async fn catalog_registered_label(&self, model: &ModelId) -> Option<String> {
        if let ModelId::System(system) = model {
            return Some(system_label(*system).to_owned());
        }
        let resolver = self.resolver.read().expect("RwLock poisoned").as_ref().and_then(std::sync::Weak::upgrade)?;
        resolver.get_model_label(model).await
    }

    /// Return the immutable physical table registration for `model`, assigning
    /// it on first use. The SQLite-private map is authoritative, so reopening
    /// an existing model does not require a ready catalog resolver.
    async fn table_for_model(&self, model: &ModelId) -> Result<String, RetrievalError> {
        if let Some(name) = self.registered_table_name(model).await? {
            return Ok(name);
        }

        // Resolver access is intentionally after the durable miss.
        let registered_label = self.catalog_registered_label(model).await;
        let desired = registered_label.as_deref().map(naming::sanitize);
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
                ModelId::EntityId(id) => match desired.as_deref() {
                    Some(label) => naming::dedupe(label, &id, is_taken),
                    None => naming::fallback("m", &id, is_taken),
                }
                .map_err(|error| SqliteError::CorruptRecord(error.to_string()))?,
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
        let engine = Self { pool, materializations: SafeMap::new(), resolver: Arc::new(std::sync::RwLock::new(None)) };
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

    pub(crate) async fn ensure_shared_tables(&self, conn: &PooledConnection) -> Result<(), SqliteError> {
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
                        "body" BLOB NOT NULL,
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

    async fn materialization(&self, model: &ModelId) -> Result<Arc<Materialization>, RetrievalError> {
        self.materializations
            .get_or_default(*model)
            .get_or_try_init(|| async { Ok(Arc::new(Materialization::open(self, model).await?)) })
            .await
            .cloned()
    }

    /// Check the store against [`ankurah_proto::PROTOCOL_VERSION`]:
    ///
    /// - fresh store (no record, no ankurah tables): write the record, proceed
    /// - record present and equal: proceed
    /// - record present and different: refuse
    /// - ankurah tables present but no record: refuse as an unversioned store
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

#[async_trait]
impl StorageEngine for SqliteStorageEngine {
    type Value = SqliteValue;
    type Transaction<'a> = SqliteTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { SqliteTransaction::new(self) }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        load_state(&conn, id).await
    }

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        query::Query::prepare(self, selection).await?.states(self).await
    }

    async fn filter_entity_ids(&self, ids: &[EntityId], predicate: &ankql::ast::Predicate<Resolved>) -> Result<Vec<EntityId>, RetrievalError> {
        if ids.is_empty() { return Ok(Vec::new()); }
        let selection = ankurah_storage_common::selection::for_entity_ids(ids, predicate);
        query::Query::prepare(self, &selection).await?.ids(self).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>, predicate: &ankql::ast::Predicate<Resolved>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let event_ids: Vec<String> = event_ids.into_iter().map(|id| id.to_base64()).collect();
        let events = conn.with_connection(move |c| {
            let placeholders = (0..event_ids.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
            let mut stmt = c.prepare(&format!(
                r#"SELECT "entity_id", "body", "parent", "attestations"
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
        .collect::<Result<_, _>>()?;
        drop(conn);
        ankurah_core::storage::filter_events(self, events, predicate).await
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let entity_id = entity_id.to_base64();
        conn.with_connection(move |c| {
            let mut stmt = c.prepare(&format!(
                r#"SELECT "entity_id", "body", "parent", "attestations"
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
        self.materializations.clear();
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

type RawStateRow = (String, Vec<u8>, String, Vec<u8>);

fn raw_state_from_sqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawStateRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn decode_state_row(
    (id, state_buffer, head, attestations): RawStateRow,
    memberships: BTreeSet<ModelId>,
) -> Result<Attested<EntityState>, SqliteError> {
    let entity_id = EntityId::from_base64(&id).map_err(|error| SqliteError::CorruptRecord(format!("invalid entity id {id:?}: {error}")))?;
    Ok(Attested {
        payload: EntityState {
            entity_id,
            state: State {
                state_buffers: StateBuffers(bincode::deserialize(&state_buffer)?),
                memberships,
                head: serde_json::from_str(&head)?,
            },
        },
        attestations: bincode::deserialize(&attestations)?,
    })
}

async fn load_state(conn: &PooledConnection, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
    conn.with_connection_mut(move |c| load_states(&c.transaction()?, &[id]))
        .await?
        .into_iter()
        .next()
        .ok_or(RetrievalError::EntityNotFound(id))
}

/// Reconstruct state and memberships within the caller's read snapshot.
fn load_states(snapshot: &rusqlite::Transaction<'_>, ids: &[EntityId]) -> Result<Vec<Attested<EntityState>>, SqliteError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let id_strings: Vec<String> = ids.iter().map(EntityId::to_base64).collect();
    let placeholders = (0..ids.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let mut stmt = snapshot.prepare(&format!(
        r#"SELECT "id", "state_buffer", "head", "attestations"
           FROM "{ENTITY_TABLE}" WHERE "id" IN ({placeholders})"#
    ))?;
    let rows = stmt.query_map(params_from_iter(id_strings.iter()), raw_state_from_sqlite_row)?;
    let mut by_id = BTreeMap::new();
    for row in rows {
        let row = row?;
        let memberships = memberships_from_sqlite_connection(snapshot, &row.0)?;
        let state = decode_state_row(row, memberships)?;
        by_id.insert(state.payload.entity_id, state);
    }
    Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
}

fn memberships_from_sqlite_connection(conn: &rusqlite::Connection, entity_id: &str) -> Result<BTreeSet<ModelId>, SqliteError> {
    let mut stmt = conn.prepare(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ?"#))?;
    let model_keys = stmt.query_map([entity_id], |row| row.get::<_, Vec<u8>>(0))?.collect::<Result<Vec<_>, _>>()?;
    model_keys.into_iter().map(|key| bincode::deserialize(&key).map_err(SqliteError::from)).collect()
}

type RawEventRow = (String, Vec<u8>, String, Vec<u8>);

fn event_from_sqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawEventRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}

fn decode_event_row((entity_id, body, parent, attestations): RawEventRow) -> Result<Attested<Event>, RetrievalError> {
    let entity_id = EntityId::from_base64(&entity_id).map_err(|error| RetrievalError::storage(std::io::Error::other(error)))?;
    Ok(Attested {
        payload: Event {
            entity_id,
            body: bincode::deserialize::<EventBody>(&body).map_err(RetrievalError::storage)?,
            parent: serde_json::from_str(&parent).map_err(RetrievalError::storage)?,
        },
        attestations: bincode::deserialize(&attestations).map_err(RetrievalError::storage)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::{
        property::backend::{lww::LWWBackend, PropertyBackend},
        value::Value,
    };
    use ankurah_proto::Clock;
    use ankurah_proto::PropertyId;
    use ankurah_storage_common::ColumnPath;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestResolver {
        model_names: BTreeMap<ModelId, String>,
        property_names: BTreeMap<PropertyId, String>,
    }

    #[async_trait::async_trait]
    impl CatalogResolver for TestResolver {
        async fn get_model_label(&self, model: &ModelId) -> Option<String> { self.model_names.get(model).cloned() }

        async fn get_property_label(&self, property: &PropertyId) -> Option<String> { self.property_names.get(property).cloned() }
    }

    fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; EntityId::BYTE_LEN]) }

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
                    memberships: BTreeSet::new(),
                    head: Clock::from(vec![event_id]),
                },
            },
            None,
        )
    }

    fn state_for_model(mut state: Attested<EntityState>, model: ModelId) -> Attested<EntityState> {
        state.payload.state.memberships = [model].into();
        state
    }

    fn state_for_models(mut state: Attested<EntityState>, models: impl IntoIterator<Item = ModelId>) -> Attested<EntityState> {
        state.payload.state.memberships = models.into_iter().collect();
        state
    }

    async fn commit_canonical_state(engine: &SqliteStorageEngine, expected_head: Clock, state: Attested<EntityState>) {
        let mut transaction = engine.transaction();
        transaction.set_state(&expected_head, &state).await.unwrap();
        let outcome = transaction.commit().await.unwrap();
        assert!(matches!(outcome, StorageCommitOutcome::Committed(_)));
    }

    async fn commit_state(engine: &SqliteStorageEngine, expected_head: Clock, model: ModelId, state: Attested<EntityState>) {
        commit_canonical_state(engine, expected_head, state_for_model(state, model)).await;
    }

    fn install_resolver(engine: &SqliteStorageEngine, resolver: Arc<dyn CatalogResolver>) -> Arc<dyn CatalogResolver> {
        engine.set_catalog_resolver(Arc::downgrade(&resolver));
        resolver
    }

    #[tokio::test]
    async fn fallback_names_survive_late_labels_and_engine_reopen() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let model = ModelId::EntityId(entity_id(0xe1));
        let property = PropertyId::EntityId(entity_id(0xe2));
        let entity = entity_id(0xe3);
        let resolver = install_resolver(&engine, Arc::new(TestResolver::default()));
        let initial = state_for_model(state_with_strings(entity, 1, &[(property, "before")]), model);
        commit_canonical_state(&engine, Clock::default(), initial.clone()).await;
        let bucket = engine.materialization(&model).await.unwrap();
        let table = bucket.table().to_owned();
        let column = bucket.column_for_property(&property).await.unwrap();
        assert!(table.starts_with("m_"));
        assert!(column.starts_with("p_"));
        drop(bucket);
        drop(resolver);

        let reopened = SqliteStorageEngine::new(engine.pool.clone()).await.unwrap();
        drop(engine);
        let _resolver = install_resolver(
            &reopened,
            Arc::new(TestResolver {
                model_names: BTreeMap::from([(model, "LateModelLabel".into())]),
                property_names: BTreeMap::from([(property, "LatePropertyLabel".into())]),
            }),
        );
        let updated = state_for_model(state_with_strings(entity, 2, &[(property, "after")]), model);
        commit_canonical_state(&reopened, initial.payload.state.head, updated.clone()).await;
        let bucket = reopened.materialization(&model).await.unwrap();
        assert_eq!(bucket.table(), table);
        assert_eq!(bucket.column_for_property(&property).await.unwrap(), column);
        let selection = ankql::ast::Selection {
            predicate: ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Path(property.into())),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(Value::String("after".into()))),
            },
            order_by: None,
            limit: None,
        };
        let found = reopened.fetch_states(&selection.clone().and_member_of(model)).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].payload.entity_id, entity);

        let other = PropertyId::EntityId(entity_id(0xe8));
        let removed = state_for_model(state_with_strings(entity, 3, &[(other, "remaining")]), model);
        commit_canonical_state(&reopened, updated.payload.state.head, removed).await;
        assert!(reopened.fetch_states(&selection.clone().and_member_of(model)).await.unwrap().is_empty(), "removed properties must not retain old values");
    }

    #[tokio::test]
    async fn selected_entities_keep_their_state_and_memberships_from_one_snapshot() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("snapshot.sqlite");
        let engine = SqliteStorageEngine::open(&path).await?;
        let model = ModelId::System(SystemModel::Model);
        let second_model = ModelId::System(SystemModel::Property);
        let property = PropertyId::EntityId(entity_id(0xd1));
        let id = entity_id(0xd2);
        let before = state_for_model(state_with_strings(id, 1, &[(property, "before")]), model);
        commit_canonical_state(&engine, Clock::default(), before.clone()).await;

        let mut reader = Connection::open(path)?;
        let snapshot = reader.transaction()?;
        let selected: String = snapshot.query_row("SELECT id FROM _ankurah_model", [], |row| row.get(0))?;
        let selected = EntityId::from_base64(&selected)?;
        let after = state_for_models(state_with_strings(id, 2, &[(property, "after")]), [model, second_model]);
        commit_canonical_state(&engine, before.payload.state.head.clone(), after.clone()).await;

        assert_eq!(load_states(&snapshot, &[selected])?, vec![before]);
        drop(snapshot);
        assert_eq!(engine.get_state(id).await?, after);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_first_writes_share_ddl_coordination() {
        let directory = tempfile::tempdir().unwrap();
        let engine = SqliteStorageEngine::open(directory.path().join("concurrent.sqlite")).await.unwrap();
        let model = ModelId::EntityId(entity_id(0xe4));
        let property = PropertyId::EntityId(entity_id(0xe5));
        tokio::join!(
            commit_state(&engine, Clock::default(), model, state_with_strings(entity_id(0xe6), 1, &[(property, "first")])),
            commit_state(&engine, Clock::default(), model, state_with_strings(entity_id(0xe7), 2, &[(property, "second")])),
        );
        let all = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert_eq!(engine.fetch_states(&all.clone().and_member_of(model)).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_open_in_memory() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let all = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(engine.fetch_states(&all.clone().and_member_of(ModelId::System(SystemModel::System))).await.unwrap().is_empty());
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

    /// A canonical write refreshes every materialization named by its explicit
    /// canonical membership set.
    #[tokio::test]
    async fn write_refreshes_every_canonical_membership_materialization() {
        let engine = SqliteStorageEngine::open_in_memory().await.unwrap();
        let model_a = ModelId::EntityId(entity_id(0x61));
        let model_b = ModelId::EntityId(entity_id(0x62));
        let property_a = PropertyId::EntityId(entity_id(0x71));
        let property_b = PropertyId::EntityId(entity_id(0x72));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            model_names: BTreeMap::from([(model_a, "Alpha".to_owned()), (model_b, "Beta".to_owned())]),
            property_names: BTreeMap::from([(property_a, "alpha".to_owned()), (property_b, "beta".to_owned())]),
        });
        let _resolver = install_resolver(&engine, resolver);
        let entity = entity_id(0x73);

        let initial = state_for_models(state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]), [model_a, model_b]);
        commit_canonical_state(&engine, Clock::default(), initial.clone()).await;
        let updated = state_for_models(state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]), [model_a, model_b]);
        commit_canonical_state(&engine, initial.payload.state.head, updated).await;

        let selection = ankql::ast::Selection {
            predicate: ankql::ast::Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Path(property_b.into())),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(Value::String("b2".into()))),
            },
            order_by: None,
            limit: None,
        };
        let found = engine.fetch_states(&selection.clone().and_member_of(model_b)).await.unwrap();
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
            property_names: BTreeMap::from([(property, "value".to_owned())]),
        });
        let _resolver = install_resolver(&engine, resolver);
        let first_id = entity_id(0x84);
        let second_id = entity_id(0x85);
        let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
        let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
        commit_state(&engine, Clock::default(), model_a, first.clone()).await;
        commit_state(&engine, Clock::default(), model_a, second.clone()).await;

        let mut transaction = engine.transaction();
        transaction.set_state(
            &first.payload.state.head,
            &state_for_model(state_with_strings(first_id, 3, &[(property, "first-new")]), model_b),
        ).await.unwrap();
        transaction.set_state(
            &Clock::default(),
            &state_for_model(state_with_strings(second_id, 4, &[(property, "second-new")]), model_b),
        ).await.unwrap();
        let outcome = transaction.commit().await.unwrap();
        let StorageCommitOutcome::Conflict { observed } = outcome else {
            panic!("one stale expected head must reject the complete batch");
        };
        assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
        assert_eq!(engine.get_state(first_id).await.unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(engine.get_state(second_id).await.unwrap().payload.state.head, second.payload.state.head);

        let all = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(
            engine.fetch_states(&all.clone().and_member_of(model_b)).await.unwrap().is_empty(),
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

    #[tokio::test]
    async fn events_and_states_rollback_together_on_write_failure() -> anyhow::Result<()> {
        let engine = SqliteStorageEngine::open_in_memory().await?;
        let property = PropertyId::System(ankurah_proto::SystemProperty::Name);
        let initial = [
            state_with_strings(entity_id(0x91), 1, &[(property, "before")]),
            state_with_strings(entity_id(0x92), 2, &[(property, "before")]),
        ];
        for state in &initial {
            commit_canonical_state(&engine, Clock::default(), state.clone()).await;
        }
        let mut events = Vec::new();
        let mut writes = Vec::new();
        for old in &initial {
            let event =
                Event::update(old.payload.entity_id, old.payload.state.head.clone(), ankurah_proto::AuthorId::Unknown, Default::default());
            let mut state = state_with_strings(old.payload.entity_id, 3, &[(property, "after")]);
            state.payload.state.head = event.id().into();
            events.push(Attested::opt(event, None));
            writes.push((old.payload.state.head.clone(), state));
        }
        let conn = engine.pool.get().await?;
        let rejected = initial[1].payload.entity_id.to_base64();
        conn.with_connection(move |c| {
            c.execute_batch(&format!(
                r#"CREATE TRIGGER reject_second BEFORE UPDATE ON "{ENTITY_TABLE}"
                WHEN NEW.id = '{rejected}' BEGIN SELECT RAISE(ABORT, 'test write failure'); END;"#
            ))?;
            Ok(())
        })
        .await?;
        drop(conn);

        let mut transaction = engine.transaction();
        transaction.add_events(&events).await?;
        for (expected_head, state) in &writes {
            transaction.set_state(expected_head, state).await?;
        }
        assert!(transaction.commit().await.is_err());
        assert!(engine.get_events(events.iter().map(|event| event.payload.id()).collect(), &ankql::ast::Predicate::True).await?.is_empty());
        for state in &initial {
            assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state, state.payload.state);
        }

        let conn = engine.pool.get().await?;
        conn.with_connection(|c| {
            c.execute_batch("DROP TRIGGER reject_second")?;
            Ok(())
        })
        .await?;
        drop(conn);
        let mut transaction = engine.transaction();
        transaction.add_events(&events).await?;
        for (expected_head, state) in &writes {
            transaction.set_state(expected_head, state).await?;
        }
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
        assert_eq!(engine.get_events(events.iter().map(|event| event.payload.id()).collect(), &ankql::ast::Predicate::True).await?.len(), 2);
        for (_, state) in writes {
            assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state, state.payload.state);
        }
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
        builder
            .selection(&ankql::selection::map_references(
                &selection,
                &|path| ColumnPath::new(path.steps[0].clone(), path.steps[1..].to_vec()),
                &|model| *model.as_id().expect("model ID in physical-column fixture"),
            ))
            .map_err(|e| SqliteError::SqlGeneration(e.to_string()))?;

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
