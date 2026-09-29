//! The SQLite engine over Node.js's built-in `node:sqlite`.
//!
//! The tables, the model materializations, the pushdown query and the
//! exact-head commit mirror `storage/sqlite` (the native engine) statement for
//! statement, so a database file is interchangeable between the two. Keep the
//! two in step: a change to one engine's SQL belongs in the other as well.

use ankurah_core::util::safemap::SafeMap;

use ankql::ast::Resolved;
use ankurah_storage_common::naming;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::schema::CatalogResolver;
use ankurah_core::storage::{CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageEngine, StorageTransaction};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventBody, EventId, State, StateBuffers, PROTOCOL_VERSION};
use ankurah_proto::{ModelId, SystemModel};
use async_trait::async_trait;

use crate::error::SqliteNodeError;
use crate::exec::{Behavior, Row, SqliteExecutor};
pub(crate) use crate::node::{NodeConnection as Conn, NodePool as Pool};
use crate::value::SqliteValue;

mod materialization;
mod query;
mod transaction;
use materialization::{Materialization, PreparedMaterialization};
pub use transaction::SqliteNodeTransaction;

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

fn table_exists(executor: &dyn SqliteExecutor, table: &str) -> Result<bool, SqliteNodeError> {
    Ok(executor
        .query_row("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", &[SqliteValue::Text(table.to_owned())])?
        .is_some())
}

/// SQLite storage engine
pub struct SqliteNodeStorageEngine {
    /// One shared schema and physical-name map per materialization.
    materializations: SafeMap<ModelId, Arc<tokio::sync::OnceCell<Arc<Materialization>>>>,
    pub(crate) pool: Pool,
    /// Optional labels for first-use physical name assignment.
    resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl SqliteNodeStorageEngine {
    async fn registered_table_name(&self, model: &ModelId) -> Result<Option<String>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteNodeError::Pool(e.to_string()))?;
        let model_key = bincode::serialize(model).map_err(RetrievalError::storage)?;
        conn.with_executor(move |executor| {
            if !table_exists(executor, MODEL_MAP_TABLE)? {
                return Ok(None);
            }
            executor
                .query_row(
                    &format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}" WHERE "model_key" = ?"#),
                    &[SqliteValue::Blob(model_key)],
                )?
                .map(|row| row.text(0))
                .transpose()
        })
        .await
        .map_err(RetrievalError::storage)
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
        let conn = self.pool.get().await.map_err(|e| SqliteNodeError::Pool(e.to_string()))?;
        conn.with_executor(move |executor| {
            let tx = executor.begin(Behavior::Deferred)?;
            tx.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{MODEL_MAP_TABLE}" (
                        "model_key" BLOB PRIMARY KEY,
                        "materialization_table_name" TEXT NOT NULL UNIQUE
                    )"#
                ),
                &[],
            )?;
            if let Some(row) = tx.query_row(
                &format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}" WHERE "model_key" = ?"#),
                &[SqliteValue::Blob(model_key.clone())],
            )? {
                let name = row.text(0)?;
                tx.commit()?;
                return Ok(name);
            }

            let mut taken = std::collections::HashSet::new();
            for row in tx.query(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}""#), &[])? {
                taken.insert(row.text(0)?);
            }
            for row in tx.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'", &[])? {
                taken.insert(row.text(0)?);
            }
            taken.extend(FIXED_STORAGE_TABLES.iter().map(|name| (*name).to_owned()));
            let is_taken =
                |candidate: &str| taken.contains(candidate) || reserved_system_table_names().iter().any(|reserved| reserved == candidate);
            let materialization_table_name = match model {
                ModelId::EntityId(id) => match desired.as_deref() {
                    Some(label) => naming::dedupe(label, &id, is_taken),
                    None => naming::fallback("m", &id, is_taken),
                }
                .map_err(|error| SqliteNodeError::CorruptRecord(error.to_string()))?,
                ModelId::System(system) => {
                    let fixed = naming::sanitize(system_label(system));
                    if taken.contains(&fixed) {
                        return Err(SqliteNodeError::CorruptRecord(format!(
                            "reserved system model {model} cannot claim its physical table name {fixed:?}"
                        )));
                    }
                    fixed
                }
            };
            tx.execute(
                &format!(r#"INSERT INTO "{MODEL_MAP_TABLE}" ("model_key", "materialization_table_name") VALUES (?, ?)"#),
                &[SqliteValue::Blob(model_key), SqliteValue::Text(materialization_table_name.clone())],
            )?;
            tx.commit()?;
            Ok(materialization_table_name)
        })
        .await
        .map_err(RetrievalError::from)
    }

    async fn with_pool(pool: Pool) -> anyhow::Result<Self> {
        let engine = Self { pool, materializations: SafeMap::new(), resolver: Arc::new(std::sync::RwLock::new(None)) };
        engine.check_protocol_version().await?;
        Ok(engine)
    }

    /// Open a file-based SQLite database through Node's built-in `node:sqlite`.
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_str().ok_or_else(|| anyhow::anyhow!("the database path is not valid UTF-8"))?;
        Self::with_pool(Pool::new(Conn::open(path)?)).await
    }

    /// Open an in-memory SQLite database (for testing, and for ephemeral nodes)
    pub async fn open_in_memory() -> anyhow::Result<Self> { Self::open(":memory:").await }

    /// Adopt a database object the host constructed: Node's `DatabaseSync`, or
    /// anything with the same `prepare` and `exec` methods.
    pub async fn from_database(database: wasm_bindgen::JsValue) -> anyhow::Result<Self> {
        Self::with_pool(Pool::new(Conn::from_database(database)?)).await
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

    /// The JavaScript database object this engine runs on, for a host that
    /// wants to share it or reopen the engine over it.
    pub fn database(&self) -> &wasm_bindgen::JsValue { self.pool.connection().database() }

    /// Run a closure on the database through the executor boundary: raw SQL
    /// access for embedders and tests.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteNodeError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteNodeError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.pool.get().await.map_err(|e| SqliteNodeError::Pool(e.to_string()))?;
        conn.with_executor(f).await
    }

    pub(crate) async fn ensure_shared_tables(&self, conn: &Conn) -> Result<(), SqliteNodeError> {
        conn.with_executor(|executor| {
            executor.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{ENTITY_TABLE}" (
                        "id" TEXT PRIMARY KEY,
                        "state_buffer" BLOB NOT NULL,
                        "head" TEXT NOT NULL,
                        "attestations" BLOB NOT NULL
                    )"#
                ),
                &[],
            )?;
            executor.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{EVENT_TABLE}" (
                        "id" TEXT PRIMARY KEY,
                        "entity_id" TEXT NOT NULL,
                        "body" BLOB NOT NULL,
                        "parent" TEXT NOT NULL,
                        "attestations" BLOB NOT NULL
                    )"#
                ),
                &[],
            )?;
            executor.execute(
                &format!(
                    r#"CREATE INDEX IF NOT EXISTS "{EVENT_TABLE}_entity_id_idx"
                       ON "{EVENT_TABLE}" ("entity_id")"#
                ),
                &[],
            )?;
            executor.execute(
                &format!(
                    r#"CREATE TABLE IF NOT EXISTS "{ENTITY_MODEL_TABLE}" (
                        "entity_id" TEXT NOT NULL,
                        "model_key" BLOB NOT NULL,
                        PRIMARY KEY ("entity_id", "model_key")
                    )"#
                ),
                &[],
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
    async fn check_protocol_version(&self) -> Result<(), SqliteNodeError> {
        let conn = self.pool.get().await.map_err(|e| SqliteNodeError::Pool(e.to_string()))?;
        conn.with_executor(|executor| {
            let tables: Vec<String> = executor
                .query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", &[])?
                .iter()
                .map(|row| row.text(0))
                .collect::<Result<_, _>>()?;
            let has_ankurah_tables = tables.iter().any(|table| FIXED_STORAGE_TABLES.contains(&table.as_str()));

            let version_query = format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#);
            let recorded: Option<String> = if tables.iter().any(|t| t == META_TABLE) {
                executor.query_row(&version_query, &[])?.map(|row| row.text(0)).transpose()?
            } else {
                None
            };

            let expected = PROTOCOL_VERSION.to_string();
            match recorded {
                Some(found) if found == expected => Ok(()),
                Some(found) => Err(SqliteNodeError::ProtocolVersionMismatch { found, expected: PROTOCOL_VERSION }),
                None if has_ankurah_tables => Err(SqliteNodeError::UnversionedStore { expected: PROTOCOL_VERSION }),
                None => {
                    // Fresh store: claim the record.
                    executor
                        .execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{META_TABLE}" ("key" TEXT PRIMARY KEY, "value" TEXT)"#), &[])?;
                    executor.execute(
                        &format!(r#"INSERT OR IGNORE INTO "{META_TABLE}" ("key", "value") VALUES ('protocol_version', ?)"#),
                        &[SqliteValue::Text(expected.clone())],
                    )?;
                    // Re-read: if another process recorded between our scan and
                    // our insert, the store must still match this binary.
                    let reread = executor
                        .query_row(&version_query, &[])?
                        .ok_or_else(|| SqliteNodeError::CorruptRecord("the protocol version record vanished after it was written".into()))?
                        .text(0)?;
                    if reread == expected {
                        Ok(())
                    } else {
                        Err(SqliteNodeError::ProtocolVersionMismatch { found: reread, expected: PROTOCOL_VERSION })
                    }
                }
            }
        })
        .await
    }
}

#[async_trait]
impl StorageEngine for SqliteNodeStorageEngine {
    type Value = SqliteValue;
    type Transaction<'a> = SqliteNodeTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { SqliteNodeTransaction::new(self) }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|error| SqliteNodeError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        load_state(&conn, id).await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        query::Query::prepare(self, selection).await?.states(self).await
    }

    async fn filter_entity_ids(
        &self,
        ids: &[EntityId],
        predicate: &ankql::ast::Predicate<Resolved>,
    ) -> Result<Vec<EntityId>, RetrievalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let selection = ankurah_storage_common::selection::for_entity_ids(ids, predicate);
        query::Query::prepare(self, &selection).await?.ids(self).await
    }

    async fn get_events(
        &self,
        event_ids: Vec<EventId>,
        predicate: &ankql::ast::Predicate<Resolved>,
    ) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.pool.get().await.map_err(|error| SqliteNodeError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let params: Vec<SqliteValue> = event_ids.into_iter().map(|id| SqliteValue::Text(id.to_base64())).collect();
        let events = conn
            .with_executor(move |executor| {
                let placeholders = (0..params.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
                executor
                    .query(
                        &format!(
                            r#"SELECT "entity_id", "body", "parent", "attestations"
                               FROM "{EVENT_TABLE}" WHERE "id" IN ({placeholders})"#
                        ),
                        &params,
                    )?
                    .iter()
                    .map(raw_event_row)
                    .collect::<Result<Vec<_>, _>>()
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
        let conn = self.pool.get().await.map_err(|error| SqliteNodeError::Pool(error.to_string()))?;
        self.ensure_shared_tables(&conn).await?;
        let entity_id = entity_id.to_base64();
        conn.with_executor(move |executor| {
            executor
                .query(
                    &format!(
                        r#"SELECT "entity_id", "body", "parent", "attestations"
                           FROM "{EVENT_TABLE}" WHERE "entity_id" = ?"#
                    ),
                    &[SqliteValue::Text(entity_id)],
                )?
                .iter()
                .map(raw_event_row)
                .collect::<Result<Vec<_>, _>>()
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
        let conn = self.pool.get().await.map_err(|e| MutationError::General(Box::new(SqliteNodeError::Pool(e.to_string()))))?;

        conn.with_executor(|executor| {
            // Dynamic materializations are engine-owned only when named by the
            // durable model map. Arbitrary tables may belong to the embedding
            // application and must survive an Ankurah reset.
            let existing: BTreeSet<String> = executor
                .query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", &[])?
                .iter()
                .map(|row| row.text(0))
                .collect::<Result<_, _>>()?;

            let mut owned: BTreeSet<String> = FIXED_STORAGE_TABLES
                .iter()
                .copied()
                .filter(|name| *name != META_TABLE && existing.contains(*name))
                .map(str::to_owned)
                .collect();
            if existing.contains(MODEL_MAP_TABLE) {
                owned.extend(
                    executor
                        .query(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_MAP_TABLE}""#), &[])?
                        .iter()
                        .map(|row| row.text(0))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter(|name| existing.contains(name)),
                );
            }

            if owned.is_empty() {
                return Ok(false);
            }

            for table in owned {
                executor.execute(&format!("DROP TABLE IF EXISTS {}", quote_identifier(&table)), &[])?;
            }

            Ok(true)
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))
    }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        let conn = self.pool.get().await.map_err(|e| SqliteNodeError::Pool(e.to_string()))?;
        let models = conn
            .with_executor(|executor| {
                if !table_exists(executor, MODEL_MAP_TABLE)? {
                    return Ok(Vec::new());
                }
                executor
                    .query(&format!(r#"SELECT "model_key" FROM "{MODEL_MAP_TABLE}""#), &[])?
                    .iter()
                    .map(|row| bincode::deserialize::<ModelId>(&row.blob(0)?).map_err(SqliteNodeError::from))
                    .collect::<Result<Vec<ModelId>, SqliteNodeError>>()
            })
            .await?;
        Ok(models)
    }
}

type RawStateRow = (String, Vec<u8>, String, Vec<u8>);

fn raw_state_row(row: &Row) -> Result<RawStateRow, SqliteNodeError> { Ok((row.text(0)?, row.blob(1)?, row.text(2)?, row.blob(3)?)) }

fn decode_state_row(
    (id, state_buffer, head, attestations): RawStateRow,
    memberships: BTreeSet<ModelId>,
) -> Result<Attested<EntityState>, SqliteNodeError> {
    let entity_id =
        EntityId::from_base64(&id).map_err(|error| SqliteNodeError::CorruptRecord(format!("invalid entity id {id:?}: {error}")))?;
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

async fn load_state(conn: &Conn, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
    conn.with_executor(move |executor| {
        let snapshot = executor.begin(Behavior::Deferred)?;
        let states = load_states(&snapshot, &[id])?;
        snapshot.commit()?;
        Ok(states)
    })
    .await?
    .into_iter()
    .next()
    .ok_or(RetrievalError::EntityNotFound(id))
}

/// Reconstruct state and memberships within the caller's read snapshot.
fn load_states(snapshot: &dyn SqliteExecutor, ids: &[EntityId]) -> Result<Vec<Attested<EntityState>>, SqliteNodeError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let params: Vec<SqliteValue> = ids.iter().map(|id| SqliteValue::Text(id.to_base64())).collect();
    let placeholders = (0..ids.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let rows = snapshot.query(
        &format!(
            r#"SELECT "id", "state_buffer", "head", "attestations"
               FROM "{ENTITY_TABLE}" WHERE "id" IN ({placeholders})"#
        ),
        &params,
    )?;
    let mut by_id = BTreeMap::new();
    for row in &rows {
        let row = raw_state_row(row)?;
        let memberships = memberships_from_executor(snapshot, &row.0)?;
        let state = decode_state_row(row, memberships)?;
        by_id.insert(state.payload.entity_id, state);
    }
    Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
}

fn memberships_from_executor(executor: &dyn SqliteExecutor, entity_id: &str) -> Result<BTreeSet<ModelId>, SqliteNodeError> {
    executor
        .query(
            &format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = ?"#),
            &[SqliteValue::Text(entity_id.to_owned())],
        )?
        .iter()
        .map(|row| bincode::deserialize(&row.blob(0)?).map_err(SqliteNodeError::from))
        .collect()
}

type RawEventRow = (String, Vec<u8>, String, Vec<u8>);

fn raw_event_row(row: &Row) -> Result<RawEventRow, SqliteNodeError> { Ok((row.text(0)?, row.blob(1)?, row.text(2)?, row.blob(3)?)) }

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
