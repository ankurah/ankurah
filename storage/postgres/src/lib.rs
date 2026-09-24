use ankql::ast::Resolved;
use ankurah_core::util::safemap::SafeMap;
use ankurah_storage_common::naming;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    schema::CatalogResolver,
    storage::{CommittedEntityWrite, StorageCommitOutcome, StorageCommitResult, StorageEngine, StorageTransaction},
};
use ankurah_proto::{Attestation, AttestationSet, Attested, EntityState, EventBody, EventId, State, StateBuffers, PROTOCOL_VERSION};
use ankurah_proto::{ModelId, SystemModel};

pub mod sql_builder;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, EntityId, Event};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, GenericClient};
use tracing::{debug, error};

mod dump;
mod materialization;
mod query;
mod transaction;
pub use transaction::PostgresTransaction;
use materialization::{Materialization, PreparedMaterialization};

/// Default connection pool size for `Postgres::open()`.
/// Production applications should configure their own pool via `Postgres::new()`.
pub const DEFAULT_POOL_SIZE: u32 = 15;

/// Default connection timeout in seconds
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Engine-level key/value metadata for the store itself (currently just the
/// 'protocol_version' record). Not application data: it survives
/// [`StorageEngine::delete_all`], because wiping the record would make the store
/// read as unversioned and refuse its own reopen.
const META_TABLE: &str = "_ankurah_meta";
const MODEL_REGISTRATION_TABLE: &str = "_ankurah_postgres_model_map";
const COLUMN_MAP_TABLE: &str = "_ankurah_postgres_column_map";
const ENTITY_TABLE: &str = "_ankurah_entity";
const EVENT_TABLE: &str = "_ankurah_event";
const ENTITY_MODEL_TABLE: &str = "_ankurah_entity_model";
const IDENTIFIER_MAX_BYTES: usize = 63;
const FIXED_STORAGE_TABLES: &[&str] =
    &[META_TABLE, MODEL_REGISTRATION_TABLE, COLUMN_MAP_TABLE, ENTITY_TABLE, EVENT_TABLE, ENTITY_MODEL_TABLE];

fn quote_identifier(identifier: &str) -> String { format!(r#""{}""#, identifier.replace('"', "\"\"")) }

fn system_label(model: SystemModel) -> &'static str {
    match model {
        SystemModel::System => "_ankurah_system",
        SystemModel::Model => "_ankurah_model",
        SystemModel::Property => "_ankurah_property",
        SystemModel::ModelProperty => "_ankurah_model_property",
    }
}

/// Built-in materialization names are reserved even before their rows are
/// inserted, so assignment order cannot let an ordinary model steal one.
fn reserved_system_table_names() -> Vec<String> {
    [SystemModel::System, SystemModel::Model, SystemModel::Property, SystemModel::ModelProperty]
        .into_iter()
        .map(|model| naming::sanitize(system_label(model)))
        .collect()
}

/// PostgreSQL implementation of the model-independent storage contract.
pub struct Postgres {
    /// One shared schema and physical-name map per materialization.
    materializations: SafeMap<ModelId, Arc<tokio::sync::OnceCell<Arc<Materialization>>>>,
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    /// Optional labels for first-use physical name assignment.
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl Postgres {
    /// Create a new storage engine with an existing pool.
    ///
    /// Records or checks the store's protocol version (see
    /// `check_protocol_version`) so every construction path verifies
    /// the store it is about to serve.
    pub async fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> {
        let engine = Self { pool, materializations: SafeMap::new(), resolver: Arc::new(RwLock::new(None)) };
        engine.check_protocol_version().await?;
        Ok(engine)
    }

    /// Open a pooled PostgreSQL storage engine from a connection URI.
    pub async fn open(uri: &str) -> anyhow::Result<Self> {
        let manager = PostgresConnectionManager::new_from_stringlike(uri, NoTls)?;
        let pool = bb8::Pool::builder()
            .max_size(DEFAULT_POOL_SIZE)
            .connection_timeout(Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS))
            .build(manager)
            .await?;
        Self::new(pool).await
    }

    /// Check whether a physical PostgreSQL name uses only the supported
    /// characters.
    ///
    /// TODO: newtype this to `BucketName(&str)` with a constructor that only
    /// accepts this subset.
    pub fn sane_name(name: &str) -> bool {
        if name.len() > IDENTIFIER_MAX_BYTES {
            return false;
        }
        for char in name.chars() {
            match char {
                char if char.is_alphanumeric() => {}
                char if char.is_numeric() => {}
                '_' | '.' | ':' => {}
                _ => return false,
            }
        }

        true
    }

    async fn catalog_registered_label(&self, model_id: &ModelId) -> Option<String> {
        if let ModelId::System(system) = model_id {
            return Some(system_label(*system).to_owned());
        }
        let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade)?;
        resolver.get_model_label(model_id).await
    }

    async fn registered_table_name(client: &tokio_postgres::Client, model_key: &[u8]) -> Result<Option<String>, RetrievalError> {
        let row = client
            .query_opt(
                &format!(r#"SELECT "materialization_table_name" FROM "{MODEL_REGISTRATION_TABLE}" WHERE "model_key" = $1"#),
                &[&model_key],
            )
            .await
            .map_err(RetrievalError::storage)?;
        Ok(row.map(|row| row.get("materialization_table_name")))
    }

    /// Return the immutable physical table registration for `model_id`,
    /// assigning it on first use. The Postgres-private table is authoritative:
    /// the catalog is consulted only after a durable lookup misses.
    async fn table_for_model(&self, model_id: &ModelId) -> Result<String, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let model_key = bincode::serialize(model_id).map_err(RetrievalError::storage)?;
        if let Some(name) = Self::registered_table_name(&client, &model_key).await? {
            return Ok(name);
        }
        drop(client);
        let registered_label = self.catalog_registered_label(model_id).await;
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let lock_key = acquire_ddl_lock(&client, MODEL_REGISTRATION_TABLE).await?;
        let result = async {
            if let Some(name) = Self::registered_table_name(&client, &model_key).await? {
                return Ok(name);
            }

            let desired = registered_label.as_deref().map(naming::sanitize);
            let rows = client
                .query(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_REGISTRATION_TABLE}""#), &[])
                .await
                .map_err(RetrievalError::storage)?;
            let mut taken = std::collections::HashSet::new();
            for row in rows {
                taken.insert(row.get::<_, String>("materialization_table_name"));
            }
            let physical_rows = client
                .query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[])
                .await
                .map_err(RetrievalError::storage)?;
            taken.extend(physical_rows.into_iter().map(|row| row.get::<_, String>("table_name")));
            taken.extend(FIXED_STORAGE_TABLES.iter().map(|name| (*name).to_owned()));

            let is_taken =
                |candidate: &str| taken.contains(candidate) || reserved_system_table_names().iter().any(|reserved| reserved == candidate);
            let materialization_table_name = match model_id {
                ModelId::EntityId(id) => match desired.as_deref() {
                    Some(label) => naming::dedupe_bounded(label, id, IDENTIFIER_MAX_BYTES, is_taken),
                    None => naming::fallback("m", id, is_taken),
                }
                .map_err(|error| RetrievalError::Other(error.to_string()))?,
                ModelId::System(system) => {
                    let fixed = naming::sanitize(system_label(*system));
                    if taken.contains(&fixed) {
                        return Err(RetrievalError::Other(format!(
                            "reserved system model {model_id} cannot claim its physical table name {fixed:?}"
                        )));
                    }
                    fixed
                }
            };

            client
                .execute(
                    &format!(r#"INSERT INTO "{MODEL_REGISTRATION_TABLE}" ("model_key", "materialization_table_name") VALUES ($1, $2)"#),
                    &[&model_key, &materialization_table_name],
                )
                .await
                .map_err(RetrievalError::storage)?;
            Ok(materialization_table_name)
        }
        .await;
        release_ddl_lock(&client, lock_key).await?;
        result
    }

    /// Ensure the model-independent canonical tables and the private
    /// entity-to-model association table exist.
    async fn ensure_shared_tables(&self, client: &tokio_postgres::Client) -> Result<(), StateError> {
        let lock_key = acquire_ddl_lock(client, "ankurah_shared_tables").await?;
        let result = async {
            client
                .execute(
                    &format!(
                        r#"CREATE TABLE IF NOT EXISTS "{MODEL_REGISTRATION_TABLE}" (
                            "model_key" bytea PRIMARY KEY,
                            "materialization_table_name" text NOT NULL UNIQUE
                        )"#
                    ),
                    &[],
                )
                .await
                .map_err(|error| StateError::DDLError(Box::new(error)))?;

            client
                .execute(
                    &format!(
                        r#"CREATE TABLE IF NOT EXISTS "{ENTITY_TABLE}" (
                            "id" character(43) PRIMARY KEY,
                            "state_buffer" bytea NOT NULL,
                            "head" character(43)[] NOT NULL,
                            "attestations" bytea[] NOT NULL
                        )"#
                    ),
                    &[],
                )
                .await
                .map_err(|error| StateError::DDLError(Box::new(error)))?;
            client
                .execute(
                    &format!(
                        r#"CREATE TABLE IF NOT EXISTS "{EVENT_TABLE}" (
                            "id" character(43) PRIMARY KEY,
                            "entity_id" character(43) NOT NULL,
                            "body" bytea NOT NULL,
                            "parent" character(43)[] NOT NULL,
                            "attestations" bytea NOT NULL
                        )"#
                    ),
                    &[],
                )
                .await
                .map_err(|error| StateError::DDLError(Box::new(error)))?;
            client
                .execute(
                    &format!(
                        r#"CREATE TABLE IF NOT EXISTS "{ENTITY_MODEL_TABLE}" (
                            "entity_id" character(43) NOT NULL,
                            "model_key" bytea NOT NULL,
                            PRIMARY KEY ("entity_id", "model_key")
                        )"#
                    ),
                    &[],
                )
                .await
                .map_err(|error| StateError::DDLError(Box::new(error)))?;
            Ok(())
        }
        .await;
        release_ddl_lock(client, lock_key).await?;
        result
    }

    /// Open or create the private query surface for a model.
    async fn materialization(&self, model_id: &ModelId) -> Result<Arc<Materialization>, RetrievalError> {
        self.materializations
            .get_or_default(*model_id)
            .get_or_try_init(|| async { Ok(Arc::new(Materialization::open(self, model_id).await?)) })
            .await
            .cloned()
    }

    async fn associated_models<C>(&self, client: &C, entity_id: EntityId) -> Result<Vec<ModelId>, RetrievalError>
    where C: GenericClient + Sync {
        let rows = client
            .query(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = $1"#), &[&entity_id])
            .await
            .map_err(RetrievalError::storage)?;
        let mut models = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("model_key");
                bincode::deserialize(&bytes).map_err(RetrievalError::storage)
            })
            .collect::<Result<Vec<_>, _>>()?;
        models.sort();
        Ok(models)
    }

    /// Check the store against [`ankurah_proto::PROTOCOL_VERSION`]:
    ///
    /// - fresh store (no record, no ankurah tables): write the record, proceed
    /// - record present and equal: proceed
    /// - record present and different: refuse
    /// - ankurah tables present but no record: refuse as an unversioned store
    async fn check_protocol_version(&self) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        let rows = client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[]).await?;
        let tables: Vec<String> = rows.iter().map(|row| row.get("table_name")).collect();
        let has_ankurah_tables = tables.iter().any(|table| FIXED_STORAGE_TABLES.contains(&table.as_str()));

        let recorded: Option<String> = if tables.iter().any(|t| t == META_TABLE) {
            client
                .query_opt(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), &[])
                .await?
                .map(|row| row.get(0))
        } else {
            None
        };

        let expected = PROTOCOL_VERSION.to_string();
        match recorded {
            Some(found) if found == expected => Ok(()),
            Some(found) => anyhow::bail!(
                "incompatible store protocol version: found {found}, required {PROTOCOL_VERSION}; reset your development database (or migrate the store) before opening it with this binary"
            ),
            None if has_ankurah_tables => anyhow::bail!(
                "store has existing ankurah tables but no recorded protocol version (pre-{PROTOCOL_VERSION} store); reset your development database (or migrate the store) before opening it with this binary"
            ),
            None => {
                // Fresh store: claim the record, serializing concurrent first
                // opens the same way other engine DDL is serialized.
                let lock_key = acquire_ddl_lock(&client, META_TABLE).await?;
                let result = async {
                    client
                        .execute(&format!(r#"CREATE TABLE IF NOT EXISTS "{META_TABLE}" ("key" TEXT PRIMARY KEY, "value" TEXT)"#), &[])
                        .await?;
                    client
                        .execute(
                            &format!(
                                r#"INSERT INTO "{META_TABLE}" ("key", "value") VALUES ('protocol_version', $1) ON CONFLICT ("key") DO NOTHING"#
                            ),
                            &[&expected],
                        )
                        .await?;
                    // Re-read: if another process recorded between our scan and
                    // our insert, the store must still match this binary.
                    let reread: String = client
                        .query_one(&format!(r#"SELECT "value" FROM "{META_TABLE}" WHERE "key" = 'protocol_version'"#), &[])
                        .await?
                        .get(0);
                    if reread == expected {
                        Ok(())
                    } else {
                        anyhow::bail!(
                            "incompatible store protocol version: found {reread}, required {PROTOCOL_VERSION}; reset your development database (or migrate the store) before opening it with this binary"
                        )
                    }
                }
                .await;
                release_ddl_lock(&client, lock_key).await?;
                result
            }
        }
    }
}

/// Compute advisory lock key from a string identifier
fn advisory_lock_key(identifier: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    identifier.hash(&mut hasher);
    hasher.finish() as i64
}

/// Acquire a PostgreSQL advisory lock for DDL operations on an engine-owned
/// physical structure.
async fn acquire_ddl_lock(client: &tokio_postgres::Client, physical_name: &str) -> Result<i64, StateError> {
    let lock_key = advisory_lock_key(&format!("ankurah_ddl:{}", physical_name));
    debug!("Acquiring advisory lock {} for {}", lock_key, physical_name);
    client.execute("SELECT pg_advisory_lock($1)", &[&lock_key]).await.map_err(|err| {
        error!("Failed to acquire advisory lock for {}: {:?}", physical_name, err);
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
    type Transaction<'a> = PostgresTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { PostgresTransaction::new(self) }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let mut client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let snapshot = read_snapshot(&mut client).await?;
        load_states(&snapshot, &[id]).await?.into_iter().next().ok_or(RetrievalError::EntityNotFound(id))
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
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let rows = client
            .query(
                &format!(
                    r#"SELECT "entity_id", "body", "parent", "attestations"
                       FROM "{EVENT_TABLE}" WHERE "id" = ANY($1)"#
                ),
                &[&event_ids],
            )
            .await
            .map_err(RetrievalError::storage)?;
        let events = rows.into_iter().map(event_from_row).collect::<Result<_, _>>()?;
        drop(client);
        ankurah_core::storage::filter_events(self, events, predicate).await
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        client
            .query(
                &format!(
                    r#"SELECT "entity_id", "body", "parent", "attestations"
                       FROM "{EVENT_TABLE}" WHERE "entity_id" = $1"#
                ),
                &[&entity_id],
            )
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(event_from_row)
            .collect()
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        self.materializations.clear();
        let mut client = self.pool.get().await.map_err(|err| MutationError::General(Box::new(err)))?;

        let rows = client
            .query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", &[])
            .await
            .map_err(|err| MutationError::General(Box::new(err)))?;
        let existing: BTreeSet<String> = rows.into_iter().map(|row| row.get("table_name")).collect();

        // Dynamic materialization names are engine-owned only when recorded in
        // the durable model map. Never infer ownership from an arbitrary table
        // in the shared schema.
        let mut owned: BTreeSet<String> = FIXED_STORAGE_TABLES
            .iter()
            .copied()
            .filter(|name| *name != META_TABLE && existing.contains(*name))
            .map(str::to_owned)
            .collect();
        if existing.contains(MODEL_REGISTRATION_TABLE) {
            let rows = client
                .query(&format!(r#"SELECT "materialization_table_name" FROM "{MODEL_REGISTRATION_TABLE}""#), &[])
                .await
                .map_err(|err| MutationError::General(Box::new(err)))?;
            owned.extend(
                rows.into_iter().map(|row| row.get::<_, String>("materialization_table_name")).filter(|name| existing.contains(name)),
            );
        }
        if owned.is_empty() {
            return Ok(false);
        }

        // Start a transaction to drop all tables atomically
        let transaction = client.transaction().await.map_err(|err| MutationError::General(Box::new(err)))?;

        // Drop each table
        for table_name in owned {
            let drop_query = format!("DROP TABLE IF EXISTS {}", quote_identifier(&table_name));
            transaction.execute(&drop_query, &[]).await.map_err(|err| MutationError::General(Box::new(err)))?;
        }

        // Commit the transaction
        transaction.commit().await.map_err(|err| MutationError::General(Box::new(err)))?;

        Ok(true)
    }

    /// Non-creating durable materialization discovery. Physical table names
    /// are deliberately not reverse-resolved; the private registration table
    /// stores the logical model identities directly.
    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        let exists = client
            .query_opt(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
                &[&MODEL_REGISTRATION_TABLE],
            )
            .await
            .map_err(RetrievalError::storage)?
            .is_some();
        if !exists {
            return Ok(Vec::new());
        }
        client
            .query(&format!(r#"SELECT "model_key" FROM "{MODEL_REGISTRATION_TABLE}""#), &[])
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("model_key");
                bincode::deserialize(&bytes).map_err(RetrievalError::storage)
            })
            .collect()
    }
}

fn state_from_row(row: &tokio_postgres::Row, memberships: BTreeSet<ModelId>) -> Result<Attested<EntityState>, RetrievalError> {
    let entity_id: EntityId = row.try_get("id").map_err(RetrievalError::storage)?;
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
        payload: EntityState { entity_id, state: State { state_buffers: StateBuffers(state_buffers), memberships, head } },
        attestations: AttestationSet(attestations),
    })
}

/// Keep canonical state and membership reads on the same committed version.
async fn read_snapshot(client: &mut tokio_postgres::Client) -> Result<tokio_postgres::Transaction<'_>, RetrievalError> {
    client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(RetrievalError::storage)
}

async fn load_states(client: &tokio_postgres::Transaction<'_>, ids: &[EntityId]) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids_param = ids.to_vec();
    let rows = client
        .query(
            &format!(
                r#"SELECT "id", "state_buffer", "head", "attestations"
                   FROM "{ENTITY_TABLE}" WHERE "id" = ANY($1)"#
            ),
            &[&ids_param],
        )
        .await
        .map_err(RetrievalError::storage)?;
    let mut by_id = BTreeMap::new();
    for row in rows {
        let entity_id: EntityId = row.try_get("id").map_err(RetrievalError::storage)?;
        let memberships = client
            .query(&format!(r#"SELECT "model_key" FROM "{ENTITY_MODEL_TABLE}" WHERE "entity_id" = $1"#), &[&entity_id])
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("model_key");
                bincode::deserialize(&bytes).map_err(RetrievalError::storage)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let state = state_from_row(&row, memberships)?;
        by_id.insert(state.payload.entity_id, state);
    }
    Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
}

fn event_from_row(row: tokio_postgres::Row) -> Result<Attested<Event>, RetrievalError> {
    let entity_id: EntityId = row.try_get("entity_id").map_err(RetrievalError::storage)?;
    let body_bytes: Vec<u8> = row.try_get("body").map_err(RetrievalError::storage)?;
    let body: EventBody = bincode::deserialize(&body_bytes).map_err(RetrievalError::storage)?;
    let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
    let attestations_bytes: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
    let attestations: AttestationSet = bincode::deserialize(&attestations_bytes).map_err(RetrievalError::storage)?;
    Ok(Attested { payload: Event { entity_id, body, parent }, attestations })
}

// Some hacky shit because rust-postgres doesn't let us ask for the error kind
// TODO: remove this when https://github.com/sfackler/rust-postgres/pull/1185
//       gets merged
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ErrorKind {
    RowCount,
    UndefinedTable { table: String },
    UndefinedColumn { table: Option<String>, column: String },
    UniqueViolation,
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
        Some(SqlState::UNIQUE_VIOLATION) => ErrorKind::UniqueViolation,
        _ => ErrorKind::Unknown,
    }
}

#[allow(unused)]
pub struct MissingMaterialized {
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

    #[tokio::test]
    async fn selected_entities_keep_their_state_and_memberships_from_one_snapshot() -> anyhow::Result<()> {
        let container = postgres::Postgres::default().with_init_sql(include_bytes!("../tests/pg_init.sql").to_vec()).start().await?;
        let uri = format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?,
        );
        let engine = Postgres::open(&uri).await?;
        let id = EntityId::from_bytes([0xd2; EntityId::BYTE_LEN]);
        let before = Attested::opt(
            EntityState {
                entity_id: id,
                state: State {
                    state_buffers: StateBuffers::default(),
                    memberships: [ModelId::System(SystemModel::Model)].into(),
                    head: Clock::from(vec![EventId::from_bytes([1; 32])]),
                },
            },
            None,
        );
        let mut transaction = engine.transaction();
        transaction.set_state(&Clock::default(), &before).await?;
        let result = transaction.commit().await?;
        assert!(matches!(result, StorageCommitOutcome::Committed(_)));

        let mut reader = engine.pool.get().await?;
        let snapshot = read_snapshot(&mut reader).await?;
        let selected = snapshot.query_one("SELECT id FROM _ankurah_model", &[]).await?.get(0);
        let mut after = before.clone();
        after.payload.state.head = Clock::from(vec![EventId::from_bytes([2; 32])]);
        after.payload.state.memberships.insert(ModelId::System(SystemModel::Property));
        let mut transaction = engine.transaction();
        transaction.set_state(&before.payload.state.head, &after).await?;
        let result = transaction.commit().await?;
        assert!(matches!(result, StorageCommitOutcome::Committed(_)));

        assert_eq!(load_states(&snapshot, &[selected]).await?, vec![before]);
        drop(snapshot);
        assert_eq!(engine.get_state(id).await?, after);
        Ok(())
    }
}
