use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
    time::Duration,
};

use ankurah_core::{
    error::{MutationError, RetrievalError, StateError},
    property::backend::backend_from_string,
    schema::CatalogResolver,
    storage::{
        naming, CommitBatchOutcome, CommittedEntityWrite, PreparedEntityWrite, StorageCommitResult, StorageEngine, StorageWriteBatch,
    },
};
use ankurah_proto::{Attestation, AttestationSet, Attested, EntityState, EventId, OperationSet, State, StateBuffers, PROTOCOL_VERSION};
use ankurah_proto::{ModelId, PropertyId, SystemModel, ValueType};

use futures_util::{pin_mut, TryStreamExt};

pub mod sql_builder;
pub mod value;

use value::PGValue;

use ankurah_proto::{Clock, EntityId, Event};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};
use tokio_postgres::{error::SqlState, types::ToSql, GenericClient};
use tracing::{debug, error, info, warn};

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
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). The engine consults it only on a
    /// durable model-registration miss, before constructing the bucket.
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
}

impl Postgres {
    /// Create a new storage engine with an existing pool.
    ///
    /// Records or checks the store's protocol version (see
    /// `check_protocol_version`) so every construction path verifies
    /// the store it is about to serve.
    pub async fn new(pool: bb8::Pool<PostgresConnectionManager<NoTls>>) -> anyhow::Result<Self> {
        let engine = Self { pool, resolver: Arc::new(RwLock::new(None)) };
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

    fn catalog_registered_label(&self, model_id: &ModelId) -> Result<String, RetrievalError> {
        if let ModelId::System(system) = model_id {
            return Ok(system_label(*system).to_owned());
        }
        let resolver = self
            .resolver
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {model_id}")))?;
        resolver.model_name(model_id).map_err(|error| RetrievalError::Other(error.to_string()))
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
    async fn get_or_insert_unique_durable_table_registration(
        &self,
        client: &tokio_postgres::Client,
        model_id: &ModelId,
    ) -> Result<String, RetrievalError> {
        let lock_key = acquire_ddl_lock(client, MODEL_REGISTRATION_TABLE).await?;
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
                .map_err(RetrievalError::storage)?;

            let model_key = bincode::serialize(model_id).map_err(RetrievalError::storage)?;
            if let Some(name) = Self::registered_table_name(client, &model_key).await? {
                return Ok(name);
            }

            // Only a true first assignment needs the catalog. Reopening a
            // durable model works before catalog warm-up and a later rename
            // cannot move either physical table.
            let registered_label = self.catalog_registered_label(model_id)?;
            let desired = naming::sanitize(&registered_label);
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
                ModelId::EntityId(id) => {
                    naming::dedupe(&desired, id, is_taken).map_err(|error| RetrievalError::Other(error.to_string()))?
                }
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
        release_ddl_lock(client, lock_key).await?;
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
                        r#"CREATE TABLE IF NOT EXISTS "{ENTITY_TABLE}" (
                            "id" character(22) PRIMARY KEY,
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
                            "entity_id" character(22) NOT NULL,
                            "operations" bytea NOT NULL,
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
                            "entity_id" character(22) NOT NULL,
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
    async fn materialization(&self, model_id: &ModelId) -> Result<PostgresBucket, RetrievalError> {
        let mut client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let materialization_table_name = self.get_or_insert_unique_durable_table_registration(&client, model_id).await?;
        let model_key = bincode::serialize(model_id).map_err(RetrievalError::storage)?;
        let schema: String =
            client.query_one("SELECT current_database()", &[]).await.map_err(RetrievalError::storage)?.get("current_database");

        let bucket = PostgresBucket {
            pool: self.pool.clone(),
            schema,
            model_id: *model_id,
            model_key,
            materialization_table_name,
            resolver: self.resolver.clone(),
            columns: Arc::new(RwLock::new(Vec::new())),
            property_columns: Arc::new(RwLock::new(BTreeMap::new())),
            #[cfg(debug_assertions)]
            last_spilled_predicate: Arc::new(RwLock::new(None)),
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

    async fn append_events(&self, events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let mut client = self.pool.get().await.map_err(|error| MutationError::General(Box::new(error)))?;
        self.ensure_shared_tables(&client).await?;
        let transaction = client.transaction().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        let mut inserted = Vec::with_capacity(events.len());
        for event in events {
            let operations = bincode::serialize(&event.payload.operations)?;
            let attestations = bincode::serialize(&event.attestations)?;
            let affected = transaction
                .execute(
                    &format!(
                        r#"INSERT INTO "{EVENT_TABLE}" ("id", "entity_id", "operations", "parent", "attestations")
                           VALUES ($1, $2, $3, $4, $5) ON CONFLICT ("id") DO NOTHING"#
                    ),
                    &[&event.payload.id(), &event.payload.entity_id, &operations, &event.payload.parent, &attestations],
                )
                .await
                .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
            inserted.push(affected > 0);
        }
        transaction.commit().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
        Ok(inserted)
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

        // Physical schema and durable identity-to-name assignments may be
        // prepared before the data transaction. They expose no entity,
        // association, or projected record if the later CAS fails.
        let client = self.pool.get().await.map_err(|error| MutationError::General(Box::new(error)))?;
        self.ensure_shared_tables(&client).await?;
        let mut models_by_entity = BTreeMap::<EntityId, BTreeSet<ModelId>>::new();
        for write in &batch.entities {
            let entity_id = write.state.payload.entity_id;
            let mut models: BTreeSet<ModelId> = self
                .associated_models(&*client, entity_id)
                .await
                .map_err(|error| MutationError::General(error.to_string().into()))?
                .into_iter()
                .collect();
            models.extend(write.associate_with.iter().copied());
            models_by_entity.insert(entity_id, models);
        }
        drop(client);

        let mut prepared = BTreeMap::<(EntityId, ModelId), PreparedPostgresMaterialization>::new();
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

        // A model association can appear between schema preparation and the
        // transaction (including from another durable node). In that case the
        // data transaction is rolled back, the missing empty schema is
        // prepared, and the exact-head attempt is repeated.
        loop {
            let mut client = self.pool.get().await.map_err(|error| MutationError::General(Box::new(error)))?;
            let transaction = client.transaction().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;

            let mut ordered: Vec<(usize, &PreparedEntityWrite)> = batch.entities.iter().enumerate().collect();
            ordered.sort_by_key(|(_, write)| write.state.payload.entity_id);

            let mut observed = BTreeMap::new();
            let mut conflict = false;
            for (_, write) in &ordered {
                let entity_id = write.state.payload.entity_id;
                // Unlike a row lock, this also serializes competing inserts for
                // an entity which does not yet have a canonical row.
                let lock_identity = entity_id.to_base64();
                transaction
                    .execute("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))", &[&lock_identity])
                    .await
                    .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
                let current = transaction
                    .query_opt(
                        &format!(
                            r#"SELECT "id", "state_buffer", "head", "attestations"
                               FROM "{ENTITY_TABLE}" WHERE "id" = $1 FOR UPDATE"#
                        ),
                        &[&entity_id],
                    )
                    .await
                    .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?
                    .map(|row| state_from_row(&row))
                    .transpose()
                    .map_err(|error| MutationError::General(error.to_string().into()))?;
                let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
                if current_head != write.expected_head {
                    conflict = true;
                }
                observed.insert(entity_id, current);
            }

            if conflict {
                transaction.rollback().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
                return Ok(CommitBatchOutcome::Conflict { observed });
            }

            let mut complete_models = BTreeMap::<EntityId, Vec<ModelId>>::new();
            let mut associations_added = BTreeMap::<EntityId, Vec<ModelId>>::new();
            let mut missing_schema = Vec::<(EntityId, ModelId)>::new();
            for (_, write) in &ordered {
                let entity_id = write.state.payload.entity_id;
                let mut added = Vec::new();
                for model in &write.associate_with {
                    let model_key = bincode::serialize(model)?;
                    let affected = transaction
                        .execute(
                            &format!(
                                r#"INSERT INTO "{ENTITY_MODEL_TABLE}" ("entity_id", "model_key")
                                   VALUES ($1, $2) ON CONFLICT DO NOTHING"#
                            ),
                            &[&entity_id, &model_key],
                        )
                        .await
                        .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
                    if affected > 0 {
                        added.push(*model);
                    }
                }
                let models = self
                    .associated_models(&transaction, entity_id)
                    .await
                    .map_err(|error| MutationError::General(error.to_string().into()))?;
                for model in &models {
                    if !prepared.contains_key(&(entity_id, *model)) {
                        missing_schema.push((entity_id, *model));
                    }
                }
                associations_added.insert(entity_id, added);
                complete_models.insert(entity_id, models);
            }

            if !missing_schema.is_empty() {
                transaction.rollback().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
                for (entity_id, model) in missing_schema {
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
                continue;
            }

            let mut committed = Vec::with_capacity(batch.entities.len());
            for (original_index, write) in ordered {
                let state = &write.state;
                let entity_id = state.payload.entity_id;
                let state_buffers = bincode::serialize(&state.payload.state.state_buffers)?;
                let attestations: Vec<Vec<u8>> = state.attestations.iter().map(bincode::serialize).collect::<Result<_, _>>()?;
                transaction
                    .execute(
                        &format!(
                            r#"INSERT INTO "{ENTITY_TABLE}" ("id", "state_buffer", "head", "attestations")
                               VALUES ($1, $2, $3, $4)
                               ON CONFLICT ("id") DO UPDATE SET
                                   "state_buffer" = EXCLUDED."state_buffer",
                                   "head" = EXCLUDED."head",
                                   "attestations" = EXCLUDED."attestations""#
                        ),
                        &[&entity_id, &state_buffers, &state.payload.state.head, &attestations],
                    )
                    .await
                    .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;

                let models = complete_models
                    .remove(&entity_id)
                    .ok_or_else(|| MutationError::General(format!("storage batch omitted the model set for entity {entity_id}").into()))?;
                for model in &models {
                    prepared
                        .get(&(entity_id, *model))
                        .ok_or_else(|| {
                            MutationError::General(
                                format!("storage batch omitted the prepared materialization for entity {entity_id} under model {model}")
                                    .into(),
                            )
                        })?
                        .write(&transaction)
                        .await?;
                }
                let canonical_changed = write.expected_head != state.payload.state.head;
                committed.push((
                    original_index,
                    CommittedEntityWrite {
                        entity_id,
                        canonical_changed,
                        associations_added: associations_added.remove(&entity_id).unwrap_or_default(),
                        materialized_as: models,
                    },
                ));
            }

            committed.sort_by_key(|(index, _)| *index);
            let entities = committed.into_iter().map(|(_, result)| result).collect();
            transaction.commit().await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
            return Ok(CommitBatchOutcome::Committed(StorageCommitResult { entities }));
        }
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let row = client
            .query_opt(
                &format!(
                    r#"SELECT "id", "state_buffer", "head", "attestations"
                       FROM "{ENTITY_TABLE}" WHERE "id" = $1"#
                ),
                &[&id],
            )
            .await
            .map_err(RetrievalError::storage)?
            .ok_or(RetrievalError::EntityNotFound(id))?;
        state_from_row(&row)
    }

    async fn fetch_states(&self, model: &ModelId, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.materialization(model).await?.fetch_states(selection).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let rows = client
            .query(
                &format!(
                    r#"SELECT "entity_id", "operations", "parent", "attestations"
                       FROM "{EVENT_TABLE}" WHERE "id" = ANY($1)"#
                ),
                &[&event_ids],
            )
            .await
            .map_err(RetrievalError::storage)?;
        rows.into_iter().map(event_from_row).collect()
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let client = self.pool.get().await.map_err(RetrievalError::storage)?;
        self.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        client
            .query(
                &format!(
                    r#"SELECT "entity_id", "operations", "parent", "attestations"
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

fn state_from_row(row: &tokio_postgres::Row) -> Result<Attested<EntityState>, RetrievalError> {
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
        payload: EntityState { entity_id, state: State { state_buffers: StateBuffers(state_buffers), head } },
        attestations: AttestationSet(attestations),
    })
}

async fn load_states_from_client(client: &tokio_postgres::Client, ids: &[EntityId]) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
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
        let state = state_from_row(&row)?;
        by_id.insert(state.payload.entity_id, state);
    }
    Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
}

fn event_from_row(row: tokio_postgres::Row) -> Result<Attested<Event>, RetrievalError> {
    let entity_id: EntityId = row.try_get("entity_id").map_err(RetrievalError::storage)?;
    let operations_bytes: Vec<u8> = row.try_get("operations").map_err(RetrievalError::storage)?;
    let operations: OperationSet = bincode::deserialize(&operations_bytes).map_err(RetrievalError::storage)?;
    let parent: Clock = row.try_get("parent").map_err(RetrievalError::storage)?;
    let attestations_bytes: Vec<u8> = row.try_get("attestations").map_err(RetrievalError::storage)?;
    let attestations: AttestationSet = bincode::deserialize(&attestations_bytes).map_err(RetrievalError::storage)?;
    Ok(Attested { payload: Event { entity_id, operations, parent }, attestations })
}

fn postgres_type_for_value_type(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::I16 => "int2",
        ValueType::I32 => "int4",
        ValueType::I64 => "int8",
        ValueType::F64 => "float8",
        ValueType::Bool => "boolean",
        ValueType::String | ValueType::EntityId => "varchar",
        ValueType::Object | ValueType::Binary => "bytea",
        ValueType::Json => "jsonb",
    }
}

#[derive(Clone, Debug)]
struct PostgresColumn {
    pub name: String,
    pub is_nullable: bool,
    pub data_type: String,
}

/// Logical binding domain exposed by a PostgreSQL physical column. This is
/// intentionally derived from the materialized schema at execution time; a
/// bucket does not retain a catalog resolver merely to trust an earlier AST
/// normalization pass.
fn postgres_column_value_type(data_type: &str) -> Option<ValueType> {
    Some(match data_type {
        "smallint" => ValueType::I16,
        "integer" => ValueType::I32,
        "bigint" => ValueType::I64,
        "real" | "double precision" | "numeric" => ValueType::F64,
        "boolean" => ValueType::Bool,
        "character" | "character varying" | "text" => ValueType::String,
        "bytea" => ValueType::Binary,
        "json" | "jsonb" => ValueType::Json,
        _ => return None,
    })
}

/// Storage handle for one model's durably assigned PostgreSQL materialization.
pub(crate) struct PostgresBucket {
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    model_id: ModelId,
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
    /// Tracks the last predicate that spilled to post-filtering (debug builds only)
    #[cfg(debug_assertions)]
    last_spilled_predicate: Arc<RwLock<Option<ankql::ast::Predicate>>>,
}

/// A projection whose physical table and columns have already been prepared.
///
/// The remaining write is ordinary transactional DML, so a caller can include
/// it in the same PostgreSQL transaction as the canonical entity and its
/// model associations.
struct PreparedPostgresMaterialization {
    entity_id: EntityId,
    table: String,
    materialized: Vec<(String, Option<PGValue>)>,
}

impl PreparedPostgresMaterialization {
    async fn write<C>(&self, client: &C) -> Result<(), MutationError>
    where C: GenericClient + Sync {
        let mut columns: Vec<String> = vec!["id".to_owned()];
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&self.entity_id];
        for (name, parameter) in &self.materialized {
            columns.push(name.clone());
            match parameter {
                Some(PGValue::CharacterVarying(value)) => params.push(value),
                Some(PGValue::SmallInt(value)) => params.push(value),
                Some(PGValue::Integer(value)) => params.push(value),
                Some(PGValue::BigInt(value)) => params.push(value),
                Some(PGValue::DoublePrecision(value)) => params.push(value),
                Some(PGValue::Bytea(value)) => params.push(value),
                Some(PGValue::Boolean(value)) => params.push(value),
                Some(PGValue::Jsonb(value)) => params.push(value),
                None => params.push(&UntypedNull),
            }
        }
        let columns_str = columns.iter().map(|name| format!("\"{}\"", name)).collect::<Vec<String>>().join(", ");
        let values_str = params.iter().enumerate().map(|(index, _)| format!("${}", index + 1)).collect::<Vec<String>>().join(", ");
        let update = if columns.len() == 1 {
            r#""id" = EXCLUDED."id""#.to_owned()
        } else {
            columns.iter().skip(1).map(|name| format!(r#""{name}" = EXCLUDED."{name}""#)).collect::<Vec<_>>().join(", ")
        };
        let query = format!(
            r#"INSERT INTO "{0}" ({1}) VALUES ({2})
               ON CONFLICT ("id") DO UPDATE SET {3}"#,
            self.table, columns_str, values_str, update
        );
        debug!("materialize_state {}: {}", self.table, query);
        client.execute(&query, params.as_slice()).await.map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
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

impl PostgresBucket {
    fn table(&self) -> &str { &self.materialization_table_name }

    /// Create the engine-wide durable property-to-column map table. One table
    /// for the whole database; rows are scoped by model (dedup scope is
    /// per-materialization, the ratified naming rule). `property_key` is a serialized
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

    /// The materialized column for a property key, addressed by durable
    /// [`PropertyId`]. Both value-bearing arms route through the map (there is
    /// no name short-circuit), so a system property is addressed by identity on
    /// reads exactly like a registered one.
    ///
    /// A cache miss assigns a column NOW, then records it under the serialized
    /// `PropertyId`:
    /// - a **system** property (`PropertyId::System(SystemProperty)`) has a
    ///   closed, globally unique identity and stable label. Its sanitized label
    ///   is the column directly, recorded as a durable map row so reads resolve
    ///   it by identity. It has no entity id to suffix-dedupe with, so a
    ///   candidate that lands on a reserved base column or on a column assigned
    ///   to a different identity is a hard error, never a silent takeover;
    /// - a **registered** property (`PropertyId::EntityId`) seeds from the catalog
    ///   resolver's display name (sanitized), deduped against the base columns
    ///   and this model's other assignments (`{name}_{trailing id chars}`, the
    ///   ratified collision rule). A missing catalog label is an error: an
    ///   identity-derived placeholder would defeat the purpose of the durable
    ///   human-readable physical-name map.
    ///
    /// The assignment is CAS'd into the map table (`ON CONFLICT DO NOTHING` on
    /// the primary key, then read back) so concurrent writers on the SAME
    /// property converge on one winner, while the `UNIQUE ("model_key",
    /// "column_name")` constraint surfaces a DIFFERENT property racing for the
    /// same NAME as a unique violation, which reloads the taken-set and
    /// re-dedupes (registered properties only: a system property has exactly
    /// one candidate name, so its unique violation is refused on first sight).
    async fn column_for_key(&self, client: &tokio_postgres::Client, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(column) = self.property_columns.read().unwrap().get(property_id) {
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
                    let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade).ok_or_else(|| {
                        MutationError::UpdateFailed(anyhow::anyhow!("catalog resolver is unavailable for property {property_id}").into())
                    })?;
                    let label =
                        resolver.property_name(property_id).map_err(|error| MutationError::UpdateFailed(error.to_string().into()))?;
                    let desired = naming::sanitize(&label);
                    let assigned = self.property_columns.read().unwrap();
                    let is_taken = |candidate: &str| {
                        BASE_COLUMNS.contains(&candidate) || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    naming::dedupe(&desired, &id, is_taken).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?
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
                    self.load_column_map(client).await.map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
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
        debug!("PostgresBucket({}).rebuild_columns_cache", self.materialization_table_name);
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

    pub fn column(&self, column_name: &String) -> Option<PostgresColumn> {
        let columns = self.columns.read().unwrap();
        columns.iter().find(|column| column.name == *column_name).cloned()
    }

    pub fn has_column(&self, column_name: &String) -> bool { self.column(column_name).is_some() }

    /// Create this model's projection table. Canonical state is stored only in
    /// [`ENTITY_TABLE`]; materializations contain the entity id and projected
    /// property columns used for model-scoped querying.
    pub async fn create_materialization_table(&self, client: &mut tokio_postgres::Client) -> Result<(), StateError> {
        let create_query = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"(
                "id" character(22) PRIMARY KEY
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
                        "PostgresBucket({}).create_materialization_table error: {} (code: {:?})",
                        self.materialization_table_name,
                        db_err,
                        db_err.code()
                    );
                } else {
                    error!("PostgresBucket({}).create_materialization_table error: {:?}", self.materialization_table_name, err);
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
                    info!("PostgresBucket({}).add_missing_columns: {}", self.materialization_table_name, alter_query);
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

impl PostgresBucket {
    /// Prepare this model's projection without publishing an entity record.
    ///
    /// Durable table/column assignments and empty schema may be created here;
    /// the returned DML remains suitable for the caller's canonical-state
    /// transaction.
    async fn prepare_state(&self, state: &Attested<EntityState>) -> Result<PreparedPostgresMaterialization, MutationError> {
        let mut client = self.pool.get().await.map_err(|err| MutationError::General(err.into()))?;
        let resolver = self
            .resolver
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| MutationError::General(format!("catalog resolver is unavailable for model {}", self.model_id).into()))?;
        let projected = resolver.model_properties(&self.model_id).map_err(|error| MutationError::General(error.to_string().into()))?;
        let projected_set: std::collections::HashSet<PropertyId> = projected.iter().cloned().collect();
        let mut values = BTreeMap::new();
        for (name, state_buffer) in state.payload.state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            for (property_id, value) in backend.property_values() {
                if projected_set.contains(&property_id) {
                    values.entry(property_id).or_insert_with(|| value.map(PGValue::from));
                }
            }
        }

        // Include every registered projected property, not only properties
        // present in this state, so a later write reliably clears removed
        // values to NULL.
        let mut materialized: Vec<(String, Option<PGValue>)> = Vec::new();
        let mut missing = Vec::new();
        for property_id in projected {
            let column = self.column_for_key(&client, &property_id).await?;
            if !self.has_column(&column) {
                let value_type =
                    resolver.property_value_type(&property_id).map_err(|error| MutationError::General(error.to_string().into()))?;
                missing.push((column.clone(), postgres_type_for_value_type(value_type)));
            }
            materialized.push((column, values.remove(&property_id).unwrap_or(None)));
        }
        self.add_missing_columns(&mut client, missing).await?;
        Ok(PreparedPostgresMaterialization { entity_id: state.payload.entity_id, table: self.table().to_owned(), materialized })
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        debug!("fetch_states: {:?}", selection);
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name.
        selection.check()?;
        let mut client = self.pool.get().await.map_err(|err| RetrievalError::StorageError(Box::new(err)))?;

        // Resolve, through this engine's own durable map, the column for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned column
        // -- or whose column was never materialized -- is ABSENT (evaluates
        // NULL, folded below), never re-derived from a raw name. The SQL builder
        // then translates each surviving identity to its column at emit time,
        // and the in-memory post-filter keeps reading by identity, so a rename
        // is harmless end to end.
        let assigned = self.property_columns.read().unwrap().clone();
        let referenced = selection.referenced_properties();

        // If the snapshot lacks a referenced property, reload the durable map
        // once before folding it to absent: another handle or durable node
        // sharing this database may have assigned the column after this
        // snapshot was loaded. Reloading the engine-owned durable map is part
        // of the shared-database contract.
        let assigned = if referenced.iter().any(|p| !assigned.contains_key(p)) {
            self.load_column_map(&client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
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
                "PostgresBucket({}).fetch_states: unseen assigned column referenced, refreshing schema cache",
                self.materialization_table_name
            );
            self.rebuild_columns_cache(&mut client).await.map_err(|e| RetrievalError::StorageError(e.into()))?;
        }
        let existing = self.existing_columns();

        // A property is absent when it has no assigned column, or its assigned
        // column is not (yet) a physical column in the materialization. Matched
        // by durable identity, so a rename never mis-targets which one is absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|p| assigned.get(p).map_or(true, |c| !existing.contains(c))).collect();
        if !absent.is_empty() {
            debug!("PostgresBucket({}).fetch_states: absent properties {:?}, treating as NULL", self.materialization_table_name, absent);
        }
        let effective_selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent)? };

        // Re-cast immediately before SQL planning/binding. Name resolution may
        // already have normalized these values, but the materialized PostgreSQL
        // schema is the authority at this execution boundary.
        let physical_types: BTreeMap<String, ValueType> = self
            .columns
            .read()
            .unwrap()
            .iter()
            .filter_map(|column| postgres_column_value_type(&column.data_type).map(|ty| (column.name.clone(), ty)))
            .collect();
        let effective_selection = effective_selection
            .cast_comparison_values(&|path| assigned.get(&path.id()).and_then(|column| physical_types.get(column)).copied())
            .map_err(|error| RetrievalError::Other(error.to_string()))?;

        // Split predicate into parts we can pushdown to PostgreSQL vs post-filter in Rust
        let split = sql_builder::split_predicate_for_postgres(&effective_selection.predicate);
        let needs_post_filter = split.needs_post_filter();
        let remaining_predicate = split.remaining_predicate; // Cache before moving sql_predicate
        debug!(
            "PostgresBucket({}).fetch_states: SQL predicate: {:?}, remaining: {:?}, needs_post_filter: {}",
            self.materialization_table_name, split.sql_predicate, remaining_predicate, needs_post_filter
        );

        // Track spilled predicate for test assertions (debug builds only)
        #[cfg(debug_assertions)]
        {
            let mut spilled = self.last_spilled_predicate.write().unwrap();
            *spilled = if needs_post_filter { Some(remaining_predicate.clone()) } else { None };
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

        let mut ids = Vec::new();
        let mut builder = SqlBuilder::with_fields(vec!["id"]);
        builder.table_name(self.table());
        // The builder translates each resolved identity to its assigned column
        // via `assigned`; every identity that survives the split has one (absent
        // properties were folded to NULL above).
        builder.column_map(assigned);
        builder.selection(&sql_selection)?;

        let (sql, args) = builder.build()?;
        debug!("PostgresBucket({}).fetch_states: SQL: {} with args: {:?}", self.materialization_table_name, sql, args);

        let stream = match client.query_raw(&sql, args).await {
            Ok(stream) => stream,
            Err(err) => {
                let kind = error_kind(&err);
                if let ErrorKind::UndefinedTable { table } = kind {
                    if table == self.table() {
                        // Table doesn't exist yet, return empty results
                        return Ok(Vec::new());
                    }
                }
                return Err(RetrievalError::StorageError(err.into()));
            }
        };
        pin_mut!(stream);

        while let Some(row) = stream.try_next().await.map_err(RetrievalError::storage)? {
            ids.push(row.try_get(0).map_err(RetrievalError::storage)?);
        }
        drop(stream);
        let results = load_states_from_client(&client, &ids).await?;

        // Post-filter results if we have remaining predicate that couldn't be pushed down
        let results = if needs_post_filter {
            debug!(
                "PostgresBucket({}).fetch_states: Post-filtering {} results with remaining predicate",
                self.materialization_table_name,
                results.len()
            );
            let filtered = post_filter_states(&results, &remaining_predicate, &self.model_id);

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
    model_id: &ModelId,
) -> Vec<Attested<EntityState>> {
    use ankurah_core::entity::TemporaryEntity;
    use ankurah_core::selection::filter::evaluate_predicate;

    states
        .iter()
        .filter(|attested| {
            // Create a TemporaryEntity for filtering (implements Filterable)
            match TemporaryEntity::new(attested.payload.entity_id, *model_id, &attested.payload.state) {
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
