use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc, Mutex};
#[cfg(not(debug_assertions))]
use std::sync::{Arc, Mutex};

use ankurah_core::{
    error::{MutationError, RetrievalError},
    schema::CatalogResolver,
    storage::{CommitBatchOutcome, CommittedEntityWrite, PreparedEntityWrite, StorageCommitResult, StorageEngine, StorageWriteBatch},
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, EventId, ModelId, StateFragment};
use async_trait::async_trait;
use sled::transaction::{ConflictableTransactionError, TransactionError, Transactional};
use sled::Config;

use crate::{
    database::{model_from_tree_name, model_tree_name, Database},
    error::SledRetrievalError,
    index::Index,
    model_store::{project_state, SledModelStore},
};

#[derive(Clone)]
struct PreparedSledMaterialization {
    model: ModelId,
    tree: sled::Tree,
    values: Vec<(u32, ankurah_core::value::Value)>,
    encoded: Vec<u8>,
    indexes: Vec<Index>,
}

#[derive(Clone)]
struct PreparedSledEntity {
    original_index: usize,
    write: PreparedEntityWrite,
    encoded_state: Vec<u8>,
}

enum SledBatchAbort {
    Conflict(BTreeMap<EntityId, Option<Attested<EntityState>>>),
    MissingSchema,
    Invalid(String),
}

fn sled_abort(error: impl std::fmt::Display) -> ConflictableTransactionError<SledBatchAbort> {
    ConflictableTransactionError::Abort(SledBatchAbort::Invalid(error.to_string()))
}

/// Sled implementation of the model-independent storage contract.
pub struct SledStorageEngine {
    /// Shared canonical stores, materializations, and engine metadata.
    pub database: Mutex<Arc<Database>>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Lives on the engine -- not on
    /// [`Database`] -- so a hard reset (which recreates the `Database`) keeps
    /// it; buckets get a clone at creation.
    pub(crate) resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    #[cfg(debug_assertions)]
    /// Runtime test switch for disabling open-ended scan prefix guards.
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

impl SledStorageEngine {
    #[cfg(debug_assertions)]
    /// Enable or disable open-ended scan prefix guards in debug builds.
    pub fn set_prefix_guard_disabled(&self, disabled: bool) {
        use std::sync::atomic::Ordering;
        self.prefix_guard_disabled.store(disabled, Ordering::Relaxed);
    }
}

impl SledStorageEngine {
    /// Open a database under a folder in the current user's home directory.
    pub fn with_homedir_folder(folder_name: &str) -> anyhow::Result<Self> {
        let dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?.join(folder_name);

        Self::with_path(dir)
    }

    /// Open or create a Sled storage engine at `path`.
    pub fn with_path(path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dbpath = path.join("sled");
        let db = sled::open(&dbpath)?;
        Ok(Self {
            database: Mutex::new(Arc::new(Database::open(db)?)),
            resolver: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Open the default `.ankurah` database in the current user's home
    /// directory.
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    /// Construct an isolated temporary engine for tests.
    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self {
            database: Mutex::new(Arc::new(Database::open(db)?)),
            resolver: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// List model identities which already have durable materialization trees.
    pub fn list_models(&self) -> Result<Vec<ModelId>, RetrievalError> {
        Ok(self.database.lock().unwrap().db.tree_names().iter().filter_map(|name| model_from_tree_name(name)).collect())
    }

    fn materialization(&self, model_id: &ModelId) -> Result<SledModelStore, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let tree_name = model_tree_name(model_id);
        let tree = database.db.open_tree(tree_name).map_err(SledRetrievalError::StorageError)?;
        Ok(SledModelStore::new(
            *model_id,
            database,
            tree,
            self.resolver.clone(),
            #[cfg(debug_assertions)]
            self.prefix_guard_disabled.clone(),
        ))
    }

    fn commit_batch_blocking(
        database: Arc<Database>,
        resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
        batch: StorageWriteBatch,
    ) -> Result<CommitBatchOutcome, MutationError> {
        let mut seen = BTreeSet::new();
        for write in &batch.entities {
            if !seen.insert(write.state.payload.entity_id) {
                return Err(MutationError::General(
                    format!("storage write batch contains duplicate entity {}", write.state.payload.entity_id).into(),
                ));
            }
        }

        loop {
            // Index creation/backfill uses the same guard. This is not the
            // entity CAS mechanism; it only freezes sled's in-process set of
            // physical index trees while the native multi-tree transaction is
            // assembled and committed.
            let index_guard = database.index_manager.mutation_lock.lock().unwrap();
            let index_snapshot: Vec<Index> = database.index_manager.indexes.read().unwrap().values().cloned().collect();

            let mut prepared_entities = Vec::with_capacity(batch.entities.len());
            let mut prepared_materializations = BTreeMap::<(EntityId, ModelId), PreparedSledMaterialization>::new();
            for (original_index, write) in batch.entities.iter().cloned().enumerate() {
                let entity_id = write.state.payload.entity_id;
                let associated: BTreeSet<ModelId> = database
                    .entity_models_tree
                    .get(entity_id.to_bytes())
                    .map_err(|error| MutationError::UpdateFailed(Box::new(error)))?
                    .map(|bytes| bincode::deserialize(bytes.as_ref()))
                    .transpose()?
                    .unwrap_or_default();
                let models: BTreeSet<ModelId> = associated.union(&write.associate_with).copied().collect();
                let (_, state_fragment) = write.state.clone().to_parts();
                for model in models {
                    let values = project_state(&database, &resolver, &model, &state_fragment)?;
                    let encoded = bincode::serialize(&values)?;
                    let tree =
                        database.db.open_tree(model_tree_name(&model)).map_err(|error| MutationError::UpdateFailed(Box::new(error)))?;
                    let indexes = index_snapshot.iter().filter(|index| index.model_id() == &model).cloned().collect();
                    prepared_materializations
                        .insert((entity_id, model), PreparedSledMaterialization { model, tree, values, encoded, indexes });
                }
                let encoded_state = bincode::serialize(&state_fragment)?;
                prepared_entities.push(PreparedSledEntity { original_index, write, encoded_state });
            }
            prepared_entities.sort_by_key(|prepared| prepared.write.state.payload.entity_id);

            let mut trees = vec![database.entities_tree.clone(), database.entity_models_tree.clone()];
            let mut tree_positions = BTreeMap::<Vec<u8>, usize>::new();
            tree_positions.insert(trees[0].name().to_vec(), 0);
            tree_positions.insert(trees[1].name().to_vec(), 1);
            for materialization in prepared_materializations.values() {
                for tree in std::iter::once(&materialization.tree).chain(materialization.indexes.iter().map(Index::tree)) {
                    let name = tree.name().to_vec();
                    if !tree_positions.contains_key(&name) {
                        let position = trees.len();
                        trees.push(tree.clone());
                        tree_positions.insert(name, position);
                    }
                }
            }

            let attempt: Result<StorageCommitResult, TransactionError<SledBatchAbort>> = trees.as_slice().transaction(|transactional| {
                let entities = &transactional[0];
                let associations = &transactional[1];
                let mut observed = BTreeMap::new();
                let mut conflict = false;
                for prepared in &prepared_entities {
                    let entity_id = prepared.write.state.payload.entity_id;
                    let key = entity_id.to_bytes();
                    let current = entities
                        .get(key)?
                        .map(|bytes| {
                            bincode::deserialize::<StateFragment>(bytes.as_ref())
                                .map(|fragment| Attested::<EntityState>::from_parts(entity_id, fragment))
                                .map_err(sled_abort)
                        })
                        .transpose()?;
                    let current_head = current.as_ref().map(|state| state.payload.state.head.clone()).unwrap_or_default();
                    if current_head != prepared.write.expected_head {
                        conflict = true;
                    }
                    observed.insert(entity_id, current);
                }
                if conflict {
                    return Err(ConflictableTransactionError::Abort(SledBatchAbort::Conflict(observed)));
                }

                let mut results = Vec::with_capacity(prepared_entities.len());
                for prepared in &prepared_entities {
                    let write = &prepared.write;
                    let entity_id = write.state.payload.entity_id;
                    let key = entity_id.to_bytes().to_vec();
                    let previous_models: BTreeSet<ModelId> = associations
                        .get(&key)?
                        .map(|bytes| bincode::deserialize(bytes.as_ref()).map_err(sled_abort))
                        .transpose()?
                        .unwrap_or_default();
                    let models: BTreeSet<ModelId> = previous_models.union(&write.associate_with).copied().collect();
                    if models.iter().any(|model| !prepared_materializations.contains_key(&(entity_id, *model))) {
                        return Err(ConflictableTransactionError::Abort(SledBatchAbort::MissingSchema));
                    }
                    associations.insert(key.clone(), bincode::serialize(&models).map_err(sled_abort)?)?;
                    entities.insert(key.clone(), prepared.encoded_state.clone())?;

                    for model in &models {
                        let materialization = prepared_materializations
                            .get(&(entity_id, *model))
                            .ok_or(ConflictableTransactionError::Abort(SledBatchAbort::MissingSchema))?;
                        debug_assert_eq!(&materialization.model, model);
                        let model_position = *tree_positions
                            .get(materialization.tree.name().as_ref())
                            .ok_or_else(|| sled_abort("prepared sled materialization tree is absent from transaction"))?;
                        let model_tree = &transactional[model_position];
                        let old_values = model_tree
                            .get(&key)?
                            .map(|bytes| bincode::deserialize::<Vec<(u32, ankurah_core::value::Value)>>(bytes.as_ref()).map_err(sled_abort))
                            .transpose()?;
                        model_tree.insert(key.clone(), materialization.encoded.clone())?;

                        for index in &materialization.indexes {
                            let old_key = old_values
                                .as_deref()
                                .map(|values| index.build_key(&entity_id, values))
                                .transpose()
                                .map_err(sled_abort)?
                                .flatten();
                            let new_key = index.build_key(&entity_id, &materialization.values).map_err(sled_abort)?;
                            if old_key == new_key {
                                continue;
                            }
                            let index_position = *tree_positions
                                .get(index.tree().name().as_ref())
                                .ok_or_else(|| sled_abort("prepared sled index tree is absent from transaction"))?;
                            let index_tree = &transactional[index_position];
                            if let Some(old_key) = old_key {
                                index_tree.remove(old_key)?;
                            }
                            if let Some(new_key) = new_key {
                                index_tree.insert(new_key, Vec::new())?;
                            }
                        }
                    }

                    results.push((
                        prepared.original_index,
                        CommittedEntityWrite {
                            entity_id,
                            canonical_changed: write.expected_head != write.state.payload.state.head,
                            associations_added: write.associate_with.difference(&previous_models).copied().collect(),
                            materialized_as: models.into_iter().collect(),
                        },
                    ));
                }
                results.sort_by_key(|(index, _)| *index);
                let entities = results.into_iter().map(|(_, result)| result).collect();
                Ok(StorageCommitResult { entities })
            });
            drop(index_guard);

            match attempt {
                Ok(result) => return Ok(CommitBatchOutcome::Committed(result)),
                Err(TransactionError::Abort(SledBatchAbort::Conflict(observed))) => return Ok(CommitBatchOutcome::Conflict { observed }),
                Err(TransactionError::Abort(SledBatchAbort::MissingSchema)) => continue,
                Err(TransactionError::Abort(SledBatchAbort::Invalid(message))) => return Err(MutationError::General(message.into())),
                Err(TransactionError::Storage(error)) => return Err(MutationError::UpdateFailed(Box::new(error))),
            }
        }
    }
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    type Value = Vec<u8>;

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { SledStorageEngine::list_models(self) }

    async fn append_events(&self, events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let database = self.database.lock().unwrap().clone();
        let encoded = events
            .iter()
            .map(|event| Ok((event.payload.id().as_bytes().to_vec(), bincode::serialize(event)?)))
            .collect::<Result<Vec<(Vec<u8>, Vec<u8>)>, MutationError>>()?;
        tokio::task::spawn_blocking(move || {
            database
                .events_tree
                .transaction(|tree| {
                    let mut inserted = Vec::with_capacity(encoded.len());
                    for (key, value) in &encoded {
                        if tree.get(key)?.is_some() {
                            inserted.push(false);
                        } else {
                            tree.insert(key.clone(), value.clone())?;
                            inserted.push(true);
                        }
                    }
                    Ok(inserted)
                })
                .map_err(|error: TransactionError<()>| match error {
                    TransactionError::Abort(()) => MutationError::General("sled event append aborted".into()),
                    TransactionError::Storage(error) => MutationError::UpdateFailed(Box::new(error)),
                })
        })
        .await?
    }

    async fn commit_batch(&self, batch: StorageWriteBatch) -> Result<CommitBatchOutcome, MutationError> {
        if batch.entities.is_empty() {
            return Ok(CommitBatchOutcome::Committed(StorageCommitResult::default()));
        }
        let database = self.database.lock().unwrap().clone();
        let resolver = self.resolver.clone();
        tokio::task::spawn_blocking(move || Self::commit_batch_blocking(database, resolver, batch)).await?
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || match database.entities_tree.get(id.to_bytes()).map_err(crate::error::sled_error)? {
            Some(bytes) => {
                let fragment: StateFragment = bincode::deserialize(bytes.as_ref())?;
                Ok(Attested::<EntityState>::from_parts(id, fragment))
            }
            None => Err(RetrievalError::EntityNotFound(id)),
        })
        .await?
    }

    async fn fetch_states(&self, model: &ModelId, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.materialization(model)?.fetch_states(selection).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            let mut events = Vec::new();
            for event_id in event_ids {
                if let Some(bytes) = database.events_tree.get(event_id.as_bytes()).map_err(SledRetrievalError::StorageError)? {
                    events.push(bincode::deserialize(bytes.as_ref())?);
                }
            }
            Ok(events)
        })
        .await?
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        tokio::task::spawn_blocking(move || {
            let mut events = Vec::new();
            for item in database.events_tree.iter() {
                let (_, bytes) = item.map_err(SledRetrievalError::StorageError)?;
                let event: Attested<Event> = bincode::deserialize(bytes.as_ref())?;
                if event.payload.entity_id == entity_id {
                    events.push(event);
                }
            }
            Ok(events)
        })
        .await?
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn delete_all(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names and drop them
        {
            let database = self.database.lock().unwrap();
            let tree_names = database.db.tree_names();

            // Drop each tree
            for name in tree_names {
                if name == "__sled__default" {
                    continue;
                }

                match database.db.drop_tree(&name) {
                    Ok(true) => any_deleted = true,
                    Ok(false) => {}
                    Err(err) => {
                        return Err(MutationError::General(Box::new(err)));
                    }
                }
            }
        }

        // Recreate the Database to ensure all tree references are fresh
        {
            let mut database_guard = self.database.lock().unwrap();
            let old_database = database_guard.clone();
            let new_database = Database::open(old_database.db.clone())
                .map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
            *database_guard = Arc::new(new_database);
        }

        Ok(any_deleted)
    }
}
