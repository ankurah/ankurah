use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc, Mutex};
#[cfg(not(debug_assertions))]
use std::sync::{Arc, Mutex};

use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::StorageEngine,
};
use ankurah_proto::{Attested, EntityId, EntityState, Event, EventId, ModelId, StateFragment};
use async_trait::async_trait;
use sled::transaction::{TransactionError, Transactional};
use sled::Config;

use crate::{
    database::{model_from_tree_name, model_tree_name, Database, META_TREE},
    error::SledRetrievalError,
    model_store::SledModelStore,
};

mod transaction;
mod query;
pub use transaction::SledTransaction;

/// Sled implementation of the model-independent storage contract.
pub struct SledStorageEngine {
    /// Shared canonical stores, materializations, and engine metadata.
    pub database: Mutex<Arc<Database>>,
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
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// List model identities which already have durable materialization trees.
    pub fn list_models(&self) -> Result<Vec<ModelId>, RetrievalError> {
        Ok(self.database.lock().unwrap().db.tree_names().iter().filter_map(|name| model_from_tree_name(name)).collect())
    }

    fn materialization(&self, model_id: &ModelId) -> Result<Option<SledModelStore>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let Some(tree) = database.materialization(model_id)? else {
            return Ok(None);
        };
        Ok(Some(SledModelStore::new(
            *model_id,
            database,
            tree,
            #[cfg(debug_assertions)]
            self.prefix_guard_disabled.clone(),
        )))
    }
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    type Value = Vec<u8>;
    type Transaction<'a> = SledTransaction<'a>;

    fn transaction(&self) -> Self::Transaction<'_> { SledTransaction::new(self) }

    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { SledStorageEngine::list_models(self) }

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

    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<ankql::ast::Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let plan = ankurah_storage_common::materialization_plan::MaterializationPlan::new(selection);
        if let Some((model, selection)) = plan.indexed_materialization() {
            return match self.materialization(&model)? {
                Some(materialization) => materialization.fetch_states(&selection).await,
                None => Ok(Vec::new()),
            };
        }
        let database = self.database.lock().unwrap().clone();
        query::fetch(database, selection.clone()).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>, predicate: &ankql::ast::Predicate<ankql::ast::Resolved>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let events = tokio::task::spawn_blocking(move || {
            let mut events = Vec::new();
            for event_id in event_ids {
                if let Some(bytes) = database.events_tree.get(event_id.as_bytes()).map_err(SledRetrievalError::StorageError)? {
                    events.push(bincode::deserialize(bytes.as_ref())?);
                }
            }
            Ok::<_, RetrievalError>(events)
        })
        .await??;
        ankurah_core::storage::filter_events(self, events, predicate).await
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

    async fn delete_all(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names and drop them
        {
            let database = self.database.lock().unwrap();
            let tree_names = database.db.tree_names();

            // Drop each tree
            for name in tree_names {
                if name == "__sled__default" || name == META_TREE {
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
