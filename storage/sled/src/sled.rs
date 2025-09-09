use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId, StateFragment};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::warn;

use ankurah_core::{
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    storage::{StorageCollection, StorageEngine},
};

use ankql::selection::filter::evaluate_predicate;
use sled::{Config, Db};
use tokio::task;

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    pub fn with_homedir_folder(folder_name: &str) -> anyhow::Result<Self> {
        let dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?.join(folder_name);

        Self::with_path(dir)
    }

    pub fn with_path(path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dbpath = path.join("sled");
        let db = sled::open(&dbpath)?;
        Ok(Self { db })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self { db })
    }

    /// List all collections in the storage engine by looking for trees that end in _state
    pub fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let collections: Vec<CollectionId> = self
            .db
            .tree_names()
            .into_iter()
            .filter_map(|name| {
                // Convert &[u8] to String, skip if invalid UTF-8
                let name_str = String::from_utf8(name.to_vec()).ok()?;
                // Only include collections that end in _state
                if name_str.ends_with("_state") {
                    // Strip _state suffix and convert to CollectionId
                    Some(name_str.strip_suffix("_state")?.to_string().into())
                } else {
                    None
                }
            })
            .collect();
        Ok(collections)
    }
}

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

pub struct SledStorageCollection {
    pub collection_id: CollectionId,
    pub state: sled::Tree,
    pub events: sled::Tree,
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    type Value = Vec<u8>;
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        // could this block for any meaningful period of time? We might consider spawn_blocking
        let state = self.db.open_tree(state_name(id.as_str())).map_err(SledRetrievalError::StorageError)?;
        let events = self.db.open_tree(event_name(id.as_str())).map_err(SledRetrievalError::StorageError)?;
        Ok(Arc::new(SledStorageCollection { collection_id: id.to_owned(), state, events }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names
        let tree_names = self.db.tree_names();

        // Drop each tree
        for name in tree_names {
            if name == "__sled__default" {
                continue;
            }
            match self.db.drop_tree(&name) {
                Ok(true) => any_deleted = true,
                Ok(false) => {}
                Err(err) => return Err(MutationError::General(Box::new(err))),
            }
        }

        Ok(any_deleted)
    }
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let tree = self.state.clone();
        let (entity_id, _collection, sfrag) = state.to_parts();
        let binary_state = bincode::serialize(&sfrag)?;
        let id_bytes = entity_id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        task::spawn_blocking(move || {
            let last = tree.insert(id_bytes, binary_state.clone()).map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;
            if let Some(last_bytes) = last {
                Ok(last_bytes != binary_state)
            } else {
                Ok(true)
            }
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))?
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let tree = self.state.clone();
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        let result = task::spawn_blocking(move || -> Result<Option<sled::IVec>, sled::Error> { tree.get(id_bytes) })
            .await
            .map_err(|e| SledRetrievalError::Other(Box::new(e)))?;

        match result.map_err(SledRetrievalError::StorageError)? {
            Some(ivec) => {
                let sfrag: StateFragment = bincode::deserialize(&ivec)?;
                let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);
                Ok(es)
            }
            None => Err(SledRetrievalError::EntityNotFound(id).into()),
        }
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let predicate = selection.predicate.clone();
        let collection_id = self.collection_id.clone();
        let copied_state = self.state.iter().collect::<Vec<_>>();

        // Use spawn_blocking for the full scan operation
        task::spawn_blocking(move || -> Result<Vec<Attested<EntityState>>, RetrievalError> {
            let mut results: Vec<Attested<EntityState>> = Vec::new();
            let mut seen_ids = HashSet::new();

            // For now, do a full table scan
            for item in copied_state {
                let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
                let id = EntityId::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?);

                // Skip if we've already seen this ID
                if seen_ids.contains(&id) {
                    warn!("Skipping duplicate entity with ID: {:?}", id);
                    continue;
                }

                let sfrag: StateFragment = bincode::deserialize(&value_bytes)?;
                let es = Attested::<EntityState>::from_parts(id, collection_id.clone(), sfrag);

                // Create entity to evaluate predicate
                let entity = TemporaryEntity::new(id, collection_id.clone(), &es.payload.state)?;

                // Apply predicate filter
                if evaluate_predicate(&entity, &predicate)? {
                    seen_ids.insert(id);
                    results.push(es);
                }
            }

            Ok(results)
        })
        .await
        .map_err(RetrievalError::future_join)?
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        let binary_state = bincode::serialize(event)?;

        let last = self
            .events
            .insert(event.payload.id().as_bytes(), binary_state.clone())
            .map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;

        if let Some(last_bytes) = last {
            Ok(last_bytes != binary_state)
        } else {
            Ok(true)
        }
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();
        for event_id in event_ids {
            match self.events.get(event_id.as_bytes()).map_err(SledRetrievalError::StorageError)? {
                Some(event) => {
                    let event: Attested<Event> = bincode::deserialize(&event)?;
                    events.push(event);
                }
                None => continue,
            }
        }
        Ok(events)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();

        // TODO: this is a full table scan. If we actually need this for more than just tests, we should index the events by entity_id
        for event_data in self.events.iter() {
            let (_key, data) = event_data.map_err(SledRetrievalError::StorageError)?;
            let event: Attested<Event> = bincode::deserialize(&data)?;
            if event.payload.entity_id == entity_id {
                events.push(event);
            }
        }

        Ok(events)
    }
}

pub enum SledRetrievalError {
    StorageError(sled::Error),
    EntityNotFound(EntityId),
    EventNotFound(EventId),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sled::Error> for SledRetrievalError {
    fn from(val: sled::Error) -> Self { SledRetrievalError::StorageError(val) }
}

impl From<SledRetrievalError> for RetrievalError {
    fn from(err: SledRetrievalError) -> Self {
        match err {
            SledRetrievalError::StorageError(e) => RetrievalError::StorageError(Box::new(e)),
            SledRetrievalError::EntityNotFound(id) => RetrievalError::EntityNotFound(id),
            SledRetrievalError::EventNotFound(id) => RetrievalError::EventNotFound(id),
            SledRetrievalError::Other(e) => RetrievalError::StorageError(e),
        }
    }
}
