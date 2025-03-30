use ankurah_proto::{CollectionId, Event, State, ID};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::warn;

use ankurah_core::{
    entity::Entity,
    error::RetrievalError,
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
        let state = self.db.open_tree(state_name(id.as_str())).map_err(|err| SledRetrievalError::StorageError(err))?;
        let events = self.db.open_tree(event_name(id.as_str())).map_err(|err| SledRetrievalError::StorageError(err))?;
        Ok(Arc::new(SledStorageCollection { collection_id: id.to_owned(), state, events }))
    }
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool> {
        let tree = self.state.clone();
        let binary_state = bincode::serialize(state)?;
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        task::spawn_blocking(move || {
            let last = tree.insert(id_bytes, binary_state.clone())?;
            if let Some(last_bytes) = last {
                Ok(last_bytes != binary_state)
            } else {
                Ok(true)
            }
        })
        .await?
    }

    async fn get_state(&self, id: ID) -> Result<State, RetrievalError> {
        let tree = self.state.clone();
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        let result = task::spawn_blocking(move || -> Result<Option<sled::IVec>, sled::Error> { tree.get(id_bytes) })
            .await
            .map_err(|e| SledRetrievalError::Other(Box::new(e)))?;

        match result.map_err(SledRetrievalError::StorageError)? {
            Some(ivec) => {
                let entity_state = bincode::deserialize(&ivec)?;
                Ok(entity_state)
            }
            None => Err(SledRetrievalError::NotFound(id).into()),
        }
    }

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, State)>, RetrievalError> {
        let predicate = predicate.clone();
        let collection_id = self.collection_id.clone();
        let copied_state = self.state.iter().collect::<Vec<_>>();

        // Use spawn_blocking for the full scan operation
        task::spawn_blocking(move || -> Result<Vec<(ID, State)>, RetrievalError> {
            let mut results = Vec::new();
            let mut seen_ids = HashSet::new();

            // For now, do a full table scan
            for item in copied_state {
                let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
                let id = ID::from_ulid(ulid::Ulid::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?));

                // Skip if we've already seen this ID
                if seen_ids.contains(&id) {
                    warn!("Skipping duplicate entity with ID: {:?}", id);
                    continue;
                }

                let entity_state: State = bincode::deserialize(&value_bytes)?;

                // Create entity to evaluate predicate
                let entity = Entity::from_state(id, collection_id.clone(), &entity_state)?;

                // Apply predicate filter
                if evaluate_predicate(&entity, &predicate)? {
                    seen_ids.insert(id);
                    results.push((id, entity_state));
                }
            }

            Ok(results)
        })
        .await
        .map_err(RetrievalError::future_join)?
    }

    async fn add_event(&self, entity_event: &Event) -> anyhow::Result<bool> {
        // Maybe it is worthwhile for us to separate the events table into
        // `collection-entityid` names until we have indices?
        let events = self.events.clone();

        // TODO: Shorten `Event` struct to not include `id`/`collection`
        // since we can infer that based on key/tree respectively
        let binary_state = bincode::serialize(entity_event)?;
        let id_bytes = entity_event.id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        task::spawn_blocking(move || {
            let last = events.insert(id_bytes, binary_state.clone())?;
            if let Some(last_bytes) = last {
                Ok(last_bytes != binary_state)
            } else {
                Ok(true)
            }
        })
        .await?
    }

    async fn get_events(&self, entity_id: ID) -> Result<Vec<Event>, ankurah_core::error::RetrievalError> {
        let mut matching_events = Vec::new();

        // full events table scan searching for matching entity ids
        for event_data in self.events.iter() {
            let (_key, data) = event_data.map_err(|err| SledRetrievalError::StorageError(err))?;
            let event: Event = bincode::deserialize(&data)?;
            if entity_id == event.entity_id {
                matching_events.push(event);
            }
        }

        Ok(matching_events)
    }
}

enum SledRetrievalError {
    StorageError(sled::Error),
    NotFound(ID),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Into<SledRetrievalError> for sled::Error {
    fn into(self) -> SledRetrievalError { SledRetrievalError::StorageError(self) }
}

impl From<SledRetrievalError> for RetrievalError {
    fn from(err: SledRetrievalError) -> Self {
        match err {
            SledRetrievalError::StorageError(e) => RetrievalError::StorageError(Box::new(e)),
            SledRetrievalError::NotFound(id) => RetrievalError::NotFound(id),
            SledRetrievalError::Other(e) => RetrievalError::StorageError(e),
        }
    }
}
