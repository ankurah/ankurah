use ankurah_proto::{Attested, CollectionId, EntityId, Event, EventId, State};
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
    async fn set_state(&self, id: EntityId, state: &State) -> Result<bool, MutationError> {
        let tree = self.state.clone();
        let binary_state = bincode::serialize(state)?;
        let id_bytes = id.to_bytes();

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

    async fn get_state(&self, id: EntityId) -> Result<State, RetrievalError> {
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

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(EntityId, State)>, RetrievalError> {
        let predicate = predicate.clone();
        let collection_id = self.collection_id.clone();
        let copied_state = self.state.iter().collect::<Vec<_>>();

        // Use spawn_blocking for the full scan operation
        task::spawn_blocking(move || -> Result<Vec<(EntityId, State)>, RetrievalError> {
            let mut results = Vec::new();
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

                let entity_state: State = bincode::deserialize(&value_bytes)?;

                // Create entity to evaluate predicate
                let entity = TemporaryEntity::new(id, collection_id.clone(), &entity_state)?;

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

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
        // Maybe it is worthwhile for us to separate the events table into
        // `collection-entityid` names until we have indices?
        let events = self.events.clone();

        // TODO: Shorten `Event` struct to not include `id`/`collection`
        // since we can infer that based on key/tree respectively
        let binary_state = bincode::serialize(entity_event)?;

        let mut key = [0u8; 48];
        key[..16].copy_from_slice(&entity_event.payload.entity_id.to_bytes());
        key[16..32].copy_from_slice(entity_event.payload.id.as_bytes());

        // Use spawn_blocking since sled operations are not async

        let last = events.insert(key, binary_state.clone()).map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;

        if let Some(last_bytes) = last {
            Ok(last_bytes != binary_state)
        } else {
            Ok(true)
        }
    }

    async fn get_event(&self, entity_id: EntityId, event_id: EventId) -> Result<Attested<Event>, RetrievalError> {
        let mut key = [0u8; 48];
        key[..16].copy_from_slice(&entity_id.to_bytes());
        key[16..32].copy_from_slice(event_id.as_bytes());

        let event =
            self.events.get(key).map_err(|err| SledRetrievalError::StorageError(err))?.ok_or(SledRetrievalError::NotFound(entity_id))?;
        let event: Attested<Event> = bincode::deserialize(&event)?;
        Ok(event)
    }

    async fn get_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, ankurah_core::error::RetrievalError> {
        let mut events = Vec::new();

        for event_data in self.events.range(entity_id.to_bytes()..) {
            let (_key, data) = event_data.map_err(|err| SledRetrievalError::StorageError(err))?;
            let event: Attested<Event> = bincode::deserialize(&data)?;
            events.push(event);
        }

        Ok(events)
    }
}

enum SledRetrievalError {
    StorageError(sled::Error),
    NotFound(EntityId),
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
