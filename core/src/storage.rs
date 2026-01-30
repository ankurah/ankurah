use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::error::StorageError;
use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId};

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, StorageErrorChangeMe>;
    // Delete all collections and their data from the storage engine
    async fn delete_all_collections(&self) -> Result<bool, StorageErrorChangeMe>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, StorageErrorChangeMe>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, StorageErrorChangeMe>;

    // Fetch raw entity states matching a selection (predicate + order by + limit)
    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, StorageErrorChangeMe>;

    async fn set_states(&self, states: Vec<Attested<EntityState>>) -> Result<(), StorageErrorChangeMe> {
        for state in states {
            self.set_state(state).await?;
        }
        Ok(())
    }

    async fn get_states(&self, ids: Vec<EntityId>) -> Result<Vec<Attested<EntityState>>, StorageErrorChangeMe> {
        let mut states = Vec::new();
        for id in ids {
            match self.get_state(id).await {
                Ok(state) => states.push(state),
                Err(StorageError::EntityNotFound(_)) => {
                    warn!("Entity not found: {:?}", id);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(states)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, StorageErrorChangeMe>;

    /// Retrieve a list of events
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, StorageErrorChangeMe>;

    /// Retrieve all events from the collection
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, StorageErrorChangeMe>;
}

/// Manages the storage and state of the collection without any knowledge of the model type
#[derive(Clone)]
pub struct StorageCollectionWrapper(pub(crate) Arc<dyn StorageCollection>);

/// Storage interface for a collection
impl StorageCollectionWrapper {
    pub fn new(bucket: Arc<dyn StorageCollection>) -> Self { Self(bucket) }
}

impl std::ops::Deref for StorageCollectionWrapper {
    type Target = Arc<dyn StorageCollection>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
