use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, CollectionId, EntityID, Event, EventID, State};

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    // async fn delete_collection(&self, id: &CollectionId) -> Result<(), RetrievalError>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    // TODO - implement merge_states based on event history.
    // Consider whether to play events forward from a prior checkpoint (probably this)
    // or maybe to require PropertyBackends to be able to merge states.
    async fn set_state(&self, id: EntityID, state: &State) -> Result<bool, MutationError>;
    async fn get_state(&self, id: EntityID) -> Result<State, RetrievalError>;

    // Fetch raw entity states matching a predicate
    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(EntityID, State)>, RetrievalError>;

    async fn set_states(&self, entities: Vec<(EntityID, &State)>) -> Result<(), MutationError> {
        for (id, state) in entities {
            self.set_state(id, state).await?;
        }
        Ok(())
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError>;

    async fn get_event(&self, id: EntityID, event_id: EventID) -> Result<Attested<Event>, RetrievalError>;
    async fn get_events(&self, id: EntityID) -> Result<Vec<Attested<Event>>, RetrievalError>;
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
