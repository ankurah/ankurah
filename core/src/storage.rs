use ankql::ast::{Predicate, Resolved};
use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::Stream;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};

/// One raw logical record emitted by a storage dump.
#[derive(Debug)]
pub enum StorageDumpItem {
    Event(Attested<Event>),
    State(Attested<EntityState>),
}

/// Raw logical export implemented by storage engines that support portable
/// dumps. Physical tables, cursor pages, and materialized values remain an
/// implementation detail of the engine.
#[async_trait]
pub trait StorageDump: StorageEngine {
    type DumpStream: Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static;

    /// Stream events followed by states without exposing the engine's
    /// physical partitioning.
    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError>;
}

mod catalog;
pub use catalog::CatalogResolver;

mod read;
pub use read::{filter_events, GetStateResult};

/// One atomic storage transaction. Engines may execute writes as they arrive or buffer them until commit.
/// Dropping the handle without committing must leave its writes uncommitted.
#[async_trait]
pub trait StorageTransaction: Send {
    /// Write validated events with this transaction's states.
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError>;

    /// Replace an entity's state, including its memberships and materializations.
    /// `expected_head` is the state this update was derived from, including prior writes in this transaction.
    /// A missing entity has an empty head. Repeated calls for an entity replace its preceding state.
    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError>;

    /// Compare every expected head, then atomically persist events, canonical
    /// states, memberships, and materializations. Any conflict publishes none
    /// of those records and returns the observed stored states.
    async fn commit(self) -> Result<StorageCommitOutcome, MutationError>;
}

#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    // Delete all collections and their data from the storage engine
    async fn delete_all_collections(&self) -> Result<bool, MutationError>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    // Fetch raw entity states matching a selection (predicate + order by + limit)
    async fn fetch_states(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn set_states(&self, states: Vec<Attested<EntityState>>) -> Result<(), MutationError> {
        for state in states {
            self.set_state(state).await?;
        }
        Ok(())
    }

    async fn get_states(&self, ids: Vec<EntityId>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut states = Vec::new();
        for id in ids {
            match self.get_state(id).await {
                Ok(state) => states.push(state),
                Err(RetrievalError::EntityNotFound(_)) => {
                    warn!("Entity not found: {:?}", id);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(states)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError>;

    /// Retrieve a list of events
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve all events from the collection
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;
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
