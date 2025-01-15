use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RetrievalError;
use ankurah_proto::{CollectionId, Event, State, ID};

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> anyhow::Result<Arc<dyn StorageCollection>>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    // TODO - implement merge_states based on event history.
    // Consider whether to play events forward from a prior checkpoint (probably this)
    // or maybe to require PropertyBackends to be able to merge states.
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool>;
    async fn get_state(&self, id: ID) -> Result<State, RetrievalError>;

    // Fetch raw entity states matching a predicate
    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, State)>, RetrievalError>;

    async fn set_states(&self, entities: Vec<(ID, &State)>) -> anyhow::Result<()> {
        for (id, state) in entities {
            self.set_state(id, state).await?;
        }
        Ok(())
    }

    // TODO:
    async fn add_event(&self, entity_event: &Event) -> anyhow::Result<bool>;
    async fn get_events(&self, id: ID) -> Result<Vec<Event>, crate::error::RetrievalError>;
}

#[derive(Serialize, Deserialize)]
pub enum MaterializedTag {
    String,
    Number,
}

pub enum Materialized {
    String(String),
    Number(i64),
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
