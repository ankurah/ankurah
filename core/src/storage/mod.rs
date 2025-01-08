use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RetrievalError;
use ankurah_proto::{State, ID};

#[cfg(all(feature = "postgres", not(target_arch = "wasm32")))]
mod postgres;
#[cfg(not(target_arch = "wasm32"))]
pub mod sled;

#[cfg(all(feature = "postgres", not(target_arch = "wasm32")))]
pub use postgres::Postgres;
#[cfg(not(target_arch = "wasm32"))]
pub use sled::SledStorageEngine;

#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Opens and/or creates a storage bucket.
    async fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageCollection>>;

    // Fetch raw entity states matching a predicate
    // TODO: Move this to the StorageCollection trait
    async fn fetch_states(&self, collection: String, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, State)>, RetrievalError>;
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    // TODO - implement merge_states based on event history.
    // Consider whether to play events forward from a prior checkpoint (probably this)
    // or maybe to require PropertyBackends to be able to merge states.
    async fn set_state(&self, id: ID, state: &State) -> anyhow::Result<bool>;
    async fn get_state(&self, id: ID) -> Result<State, RetrievalError>;

    async fn set_states(&self, entities: Vec<(ID, &State)>) -> anyhow::Result<()> {
        for (id, state) in entities {
            self.set_state(id, state).await?;
        }
        Ok(())
    }

    // TODO:
    // fn add_event(&self, entity_event: &Event) -> anyhow::Result<()>;
    // fn get_events(&self, id: ID) -> Result<Vec<Event>, crate::error::RetrievalError>;
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
