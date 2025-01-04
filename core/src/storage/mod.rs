use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RetrievalError;
use ankurah_proto::{RecordState, ID};

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
    async fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>>;

    // Fetch raw record states matching a predicate
    // TODO: Move this to the StorageBucket trait
    async fn fetch_states(
        &self,
        bucket_name: String,
        predicate: &ankql::ast::Predicate,
    ) -> Result<Vec<(ID, RecordState)>, RetrievalError>;
}

#[async_trait]
pub trait StorageBucket: Send + Sync {
    // TODO - implement merge_states based on event history.
    // Consider whether to play events forward from a prior checkpoint (probably this)
    // or maybe to require PropertyBackends to be able to merge states.
    async fn set_record(&self, id: ID, state: &RecordState) -> anyhow::Result<bool>;
    async fn get_record(&self, id: ID) -> Result<RecordState, RetrievalError>;

    async fn set_records(&self, records: Vec<(ID, &RecordState)>) -> anyhow::Result<()> {
        for (id, state) in records {
            self.set_record(id, state).await?;
        }
        Ok(())
    }

    // TODO:
    // fn add_record_event(&self, record_event: &RecordEvent) -> anyhow::Result<()>;
    // fn get_record_events(&self, id: ID) -> Result<Vec<RecordEvent>, crate::error::RetrievalError>;
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
pub struct Bucket(pub(crate) Arc<dyn StorageBucket>);

/// Storage interface for a collection
impl Bucket {
    pub fn new(bucket: Arc<dyn StorageBucket>) -> Self {
        Self(bucket)
    }
}

impl std::ops::Deref for Bucket {
    type Target = Arc<dyn StorageBucket>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
