use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    error::RetrievalError,
    model::{Record, ID},
    property::Backends,
};

#[cfg(feature = "postgres")]
mod postgres;
pub mod sled;
#[cfg(feature = "postgres")]
pub use postgres::Postgres;
pub use sled::SledStorageEngine;

#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Opens and/or creates a storage bucket.
    fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>>;

    // Fetch raw record states matching a predicate
    async fn fetch_states(
        &self,
        bucket_name: &'static str,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<Vec<(ID, RecordState)>>;
}

#[async_trait]
pub trait StorageBucket: Send + Sync {
    async fn set_record(&self, id: ID, state: &RecordState) -> anyhow::Result<()>;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordState {
    pub state_buffers: BTreeMap<String, Vec<u8>>,
}

impl RecordState {
    pub fn from_backends(backends: &Backends) -> anyhow::Result<Self> {
        backends.to_state_buffers()
    }
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
