use crate::traits::{StorageCollection, StorageEngine};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// pub fn state_name(name: &str) -> String { format!("{}_state", name) }

// pub fn event_name(name: &str) -> String { format!("{}_event", name) }

/// Optional trait that allows storage operations to be scoped to a specific namespace.
/// Storage engines may implement namespace-aware storage to partition data.
pub trait Namespace {
    /// Returns the namespace for this context, if any
    fn namespace(&self) -> Option<&str>;
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
