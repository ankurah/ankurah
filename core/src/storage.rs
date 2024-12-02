use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use sled::{Config, Db};

use crate::{model::ID, property::Backends};

pub trait StorageEngine: Send + Sync {
    // Opens and/or creates a storage bucket.
    fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>>;
}

pub trait StorageBucket: Send + Sync {
    fn set_record_state(&self, id: ID, state: &RecordState) -> anyhow::Result<()>;
    fn get_record_state(&self, id: ID) -> Result<RecordState, crate::error::RetrievalError>;

    // TODO:
    // fn add_record_event(&self, record_event: &RecordEvent) -> anyhow::Result<()>;
    // fn get_record_events(&self, id: ID) -> Result<Vec<RecordEvent>, crate::error::RetrievalError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordState {
    pub(crate) state_buffers: BTreeMap<String, Vec<u8>>,
}

impl RecordState {
    pub fn from_backends(backends: &Backends) -> anyhow::Result<Self> {
        backends.to_state_buffers()
    }
}

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> {
        let dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?
            .join(".ankurah");

        std::fs::create_dir_all(&dir)?;

        let dbpath = dir.join("sled");

        let db = sled::open(&dbpath)?;

        Ok(Self { db })
    }
    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        Ok(Self { db })
    }
}

pub struct SledStorageBucket {
    pub tree: sled::Tree,
}

impl StorageEngine for SledStorageEngine {
    fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>> {
        let tree = self.db.open_tree(name)?;
        Ok(Arc::new(SledStorageBucket { tree }))
    }
}

impl SledStorageBucket {
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }
}

impl StorageBucket for SledStorageBucket {
    fn set_record_state(&self, id: ID, state: &RecordState) -> anyhow::Result<()> {
        let binary_state = bincode::serialize(state)?;
        self.tree.insert(id.0.to_bytes(), binary_state)?;
        Ok(())
    }
    fn get_record_state(&self, id: ID) -> Result<RecordState, crate::error::RetrievalError> {
        match self
            .tree
            .get(id.0.to_bytes())
            .map_err(|e| crate::error::RetrievalError::StorageError(Box::new(e)))?
        {
            Some(ivec) => {
                let record_state = bincode::deserialize(&*ivec)?;
                Ok(record_state)
            }
            None => {
                //Ok(RecordState { field_states: Vec::new() });
                //Err(format!("Missing Ivec for id"))
                Err(crate::error::RetrievalError::NotFound(id))
            }
        }
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
