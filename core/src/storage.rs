use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sled::{Config, Db};

use crate::types::ID;

pub trait StorageEngine: Sized {
    type StorageBucket: StorageBucket;
    fn bucket(&self, name: &str) -> Result<Self::StorageBucket>;
}

pub trait StorageBucket {
    fn set_state(&mut self, id: ID, state: RecordState) -> Result<()>;
    // fn get(&self, id: ID) -> Result<RecordState> {}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RecordState {
    pub field_states: Vec<FieldState>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FieldState {
    pub field_value: FieldValue, // is this even necessary given we know the type in the code?
    pub state: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum FieldValue {
    StringValue,
}

pub trait TypeValue {
    fn field_value() -> FieldValue;
}

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    // Open the storage engine without any specific column families
    pub fn new() -> Result<Self> {
        let dir = dirs::home_dir()
            .ok_or_else(|| anyhow!("Failed to get home directory"))?
            .join(".ankurah");

        std::fs::create_dir_all(&dir)?;

        let dbpath = dir.join("sled");

        let db = sled::open(&dbpath)?;

        Ok(Self { db })
    }
    pub fn new_test() -> Result<Self> {
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
    type StorageBucket = SledStorageBucket;
    fn bucket(&self, name: &str) -> Result<SledStorageBucket> {
        let tree = self.db.open_tree(name)?;
        Ok(SledStorageBucket { tree })
    }
}

impl SledStorageBucket {
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }
}

impl StorageBucket for SledStorageBucket {
    fn set_state(&mut self, id: ID, state: RecordState) -> Result<()> {
        let binary_state = bincode::serialize(&state)?;
        self.tree.insert(id.0.to_bytes(), binary_state)?;
        Ok(())
    }
    // fn get(&self, id: ID) -> Result<RecordState> {}
}
