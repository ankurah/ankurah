use anyhow::{anyhow, Result};
use sled::{Config, Db}; // Import Result and anyhow from the anyhow crate

pub trait StorageEngine: Sized {
    type StorageBucket: StorageBucket;
    fn collection(&self, name: &str) -> Result<Self::StorageBucket>;
}

pub trait StorageBucket: Clone {}

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

#[derive(Clone)]
pub struct SledStorageBucket {
    pub tree: sled::Tree,
}

impl StorageEngine for SledStorageEngine {
    type StorageBucket = SledStorageBucket;
    fn collection(&self, name: &str) -> Result<SledStorageBucket> {
        let tree = self.db.open_tree(name)?;
        Ok(SledStorageBucket { tree })
    }
}

impl SledStorageBucket {
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }
}

impl StorageBucket for SledStorageBucket {}
