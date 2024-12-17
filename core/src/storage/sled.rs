use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    model::ID,
    storage::{RecordState, StorageBucket, StorageEngine},
};

use sled::{Config, Db};

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    pub fn with_homedir_folder(folder_name: &str) -> anyhow::Result<Self> {
        let dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?
            .join(folder_name);

        Self::with_path(dir)
    }

    pub fn with_path(path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dbpath = path.join("sled");
        let db = sled::open(&dbpath)?;
        Ok(Self { db })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> {
        Self::with_homedir_folder(".ankurah")
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
    fn set_record(&self, id: ID, state: &RecordState) -> anyhow::Result<()> {
        let binary_state = bincode::serialize(state)?;
        self.tree.insert(id.0.to_bytes(), binary_state)?;
        Ok(())
    }
    fn get_record(&self, id: ID) -> Result<RecordState, crate::error::RetrievalError> {
        match self
            .tree
            .get(id.0.to_bytes())
            .map_err(|e| crate::error::RetrievalError::StorageError(Box::new(e)))?
        {
            Some(ivec) => {
                let record_state = bincode::deserialize(&*ivec)?;
                Ok(record_state)
            }
            None => Err(crate::error::RetrievalError::NotFound(id)),
        }
    }
}
