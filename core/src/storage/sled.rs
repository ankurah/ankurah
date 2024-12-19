use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    model::{RecordInner, ID},
    storage::{RecordState, StorageBucket, StorageEngine},
};

use ankql::selection::filter::evaluate_predicate;
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

    fn fetch_states(
        &self,
        bucket_name: &'static str,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<Vec<(ID, RecordState)>> {
        let tree = self.db.open_tree(bucket_name)?;
        let bucket = SledStorageBucket { tree };

        let mut results = Vec::new();
        let mut seen_ids = HashSet::new();
        println!("SledStorageEngine: Starting fetch_states scan");

        // For now, do a full table scan
        for item in bucket.tree.iter() {
            let (key_bytes, value_bytes) = item?;
            let id = ID(ulid::Ulid::from_bytes(key_bytes.as_ref().try_into()?));

            // Skip if we've already seen this ID
            if seen_ids.contains(&id) {
                println!(
                    "SledStorageEngine: Skipping duplicate record with ID: {:?}",
                    id
                );
                continue;
            }

            let record_state: RecordState = bincode::deserialize(&value_bytes)?;

            // Create record to evaluate predicate
            let record_inner = RecordInner::from_record_state(id, bucket_name, &record_state)?;

            // Apply predicate filter
            if evaluate_predicate(&record_inner, predicate)? {
                println!("SledStorageEngine: Found matching record with ID: {:?}", id);
                seen_ids.insert(id);
                results.push((id, record_state));
            }
        }

        println!(
            "SledStorageEngine: Finished fetch_states scan, found {} matches",
            results.len()
        );
        Ok(results)
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
                let record_state = bincode::deserialize(&ivec)?;
                Ok(record_state)
            }
            None => Err(crate::error::RetrievalError::NotFound(id)),
        }
    }
}
