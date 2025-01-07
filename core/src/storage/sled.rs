use ankurah_proto::{RecordState, ID};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    error::RetrievalError,
    model::RecordInner,
    storage::{StorageBucket, StorageEngine},
};

use ankql::selection::filter::evaluate_predicate;
use sled::{Config, Db};
use tokio::task;

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    pub fn with_homedir_folder(folder_name: &str) -> anyhow::Result<Self> {
        let dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?.join(folder_name);

        Self::with_path(dir)
    }

    pub fn with_path(path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dbpath = path.join("sled");
        let db = sled::open(&dbpath)?;
        Ok(Self { db })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self { db })
    }
}

pub struct SledStorageBucket {
    pub tree: sled::Tree,
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    async fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>> {
        // could this block for any meaningful period of time? We might consider spawn_blocking
        let tree = self.db.open_tree(name)?;
        Ok(Arc::new(SledStorageBucket { tree }))
    }

    async fn fetch_states(&self, bucket_name: String, predicate: &ankql::ast::Predicate) -> Result<Vec<(ID, RecordState)>, RetrievalError> {
        let tree = self.db.open_tree(&bucket_name)?;
        let bucket = SledStorageBucket { tree };

        let predicate = predicate.clone();

        // Use spawn_blocking for the full scan operation
        task::spawn_blocking(move || -> Result<Vec<(ID, RecordState)>, RetrievalError> {
            let mut results = Vec::new();
            let mut seen_ids = HashSet::new();
            // println!("SledStorageEngine: Starting fetch_states scan");

            // For now, do a full table scan
            for item in bucket.tree.iter() {
                let (key_bytes, value_bytes) = item?;
                let id = ID::from_ulid(ulid::Ulid::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?));

                // Skip if we've already seen this ID
                if seen_ids.contains(&id) {
                    println!("SledStorageEngine: Skipping duplicate record with ID: {:?}", id);
                    continue;
                }

                let record_state: RecordState = bincode::deserialize(&value_bytes)?;

                // Create record to evaluate predicate
                let record_inner = RecordInner::from_record_state(id, &bucket_name, &record_state)?;

                // Apply predicate filter
                if evaluate_predicate(&record_inner, &predicate)? {
                    // println!("SledStorageEngine: Found matching record with ID: {:?}", id);
                    seen_ids.insert(id);
                    results.push((id, record_state));
                }
            }

            // println!(
            //     "SledStorageEngine: Finished fetch_states scan, found {} matches",
            //     results.len()
            // );
            Ok(results)
        })
        .await
        .map_err(RetrievalError::future_join)?
    }
}

#[async_trait]
impl StorageBucket for SledStorageBucket {
    async fn set_record(&self, id: ID, state: &RecordState) -> anyhow::Result<bool> {
        let tree = self.tree.clone();
        let binary_state = bincode::serialize(state)?;
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        task::spawn_blocking(move || {
            let last = tree.insert(id_bytes, binary_state.clone())?;
            if let Some(last_bytes) = last {
                Ok(last_bytes != binary_state)
            } else {
                Ok(true)
            }
        })
        .await?
    }

    async fn get_record(&self, id: ID) -> Result<RecordState, RetrievalError> {
        let tree = self.tree.clone();
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        let result = task::spawn_blocking(move || -> Result<Option<sled::IVec>, sled::Error> { tree.get(id_bytes) })
            .await
            .map_err(|e| RetrievalError::StorageError(Box::new(e)))?;

        match result? {
            Some(ivec) => {
                let record_state = bincode::deserialize(&ivec)?;
                Ok(record_state)
            }
            None => Err(RetrievalError::NotFound(id)),
        }
    }
}

impl From<sled::Error> for RetrievalError {
    fn from(err: sled::Error) -> Self { RetrievalError::StorageError(Box::new(err)) }
}
