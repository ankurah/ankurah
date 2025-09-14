use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc};

use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::CollectionId;
use async_trait::async_trait;
use sled::Config;

use crate::{collection::SledStorageCollection, database::Database, error::SledRetrievalError};

pub struct SledStorageEngine {
    pub database: Arc<Database>,
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

#[cfg(debug_assertions)]
impl SledStorageEngine {
    pub fn set_prefix_guard_disabled(&self, disabled: bool) {
        use std::sync::atomic::Ordering;
        self.prefix_guard_disabled.store(disabled, Ordering::Relaxed);
    }
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
        Ok(Self {
            database: Arc::new(Database::open(db)?),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self {
            database: Arc::new(Database::open(db)?),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// List all collections in the storage engine by looking for trees that end in _state
    pub fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let collections: Vec<CollectionId> = self
            .database
            .db
            .tree_names()
            .into_iter()
            .filter_map(|name| {
                // Convert &[u8] to String, skip if invalid UTF-8
                let name_str = String::from_utf8(name.to_vec()).ok()?;
                // Only include collections that end in _state
                if name_str.ends_with("_state") {
                    // Strip _state suffix and convert to CollectionId
                    Some(name_str.strip_suffix("_state")?.to_string().into())
                } else {
                    None
                }
            })
            .collect();
        Ok(collections)
    }
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    type Value = Vec<u8>;
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        // could this block for any meaningful period of time? We might consider spawn_blocking

        let collection_name = format!("collection_{id}");
        let tree = self.database.db.open_tree(collection_name).map_err(SledRetrievalError::StorageError)?;
        Ok(Arc::new(SledStorageCollection {
            collection_id: id.to_owned(),
            database: self.database.clone(),
            tree,
            #[cfg(debug_assertions)]
            prefix_guard_disabled: self.prefix_guard_disabled.clone(),
        }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names
        let tree_names = self.database.db.tree_names();

        // Drop each tree
        for name in tree_names {
            if name == "__sled__default" {
                continue;
            }
            match self.database.db.drop_tree(&name) {
                Ok(true) => any_deleted = true,
                Ok(false) => {}
                Err(err) => return Err(MutationError::General(Box::new(err))),
            }
        }

        Ok(any_deleted)
    }
}
