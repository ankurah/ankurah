use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc, Mutex};
#[cfg(not(debug_assertions))]
use std::sync::{Arc, Mutex};

use ankurah_core::{
    error::{MutationError, RetrievalError},
    schema::CatalogResolver,
    storage::{StorageCollection, StorageEngine, SystemRootClaim},
};
use ankurah_proto::{CollectionId, SystemRootProof};
use async_trait::async_trait;
use sled::Config;

use crate::{collection::SledStorageCollection, database::Database, error::SledRetrievalError};

const ENGINE_METADATA_TREE: &str = "ankurah_engine_metadata";
const SYSTEM_ROOT_CLAIM_KEY: &[u8] = b"system_root";

fn decode_root_claim(bytes: &[u8]) -> Result<SystemRootProof, bincode::Error> { bincode::deserialize(bytes) }

pub struct SledStorageEngine {
    pub database: Mutex<Arc<Database>>,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_catalog_resolver`). Lives on the engine -- not on
    /// [`Database`] -- so a hard reset (which recreates the `Database`) keeps
    /// it; buckets get a clone at creation.
    pub(crate) resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
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
            database: Mutex::new(Arc::new(Database::open(db)?)),
            resolver: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self {
            database: Mutex::new(Arc::new(Database::open(db)?)),
            resolver: Arc::new(std::sync::RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// List all collections in the storage engine by looking for trees that start with collection_
    pub fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let database = self.database.lock().unwrap();
        let collections: Vec<CollectionId> = database
            .db
            .tree_names()
            .into_iter()
            .filter_map(|name| {
                // Convert &[u8] to String, skip if invalid UTF-8
                let name_str = String::from_utf8(name.to_vec()).ok()?;
                // Only include collections that start with collection_
                if name_str.starts_with("collection_") {
                    // Strip collection_ prefix and convert to CollectionId
                    Some(name_str.strip_prefix("collection_")?.to_string().into())
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

    /// Trait-level listing (see `StorageEngine::list_collections`); delegates
    /// to the inherent method, which reads existing sled tree names without
    /// opening (creating) any.
    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> { SledStorageEngine::list_collections(self) }

    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        // could this block for any meaningful period of time? We might consider spawn_blocking

        let database = self.database.lock().unwrap().clone();
        let collection_name = format!("collection_{id}");
        let tree = database.db.open_tree(collection_name).map_err(SledRetrievalError::StorageError)?;
        Ok(Arc::new(SledStorageCollection::new(
            id.to_owned(),
            database,
            tree,
            self.resolver.clone(),
            #[cfg(debug_assertions)]
            self.prefix_guard_disabled.clone(),
        )))
    }

    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn claim_system_root(&self, candidate: &SystemRootProof) -> Result<SystemRootClaim, MutationError> {
        let database = self.database.lock().unwrap().clone();
        let tree = database.db.open_tree(ENGINE_METADATA_TREE).map_err(|error| MutationError::General(Box::new(error)))?;
        let bytes = bincode::serialize(candidate)?;
        match tree
            .compare_and_swap(SYSTEM_ROOT_CLAIM_KEY, None::<&[u8]>, Some(bytes.as_slice()))
            .map_err(|error| MutationError::General(Box::new(error)))?
        {
            Ok(()) => {
                tree.flush_async().await.map_err(|error| MutationError::General(Box::new(error)))?;
                Ok(SystemRootClaim::Claimed)
            }
            Err(conflict) => {
                let current = conflict.current.ok_or_else(|| {
                    MutationError::General(Box::new(std::io::Error::other("system-root claim CAS conflicted without a current value")))
                })?;
                let existing = decode_root_claim(current.as_ref())?;
                Ok(SystemRootClaim::Existing(existing))
            }
        }
    }

    async fn system_root_claim(&self) -> Result<Option<SystemRootProof>, RetrievalError> {
        let database = self.database.lock().unwrap().clone();
        let tree = database.db.open_tree(ENGINE_METADATA_TREE).map_err(SledRetrievalError::StorageError)?;
        tree.get(SYSTEM_ROOT_CLAIM_KEY)
            .map_err(SledRetrievalError::StorageError)?
            .map(|bytes| decode_root_claim(bytes.as_ref()).map_err(RetrievalError::from))
            .transpose()
    }

    async fn release_system_root_claim(&self, expected: &SystemRootProof) -> Result<bool, MutationError> {
        let database = self.database.lock().unwrap().clone();
        let tree = database.db.open_tree(ENGINE_METADATA_TREE).map_err(|error| MutationError::General(Box::new(error)))?;
        let expected = bincode::serialize(expected)?;
        let released = tree
            .compare_and_swap(SYSTEM_ROOT_CLAIM_KEY, Some(expected.as_slice()), None::<&[u8]>)
            .map_err(|error| MutationError::General(Box::new(error)))?
            .is_ok();
        if released {
            tree.flush_async().await.map_err(|error| MutationError::General(Box::new(error)))?;
        }
        Ok(released)
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names and drop them
        {
            let database = self.database.lock().unwrap();
            let tree_names = database.db.tree_names();

            // Drop each tree
            for name in tree_names {
                if name == "__sled__default" || name == ENGINE_METADATA_TREE.as_bytes() {
                    continue;
                }

                match database.db.drop_tree(&name) {
                    Ok(true) => any_deleted = true,
                    Ok(false) => {}
                    Err(err) => {
                        return Err(MutationError::General(Box::new(err)));
                    }
                }
            }
        }

        // Recreate the Database to ensure all tree references are fresh
        {
            let mut database_guard = self.database.lock().unwrap();
            let old_database = database_guard.clone();
            let new_database = Database::open(old_database.db.clone())
                .map_err(|e| MutationError::General(Box::new(std::io::Error::other(e.to_string()))))?;
            *database_guard = Arc::new(new_database);
        }

        Ok(any_deleted)
    }
}
