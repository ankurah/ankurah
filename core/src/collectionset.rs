use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{atomic::Ordering, Arc},
};

use ankurah_proto::{CollectionId, SystemRootProof};
use tokio::sync::RwLock;

use crate::{
    error::{MutationError, RetrievalError},
    storage::{StorageCollectionWrapper, StorageEngine, SystemRootClaim},
    storage_fence::{shared_storage_fence, StorageFence},
};

pub struct CollectionSet<SE>(Arc<Inner<SE>>);

impl<SE> Clone for CollectionSet<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct Inner<SE> {
    storage_engine: Arc<SE>,
    collections: RwLock<BTreeMap<CollectionId, StorageCollectionWrapper>>,
    storage_fence: Arc<StorageFence>,
}

impl<SE: StorageEngine> CollectionSet<SE> {
    pub fn new(storage_engine: Arc<SE>) -> Self {
        let storage_fence = shared_storage_fence(&storage_engine);
        Self(Arc::new(Inner { storage_engine, collections: RwLock::new(BTreeMap::new()), storage_fence }))
    }

    pub(crate) fn storage_epoch(&self) -> u64 { self.0.storage_fence.epoch.load(Ordering::Acquire) }

    pub(crate) fn storage_generation(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.0.storage_fence.generation.read().unwrap().clone()
    }

    pub(crate) async fn storage_read_lease(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.0.storage_fence.gate.clone().read_owned().await
    }

    pub(crate) async fn storage_write_lease(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.0.storage_fence.gate.clone().write_owned().await
    }

    pub(crate) fn storage_epoch_notify(&self) -> Arc<tokio::sync::Notify> { self.0.storage_fence.epoch_changed.clone() }

    /// Register this Node's sibling-teardown callback. It is invoked
    /// synchronously whenever a DIFFERENT manager sharing this engine
    /// advances the storage epoch: the shared generation token already
    /// fences this node's operations, but its physically open transports and
    /// pending request oneshots would otherwise linger until a timeout that
    /// never comes. The caller keeps the Arc alive (the fence holds a Weak).
    pub(crate) fn set_epoch_participant(&self, callback: &Arc<dyn Fn() + Send + Sync>) {
        let key = Arc::as_ptr(&self.0).cast::<()>() as usize;
        let mut participants = self.0.storage_fence.participants.lock().unwrap();
        participants.retain(|(_, existing)| existing.strong_count() > 0);
        participants.push((key, Arc::downgrade(callback)));
    }

    /// Advance only while holding the shared write lease.
    pub(crate) fn advance_storage_epoch(&self) -> (u64, Arc<std::sync::atomic::AtomicBool>) {
        let fresh_generation = {
            let mut generation = self.0.storage_fence.generation.write().unwrap();
            generation.store(false, Ordering::Release);
            let fresh = Arc::new(std::sync::atomic::AtomicBool::new(true));
            *generation = fresh.clone();
            fresh
        };
        let epoch = self.0.storage_fence.epoch.fetch_add(1, Ordering::AcqRel) + 1;

        // Tear down every SIBLING Node sharing this engine, synchronously
        // under the exclusive writer: close their peer transports and wake
        // their pending request oneshots with a typed connection error. The
        // advancing manager's own teardown runs inside its invalidation
        // (where a pending first-join reservation may be preserved).
        let own_key = Arc::as_ptr(&self.0).cast::<()>() as usize;
        let callbacks: Vec<Arc<dyn Fn() + Send + Sync>> = {
            let mut participants = self.0.storage_fence.participants.lock().unwrap();
            participants.retain(|(_, callback)| callback.strong_count() > 0);
            participants.iter().filter(|(key, _)| *key != own_key).filter_map(|(_, callback)| callback.upgrade()).collect()
        };
        for callback in callbacks {
            callback();
        }

        (epoch, fresh_generation)
    }

    /// Publish after the resetting manager has rebound itself to the fresh
    /// generation, so its own readiness waiter cannot mistake the brief
    /// epoch/binding handoff for permanent sibling staleness.
    pub(crate) fn publish_storage_epoch(&self) { self.0.storage_fence.epoch_changed.notify_waiters(); }

    pub async fn get(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        // The `_ankurah_` prefix is reserved for the system and catalog
        // collections; anything else under it never gets storage.
        if id.as_str().starts_with(crate::schema::RESERVED_COLLECTION_PREFIX) && !crate::schema::is_protected_collection(id) {
            return Err(RetrievalError::Other(format!(
                "collection id '{id}' uses the reserved prefix '{}'",
                crate::schema::RESERVED_COLLECTION_PREFIX
            )));
        }
        let collections = self.0.collections.read().await;
        if let Some(store) = collections.get(id) {
            return Ok(store.clone());
        }
        drop(collections);

        let collection = StorageCollectionWrapper::new(self.0.storage_engine.collection(id).await?);

        let mut collections = self.0.collections.write().await;

        // We might have raced with another caller to create this collection.
        // Whoever wins the map slot owns the canonical bucket and its durable
        // column-map cache; every caller must return that shared bucket, not
        // its own just-built duplicate, or the two buckets' caches diverge.
        let canonical = match collections.entry(id.clone()) {
            Entry::Vacant(entry) => entry.insert(collection).clone(),
            Entry::Occupied(entry) => entry.get().clone(),
        };
        drop(collections);

        Ok(canonical)
    }

    pub async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        // Just return collections we have in memory
        let memory_collections = self.0.collections.read().await;
        Ok(memory_collections.keys().cloned().collect())
    }

    /// Collections that already have durable storage, per the engine, WITHOUT
    /// creating any (unlike `get`). The catalog manager uses this to warm
    /// only the catalog collections that exist, so a schema-less node never
    /// materializes empty `_ankurah_*` trees.
    pub async fn engine_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> { self.0.storage_engine.list_collections().await }

    /// Forward the catalog resolver to the engine (see
    /// [`StorageEngine::set_catalog_resolver`]). Called once from `Node`
    /// construction.
    pub(crate) fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) {
        self.0.storage_engine.set_catalog_resolver(resolver);
    }

    pub(crate) async fn claim_system_root(&self, candidate: &SystemRootProof) -> Result<SystemRootClaim, MutationError> {
        self.0.storage_engine.claim_system_root(candidate).await
    }

    pub(crate) async fn system_root_claim(&self) -> Result<Option<SystemRootProof>, RetrievalError> {
        self.0.storage_engine.system_root_claim().await
    }

    pub async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        // Keep the complete proposal as a cross-instance fence while data is
        // deleted. Engines deliberately preserve metadata during their raw
        // collection wipe; afterward remove only the exact proposal observed
        // before deletion. An unconditional second clear could erase a new
        // winner installed between the two operations.
        let claim = self.0.storage_engine.system_root_claim().await?;

        // Clear in-memory collections first
        {
            let mut collections = self.0.collections.write().await;
            collections.clear();
        }

        // Then delete all collections from storage
        let deleted = self.0.storage_engine.delete_all_collections().await?;
        match claim {
            Some(proof) => {
                if !self.0.storage_engine.release_system_root_claim(&proof).await? {
                    return Err(MutationError::General(Box::new(std::io::Error::other("system-root claim changed during storage reset"))));
                }
            }
            None => {
                if self.0.storage_engine.system_root_claim().await?.is_some() {
                    return Err(MutationError::General(Box::new(std::io::Error::other(
                        "system-root claim appeared during unfenced storage reset",
                    ))));
                }
            }
        }
        Ok(deleted)
    }
}
