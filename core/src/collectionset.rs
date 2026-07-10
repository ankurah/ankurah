use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use ankurah_proto::CollectionId;
use tokio::sync::RwLock;

use crate::{
    error::{MutationError, RetrievalError},
    storage::{StorageCollectionWrapper, StorageEngine},
};

pub struct CollectionSet<SE>(Arc<Inner<SE>>);

impl<SE> Clone for CollectionSet<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct Inner<SE> {
    storage_engine: Arc<SE>,
    collections: RwLock<BTreeMap<CollectionId, StorageCollectionWrapper>>,
}

impl<SE: StorageEngine> CollectionSet<SE> {
    pub fn new(storage_engine: Arc<SE>) -> Self { Self(Arc::new(Inner { storage_engine, collections: RwLock::new(BTreeMap::new()) })) }

    pub async fn get(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        // The `_ankurah_` prefix is reserved for the system and catalog
        // collections; anything else under it never gets storage.
        if id.as_str().starts_with(crate::schema::RESERVED_COLLECTION_PREFIX)
            && !crate::system::PROTECTED_COLLECTIONS.contains(&id.as_str())
        {
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
    /// [`StorageEngine::set_property_resolver`]). Called once from `Node`
    /// construction.
    pub(crate) fn set_property_resolver(&self, resolver: std::sync::Weak<dyn crate::property::PropertyResolver>) {
        self.0.storage_engine.set_property_resolver(resolver);
    }

    pub async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        // Clear in-memory collections first
        {
            let mut collections = self.0.collections.write().await;
            collections.clear();
        }

        // Then delete all collections from storage
        self.0.storage_engine.delete_all_collections().await
    }
}
