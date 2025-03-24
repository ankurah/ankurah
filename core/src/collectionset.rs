use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use ankurah_proto::CollectionId;
use tokio::sync::RwLock;

use crate::{
    error::RetrievalError,
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
        let collections = self.0.collections.read().await;
        if let Some(store) = collections.get(id) {
            return Ok(store.clone());
        }
        drop(collections);

        let collection = StorageCollectionWrapper::new(self.0.storage_engine.collection(id).await?);

        let mut collections = self.0.collections.write().await;

        // We might have raced with another node to create this collection
        if let Entry::Vacant(entry) = collections.entry(id.clone()) {
            entry.insert(collection.clone());
        }
        drop(collections);

        Ok(collection)
    }
}
