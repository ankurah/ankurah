use std::{collections::BTreeMap, sync::Arc};

use ankurah_proto::CollectionId;

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
    collections: std::sync::RwLock<BTreeMap<CollectionId, StorageCollectionWrapper>>,
}

impl<SE: StorageEngine> CollectionSet<SE> {
    pub fn new(storage_engine: Arc<SE>) -> Self {
        Self(Arc::new(Inner { storage_engine, collections: std::sync::RwLock::new(BTreeMap::new()) }))
    }

    pub async fn get(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        {
            if let Some(store) = self.0.collections.read().unwrap().get(id) {
                return Ok(store.clone());
            }
        }

        let collection = StorageCollectionWrapper::new(self.0.storage_engine.collection(id).await?);

        {
            // We might have raced with another node to create this collection
            let mut collections = self.0.collections.write().unwrap();
            Ok(collections.entry(id.clone()).or_insert_with(|| collection.clone()).clone())
        }
    }
}
