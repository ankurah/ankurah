use ankurah_proto::CollectionId;

use crate::{
    collectionset::CollectionSet,
    entity::{Entity, WeakEntitySet},
    error::RetrievalError,
    getdata::LocalGetter,
    storage::StorageEngine,
};

use super::Fetch;

/// Local entity retriever that only fetches from local storage (for durable nodes)
pub struct LocalFetcher<SE: StorageEngine + Send + Sync + 'static> {
    collections: CollectionSet<SE>,
    entityset: WeakEntitySet,
}

impl<SE: StorageEngine + Send + Sync + 'static> LocalFetcher<SE> {
    pub fn new(collections: CollectionSet<SE>, entityset: WeakEntitySet) -> Self { Self { collections, entityset } }
}

#[async_trait::async_trait]
impl<SE: StorageEngine + Send + Sync + 'static> Fetch<Entity> for LocalFetcher<SE> {
    async fn fetch(self: Self, collection_id: &CollectionId, predicate: &ankql::ast::Predicate) -> Result<Vec<Entity>, RetrievalError> {
        let storage_collection = self.collections.get(collection_id).await?;
        let matching_states = storage_collection.fetch_states(predicate).await?;
        let retriever = LocalGetter::new(storage_collection.clone());

        let mut entities = Vec::new();
        for state in matching_states {
            let (_, entity) = self
                .entityset
                .with_state(&retriever, state.payload.entity_id, collection_id.clone(), &state.payload.state)
                .await
                .map_err(|e| RetrievalError::Other(format!("Failed to process entity state: {}", e)))?;
            entities.push(entity);
        }

        Ok(entities)
    }
}
