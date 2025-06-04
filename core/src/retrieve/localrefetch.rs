use crate::collectionset::CollectionSet;
use crate::consistency::diff_resolver::DiffResolver;
use crate::getdata::LocalGetter;
use crate::{entity::Entity, error::RetrievalError};
use crate::{entity::EntityManager, storage::StorageEngine};
use ankurah_proto::{CollectionId, EntityId};
use tracing::debug;

use super::Fetch;

/// Local entity refetcher that:
/// 1. Fetches entities from local storage
/// 2. Calls resolver.local_entity_ids() to register the local IDs
/// 3. If use_cache=true, immediately returns the local entities (cached behavior)
/// 4. If use_cache=false, waits for consistency resolution and conditionally refetches if differences were detected
pub struct LocalRefetcher<SE: StorageEngine + Send + Sync + 'static> {
    collections: CollectionSet<SE>,
    entityset: EntityManager,
    resolver: DiffResolver,
    use_cache: bool,
}

impl<SE: StorageEngine + Send + Sync + 'static> LocalRefetcher<SE> {
    pub fn new(collections: CollectionSet<SE>, entityset: EntityManager, resolver: DiffResolver, use_cache: bool) -> Self {
        Self { collections, entityset, resolver, use_cache }
    }

    /// Fetch entities from local storage
    async fn fetch_local_entities(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let storage_collection = self.collections.get(collection_id).await?;
        let matching_states = storage_collection.fetch_states(predicate).await?;
        let localgetter = LocalGetter::new(storage_collection.clone());

        let mut local_entities = Vec::new();
        for state in matching_states {
            let (_, entity) = self
                .entityset
                .with_state(&localgetter, state.payload.entity_id, collection_id.clone(), &state.payload.state)
                .await
                .map_err(|e| RetrievalError::Other(format!("Failed to process entity state: {}", e)))?;
            local_entities.push(entity);
        }

        Ok(local_entities)
    }
}

#[async_trait::async_trait]
impl<SE: StorageEngine + Send + Sync + 'static> Fetch<Entity> for LocalRefetcher<SE> {
    async fn fetch(self: Self, collection_id: &CollectionId, predicate: &ankql::ast::Predicate) -> Result<Vec<Entity>, RetrievalError> {
        // First, fetch from local storage
        let local_entities = self.fetch_local_entities(collection_id, predicate).await?;
        debug!("LocalRefetcher: Fetched {} entities from local storage", local_entities.len());

        // Register local entity IDs with the resolver
        let local_entity_ids: Vec<EntityId> = local_entities.iter().map(|e| e.id).collect();
        self.resolver.local_entity_ids(local_entity_ids);

        if self.use_cache {
            // For cached mode, return immediately without waiting for consistency resolution
            debug!("LocalRefetcher: Cached mode - returning local entities immediately");
            Ok(local_entities)
        } else {
            // For non-cached mode, wait for consistency resolution
            debug!("LocalRefetcher: Non-cached mode - waiting for consistency resolution");
            let differences_detected = self.resolver.wait_resolution().await;

            if differences_detected {
                debug!("LocalRefetcher: Differences detected, refetching from local storage");
                // Refetch from local storage as entities may have been updated during resolution
                self.fetch_local_entities(collection_id, predicate).await
            } else {
                debug!("LocalRefetcher: No differences detected, returning initial entities");
                Ok(local_entities)
            }
        }
    }
}
