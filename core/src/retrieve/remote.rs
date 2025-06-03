use crate::collectionset::CollectionSet;
use crate::getdata::LocalGetter;
use crate::{entity::Entity, error::RetrievalError};
use crate::{entity::WeakEntitySet, node::ContextData, storage::StorageEngine};
use ankurah_proto::{CollectionId, EntityId};
use std::sync::Arc;
use tracing::{debug, warn};

use super::Fetch;

// TODO BEFORE MERGE - think about the refresh logic, where it should live, and what happens if it fails
// I think this should probably be a task queue managed by the node, because this has implications for consistency model enforcement
// it feels weird to roll that into a fetcher, because it's not really a fetch operation, it's more of a maintenance operation

/// Remote entity retriever that fetches locally, optionally waits for remote data, then detects and refreshes stale entities
pub struct RemoteFetcher<SE: StorageEngine + Send + Sync + 'static, CD: ContextData> {
    collections: CollectionSet<SE>,
    entityset: WeakEntitySet,
    subscription_relay: Arc<crate::subscription_relay::SubscriptionRelay<Entity, CD>>,
    context_data: CD,
    remote_ready_rx: Option<tokio::sync::oneshot::Receiver<Vec<EntityId>>>,
    use_cache: bool,
}

impl<SE: StorageEngine + Send + Sync + 'static, CD: ContextData> RemoteFetcher<SE, CD> {
    pub fn new(
        collections: CollectionSet<SE>,
        entityset: WeakEntitySet,
        subscription_relay: Arc<crate::subscription_relay::SubscriptionRelay<Entity, CD>>,
        context_data: CD,
        remote_ready_rx: Option<tokio::sync::oneshot::Receiver<Vec<EntityId>>>,
        use_cache: bool,
    ) -> Self {
        Self { collections, entityset, subscription_relay, context_data, remote_ready_rx, use_cache }
    }

    /// Perform stale entity detection by comparing local entities with server entity IDs
    async fn detect_and_refresh_stale_entities(
        &self,
        collection_id: &CollectionId,
        local_entities: &[Entity],
        server_entity_ids: Vec<EntityId>,
    ) -> Result<(), RetrievalError> {
        // Create HashSet from server's entity IDs for fast lookup
        let server_set: std::collections::HashSet<EntityId> = server_entity_ids.iter().copied().collect();

        // Find entities that are local but not on server (stale)
        let mut stale_entity_ids = Vec::new();
        for entity in local_entities {
            debug!("RemoteEntityRetriever: Entity {} is in local entities", entity.id);
            if server_set.contains(&entity.id) {
                debug!("RemoteEntityRetriever: Entity {} is also on server (not stale)", entity.id);
            } else {
                debug!("RemoteEntityRetriever: Entity {} is NOT on server (stale)", entity.id);
                stale_entity_ids.push(entity.id);
            }
        }

        if !stale_entity_ids.is_empty() {
            debug!("RemoteEntityRetriever: Found {} stale entities, refreshing from remote peer...", stale_entity_ids.len());

            // Get node interface from subscription relay to refresh stale entities
            if let Some(node) = self.subscription_relay.get_node() {
                node.get_from_peer(collection_id, stale_entity_ids, &self.context_data).await?;
                debug!("RemoteEntityRetriever: Successfully refreshed stale entities");
            } else {
                warn!("RemoteEntityRetriever: No node available to refresh stale entities");
            }
        } else {
            debug!("RemoteEntityRetriever: No stale entities found (server sent {} entities)", server_entity_ids.len());
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, CD: ContextData> Fetch<Entity> for RemoteFetcher<SE, CD> {
    async fn fetch(mut self: Self, collection_id: &CollectionId, predicate: &ankql::ast::Predicate) -> Result<Vec<Entity>, RetrievalError> {
        // First, do the local fetch just like LocalEntityRetriever
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

        if let Some(rx) = self.remote_ready_rx.take() {
            if self.use_cache {
                // For cached requests, spawn background task for stale detection
                debug!("RemoteEntityRetriever: Cached mode - spawning background stale detection task");
                let subscription_relay = self.subscription_relay.clone();
                let context_data = self.context_data.clone();
                let collection_id = collection_id.clone();
                let local_entity_ids: Vec<EntityId> = local_entities.iter().map(|e| e.id).collect();

                crate::task::spawn(async move {
                    match rx.await {
                        Ok(server_entity_ids) => {
                            debug!("RemoteEntityRetriever: Background task received {} entity IDs from server", server_entity_ids.len());

                            // For background mode, we only have entity IDs, not full entities
                            // So we'll find stale IDs by comparing the ID sets directly
                            let server_set: std::collections::HashSet<EntityId> = server_entity_ids.iter().copied().collect();
                            let mut stale_entity_ids = Vec::new();
                            for local_id in local_entity_ids {
                                if !server_set.contains(&local_id) {
                                    stale_entity_ids.push(local_id);
                                }
                            }

                            if !stale_entity_ids.is_empty() {
                                debug!(
                                    "RemoteEntityRetriever: Background task found {} stale entities, refreshing...",
                                    stale_entity_ids.len()
                                );
                                if let Some(node) = subscription_relay.get_node() {
                                    if let Err(e) = node.get_from_peer(&collection_id, stale_entity_ids, &context_data).await {
                                        warn!("RemoteEntityRetriever: Background stale refresh failed: {}", e);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            debug!("RemoteEntityRetriever: Background task failed to receive server entity IDs");
                        }
                    }
                });
            } else {
                // For non-cached requests, wait for remote data before returning
                match rx.await {
                    Ok(server_entity_ids) => {
                        debug!("RemoteEntityRetriever: Non-cached mode - received {} entity IDs from server", server_entity_ids.len());
                        // Perform stale entity detection and refresh synchronously
                        self.detect_and_refresh_stale_entities(collection_id, &local_entities, server_entity_ids).await?;
                    }
                    Err(_) => {
                        debug!("RemoteEntityRetriever: Failed to receive server entity IDs, proceeding with local data only");
                    }
                }
            }
        } else {
            debug!("RemoteEntityRetriever: No remote ready receiver provided");
        }

        Ok(local_entities)
    }
}
