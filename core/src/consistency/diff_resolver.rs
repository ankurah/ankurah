//! This module is a crude consistency enforcement mechanism that is used to
//! compare the results between a local fetch and a remote fetch, with the understanding
//! that items which are present in the local fetch, but missing from the remote fetch
//! are likely stale.
//!
//! This is a stopgap mechanism until we implement a more sophisticated consistency enforcement mechanism
//! based on event lineage

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use tracing::debug;

use crate::datagetter::DataGetter;
use crate::{context::NodeAndContext, policy::PolicyAgent, storage::StorageEngine, util::onetimevalue::OneTimeValue};
use ankurah_proto::{CollectionId, EntityId};

use super::{ConsistencyResolver, OnFirstSubscriptionUpdate};

/// Consistency resolver for comparing results from remote and local fetches to determine which entities might be missing events
/// and then fetching the entity states and events from the remote peer as needed to bring them up to date.
/// Items which are present in the local fetch, but missing from the remote fetch
/// are likely stale.
///
/// This is a crude stopgap mechanism until we implement a more sophisticated consistency enforcement
/// based on event lineage
#[derive(Clone)]
pub struct DiffResolver(Arc<DiffResolverInner>);

struct DiffResolverInner {
    node_and_context: Arc<dyn crate::context::TContext + Send + Sync + 'static>,
    collection_id: CollectionId,
    local_entity_ids: OnceLock<Vec<EntityId>>,
    remote_entity_ids: OnceLock<Vec<EntityId>>,
    resolution_result: OneTimeValue<bool>,
}

impl DiffResolver {
    pub fn new<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        D: DataGetter<PA::ContextData> + Send + Sync + 'static,
    >(
        node_and_context: &NodeAndContext<SE, PA, D>,
        collection_id: CollectionId,
    ) -> Self {
        Self(Arc::new(DiffResolverInner {
            node_and_context: Arc::new((*node_and_context).clone()) as Arc<dyn crate::context::TContext + Send + Sync + 'static>,
            collection_id,
            local_entity_ids: OnceLock::new(),
            remote_entity_ids: OnceLock::new(),
            resolution_result: OneTimeValue::new(),
        }))
    }

    /// Called by LocalRefetcher to register the local entity IDs
    pub fn local_entity_ids(&self, entity_ids: Vec<EntityId>) {
        // Only set if not already set (first one wins)
        if self.0.local_entity_ids.set(entity_ids).is_ok() {
            self.try_resolve();
        }
    }

    /// Called when the first subscription update arrives with remote entity IDs
    pub fn remote_entity_ids(&self, entity_ids: Vec<EntityId>) {
        // Only set if not already set (first one wins)
        if self.0.remote_entity_ids.set(entity_ids).is_ok() {
            self.try_resolve();
        }
    }

    /// Try to resolve if both local and remote entity IDs are available
    /// Only spawns async work if differences are detected
    fn try_resolve(&self) {
        // Check if both are available
        if let (Some(local_ids), Some(remote_ids)) = (self.0.local_entity_ids.get(), self.0.remote_entity_ids.get()) {
            // Build list of entities missing from remote set
            let remote_set: HashSet<EntityId> = remote_ids.iter().copied().collect();
            let missing_from_remote: Vec<EntityId> = local_ids.iter().filter(|local_id| !remote_set.contains(local_id)).copied().collect();

            if !missing_from_remote.is_empty() {
                debug!("DiffResolver: Found {} entities missing from remote, fetching...", missing_from_remote.len());

                // Spawn async task to fetch missing entities
                let resolver = self.clone();
                crate::task::spawn(async move {
                    let result = resolver.fetch_entities_from_peer(missing_from_remote).await;
                    let _ = resolver.0.resolution_result.set(result);
                });
            } else {
                debug!("DiffResolver: No differences detected between local and remote entity sets");
                // No differences, resolve immediately
                let _ = self.0.resolution_result.set(false);
            }
        }
    }

    /// Wait for remote entity IDs to arrive and resolution to complete
    /// Returns true if differences were detected and entities were refreshed
    /// Can be called multiple times safely
    pub async fn wait_resolution(&self) -> bool { self.0.resolution_result.wait().await }

    /// Fetch entities from remote peer to refresh stale data
    async fn fetch_entities_from_peer(&self, entity_ids: Vec<EntityId>) -> bool {
        debug!("DiffResolver: Fetching {} entities from remote peer...", entity_ids.len());

        // For now, we'll return true to indicate that differences were detected
        // The actual refresh logic would need access to the node's get_from_peer method
        // which we could add by storing the Node reference or implementing a refresh trait
        debug!("DiffResolver: Differences detected, refresh would be needed");
        true
    }
}

impl OnFirstSubscriptionUpdate for DiffResolver {
    fn on_first_update(&self, entity_ids: Vec<EntityId>) { self.remote_entity_ids(entity_ids); }
}

impl ConsistencyResolver for DiffResolver {}
