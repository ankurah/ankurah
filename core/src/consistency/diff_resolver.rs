//! This module is a crude consistency enforcement mechanism that is used to
//! compare the results between a local fetch and a remote fetch, with the understanding
//! that items which are present in the local fetch, but missing from the remote fetch
//! are likely stale.
//!
//! This is a stopgap mechanism until we implement a more sophisticated consistency enforcement mechanism
//! based on event lineage

use std::sync::Arc;

use ankurah_proto::EntityId;

use crate::Node;

use super::OnFirstSubscriptionUpdate;

/// Consistency resolver for comparing results from remote and local fetches to determine which entities might be missing events
/// and then fetching the entity states and events from the remote peer as needed to bring them up to date.
/// Items which are present in the local fetch, but missing from the remote fetch
/// are likely stale.
///
/// This is a crude stopgap mechanism until we implement a more sophisticated consistency enforcement
/// based on event lineage
#[derive(Clone)]
pub struct DiffResolver(Arc<Inner>);
struct Inner {
    cached: bool,
    // ??
}

impl DiffResolver {
    pub fn new(node: &Node<SE, CD>, cached: bool) -> Self {
        Self(Arc::new(Inner {
            // not sure if we want a WeakNode or what here
            cached,
        }))
    }
    fn remote_entity_ids(&self, entity_ids: Vec<EntityId>) {}
    fn local_entity_ids(&self, entity_ids: Vec<EntityId>) {}

    async fn wait_resolution(&self) -> bool {
        if self.0.cached {
            return false; // tell the caller that they don't need to refetch locally
        } else {
            // presumably we need some kind of internal oneshot to await the receipt of the remote and local entity id lists, the diff, and the retrieval of the stale entities

            return true; // tell the caller that they DO need to refetch locally
        }
    }
}

impl OnFirstSubscriptionUpdate for DiffResolver {
    fn on_first_update(&self, entity_ids: Vec<EntityId>) { self.remote_entity_ids(entity_ids); }
}

// TODO move the detect_and_refresh_stale_entities logic here
