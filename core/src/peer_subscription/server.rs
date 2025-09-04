use ankurah_proto::{self as proto, Attested};
use tracing::{debug, warn};

use crate::{
    node::Node,
    policy::PolicyAgent,
    reactor::{ReactorSubscription, ReactorUpdate},
    retrieval::LocalRetriever,
    storage::StorageEngine,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};

/// Manages a peer's subscription to this node's reactor.
///
/// This handler owns both the ReactorSubscription and the SubscriptionGuard
/// for listening to changes on that subscription.
pub struct SubscriptionHandler {
    peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    guard: SubscriptionGuard,
}

impl SubscriptionHandler {
    pub fn new<SE, PA>(peer_id: proto::EntityId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();

        // Subscribe to changes on this subscription
        let guard = subscription.subscribe(move |update: ReactorUpdate| {
            tracing::info!(
                "SubscriptionHandler[{}] received reactor update with {} items, initialized_predicate: {:?}",
                peer_id,
                update.items.len(),
                update.initialized_predicate
            );

            if let Some(node) = weak_node.upgrade() {
                tracing::info!("SubscriptionHandler[{}] sending update to peer {}", peer_id, peer_id);
                node.send_update(
                    peer_id,
                    proto::NodeUpdateBody::SubscriptionUpdate {
                        items: update.items.into_iter().filter_map(|item| convert_item(&node, peer_id, item)).collect(),
                        initialized_predicate: update.initialized_predicate, // Now includes (PredicateId, u32)
                    },
                );
            }
        });

        Self { peer_id, subscription, guard }
    }

    /// Get the subscription ID for this peer.
    pub fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.subscription.id() }

    /// Get a reference to the subscription for adding/removing predicates.
    pub fn subscription(&self) -> &ReactorSubscription { &self.subscription }

    /// Remove a predicate from this peer's subscription.
    pub fn remove_predicate(&self, predicate_id: proto::PredicateId) { self.subscription.remove_predicate(predicate_id); }

    /// Handle a subscription request for this peer.
    /// TODO: Rename this method from add_predicate to subscribe_predicate for consistency
    pub async fn add_predicate<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        predicate_id: proto::PredicateId,
        collection_id: proto::CollectionId,
        predicate: ankql::ast::Predicate,
        cdata: &PA::ContextData,
        version: u32,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // TODO: Unified handling for both initial subscription and updates
        // 1. Check if there's an existing predicate for this predicate_id in the reactor subscription
        //    - If exists: this is an update, need to compute diff
        //    - If not: this is initial subscription, old state is empty
        // 2. For updates, get the set of entity IDs currently matching the old predicate
        // 3. Fetch states matching the NEW predicate from storage
        // 4. Compute the difference:
        //    - Entities to send = (new matches) - (old matches)
        //    - These are entities that match new predicate but didn't match old (or didn't exist)
        // 5. Pass version to reactor.initialize along with the new predicate AST

        // Check access permissions first
        node.policy_agent.can_access_collection(cdata, &collection_id)?;
        let filtered_predicate = node.policy_agent.filter_predicate(cdata, &collection_id, predicate)?;
        let storage_collection = node.collections.get(&collection_id).await?;

        // TODO: For update case, need to track which entities we already have
        // and only fetch/send the NEW ones that match the updated predicate
        let initial_states = storage_collection.fetch_states(&filtered_predicate).await?;

        // TODO: Remove this add_predicate call once it's merged into initialize/set_predicate
        // The predicate should be added atomically with its initial entities in a single operation
        // For now, this creates an inactive predicate that waits for initialize() to activate
        self.subscription.add_predicate(predicate_id, &collection_id, filtered_predicate)?;
        debug!("Added predicate {} to subscription for peer {}", predicate_id, self.peer_id);

        let retriever = LocalRetriever::new(storage_collection);
        let mut initial_entities = Vec::with_capacity(initial_states.len());

        // TODO: only send entities that were not already in the old resultset
        // Only convert states to entities for truly NEW matches
        // in the version 0 case, this will be all of them - either way, it's the same logic
        // do not include entities that were removed from the old resultset because
        // if it's a local LiveQuery - they already have a clone of the ResultSet we're working on
        // and if it's a remote subscription - they have their own Reactor that will remove them - they should have all the updates already
        for state in initial_states {
            let (_, entity) =
                node.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            initial_entities.push(entity);
        }

        // TODO: Pass version and new predicate AST to set_predicate (formerly known as initialize)
        // initialize should handle both initial and update cases
        node.reactor.initialize(self.subscription.id(), predicate_id, initial_entities)?;

        Ok(proto::NodeResponseBody::PredicateSubscribed { predicate_id })
    }
}

/// Convert a single ReactorUpdateItem to a SubscriptionUpdateItem.
fn convert_item<SE, PA>(
    node: &Node<SE, PA>,
    peer_id: proto::EntityId,
    item: crate::reactor::ReactorUpdateItem,
) -> Option<proto::SubscriptionUpdateItem>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // Convert entity to EntityState and attest it
    let entity_state = match item.entity.to_entity_state() {
        Ok(entity_state) => entity_state,
        Err(e) => {
            warn!("Failed to convert entity {} to EntityState for peer {}: {}", item.entity.id(), peer_id, e);
            return None;
        }
    };

    let attestation = node.policy_agent.attest_state(node, &entity_state);
    let attested_state = Attested::opt(entity_state, attestation);

    // Events should already be attested
    let attested_events = item.events;

    // Determine content based on whether we have events
    let content = if attested_events.is_empty() {
        proto::UpdateContent::StateOnly(attested_state.into())
    } else {
        proto::UpdateContent::StateAndEvent(attested_state.into(), attested_events.into_iter().map(|e| e.into()).collect())
    };

    // Convert predicate relevance from reactor types to proto types
    let predicate_relevance = item
        .predicate_relevance
        .into_iter()
        .map(|(pred_id, membership)| {
            let proto_membership = match membership {
                crate::reactor::MembershipChange::Initial => proto::MembershipChange::Initial,
                crate::reactor::MembershipChange::Add => proto::MembershipChange::Add,
                crate::reactor::MembershipChange::Remove => proto::MembershipChange::Remove,
            };
            (pred_id, proto_membership)
        })
        .collect();

    // Create subscription update item
    Some(proto::SubscriptionUpdateItem {
        entity_id: item.entity.id(),
        collection: item.entity.collection().clone(),
        content,
        predicate_relevance,
        entity_subscribed: item.entity_subscribed,
    })
}
