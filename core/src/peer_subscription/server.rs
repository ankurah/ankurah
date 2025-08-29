use ankurah_proto::{self as proto, Attested};
use anyhow::anyhow;
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
            if let Some(node) = weak_node.upgrade() {
                node.send_update(
                    peer_id,
                    proto::NodeUpdateBody::SubscriptionUpdate {
                        items: update.items.into_iter().filter_map(|item| convert_item(&node, peer_id, item)).collect(),
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
    pub async fn add_predicate<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        predicate_id: proto::PredicateId,
        collection_id: proto::CollectionId,
        predicate: ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Check access permissions first
        node.policy_agent.can_access_collection(cdata, &collection_id)?;
        let filtered_predicate = node.policy_agent.filter_predicate(cdata, &collection_id, predicate)?;
        let storage_collection = node.collections.get(&collection_id).await?;
        let initial_states = storage_collection.fetch_states(&filtered_predicate).await?;

        // Add the predicate to this peer's subscription
        self.subscription.add_predicate(predicate_id, &collection_id, filtered_predicate)?;
        debug!("Added predicate {} to subscription for peer {}", predicate_id, self.peer_id);

        let retriever = LocalRetriever::new(storage_collection);
        let mut initial_entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) =
                node.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            initial_entities.push(entity);
        }
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
