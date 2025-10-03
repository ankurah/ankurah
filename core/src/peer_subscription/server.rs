use ankurah_proto::{self as proto, Attested};
use tracing::warn;

use crate::{
    error::SubscriptionError,
    node::Node,
    policy::PolicyAgent,
    reactor::{ReactorSubscription, ReactorUpdate},
    resultset::EntityResultSet,
    retrieval::LocalRetriever,
    storage::StorageEngine,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};

/// Manages a peer's subscription to this node's reactor.
///
/// This handler owns both the ReactorSubscription and the SubscriptionGuard
/// for listening to changes on that subscription.
pub struct SubscriptionHandler {
    _peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
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
            tracing::debug!(
                "SubscriptionHandler[{}] received reactor update with {} items, initialized_predicate: {:?}",
                peer_id,
                update.items.len(),
                update.initialized_query
            );

            if let Some(node) = weak_node.upgrade() {
                tracing::debug!("SubscriptionHandler[{}] sending update to peer {}", peer_id, peer_id);
                node.send_update(
                    peer_id,
                    proto::NodeUpdateBody::SubscriptionUpdate {
                        items: update.items.into_iter().filter_map(|item| convert_item(&node, peer_id, item)).collect(),
                        initialized_query: update.initialized_query, // Now includes (PredicateId, u32)
                    },
                );
            }
        });

        Self { _peer_id: peer_id, subscription, _guard: guard }
    }

    /// Get the subscription ID for this peer.
    pub fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.subscription.id() }

    /// Get a reference to the subscription for adding/removing predicates.
    pub fn subscription(&self) -> &ReactorSubscription { &self.subscription }

    /// Remove a predicate from this peer's subscription.
    pub fn remove_predicate(&self, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        self.subscription.remove_predicate(query_id)?;
        Ok(())
    }

    /// Handle a subscription request for this peer.
    pub async fn subscribe_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        mut selection: ankql::ast::Selection,
        cdata: &PA::ContextData,
        version: u32,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.policy_agent.can_access_collection(cdata, &collection_id)?;
        selection.predicate = node.policy_agent.filter_predicate(cdata, &collection_id, selection.predicate)?;
        let storage_collection = node.collections.get(&collection_id).await?;

        let initial_states = storage_collection.fetch_states(&selection).await?;
        let retriever = LocalRetriever::new(storage_collection);

        let mut initial_entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) =
                node.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            initial_entities.push(entity);
        }

        if version == 0 {
            let resultset = EntityResultSet::from_vec(initial_entities, true);
            let gap_fetcher = std::sync::Arc::new(crate::reactor::fetch_gap::QueryGapFetcher::new(node, cdata.clone()));
            node.reactor.add_query(self.subscription.id(), query_id, collection_id, selection, resultset, gap_fetcher)?;
        } else {
            node.reactor.update_query(
                self.subscription.id(),
                query_id,
                collection_id,
                selection,
                initial_entities,
                version,
                false, // remote subscriptions do not need remove notifications
            )?;
        }

        Ok(proto::NodeResponseBody::QuerySubscribed { query_id })
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
