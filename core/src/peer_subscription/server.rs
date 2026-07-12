use ankurah_proto::{self as proto};
use tracing::warn;

use crate::{
    error::SubscriptionError,
    node::Node,
    policy::PolicyAgent,
    reactor::{ReactorSubscription, ReactorUpdate},
    storage::StorageEngine,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};

/// Manages a peer's subscription to this node's reactor.
///
/// This handler owns both the ReactorSubscription and the SubscriptionGuard
/// for listening to changes on that subscription.
pub struct SubscriptionHandler {
    _peer_id: proto::NodeId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
}

impl SubscriptionHandler {
    pub fn new<SE, PA>(peer_id: proto::NodeId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();

        // Subscribe to changes on this subscription
        let guard = subscription.subscribe(move |update: ReactorUpdate| {
            tracing::info!("SubscriptionHandler[{}] received reactor update with {} items", peer_id, update.items.len());

            if let Some(node) = weak_node.upgrade() {
                // Building a StateAndEvent update now includes loading the
                // entity's self-certifying genesis. The signal callback is
                // synchronous, so finish that storage work in a task before
                // handing the complete wire body to Node::send_update.
                crate::task::spawn(async move {
                    let mut items = Vec::with_capacity(update.items.len());
                    for item in update.items {
                        if let Some(item) = convert_item(&node, peer_id, item).await {
                            items.push(item);
                        }
                    }
                    tracing::debug!("SubscriptionHandler[{}] sending update to peer {}", peer_id, peer_id);
                    node.send_update(peer_id, proto::NodeUpdateBody::SubscriptionUpdate { items });
                });
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
        known_matches: Vec<proto::KnownEntity>,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if version == 0 {
            return Err(anyhow::anyhow!("Invalid version 0 for subscription"));
        }
        node.policy_agent.can_access_collection(cdata, &collection_id)?;
        selection.predicate = node.policy_agent.filter_predicate(cdata, &collection_id, selection.predicate)?;
        let storage_collection = node.collections.get(&collection_id).await?;

        // Add or update the query - idempotent, works whether query exists or not
        let matching_entities = node
            .reactor
            .upsert_query(self.subscription.id(), query_id, collection_id.clone(), selection.clone(), node, cdata, version)
            .await?;

        // TASK: Audit SubscriptionUpdate vs QuerySubscribed sequencing https://github.com/ankurah/ankurah/issues/147

        // TASK: Optimize to avoid re-attesting entities fetched from storage https://github.com/ankurah/ankurah/issues/148
        // Convert matching entities to Attested<EntityState>
        let initial_states: Vec<_> = matching_entities
            .into_iter()
            .filter_map(|e| {
                let entity_state = e.to_entity_state().ok()?;
                let admission = node.policy_agent.attest_state(node, &entity_state);
                Some(node.attest_state(entity_state, admission))
            })
            .collect();

        // Expand initial_states to include entities from known_matches that weren't in the predicate results
        let expanded_states = crate::util::expand_states::expand_states(
            initial_states,
            known_matches.iter().map(|k| k.entity_id).collect::<Vec<_>>(),
            &storage_collection,
        )
        .await?;

        let known_map: std::collections::HashMap<_, _> = known_matches.into_iter().map(|k| (k.entity_id, k.head)).collect();

        // Generate deltas based on known_matches - use expanded states
        let mut deltas = Vec::with_capacity(expanded_states.len());
        for state in expanded_states {
            // Row-level read policy: the query predicate was already narrowed by
            // filter_predicate above, but expand_states can resurface entities from
            // known_matches that the subscriber can no longer read, and scope rules
            // are evaluated against entity state, not just the predicate. Skip
            // unreadable entities silently (mirroring the Fetch/Get handlers) so one
            // out-of-scope entity doesn't fail the whole subscription.
            if node
                .policy_agent
                .check_read(cdata, &state.payload.entity_id, &collection_id, &state.payload.state, Some(node.catalog.resolver_weak()))
                .is_err()
            {
                continue;
            }

            // Only include delta if heads differ (None means heads are equal)
            if let Some(delta) = node.generate_entity_delta(&known_map, state, &storage_collection, cdata).await? {
                deltas.push(delta);
            }
        }

        Ok(proto::NodeResponseBody::QuerySubscribed { query_id, deltas })
    }
}

/// Convert a single ReactorUpdateItem to a SubscriptionUpdateItem.
async fn convert_item<SE, PA>(
    node: &Node<SE, PA>,
    peer_id: proto::NodeId,
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

    let admission = node.policy_agent.attest_state(node, &entity_state);
    let attested_state = node.attest_state(entity_state, admission);

    // Events should already be attested
    let attested_events = item.events;

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

    let entity_id = item.entity.id();
    let model = item.entity.model_id().ok()?;
    let genesis_id = proto::EventId::from_bytes(entity_id.to_bytes());
    let collection = match node.collections.get(item.entity.collection()).await {
        Ok(collection) => collection,
        Err(error) => {
            warn!("Failed to open {} while proving entity {} to peer {}: {}", item.entity.collection(), entity_id, peer_id, error);
            return None;
        }
    };
    let genesis = match collection.get_events(vec![genesis_id.clone()]).await {
        Ok(events) => events.into_iter().find(|event| event.payload.id() == genesis_id),
        Err(error) => {
            warn!("Failed to load genesis for entity {} for peer {}: {}", entity_id, peer_id, error);
            return None;
        }
    };
    let Some(genesis) = genesis else {
        warn!("Cannot send state for entity {} to peer {} without its self-certifying genesis", entity_id, peer_id);
        return None;
    };
    if genesis.payload.validate_structure().is_err()
        || !genesis.payload.is_entity_create()
        || genesis.payload.entity_id != entity_id
        || genesis.payload.model != model
    {
        warn!("Refusing to send malformed genesis proof for entity {} to peer {}", entity_id, peer_id);
        return None;
    }

    let proof = proto::StateWithGenesis { genesis, state: attested_state };
    let content = proto::UpdateContent::StateAndEvent(proof, attested_events.into_iter().map(|e| e.into()).collect());

    // Create subscription update item
    Some(proto::SubscriptionUpdateItem { entity_id, model, content, predicate_relevance })
}
