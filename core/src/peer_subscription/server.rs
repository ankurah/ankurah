use ankurah_proto::{self as proto, Attested};
use tracing::warn;

use crate::{
    entity::Entity,
    error::SubscriptionError,
    node::Node,
    policy::PolicyAgent,
    reactor::{
        fetch_gap::{GapFetcher, QueryGapFetcher},
        ReactorSubscription, ReactorUpdate,
    },
    session::{ContextData, SessionSet},
    storage::StorageEngine,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};
use std::collections::HashMap;
use std::sync::Mutex;

/// Manages a peer's subscription to this node's reactor.
///
/// This handler owns both the ReactorSubscription and the SubscriptionGuard
/// for listening to changes on that subscription — and each standing
/// query's subscriber session, the typed owner of credential state the
/// reactor reads but does not manage.
pub struct SubscriptionHandler<CD: ContextData> {
    _peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
    /// Each standing query's credential source, shared with its gap
    /// fetcher and dropped with the handler, so a disconnect releases
    /// them all.
    queries: Mutex<HashMap<proto::QueryId, SessionSet<CD>>>,
}

impl<CD: ContextData> SubscriptionHandler<CD> {
    pub fn new<SE, PA>(peer_id: proto::EntityId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();

        // Subscribe to changes on this subscription
        let guard = subscription.subscribe(move |update: ReactorUpdate| {
            tracing::info!("SubscriptionHandler[{}] received reactor update with {} items", peer_id, update.items.len());

            if let Some(node) = weak_node.upgrade() {
                tracing::debug!("SubscriptionHandler[{}] sending update to peer {}", peer_id, peer_id);
                node.send_update(
                    peer_id,
                    proto::NodeUpdateBody::SubscriptionUpdate {
                        items: update.items.into_iter().filter_map(|item| convert_item(&node, peer_id, item)).collect(),
                    },
                );
            }
        });

        Self { _peer_id: peer_id, subscription, _guard: guard, queries: Mutex::new(HashMap::new()) }
    }

    /// Get the subscription ID for this peer.
    pub fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.subscription.id() }

    /// Get a reference to the subscription for adding/removing predicates.
    pub fn subscription(&self) -> &ReactorSubscription { &self.subscription }

    /// Remove a predicate from this peer's subscription, releasing the
    /// query's session with it.
    pub fn remove_predicate(&self, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        self.subscription.remove_predicate(query_id)?;
        self.queries.lock().unwrap_or_else(|e| e.into_inner()).remove(&query_id);
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
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        if version == 0 {
            return Err(anyhow::anyhow!("Invalid version 0 for subscription"));
        }
        // Re-subscribes re-validate under the peer's current credentials
        // and refresh the query's standing session below. Denial does
        // not yet tear down a standing registration — the claw-back
        // arrives with the re-permission PR:
        // https://github.com/ankurah/ankurah/pull/426
        node.policy_agent.can_access_collection(cdata, &collection_id)?;
        selection.predicate = node.policy_agent.filter_predicate(cdata, &collection_id, selection.predicate)?;
        let storage_collection = node.collections.get(&collection_id).await?;

        // TODO: consider separating session updating from SubscribeQuery,
        // reserving that request for actual subscription (selection)
        // updates and carrying credential refresh on its own wire
        // message once the protocol grows a session-update vocabulary.
        //
        // The query's credential source: minted at first subscribe,
        // shared with the gap fetcher, refreshed with the re-validated
        // credentials on re-subscribe (the equality gate makes an
        // identical credential a no-op).
        let sessions: SessionSet<CD> = {
            use std::collections::hash_map::Entry;
            let mut queries = self.queries.lock().unwrap_or_else(|e| e.into_inner());
            match queries.entry(query_id) {
                Entry::Occupied(o) => {
                    let sessions = o.get().clone();
                    for session in sessions.sessions() {
                        session.update(cdata.clone());
                    }
                    sessions
                }
                Entry::Vacant(v) => v.insert(cdata.clone().into()).clone(),
            }
        };

        let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(node, sessions));

        // Add or update the query - idempotent, works whether query exists or not
        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;
        let matching_entities = self
            .subscription
            .upsert_query(query_id, collection_id.clone(), selection.clone(), included_entities, gap_fetcher, version)
            .await?;

        // TASK: Audit SubscriptionUpdate vs QuerySubscribed sequencing https://github.com/ankurah/ankurah/issues/147

        // TASK: Optimize to avoid re-attesting entities fetched from storage https://github.com/ankurah/ankurah/issues/148
        // Convert matching entities to Attested<EntityState>
        let initial_states: Vec<_> = matching_entities
            .into_iter()
            .filter_map(|e| {
                let entity_state = e.to_entity_state().ok()?;
                let attestation = node.policy_agent.attest_state(node, &entity_state);
                Some(Attested::opt(entity_state, attestation))
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
            if node.policy_agent.check_read(cdata, &state.payload.entity_id, &collection_id, &state.payload.state).is_err() {
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
    let content = proto::UpdateContent::StateAndEvent(attested_state.into(), attested_events.into_iter().map(|e| e.into()).collect());

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
    })
}
