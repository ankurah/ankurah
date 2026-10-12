use ankql::ast::Resolved;
use ankurah_proto::{self as proto, Attested};
use tracing::warn;

use crate::{
    entity::Entity,
    error::SubscriptionError,
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    reactor::{
        fetch_gap::{GapFetcher, QueryGapFetcher},
        ReactorSubscription, ReactorUpdate,
    },
    session::{ContextData, SessionSet},
    storage::StorageEngine,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

mod delivery;
use delivery::Delivery;

/// Owns one peer's reactor subscription and per-query credential sources.
pub struct SubscriptionHandler<CD: ContextData> {
    peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    delivery: Arc<Mutex<Delivery>>,
    _guard: SubscriptionGuard,
    /// Tracks each standing query's version and the credential source shared with its gap fetcher.
    /// Held through async installation and failure cleanup so removal cannot race an
    /// installation that would leave a reactor query without its credential source.
    queries: tokio::sync::Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    /// Initial credential snapshot; peer session updates are not synchronized yet (#484).
    sessions: SessionSet<CD>,
    version: u32,
    tree: Arc<crate::node::state_sync::QueryTree>,
}

impl<CD: ContextData> SubscriptionHandler<CD> {
    pub fn new<SE, PA>(peer_id: proto::EntityId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();
        let delivery = Arc::new(Mutex::new(Delivery::default()));
        let updates = delivery.clone();

        let guard = subscription.subscribe(move |update: ReactorUpdate| {
            tracing::info!("SubscriptionHandler[{}] received reactor update with {} items", peer_id, update.items.len());

            if let Some(node) = weak_node.upgrade() {
                let items = update.items.into_iter().filter_map(|item| convert_item(&node, peer_id, item)).collect();
                let result = updates.lock().unwrap().send(&node, peer_id, proto::NodeUpdateBody::SubscriptionUpdate { items });
                if let Err(error) = result {
                    warn!(%peer_id, %error, "failed to serialize a subscription update; disconnecting peer");
                    node.deregister_peer(peer_id);
                }
            }
        });

        Self { peer_id, subscription, delivery, _guard: guard, queries: tokio::sync::Mutex::new(HashMap::new()) }
    }

    /// Get the subscription ID for this peer.
    pub fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.subscription.id() }

    /// Get a reference to the subscription for adding/removing predicates.
    pub fn subscription(&self) -> &ReactorSubscription { &self.subscription }

    /// Remove a query and its credential source even if reactor cleanup fails.
    pub async fn remove_predicate<SE, PA>(&self, node: &Node<SE, PA>, query_id: proto::QueryId) -> Result<(), SubscriptionError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        // A selection update must not reinstall the query between reactor and credential removal.
        // Reactor removal is synchronous and does not call back into this handler.
        let mut queries = self.queries.lock().await;
        let removed = self.subscription.remove_predicate(query_id);
        queries.remove(&query_id);
        self.delivery.lock().unwrap().release(node, self.peer_id, query_id);
        removed
    }

    /// Handle a subscription request for this peer.
    pub async fn subscribe_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query_id: proto::QueryId,
        mut selection: ankql::ast::Selection<Resolved>,
        cdata: Option<&PA::ContextData>,
        version: u32,
        known_state: Option<proto::KnownState>,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        if version == 0 {
            return Err(anyhow::anyhow!("Invalid version 0 for subscription"));
        }
        let spec = crate::node::state_sync::QueryTree::offered_spec(&selection, &node.catalog, &known_state)?;
        let catalog = crate::schema::is_catalog_read(&selection.predicate);
        let credentials: Vec<CD> = cdata.cloned().into_iter().collect();
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        // Re-subscribes revalidate; #426 owns denied-update claw-back.
        selection.predicate = policy.filter_predicate(selection.predicate)?;

        use std::collections::hash_map::Entry;
        let (sessions, tree) = match queries.entry(query_id) {
            Entry::Occupied(mut o) => {
                let standing = o.get_mut();
                if version < standing.version {
                    anyhow::bail!("stale subscription version {version} for query {query_id}; current version is {}", standing.version);
                }
                standing.version = version;
                (standing.sessions.clone(), standing.tree.clone())
            }
            Entry::Vacant(v) => {
                let sessions = match cdata {
                    Some(cdata) => cdata.clone().into(),
                    None => SessionSet::new(),
                };
                let tree = Arc::new(crate::node::state_sync::QueryTree::default());
                v.insert(StandingQuery { sessions: sessions.clone(), version, tree: tree.clone() });
                (sessions, tree)
            }
        };

        let peer = node.peer_connections.get(&self.peer_id).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        peer.comparisons.invalidate_query(query_id);
        self.delivery.lock().unwrap().begin(query_id, version);
        let response = async {
            let context = crate::context::Context::new_weak(node, sessions);
            let gap_fetcher: Arc<dyn GapFetcher<Entity>> = Arc::new(QueryGapFetcher::new(context.clone()));
            let matching = self.subscription.upsert_query(query_id, selection, gap_fetcher, context.0, version).await?;
            let mut states = Vec::new();
            for entity in matching {
                let state = entity.to_entity_state()?;
                if policy.check_read(&state.entity_id, &state.state).is_err() {
                    continue;
                }
                let attestation = node.policy_agent.attest_state(node, &state);
                states.push(Attested::opt(state, attestation));
            }
            let snapshot = tree.refresh(spec, states).await?;
            let state = node
                .start_comparison(
                    self.peer_id,
                    request,
                    fingerprint,
                    credentials.clone(),
                    Some((query_id, version)),
                    catalog,
                    snapshot,
                    known_state,
                )
                .await?;
            if !self.delivery.lock().unwrap().bind(query_id, version, state.page.comparison) {
                peer.comparisons.remove(&state.page.comparison);
                return Ok(proto::NodeResponseBody::ComparisonUnavailable { comparison: state.page.comparison });
            }
            Ok(proto::NodeResponseBody::QuerySubscribed { query_id, state })
        }
        .await;
        if response.is_err() {
            queries.remove(&query_id);
            let _ = self.subscription.remove_predicate(query_id);
            self.delivery.lock().unwrap().release(node, self.peer_id, query_id);
        }
        response
    }

    pub(crate) async fn finish_comparison<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query: proto::QueryId,
        version: u32,
        comparison: proto::ComparisonId,
        outcome: proto::ComparisonOutcome,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        let mut delivery = self.delivery.lock().unwrap();
        if !delivery.matches(query, version, comparison) {
            return Ok(());
        }
        if outcome == proto::ComparisonOutcome::Abandoned {
            queries.remove(&query);
            let _ = self.subscription.remove_predicate(query);
        }
        delivery.release(node, self.peer_id, query);
        Ok(())
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
    Some(proto::SubscriptionUpdateItem { entity_id: item.entity.id(), content, predicate_relevance })
}
