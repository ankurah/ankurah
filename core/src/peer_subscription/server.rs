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
    util::cancel_flag::CancelFlag,
};
use ankurah_signals::{Subscribe, SubscriptionGuard};
use std::collections::HashMap;

/// Owns one peer's reactor subscription and per-query credential sources.
pub struct SubscriptionHandler<CD: ContextData> {
    peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
    /// Tracks each standing query's version, the credential source shared with its gap fetcher,
    /// and what stops its initial push.
    /// Held through async installation and failure cleanup so removal cannot race an
    /// installation that would leave a reactor query without its credential source; not
    /// through the answer's snapshot, which can wait out catalog churn.
    queries: tokio::sync::Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    /// Initial credential snapshot; peer session updates are not synchronized yet (#484).
    sessions: SessionSet<CD>,
    version: u32,
    /// Stops the push of this version's initial data once a later version
    /// replaces the query or the query is removed.
    push: CancelFlag,
}

impl<CD: ContextData> Drop for StandingQuery<CD> {
    fn drop(&mut self) { self.push.cancel(); }
}

impl<CD: ContextData> SubscriptionHandler<CD> {
    pub fn new<SE, PA>(peer_id: proto::EntityId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();

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

        Self { peer_id, subscription, _guard: guard, queries: tokio::sync::Mutex::new(HashMap::new()) }
    }

    /// Get the subscription ID for this peer.
    pub fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.subscription.id() }

    /// Get a reference to the subscription for adding/removing predicates.
    pub fn subscription(&self) -> &ReactorSubscription { &self.subscription }

    /// Remove a query and its credential source even if reactor cleanup fails.
    pub async fn remove_predicate(&self, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        // A selection update must not reinstall the query between reactor and credential removal.
        // Reactor removal is synchronous and does not call back into this handler.
        let mut queries = self.queries.lock().await;
        let removed = self.subscription.remove_predicate(query_id);
        queries.remove(&query_id);
        removed
    }

    /// Handle a subscription request for this peer.
    pub async fn subscribe_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query_id: proto::QueryId,
        requested: ankql::ast::Selection<Resolved>,
        cdata: Option<&PA::ContextData>,
        version: u32,
        known_state: proto::KnownState,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        if version == 0 {
            return Err(anyhow::anyhow!("Invalid version 0 for subscription"));
        }
        let credentials: Vec<CD> = cdata.cloned().into_iter().collect();
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        // Re-subscribes revalidate; #426 owns denied-update claw-back.
        let mut selection = requested.clone();
        selection.predicate = policy.filter_predicate(selection.predicate)?;

        use std::collections::hash_map::Entry;
        let (sessions, push, query_created) = match queries.entry(query_id) {
            Entry::Occupied(mut o) => {
                let standing = o.get_mut();
                if version < standing.version {
                    anyhow::bail!("stale subscription version {version} for query {query_id}; current version is {}", standing.version);
                }
                standing.version = version;
                standing.push.cancel_and_swap();
                (standing.sessions.clone(), standing.push.clone(), false)
            }
            Entry::Vacant(v) => {
                let sessions = match cdata {
                    Some(cdata) => cdata.clone().into(),
                    None => SessionSet::new(),
                };
                let push = CancelFlag::default();
                v.insert(StandingQuery { sessions: sessions.clone(), version, push: push.clone() });
                (sessions, push, true)
            }
        };

        // Install before the snapshot the answer reads: a commit after the
        // install reaches the peer live, one before the snapshot is in it, and
        // one between travels both ways, which applying twice tolerates.
        if let Err(error) = self.install(node, query_id, selection, &sessions, version).await {
            if query_created {
                queries.remove(&query_id);
                let _ = self.subscription.remove_predicate(query_id);
            }
            return Err(error);
        }
        drop(queries);

        let offered_in = crate::node::state_sync::OfferedIn::SubscribeQuery { query_id, cancel: push };
        match crate::node::state_sync::answer_offer(node, self.peer_id, requested, known_state, credentials, offered_in).await {
            Ok(target) => Ok(proto::NodeResponseBody::QuerySubscribed { query_id, target }),
            Err(error) => {
                if query_created {
                    self.remove_unless_replaced(query_id, version).await;
                }
                Err(error)
            }
        }
    }

    /// Remove a query this handler created at `version`, unless a later
    /// subscription replaced it or the peer removed it meanwhile.
    async fn remove_unless_replaced(&self, query_id: proto::QueryId, version: u32) {
        let mut queries = self.queries.lock().await;
        if queries.get(&query_id).is_some_and(|standing| standing.version == version) {
            queries.remove(&query_id);
            let _ = self.subscription.remove_predicate(query_id);
        }
    }

    /// Install the query in this peer's reactor subscription, seeded with its local matches.
    async fn install<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        sessions: &SessionSet<CD>,
        version: u32,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let context = crate::context::Context::new_weak(node, sessions.clone());
        let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(context.clone()));

        let included_entities = context.0.fetch_entities_from_local(&selection).await?;
        self.subscription.upsert_query(query_id, selection, included_entities, gap_fetcher, context.0, version).await?;
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

#[cfg(test)]
mod tests;
