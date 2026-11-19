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
use std::collections::HashMap;

/// Owns one peer's reactor subscription and per-query credential sources.
pub struct SubscriptionHandler<CD: ContextData> {
    peer_id: proto::EntityId,
    release_comparison: Box<dyn Fn(proto::ComparisonId) + Send + Sync>,
    subscription: ReactorSubscription,
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
    comparison: Option<proto::ComparisonId>,
}

impl<CD: ContextData> Drop for SubscriptionHandler<CD> {
    fn drop(&mut self) {
        for query in self.queries.get_mut().values() {
            if let Some(id) = query.comparison {
                (self.release_comparison)(id);
            }
        }
    }
}

impl<CD: ContextData> SubscriptionHandler<CD> {
    pub fn new<SE, PA>(peer_id: proto::EntityId, node: &Node<SE, PA>) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();
        let weak_node = node.weak();
        let comparison_node = weak_node.clone();
        let release_comparison = Box::new(move |id| {
            if let Some(node) = comparison_node.upgrade() {
                node.release_comparison(peer_id, id);
            }
        });

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

        Self { peer_id, release_comparison, subscription, _guard: guard, queries: tokio::sync::Mutex::new(HashMap::new()) }
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
        if let Some(standing) = queries.remove(&query_id) {
            if let Some(id) = standing.comparison {
                (self.release_comparison)(id);
            }
        }
        removed
    }

    /// Handle a subscription request for this peer.
    pub async fn subscribe_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query_id: proto::QueryId,
        mut selection: ankql::ast::Selection<Resolved>,
        cdata: Option<&PA::ContextData>,
        version: u32,
        comparison: proto::ComparisonId,
        key_spec: ankurah_core_types::key_spec::KeySpec<proto::PropertyId>,
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
        let requested = selection.clone();
        // Re-subscribes revalidate; #426 owns denied-update claw-back.
        selection.predicate = policy.filter_predicate(selection.predicate)?;

        use std::collections::hash_map::Entry;
        let (sessions, query_created) = match queries.entry(query_id) {
            Entry::Occupied(mut o) => {
                let standing = o.get_mut();
                if version < standing.version {
                    anyhow::bail!("stale subscription version {version} for query {query_id}; current version is {}", standing.version);
                }
                if standing.comparison == Some(comparison) {
                    return Err(crate::error::RetrievalError::storage(proto::ComparisonFailure::InvalidRequest(
                        "comparison identity is already in use".into(),
                    ))
                    .into());
                }
                standing.version = version;
                if let Some(old) = standing.comparison.replace(comparison) {
                    (self.release_comparison)(old);
                }
                (standing.sessions.clone(), false)
            }
            Entry::Vacant(v) => {
                let sessions = match cdata {
                    Some(cdata) => cdata.clone().into(),
                    None => SessionSet::new(),
                };
                v.insert(StandingQuery { sessions: sessions.clone(), version, comparison: Some(comparison) });
                (sessions, true)
            }
        };

        let mut installation = Installation {
            queries: &mut queries,
            subscription: &self.subscription,
            query: query_id,
            created: query_created,
            completed: false,
        };
        let response = async {
            let context = crate::context::Context::new_weak(node, sessions);
            let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(context.clone()));
            let included_entities = context.0.fetch_entities_from_local(&selection).await?;
            // Install future updates before fixing the initial comparison target.
            self.subscription.upsert_query(query_id, selection.clone(), included_entities, gap_fetcher, context.0, version).await?;
            let target =
                node.begin_scoped_comparison(self.peer_id, comparison, requested, selection, key_spec, known_state, &credentials).await?;
            Ok(proto::NodeResponseBody::QuerySubscribed { query_id, target })
        }
        .await;
        installation.completed = response.is_ok();
        response
    }
}

/// Keep failure and cancellation cleanup identical while the handler's query
/// lock prevents an unsubscribe or newer installation from racing this one.
struct Installation<'a, CD: ContextData> {
    queries: &'a mut HashMap<proto::QueryId, StandingQuery<CD>>,
    subscription: &'a ReactorSubscription,
    query: proto::QueryId,
    created: bool,
    completed: bool,
}

impl<CD: ContextData> Drop for Installation<'_, CD> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if self.created {
            self.queries.remove(&self.query);
            let _ = self.subscription.remove_predicate(self.query);
        } else if let Some(query) = self.queries.get_mut(&self.query) {
            query.comparison = None;
        }
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
