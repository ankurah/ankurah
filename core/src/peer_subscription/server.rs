use ankql::ast::Resolved;
use ankurah_proto::{self as proto, Attested};
use tracing::warn;

use crate::{
    entity::Entity,
    error::SubscriptionError,
    node::Node,
    policy::{PolicyAgent, ReadPolicy},
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
    _peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
    /// Tracks each standing query's collection and version, plus the credential source shared with its gap fetcher.
    /// The mutex serializes this peer's query installation, failure cleanup, and removal.
    queries: tokio::sync::Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    collection: proto::CollectionId,
    /// Initial credential snapshot; peer session updates are not synchronized yet (#484).
    sessions: SessionSet<CD>,
    version: u32,
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

        Self { _peer_id: peer_id, subscription, _guard: guard, queries: tokio::sync::Mutex::new(HashMap::new()) }
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
        collection_id: proto::CollectionId,
        mut selection: ankql::ast::Selection<Resolved>,
        cdata: Option<&PA::ContextData>,
        version: u32,
        known_matches: Vec<proto::KnownEntity>,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        if version == 0 {
            return Err(anyhow::anyhow!("Invalid version 0 for subscription"));
        }
        if cdata.is_none() && !crate::schema::reads_bypass_policy(&collection_id) {
            return Err(anyhow::anyhow!("subscribe to '{collection_id}' requires a credential"));
        }
        let credentials: Vec<CD> = cdata.cloned().into_iter().collect();
        let policy = ReadPolicy::new(&node.policy_agent, &credentials, &collection_id);
        // Re-subscribes revalidate; #426 owns denied-update claw-back.
        policy.check_collection()?;
        selection.predicate = policy.filter_predicate(selection.predicate)?;

        use std::collections::hash_map::Entry;
        let (sessions, query_created) = match queries.entry(query_id) {
            Entry::Occupied(mut o) => {
                let standing = o.get_mut();
                if standing.collection != collection_id {
                    anyhow::bail!("query {query_id} is already bound to collection '{}'", standing.collection);
                }
                if version < standing.version {
                    anyhow::bail!("stale subscription version {version} for query {query_id}; current version is {}", standing.version);
                }
                standing.version = version;
                (standing.sessions.clone(), false)
            }
            Entry::Vacant(v) => {
                let sessions = match cdata {
                    Some(cdata) => cdata.clone().into(),
                    None => SessionSet::new(),
                };
                v.insert(StandingQuery { collection: collection_id.clone(), sessions: sessions.clone(), version });
                (sessions, true)
            }
        };

        let response =
            self.subscribe_query_inner(node, query_id, collection_id, selection, &sessions, &credentials, version, known_matches).await;

        if response.is_err() && query_created {
            queries.remove(&query_id);
            let _ = self.subscription.remove_predicate(query_id);
        }
        response
    }

    /// Install the query and build its versioned initial delta response.
    async fn subscribe_query_inner<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        sessions: &SessionSet<CD>,
        credentials: &Vec<CD>,
        version: u32,
        known_matches: Vec<proto::KnownEntity>,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let storage_collection = node.collections.get(&collection_id).await?;

        let context = crate::context::Context::new_weak(node, sessions.clone());
        let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(context));

        let included_entities = node.fetch_entities_from_local(&collection_id, &selection).await?;
        let matching_entities = self
            .subscription
            .upsert_query(query_id, collection_id.clone(), selection.clone(), included_entities, gap_fetcher, version)
            .await?;

        // TASK: Audit SubscriptionUpdate vs QuerySubscribed sequencing https://github.com/ankurah/ankurah/issues/147

        // TASK: Optimize to avoid re-attesting entities fetched from storage https://github.com/ankurah/ankurah/issues/148
        let initial_states: Vec<_> = matching_entities
            .into_iter()
            .filter_map(|e| {
                let entity_state = e.to_entity_state().ok()?;
                let attestation = node.policy_agent.attest_state(node, &entity_state);
                Some(Attested::opt(entity_state, attestation))
            })
            .collect();

        let expanded_states = crate::util::expand_states::expand_states(
            initial_states,
            known_matches.iter().map(|k| k.entity_id).collect::<Vec<_>>(),
            &storage_collection,
        )
        .await?;

        let known_map: std::collections::HashMap<_, _> = known_matches.into_iter().map(|k| (k.entity_id, k.head)).collect();

        let policy = ReadPolicy::new(&node.policy_agent, credentials, &collection_id);
        let mut deltas = Vec::with_capacity(expanded_states.len());
        for state in expanded_states {
            // `known_matches` may resurface rows outside the current policy.
            if policy.check_read(&state.payload.entity_id, &state.payload.state).is_err() {
                continue;
            }

            if let Some(delta) = node.generate_entity_delta(&known_map, state, &storage_collection, credentials).await? {
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
