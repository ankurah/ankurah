use crate::internal::prelude::*;
use ankql::ast::Resolved;
use ankurah_proto::Attested;
use tracing::warn;

use crate::error::SubscriptionError;
use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use crate::reactor::{ReactorSubscription, ReactorUpdate};
use crate::session::ContextData;
use ankurah_signals::{Subscribe, SubscriptionGuard};
use std::collections::HashMap;

/// Owns one peer's reactor subscription and per-query credential sources.
pub struct SubscriptionHandler<CD: ContextData> {
    _peer_id: proto::EntityId,
    subscription: ReactorSubscription,
    _guard: SubscriptionGuard,
    /// Query identity, version, and live credential source.
    queries: tokio::sync::Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    collection: proto::CollectionId,
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
        let mut queries = self.queries.lock().await;
        let removed = self.subscription.remove_predicate(query_id);
        queries.remove(&query_id);
        removed
    }

    /// Create or refresh a query's single-session credential source.
    fn sync_session(
        queries: &mut HashMap<proto::QueryId, StandingQuery<CD>>,
        query_id: proto::QueryId,
        collection: &proto::CollectionId,
        cdata: Option<&CD>,
        version: u32,
    ) -> anyhow::Result<(SessionSet<CD>, bool)> {
        use std::collections::hash_map::Entry;
        match queries.entry(query_id) {
            Entry::Occupied(mut o) => {
                let standing = o.get_mut();
                if standing.collection != *collection {
                    anyhow::bail!("query {query_id} is already bound to collection '{}'", standing.collection);
                }
                if version < standing.version {
                    anyhow::bail!("stale subscription version {version} for query {query_id}; current version is {}", standing.version);
                }
                standing.version = version;
                if let Some(cdata) = cdata {
                    let sessions = standing.sessions.sessions();
                    debug_assert_eq!(sessions.len(), 1, "a credentialed query's set owns exactly one session");
                    for session in sessions {
                        session.update(cdata.clone());
                    }
                }
                Ok((standing.sessions.clone(), false))
            }
            Entry::Vacant(v) => {
                let sessions = match cdata {
                    Some(cdata) => cdata.clone().into(),
                    None => SessionSet::new(),
                };
                v.insert(StandingQuery { collection: collection.clone(), sessions: sessions.clone(), version });
                Ok((sessions, true))
            }
        }
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
        let exempt = crate::schema::reads_bypass_policy(&collection_id);
        if !exempt {
            // Re-subscribes revalidate; #426 owns denied-update claw-back.
            let cdata = cdata.ok_or_else(|| anyhow::anyhow!("subscribe to '{collection_id}' requires a credential"))?;
            node.policy_agent.can_access_collection(cdata, &collection_id)?;
            selection.predicate = node.policy_agent.filter_predicate(cdata, &collection_id, selection.predicate)?;
        }

        let (sessions, session_created) = Self::sync_session(&mut queries, query_id, &collection_id, cdata, version)?;

        let response =
            self.subscribe_query_inner(node, query_id, collection_id, selection, &sessions, cdata, exempt, version, known_matches).await;

        if response.is_err() && session_created {
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
        cdata: Option<&PA::ContextData>,
        exempt: bool,
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

        let credentials: Vec<CD> = cdata.cloned().into_iter().collect();
        let mut deltas = Vec::with_capacity(expanded_states.len());
        for state in expanded_states {
            // `known_matches` may resurface rows outside the current policy.
            if !exempt
                && node
                    .policy_agent
                    .check_read(
                        cdata.expect("a non-exempt subscribe holds a credential"),
                        &state.payload.entity_id,
                        &collection_id,
                        &state.payload.state,
                    )
                    .is_err()
            {
                continue;
            }

            if let Some(delta) = node.generate_entity_delta(&known_map, state, &storage_collection, &credentials).await? {
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
    let entity_state = match item.entity.to_entity_state() {
        Ok(entity_state) => entity_state,
        Err(e) => {
            warn!("Failed to convert entity {} to EntityState for peer {}: {}", item.entity.id(), peer_id, e);
            return None;
        }
    };

    let attestation = node.policy_agent.attest_state(node, &entity_state);
    let attested_state = Attested::opt(entity_state, attestation);

    let attested_events = item.events;
    let content = proto::UpdateContent::StateAndEvent(attested_state.into(), attested_events.into_iter().map(|e| e.into()).collect());

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

    Some(proto::SubscriptionUpdateItem {
        entity_id: item.entity.id(),
        collection: item.entity.collection().clone(),
        content,
        predicate_relevance,
    })
}
