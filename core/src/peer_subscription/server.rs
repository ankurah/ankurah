use crate::reactor::maintenance::SourceError;
use crate::{
    error::SubscriptionError,
    node::{
        state_sync::{QueryTree, ResidentSubscription},
        Node,
    },
    policy::PolicyAgent,
    session::{ContextData, SessionSet},
    storage::StorageEngine,
};
use ankql::ast::Resolved;
use ankurah_proto as proto;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Owns this peer's standing routes into node-resident trees.
pub struct SubscriptionHandler<CD: ContextData> {
    peer_id: proto::EntityId,
    queries: Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    // Keep the standing credentials registered with the node policy service.
    _sessions: SessionSet<CD>,
    version: u32,
    stream_id: proto::StateStreamId,
    subscription: Arc<ResidentSubscription<CD>>,
}

impl<CD: ContextData> SubscriptionHandler<CD> {
    pub fn new(peer_id: proto::EntityId) -> Self { Self { peer_id, queries: Mutex::new(HashMap::new()) } }

    pub async fn remove_predicate<SE, PA>(&self, node: &Node<SE, PA>, query: proto::QueryId) -> Result<(), SubscriptionError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        if let Some(peer) = node.peer_connections.get(&self.peer_id) {
            peer.state_streams.invalidate_query(query);
        }
        queries.remove(&query);
        Ok(())
    }

    pub async fn subscribe_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        cdata: Option<&PA::ContextData>,
        version: u32,
        known_state: proto::KnownState,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let mut queries = self.queries.lock().await;
        anyhow::ensure!(version != 0, "invalid subscription version 0");
        if let Some(standing) = queries.get(&query_id) {
            anyhow::ensure!(version >= standing.version, "stale subscription version {version}; current version is {}", standing.version);
        }
        let spec = QueryTree::offered_spec(&selection, &node.catalog, &known_state)?;
        let catalog = crate::schema::is_catalog_read(&selection.predicate);
        let credentials: Vec<CD> = cdata.cloned().into_iter().collect();
        let sessions = cdata.cloned().map(SessionSet::from).unwrap_or_else(SessionSet::new);
        node.sessions.attach(&sessions);
        let peer = node.peer_connections.get(&self.peer_id).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
        queries.remove(&query_id);
        peer.state_streams.invalidate_query(query_id);
        let (state, subscription) = {
            let mut attempt = 0;
            loop {
                let resident = match node.resident_trees.acquire(node, selection.clone(), Some(spec.clone()), credentials.clone()).await {
                    Err(error) if attempt == 0 && matches!(error.downcast_ref::<SourceError>(), Some(SourceError::CatalogChanged)) => {
                        attempt += 1;
                        continue;
                    }
                    result => result?,
                };
                let result = resident
                    .subscribe(node, self.peer_id, request.clone(), fingerprint.clone(), query_id, version, catalog, known_state.clone())
                    .await;
                match result {
                    Err(error) if attempt == 0 && crate::node::state_sync::ResidentTrees::<CD>::can_retry(&error) => {
                        attempt += 1;
                    }
                    result => break result?,
                }
            }
        };
        let stream_id = state.frame.stream;
        queries.insert(query_id, StandingQuery { _sessions: sessions, version, stream_id, subscription: Arc::new(subscription) });
        Ok(proto::NodeResponseBody::QuerySubscribed { query_id, state })
    }

    pub(crate) async fn refresh_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        stream_id: proto::StateStreamId,
        number: u64,
        known: proto::KnownState,
        credentials: &[CD],
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let subscription = {
            let queries = self.queries.lock().await;
            let standing = queries
                .values()
                .find(|standing| standing.stream_id == stream_id)
                .ok_or_else(|| anyhow::anyhow!("query stream unavailable"))?;
            let peer = node.peer_connections.get(&self.peer_id).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
            let session = peer.state_streams.get(&stream_id).ok_or_else(|| anyhow::anyhow!("query stream unavailable"))?;
            anyhow::ensure!(session.permits(credentials), "query stream credentials changed");
            standing.subscription.clone()
        };
        subscription.refresh(number, known).await
    }

    pub(crate) async fn abandon_state_stream(&self, query: proto::QueryId, version: u32, stream_id: proto::StateStreamId) {
        let mut queries = self.queries.lock().await;
        if queries.get(&query).is_some_and(|standing| standing.version == version && standing.stream_id == stream_id) {
            queries.remove(&query);
        }
    }
}

#[cfg(test)]
impl<CD: ContextData> SubscriptionHandler<CD> {
    pub(crate) async fn query_tree_for_test(&self, query: proto::QueryId) -> Option<(u32, Arc<QueryTree>)> {
        self.queries.lock().await.get(&query).map(|standing| (standing.version, standing.subscription.tree()))
    }
}
