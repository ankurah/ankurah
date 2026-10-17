use crate::{
    error::SubscriptionError,
    node::{source_publications::SourceError, state_sync::QueryTree, Node},
    policy::PolicyAgent,
    session::{ContextData, SessionSet},
    storage::StorageEngine,
};
use ankql::ast::Resolved;
use ankurah_proto as proto;
use ankurah_signals::Wait;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot, Mutex};

mod source;
pub(crate) use source::{capture_query, CapturedQuery};

/// Owns one immutable source projection per query generation for this peer.
pub struct SubscriptionHandler<CD: ContextData> {
    peer_id: proto::EntityId,
    queries: Mutex<HashMap<proto::QueryId, StandingQuery<CD>>>,
}

struct StandingQuery<CD: ContextData> {
    // Keep the standing credentials registered with the node policy service.
    _sessions: SessionSet<CD>,
    version: u32,
    comparison: proto::ComparisonId,
    tree: Arc<QueryTree>,
    commands: mpsc::Sender<source::Refresh>,
    stop: Option<oneshot::Sender<()>>,
    stopped: oneshot::Receiver<()>,
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
            peer.comparisons.invalidate_query(query);
        }
        if let Some(mut standing) = queries.remove(&query) {
            standing.stop.take();
            let _ = standing.stopped.await;
        }
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
        let tree = if let Some(mut previous) = queries.remove(&query_id) {
            previous.stop.take();
            let _ = previous.stopped.await;
            previous.tree
        } else {
            Arc::new(QueryTree::default())
        };
        peer.comparisons.invalidate_query(query_id);
        let captured = capture_query(node, selection, spec, credentials.clone(), tree.clone()).await?;
        let CapturedQuery { revision, snapshot, projection, cursor } = captured;
        let state = crate::node::state_sync::start_comparison(
            node,
            self.peer_id,
            request,
            fingerprint,
            credentials.clone(),
            Some((query_id, version)),
            catalog,
            revision,
            snapshot,
            known_state,
        )
        .await?;
        let comparison = state.frame.comparison;
        // Moving the snapshot into the round must not consume the independent
        // cursor/projection that owns every later source publication.
        let actor = source::QuerySource::new(self.peer_id, comparison, credentials.clone(), projection, cursor, revision);
        let (commands, receiver) = mpsc::channel(8);
        let (stop, canceled) = oneshot::channel();
        let (stopped, done) = oneshot::channel();
        let weak = node.weak();
        let run = node.run.clone();
        let peer_id = self.peer_id;
        crate::task::spawn(async move {
            let result = tokio::select! {
                _ = canceled => Ok(()),
                _ = run.wait_value(false) => Ok(()),
                result = actor.run(weak.clone(), receiver) => result,
            };
            // Unblock replacement before failure cleanup takes the handler lock.
            let _ = stopped.send(());
            if let Err(error) = result {
                tracing::warn!(%peer_id, %comparison, %error, "query source stopped");
                if let Some(node) = weak.upgrade() {
                    if matches!(error.downcast_ref::<SourceError>(), Some(SourceError::Unavailable)) {
                        let _ = crate::node::state_sync::source_unavailable(&node, peer_id, comparison).await;
                    } else {
                        let _ = crate::node::state_sync::expire_comparison(&node, peer_id, comparison).await;
                    }
                }
            }
        });
        queries
            .insert(query_id, StandingQuery { _sessions: sessions, version, comparison, tree, commands, stop: Some(stop), stopped: done });
        Ok(proto::NodeResponseBody::QuerySubscribed { query_id, state })
    }

    pub(crate) async fn refresh_query<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        comparison: proto::ComparisonId,
        number: u64,
        known: proto::KnownState,
        credentials: &[CD],
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let commands = {
            let queries = self.queries.lock().await;
            let standing = queries
                .values()
                .find(|standing| standing.comparison == comparison)
                .ok_or_else(|| anyhow::anyhow!("query stream unavailable"))?;
            let peer = node.peer_connections.get(&self.peer_id).ok_or_else(|| anyhow::anyhow!("peer disconnected"))?;
            let session = peer.comparisons.get(&comparison).ok_or_else(|| anyhow::anyhow!("query stream unavailable"))?;
            anyhow::ensure!(session.permits(credentials), "query stream credentials changed");
            standing.commands.clone()
        };
        let (response, result) = oneshot::channel();
        commands.send(source::Refresh { number, known, response }).await.map_err(|_| anyhow::anyhow!("query source stopped"))?;
        result.await.map_err(|_| anyhow::anyhow!("query source stopped"))?
    }

    pub(crate) async fn abandon_comparison(&self, query: proto::QueryId, version: u32, comparison: proto::ComparisonId) {
        let mut queries = self.queries.lock().await;
        if queries.get(&query).is_some_and(|standing| standing.version == version && standing.comparison == comparison) {
            queries.remove(&query);
        }
    }
}

#[cfg(test)]
impl<CD: ContextData> SubscriptionHandler<CD> {
    pub(crate) async fn query_tree_for_test(&self, query: proto::QueryId) -> Option<(u32, Arc<QueryTree>)> {
        self.queries.lock().await.get(&query).map(|standing| (standing.version, standing.tree.clone()))
    }
}
