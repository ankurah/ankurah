// TODO: Rename this module from client_relay to remote_subscription for clarity
use crate::internal::prelude::*;
use ankql::ast::Resolved;
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};
use tracing::{debug, warn};

use crate::error::RequestError;
use crate::node::ContextData;
use crate::util::safemap::SafeMap;

/// Query lifecycle callbacks used by [`SubscriptionRelay`].
#[async_trait::async_trait]
pub trait RemoteQuerySubscriber: Clone + Send + Sync + 'static {
    /// Called after remote subscription deltas have been applied.
    async fn subscription_established(&self, version: u32);

    /// Record a permanent failure for this subscription version.
    fn set_last_error(&self, version: u32, error: RetrievalError);
}

#[derive(Debug, Clone)]
enum Status {
    PendingRemote(Option<proto::EntityId>),
    Requested(proto::EntityId, Arc<AtomicBool>),
    Established(proto::EntityId),
    Suspended(Option<proto::EntityId>),
    Failed,
}

impl Status {
    fn cancel_attempt(&self) {
        if let Self::Requested(_, active) = self {
            active.store(false, Ordering::Release);
        }
    }

    fn peer(&self) -> Option<proto::EntityId> {
        match self {
            Self::PendingRemote(peer) | Self::Suspended(peer) => *peer,
            Self::Requested(peer, _) | Self::Established(peer) => Some(*peer),
            Self::Failed => None,
        }
    }
}

#[derive(Debug)]
struct Content<CD: ContextData> {
    query_id: proto::QueryId,
    collection_id: CollectionId,
    selection: ankql::ast::Selection<Resolved>,
    /// Read at each attempt so reconnects carry refreshed credentials.
    sessions: SessionSet<CD>,
    version: u32,
}

struct RemoteQueryState<CD: ContextData, Q: RemoteQuerySubscriber> {
    content: Arc<Content<CD>>,
    status: Status,
    livequery: Q,
    seq: u64,
}

struct SubscriptionRelayInner<CD: ContextData, Q: RemoteQuerySubscriber> {
    subscriptions: std::sync::Mutex<HashMap<proto::QueryId, RemoteQueryState<CD, Q>>>,
    next_seq: std::sync::atomic::AtomicU64,
    dispatch: tokio::sync::Mutex<()>,
    connected_peers: SafeMap<proto::EntityId, u64>,
    node: OnceLock<Arc<dyn TNode<CD>>>,
}

/// Keeps local queries registered on available durable peers.
#[derive(Clone)]
pub struct SubscriptionRelay<CD: ContextData, Q: RemoteQuerySubscriber> {
    inner: Arc<SubscriptionRelayInner<CD, Q>>,
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> Default for SubscriptionRelay<CD, Q> {
    fn default() -> Self { Self::new() }
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> SubscriptionRelay<CD, Q> {
    pub fn new() -> Self {
        let relay = Self {
            inner: Arc::new(SubscriptionRelayInner {
                subscriptions: std::sync::Mutex::new(HashMap::new()),
                next_seq: std::sync::atomic::AtomicU64::new(0),
                dispatch: tokio::sync::Mutex::new(()),
                connected_peers: SafeMap::new(),
                node: OnceLock::new(),
            }),
        };

        relay.start_retry_task();

        relay
    }

    pub fn set_node(&self, node: Arc<dyn TNode<CD>>) -> Result<(), ()> { self.inner.node.set(node).map_err(|_| ()) }

    pub fn subscribe_query(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        sessions: SessionSet<CD>,
        version: u32,
        livequery: Q,
    ) {
        debug!("SubscriptionRelay.subscribe_query() - New query {} needs remote registration", query_id);
        let mut subscriptions = self.inner.subscriptions.lock().expect("poisoned lock");
        let seq = subscriptions
            .get(&query_id)
            .map(|state| state.seq)
            .unwrap_or_else(|| self.inner.next_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let peer = subscriptions.get(&query_id).and_then(|state| state.status.peer());
        let replaced = subscriptions.insert(
            query_id,
            RemoteQueryState {
                content: Arc::new(Content { collection_id, selection, sessions, query_id, version }),
                status: Status::PendingRemote(peer),
                livequery,
                seq,
            },
        );
        if let Some(replaced) = replaced {
            replaced.status.cancel_attempt();
        }
        drop(subscriptions);

        if !self.inner.connected_peers.is_empty() {
            self.setup_remote_subscriptions()
        }
    }
    pub fn update_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error> {
        debug!("SubscriptionRelay.update_query() - New query {} needs remote registration", query_id);

        {
            let mut subscriptions = self.inner.subscriptions.lock().expect("poisoned lock");
            match subscriptions.get_mut(&query_id) {
                Some(state) => {
                    let peer = state.status.peer();
                    state.status.cancel_attempt();
                    let old_content = &state.content;
                    state.content = Arc::new(Content {
                        collection_id: old_content.collection_id.clone(),
                        selection,
                        sessions: old_content.sessions.clone(),
                        query_id: old_content.query_id,
                        version,
                    });
                    state.status = Status::PendingRemote(peer);
                }
                None => return Err(anyhow!("Predicate {} not found", query_id)),
            }
        }
        self.setup_remote_subscriptions();

        Ok(())
    }

    pub fn unsubscribe_predicate(&self, query_id: proto::QueryId) {
        debug!("Unregistering predicate {}", query_id);

        {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(info) = subscriptions.remove(&query_id) {
                info.status.cancel_attempt();
                if let Status::Established(peer_id)
                | Status::Requested(peer_id, _)
                | Status::Suspended(Some(peer_id))
                | Status::PendingRemote(Some(peer_id)) = &info.status
                {
                    let node = self.inner.node.get();
                    if let Some(node) = node {
                        let node = node.clone();
                        let peer_id = *peer_id;
                        crate::task::spawn(async move {
                            if let Err(e) = node.peer_unsubscribe(peer_id, query_id).await {
                                warn!("Failed to send unsubscribe message for {}: {}", query_id, e);
                            } else {
                                debug!("Successfully sent unsubscribe message for {}", query_id);
                            }
                        });
                    }
                }
            }
        }
    }

    pub fn suspend_query(&self, query_id: proto::QueryId) {
        if let Some(state) = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&query_id) {
            state.status.cancel_attempt();
            let peer = match &state.status {
                Status::Established(peer) | Status::Requested(peer, _) => Some(*peer),
                Status::Suspended(peer) => *peer,
                Status::PendingRemote(peer) => *peer,
                Status::Failed => None,
            };
            state.status = Status::Suspended(peer);
        }
    }

    pub fn notify_peer_disconnected(&self, peer_id: proto::EntityId) {
        self.inner.connected_peers.remove(&peer_id);
        self.peer_disconnected(peer_id);
    }

    pub(crate) fn notify_peer_disconnected_generation(&self, peer_id: proto::EntityId, generation: u64) {
        if self.inner.connected_peers.remove_if(&peer_id, |current| *current == generation).is_some() {
            self.peer_disconnected(peer_id);
        }
    }

    fn peer_disconnected(&self, peer_id: proto::EntityId) {
        debug!("Peer {} disconnected, orphaning predicate registrations", peer_id);

        for info in self.inner.subscriptions.lock().expect("poisoned lock").values_mut() {
            if let Status::Established(established_peer_id) | Status::Requested(established_peer_id, _) = &info.status {
                if *established_peer_id == peer_id {
                    info.status.cancel_attempt();
                    info.status = Status::PendingRemote(None);
                    warn!("Predicate {} orphaned due to peer {} disconnect", info.content.query_id, peer_id);
                }
            } else if matches!(info.status, Status::PendingRemote(Some(pending_peer)) if pending_peer == peer_id) {
                info.status = Status::PendingRemote(None);
            } else if matches!(info.status, Status::Suspended(Some(suspended_peer)) if suspended_peer == peer_id) {
                info.status = Status::Suspended(None);
            }
        }

        self.setup_remote_subscriptions();
    }

    pub fn notify_peer_connected(&self, peer_id: proto::EntityId) { self.notify_peer_connected_generation(peer_id, 0); }

    pub(crate) fn notify_peer_connected_generation(&self, peer_id: proto::EntityId, generation: u64) {
        debug!("SubscriptionRelay.notify_peer_connected() - Peer {} connected, registering predicates on peer subscription", peer_id);

        self.inner.connected_peers.insert(peer_id, generation);
        self.setup_remote_subscriptions();
    }

    /// Get the current state of a predicate registration
    #[cfg(test)]
    fn get_status(&self, query_id: proto::QueryId) -> Option<Status> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.get(&query_id).map(|info| info.status.clone())
    }

    /// Whether a query is established with, or being sent to, this peer.
    pub fn has_subscription_with_peer(&self, peer_id: &proto::EntityId) -> bool {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.values().any(|state| match &state.status {
            Status::Established(established_peer) | Status::Requested(established_peer, _) => established_peer == peer_id,
            _ => false,
        })
    }

    /// Return the live credentials used by queries assigned to this peer.
    pub fn get_contexts_for_peer(&self, peer_id: &proto::EntityId) -> std::collections::HashSet<CD> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        let mut contexts = std::collections::HashSet::new();

        for (_, state) in subscriptions.iter() {
            match &state.status {
                Status::Established(established_peer) | Status::Requested(established_peer, _) => {
                    if established_peer == peer_id {
                        contexts.extend(state.content.sessions.current());
                    }
                }
                _ => {}
            }
        }

        contexts
    }

    fn setup_remote_subscriptions(&self) {
        let me = self.clone();
        crate::task::spawn(async move { me.flush_pending().await });
    }

    async fn flush_pending(&self) {
        let _dispatch = self.inner.dispatch.lock().await;
        let Some(node) = self.inner.node.get().cloned() else { return };
        let Some(default_peer) = self.inner.connected_peers.keys().into_iter().min() else { return };
        let pending: Vec<_> = {
            let mut pending: Vec<_> = self
                .inner
                .subscriptions
                .lock()
                .expect("poisoned lock")
                .values()
                .filter_map(|info| match info.status {
                    Status::PendingRemote(peer) => Some((info.seq, info.content.clone(), peer)),
                    _ => None,
                })
                .collect();
            pending.sort_by_key(|(seq, _, _)| *seq);
            pending
        };

        if pending.is_empty() {
            return;
        }

        debug!("Registering {} predicates on {} peer subscriptions", pending.len(), self.inner.connected_peers.len());

        for (_, content, preferred_peer) in pending {
            let target_peer = preferred_peer.filter(|peer| self.inner.connected_peers.contains_key(peer)).unwrap_or(default_peer);
            let attempt = Arc::new(AtomicBool::new(true));
            let requested = {
                let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                subscriptions
                    .get_mut(&content.query_id)
                    .filter(|info| Arc::ptr_eq(&info.content, &content) && matches!(info.status, Status::PendingRemote(_)))
                    .is_some_and(|info| {
                        info.status = Status::Requested(target_peer, attempt.clone());
                        true
                    })
            };
            if !requested {
                continue;
            }

            let (dispatched, dispatched_rx) = tokio::sync::oneshot::channel();
            let me = self.clone();
            let node = node.clone();
            crate::task::spawn(async move { me.attempt_subscribe(node, target_peer, content, attempt, dispatched).await });
            let _ = dispatched_rx.await;
        }
    }

    async fn attempt_subscribe(
        &self,
        node: Arc<dyn TNode<CD>>,
        target_peer: proto::EntityId,
        content: Arc<Content<CD>>,
        attempt: Arc<AtomicBool>,
        dispatched: tokio::sync::oneshot::Sender<()>,
    ) {
        let query_id = content.query_id;
        let predicate = content.selection.clone();
        let cdatas = content.sessions.current();
        let version = content.version;

        match node
            .remote_subscribe(target_peer, query_id, content.collection_id.clone(), predicate, cdatas, version, attempt.clone(), dispatched)
            .await
        {
            Ok(()) => {
                let Some(livequery) = self.current_livequery(target_peer, &content, &attempt) else { return };
                livequery.subscription_established(version).await;
                let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(info) = subscriptions.get_mut(&query_id).filter(|info| {
                    Arc::ptr_eq(&info.content, &content)
                        && matches!(&info.status, Status::Requested(peer, active) if *peer == target_peer && Arc::ptr_eq(active, &attempt))
                }) {
                    info.status = Status::Established(target_peer);
                }
                debug!("Successfully registered predicate {} on peer {} subscription", query_id, target_peer);
            }
            Err(e) => {
                self.handle_error(target_peer, &content, &attempt, e);
            }
        }
    }

    fn current_livequery(&self, peer: proto::EntityId, content: &Arc<Content<CD>>, attempt: &Arc<AtomicBool>) -> Option<Q> {
        self.inner
            .subscriptions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&content.query_id)
            .filter(|state| {
                Arc::ptr_eq(&state.content, content)
                    && matches!(&state.status, Status::Requested(requested_peer, active) if *requested_peer == peer && Arc::ptr_eq(active, attempt))
            })
            .map(|state| state.livequery.clone())
    }

    fn start_retry_task(&self) {
        let weak = Arc::downgrade(&self.inner);
        crate::task::spawn(async move {
            loop {
                futures_timer::Delay::new(std::time::Duration::from_secs(5)).await;
                let Some(inner) = weak.upgrade() else { break };
                SubscriptionRelay { inner }.flush_pending().await;
            }
        });
    }

    fn handle_error(&self, target_peer: proto::EntityId, content: &Arc<Content<CD>>, attempt: &Arc<AtomicBool>, error: RetrievalError) {
        let query_id = content.query_id;
        let error_msg = error.to_string();

        let is_retryable = matches!(
            &error,
            RetrievalError::RequestError(
                RequestError::PeerNotConnected
                    | RequestError::ConnectionLost
                    | RequestError::SystemNotReady
                    | RequestError::SendError(_)
                    | RequestError::InternalChannelClosed
            )
        );

        let failed_query = {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            let current = subscriptions.get_mut(&query_id).filter(|info| {
                Arc::ptr_eq(&info.content, content)
                    && matches!(&info.status, Status::Requested(peer, active) if *peer == target_peer && Arc::ptr_eq(active, attempt))
            });
            match current {
                Some(info) if is_retryable => {
                    info.status.cancel_attempt();
                    info.status = Status::PendingRemote(Some(target_peer));
                    warn!("Retryable failure for predicate {} with peer {}: {} - will retry", query_id, target_peer, error_msg);
                    None
                }
                Some(info) => {
                    info.status.cancel_attempt();
                    info.status = Status::Failed;
                    tracing::error!("Permanent failure for predicate {} with peer {}: {} - no retry", query_id, target_peer, error_msg);
                    Some(info.livequery.clone())
                }
                None => None,
            }
        };
        if let Some(query) = failed_query {
            query.set_last_error(content.version, error);
        }
    }
}

/// Remote-subscription operations supplied by a node.
#[async_trait]
pub trait TNode<CD: ContextData>: Send + Sync {
    /// Establish the query and apply its initial deltas.
    async fn remote_subscribe(
        &self,
        peer_id: proto::EntityId,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        context_data: Vec<CD>,
        version: u32,
        attempt: Arc<AtomicBool>,
        dispatched: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), RetrievalError>;

    /// Send a one-way unsubscription.
    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl<SE, PA> TNode<PA::ContextData> for crate::node::WeakNode<SE, PA>
where
    SE: crate::storage::StorageEngine + Send + Sync + 'static,
    PA: crate::policy::PolicyAgent + Send + Sync + 'static,
{
    async fn remote_subscribe(
        &self,
        peer_id: proto::EntityId,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        context_data: Vec<PA::ContextData>,
        version: u32,
        attempt: Arc<std::sync::atomic::AtomicBool>,
        dispatched: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), RetrievalError> {
        let node = self.upgrade().ok_or_else(|| RetrievalError::Other("Node has been dropped".to_string()))?;
        let epoch = node.system.schema_epoch().ok_or(RequestError::SystemNotReady)?;

        if !attempt.load(Ordering::Acquire) {
            let _ = dispatched.send(());
            return Ok(());
        }

        let known_matches: Vec<ankurah_proto::KnownEntity> = node
            .fetch_entities_from_local(&collection_id, &selection)
            .await?
            .into_iter()
            .map(|entity| ankurah_proto::KnownEntity { entity_id: entity.id(), head: entity.head() })
            .collect();

        if !attempt.load(Ordering::Acquire) {
            let _ = dispatched.send(());
            return Ok(());
        }

        let response = node
            .begin_request(
                peer_id,
                &context_data,
                ankurah_proto::NodeRequestBody::SubscribeQuery {
                    query_id,
                    collection: collection_id.clone(),
                    selection: selection.clone(),
                    version,
                    known_matches,
                },
            )
            .map_err(RetrievalError::RequestError)?;
        let _ = dispatched.send(());
        let response = response.await.map_err(|_| RetrievalError::RequestError(RequestError::InternalChannelClosed))??;

        let deltas = match response {
            ankurah_proto::NodeResponseBody::QuerySubscribed { query_id: _response_query_id, deltas } => deltas,
            ankurah_proto::NodeResponseBody::Error(e) => return Err(RetrievalError::RequestError(RequestError::ServerError(e))),
            other => return Err(RetrievalError::RequestError(RequestError::UnexpectedResponse(other))),
        };

        tracing::debug!(
            "Node.remote_subscribe: query_id: {}, collection_id: {}, received deltas: {}",
            query_id,
            collection_id,
            deltas.len()
        );
        if !attempt.load(Ordering::Acquire) {
            return Ok(());
        }
        let _root_state = node.system.lock_root_state().await;
        if !attempt.load(Ordering::Acquire) {
            return Ok(());
        }
        if node.system.schema_epoch() != Some(epoch) {
            return Err(RequestError::SystemNotReady.into());
        }
        let collection = node.collections.get(&collection_id).await?;
        let event_getter = crate::retrieval::CachedEventGetter::new(collection_id, collection.clone(), &node, &context_data);
        let state_getter = crate::retrieval::LocalStateGetter::new(collection);
        crate::node::applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &event_getter, &state_getter).await?;

        Ok(())
    }

    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error> {
        let node = self.upgrade().ok_or_else(|| anyhow!("Node has been dropped"))?;

        node.request_remote_unsubscribe(query_id, vec![peer_id]).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EntityId;
    use std::sync::{Arc, Mutex};

    impl ContextData for CollectionId {}

    #[derive(Debug)]
    struct MockMessageSender<CD: ContextData> {
        next_error: Arc<Mutex<Option<RequestError>>>,
        sent_requests: Arc<Mutex<Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection<Resolved>)>>>,
        pause_next: std::sync::atomic::AtomicBool,
        paused: tokio::sync::Semaphore,
        resume: tokio::sync::Semaphore,
        _phantom: std::marker::PhantomData<CD>,
    }

    impl<CD: ContextData> MockMessageSender<CD> {
        fn new() -> Self {
            Self {
                sent_requests: Arc::new(Mutex::new(Vec::new())),
                next_error: Arc::new(Mutex::new(None)),
                pause_next: std::sync::atomic::AtomicBool::new(false),
                paused: tokio::sync::Semaphore::new(0),
                resume: tokio::sync::Semaphore::new(0),
                _phantom: std::marker::PhantomData,
            }
        }

        fn set_fail_next(&self, error: RequestError) { *self.next_error.lock().unwrap() = Some(error); }

        fn get_sent_requests(&self) -> Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection<Resolved>)> {
            self.sent_requests.lock().unwrap().clone()
        }

        fn clear_sent_requests(&self) { self.sent_requests.lock().unwrap().clear(); }

        fn pause_one(&self) { self.pause_next.store(true, std::sync::atomic::Ordering::Release) }

        async fn wait_paused(&self) { self.paused.acquire().await.unwrap().forget() }

        fn resume(&self) { self.resume.add_permits(1) }
    }

    #[async_trait]
    impl<CD: ContextData> TNode<CD> for MockMessageSender<CD> {
        async fn remote_subscribe(
            &self,
            peer_id: EntityId,
            query_id: proto::QueryId,
            collection_id: CollectionId,
            selection: ankql::ast::Selection<Resolved>,
            _context_data: Vec<CD>,
            _version: u32,
            _attempt: Arc<AtomicBool>,
            dispatched: tokio::sync::oneshot::Sender<()>,
        ) -> Result<(), RetrievalError> {
            self.sent_requests.lock().unwrap().push((peer_id, query_id, collection_id.clone(), selection.clone()));
            let _ = dispatched.send(());
            if self.pause_next.swap(false, std::sync::atomic::Ordering::AcqRel) {
                self.paused.add_permits(1);
                self.resume.acquire().await.unwrap().forget();
            }

            if let Some(error) = self.next_error.lock().unwrap().take() {
                Err(RetrievalError::RequestError(error))
            } else {
                Ok(())
            }
        }

        async fn peer_unsubscribe(&self, peer_id: EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error> {
            self.sent_requests.lock().unwrap().push((
                peer_id,
                query_id,
                CollectionId::from("unsubscribe"),
                ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
            ));

            if let Some(error) = self.next_error.lock().unwrap().take() {
                Err(anyhow!(error.to_string()))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone)]
    struct MockLiveQuery;

    #[async_trait::async_trait]
    impl RemoteQuerySubscriber for MockLiveQuery {
        async fn subscription_established(&self, _version: u32) {}

        fn set_last_error(&self, _version: u32, _error: RetrievalError) {}
    }

    #[derive(Clone, Default)]
    struct TrackedLiveQuery(Arc<Mutex<Vec<u32>>>);

    #[async_trait::async_trait]
    impl RemoteQuerySubscriber for TrackedLiveQuery {
        async fn subscription_established(&self, version: u32) { self.0.lock().unwrap().push(version) }
        fn set_last_error(&self, _version: u32, _error: RetrievalError) {}
    }

    fn create_test_selection() -> ankql::ast::Selection<Resolved> {
        ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }
    }

    fn create_test_collection_id() -> CollectionId { CollectionId::from("test_collection") }

    #[tokio::test]
    async fn retry_task_does_not_keep_relay_alive() {
        let relay: SubscriptionRelay<CollectionId, MockLiveQuery> = SubscriptionRelay::new();
        let inner = Arc::downgrade(&relay.inner);
        drop(relay);
        assert!(inner.upgrade().is_none());
    }

    #[tokio::test]
    async fn test_new_subscription_setup() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Notify of new subscription
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);

        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_) | Status::Requested(..))));

        // Give async task time to complete (setup should happen automatically)
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify request was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, query_id);
        assert_eq!(sent_requests[0].2, collection_id);

        // Verify subscription is marked as established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));
    }

    #[tokio::test]
    async fn stale_attempt_cannot_establish_a_replaced_query() {
        let relay = SubscriptionRelay::new();
        let node = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(node.clone()).unwrap();
        let query_id = proto::QueryId::new();
        let peer_id = EntityId::random();
        let livequery = TrackedLiveQuery::default();

        node.pause_one();
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(
            query_id,
            create_test_collection_id(),
            create_test_selection(),
            create_test_collection_id().into(),
            1,
            livequery.clone(),
        );
        node.wait_paused().await;
        relay.update_query(query_id, create_test_selection(), 2).unwrap();
        node.resume();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if matches!(relay.get_status(query_id), Some(Status::Established(_))) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*livequery.0.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn independent_queries_do_not_block_each_other() {
        let relay = SubscriptionRelay::new();
        let node = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(node.clone()).unwrap();
        let first = proto::QueryId::new();
        let second = proto::QueryId::new();
        let peer = EntityId::random();

        for query_id in [first, second] {
            relay.subscribe_query(
                query_id,
                create_test_collection_id(),
                create_test_selection(),
                create_test_collection_id().into(),
                0,
                MockLiveQuery,
            );
        }

        node.pause_one();
        relay.notify_peer_connected(peer);
        node.wait_paused().await;
        assert_eq!(node.get_sent_requests()[0].1, first);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !matches!(relay.get_status(second), Some(Status::Established(_))) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(relay.get_status(first), Some(Status::Requested(..))));

        node.resume();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !matches!(relay.get_status(first), Some(Status::Established(_)))
                || !matches!(relay.get_status(second), Some(Status::Established(_)))
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stale_disconnect_keeps_the_replacement_connection() {
        let relay: SubscriptionRelay<CollectionId, MockLiveQuery> = SubscriptionRelay::new();
        let peer = EntityId::random();

        relay.notify_peer_connected_generation(peer, 1);
        relay.notify_peer_connected_generation(peer, 2);
        relay.notify_peer_disconnected_generation(peer, 1);
        assert!(relay.inner.connected_peers.contains_key(&peer));

        relay.notify_peer_disconnected_generation(peer, 2);
        assert!(!relay.inner.connected_peers.contains_key(&peer));
    }

    #[tokio::test]
    async fn update_stays_with_its_established_peer() {
        let relay = SubscriptionRelay::new();
        let node = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(node.clone()).unwrap();
        let query_id = proto::QueryId::new();
        let lower_peer = EntityId::from_bytes([1; 32]);
        let established_peer = EntityId::from_bytes([2; 32]);

        relay.notify_peer_connected(established_peer);
        relay.subscribe_query(
            query_id,
            create_test_collection_id(),
            create_test_selection(),
            create_test_collection_id().into(),
            1,
            MockLiveQuery,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !matches!(relay.get_status(query_id), Some(Status::Established(_))) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        relay.notify_peer_connected(lower_peer);
        node.clear_sent_requests();
        relay.update_query(query_id, create_test_selection(), 2).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while node.get_sent_requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(node.get_sent_requests()[0].0, established_peer);
    }

    #[tokio::test]
    async fn test_peer_disconnection_orphans_subscriptions() {
        let relay = SubscriptionRelay::new();

        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Setup established subscription by going through the full flow
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone().into(), 0, MockLiveQuery);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));

        // Simulate peer disconnection
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is marked as pending again
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));
    }

    #[tokio::test]
    async fn suspended_query_rejects_pushes_and_does_not_retry() {
        let relay = SubscriptionRelay::new();
        let node = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(node.clone()).unwrap();
        let query_id = proto::QueryId::new();
        let peer = EntityId::random();

        relay.notify_peer_connected(peer);
        relay.subscribe_query(
            query_id,
            create_test_collection_id(),
            create_test_selection(),
            create_test_collection_id().into(),
            1,
            MockLiveQuery,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !matches!(relay.get_status(query_id), Some(Status::Established(_))) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        node.clear_sent_requests();
        relay.suspend_query(query_id);
        assert!(matches!(relay.get_status(query_id), Some(Status::Suspended(Some(found))) if found == peer));
        assert!(!relay.has_subscription_with_peer(&peer));
        relay.setup_remote_subscriptions();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(node.get_sent_requests().is_empty());
    }

    #[tokio::test]
    async fn test_peer_connection_triggers_setup() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Add pending subscription (no peers connected yet)
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));

        // Clear any previous requests
        mock_sender.clear_sent_requests();

        // Simulate peer connection (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify request was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, query_id);

        // Verify subscription is established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));
    }

    #[tokio::test]
    async fn test_failed_subscription_retry() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Connect peer and add subscription (should succeed initially)
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify subscription is marked as established (since no error was set)
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));

        // Now test the retry behavior by disconnecting the peer (puts subscription back to PendingRemote)
        // then setting up the mock to fail, and reconnecting to trigger the retry
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is now in pending state
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));

        // Clear requests and set up mock to fail on the next call
        mock_sender.clear_sent_requests();
        mock_sender.set_fail_next(RequestError::ServerError("Invalid predicate".to_string()));

        // Reconnect peer to trigger retry attempt
        relay.notify_peer_connected(peer_id);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify retry was attempted (the error gets consumed)
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);

        // Verify subscription remains in failed state (non-retryable error)
        assert!(matches!(relay.get_status(query_id), Some(Status::Failed)));
    }

    #[tokio::test]
    async fn test_retryable_vs_non_retryable_failures() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let retryable_query_id = proto::QueryId::new();
        let non_retryable_query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Add subscriptions
        relay.subscribe_query(retryable_query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);
        relay.subscribe_query(
            non_retryable_query_id,
            collection_id.clone(),
            predicate.clone(),
            collection_id.clone().into(),
            0,
            MockLiveQuery,
        );

        // Manually set different failure types - retryable goes back to pending, non-retryable stays failed
        {
            let mut subscriptions = relay.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(info) = subscriptions.get_mut(&retryable_query_id) {
                info.status = Status::PendingRemote(None);
            }
            if let Some(info) = subscriptions.get_mut(&non_retryable_query_id) {
                info.status = Status::Failed; // Non-retryable errors stay failed
            }
        }

        // Connect peer and trigger retry
        relay.notify_peer_connected(peer_id);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify only the retryable subscription was attempted
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].1, retryable_query_id);

        // Verify states
        assert!(
            matches!(relay.get_status(retryable_query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id)
        );
        assert!(matches!(relay.get_status(non_retryable_query_id), Some(Status::Failed)));
    }

    #[tokio::test]
    async fn test_subscription_removal() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Connect peer and setup established subscription
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone().into(), 0, MockLiveQuery);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));

        // Clear previous requests to focus on unsubscribe
        mock_sender.clear_sent_requests();

        // Remove subscription
        relay.unsubscribe_predicate(query_id);

        // Give async task time to complete
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify unsubscribe message was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, query_id);

        // Verify subscription is gone
        assert!(matches!(relay.get_status(query_id), None));
    }

    #[tokio::test]
    async fn pending_update_unsubscribes_the_established_peer() {
        let relay = SubscriptionRelay::new();
        let node = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(node.clone()).unwrap();
        let query_id = proto::QueryId::new();
        let peer = EntityId::random();

        relay.notify_peer_connected(peer);
        relay.subscribe_query(
            query_id,
            create_test_collection_id(),
            create_test_selection(),
            create_test_collection_id().into(),
            1,
            MockLiveQuery,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !matches!(relay.get_status(query_id), Some(Status::Established(_))) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let dispatch = relay.inner.dispatch.lock().await;
        node.clear_sent_requests();
        relay.update_query(query_id, create_test_selection(), 2).unwrap();
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(Some(current))) if current == peer));
        relay.unsubscribe_predicate(query_id);
        drop(dispatch);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while node.get_sent_requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(node.get_sent_requests()[0].2, CollectionId::from("unsubscribe"));
    }

    #[tokio::test]
    async fn test_edge_cases() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Test setup without message sender - should not crash
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should still be pending since no sender
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));

        // Now set sender and test with no connected peers
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should still be pending since no peers available
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));

        // Verify no requests were sent
        assert_eq!(mock_sender.get_sent_requests().len(), 0);

        // Now connect a peer (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should now be established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id)) if established_peer_id == peer_id));
        assert_eq!(mock_sender.get_sent_requests().len(), 1);
    }

    #[tokio::test]
    async fn test_notify_unsubscribe_with_no_established_subscription() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();

        // Add subscription but don't establish it
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone().into(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote(_))));

        // Unsubscribe from pending subscription
        relay.unsubscribe_predicate(query_id);

        // Give async task time to complete (though no request should be sent)
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify no unsubscribe message was sent (since it wasn't established)
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 0);

        // Verify subscription is gone
        assert!(matches!(relay.get_status(query_id), None));
    }
}
