// TODO: Rename this module from client_relay to remote_subscription for clarity
use ankql::ast::Resolved;
use ankurah_proto::{self as proto, CollectionId};
use ankurah_signals::{Peek, Subscribe, SubscriptionGuard};
use anyhow::anyhow;
use async_trait::async_trait;
use proto::EntityId;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use tracing::{debug, warn};

use crate::error::{NodeDropped, RequestError, RetrievalError};
use crate::node::ContextData;
use crate::session::SessionSet;
use crate::util::cancel_flag::CancelFlag;
use crate::util::safeset::SafeSet;

/// Query lifecycle callbacks used by SubscriptionRelay.
#[async_trait::async_trait]
pub trait RemoteQuerySubscriber: Clone + Send + Sync + 'static {
    /// Called after remote subscription deltas have been applied.
    async fn subscription_established(&self, version: u32);

    /// Record a permanent failure for this subscription version.
    fn set_last_error(&self, version: u32, error: RetrievalError);
}

#[derive(Debug, Clone)]
pub enum Status {
    PendingRemote,
    Requested(proto::EntityId, u32),     // peer_id, version
    Established(proto::EntityId, u32),   // peer_id, version
    PendingUpdate(proto::EntityId, u32), // peer_id, version
    /// Non-retryable
    Failed,
}

#[derive(Debug)]
pub struct Content<CD: ContextData> {
    pub query_id: proto::QueryId,
    pub collection_id: CollectionId,
    pub selection: ankql::ast::Selection<Resolved>,
    /// Read at each registration; credential changes alone do not notify the peer yet (#484).
    pub sessions: SessionSet<CD>,
    pub version: u32,
}

pub struct RemoteQueryState<CD: ContextData, Q: RemoteQuerySubscriber> {
    pub content: Arc<Content<CD>>,
    pub status: Status,
    pub livequery: Q,
    cancel: CancelFlag,
}

struct SubscriptionRelayInner<CD: ContextData, Q: RemoteQuerySubscriber> {
    // All subscription information in one place
    subscriptions: std::sync::Mutex<HashMap<proto::QueryId, RemoteQueryState<CD, Q>>>,
    // Track connected durable peers
    connected_peers: SafeSet<proto::EntityId>,
    // Node for communicating with remote peers
    node: OnceLock<Arc<dyn TNode<CD>>>,
    // Wake the retry task on close; dropping the relay also closes its channel.
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
    _run_subscription: SubscriptionGuard,
}

/// Keeps this node's queries registered on remote durable peers across
/// peer availability: it registers on connect, re-registers on
/// reconnect, retries failures, and tears down removals — each attempt
/// reading the query's live credential source, so re-registrations
/// carry refreshed values.
#[derive(Clone)]
pub struct SubscriptionRelay<CD: ContextData, Q: RemoteQuerySubscriber> {
    inner: Arc<SubscriptionRelayInner<CD, Q>>,
}

struct WeakSubscriptionRelay<CD: ContextData, Q: RemoteQuerySubscriber>(Weak<SubscriptionRelayInner<CD, Q>>);

impl<CD: ContextData, Q: RemoteQuerySubscriber> WeakSubscriptionRelay<CD, Q> {
    fn upgrade(&self) -> Option<SubscriptionRelay<CD, Q>> { self.0.upgrade().map(|inner| SubscriptionRelay { inner }) }
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> SubscriptionRelay<CD, Q> {
    #[cfg(test)]
    fn new_test() -> Self { Self::new(&ankurah_signals::Mut::new(true)) }

    /// Stop retrying and discard local registration tracking when the owner stops running.
    pub(crate) fn new(run: &(impl Subscribe<bool> + Peek<bool>)) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);

        let relay = Self {
            inner: Arc::new_cyclic(|weak| {
                let weak = WeakSubscriptionRelay(weak.clone());
                SubscriptionRelayInner {
                    subscriptions: std::sync::Mutex::new(HashMap::new()),
                    connected_peers: SafeSet::new(),
                    node: OnceLock::new(),
                    shutdown_tx,
                    _run_subscription: run.subscribe(move |run: bool| {
                        if !run {
                            if let Some(relay) = weak.upgrade() {
                                relay.close();
                            }
                        }
                    }),
                }
            }),
        };
        if !run.peek() {
            relay.close();
        }

        // Start background retry task
        relay.start_retry_task(shutdown_rx);

        relay
    }

    fn weak(&self) -> WeakSubscriptionRelay<CD, Q> { WeakSubscriptionRelay(Arc::downgrade(&self.inner)) }

    fn close(&self) {
        let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
        for state in subscriptions.values() {
            state.cancel.cancel();
        }
        subscriptions.clear();
        self.inner.connected_peers.clear();
        let _ = self.inner.shutdown_tx.try_send(());
    }

    /// Inject the node (typically a WeakNode for production)
    ///
    /// This should be called once during initialization. Returns an error if
    /// the node has already been set.
    pub fn set_node(&self, node: Arc<dyn TNode<CD>>) -> Result<(), ()> { self.inner.node.set(node).map_err(|_| ()) }

    /// Register a query on a durable peer, replacing any existing registration.
    pub fn subscribe_query(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        sessions: SessionSet<CD>,
        version: u32,
        livequery: Q,
    ) {
        debug!("SubscriptionRelay.subscribe_query() - Query {} needs remote registration", query_id);
        let content = Arc::new(Content { collection_id, selection, sessions, query_id, version });
        let cancel = CancelFlag::default();
        let peer = {
            let mut subscriptions = self.inner.subscriptions.lock().expect("poisoned lock");
            let previous = subscriptions.remove(&query_id);
            let peer = previous.and_then(|state| {
                state.cancel.cancel();
                match state.status {
                    Status::Established(peer, _) | Status::Requested(peer, _) => Some(peer),
                    _ => None,
                }
            });
            subscriptions.insert(
                query_id,
                RemoteQueryState {
                    content: content.clone(),
                    status: peer.map_or(Status::PendingRemote, |peer| Status::Requested(peer, version)),
                    livequery,
                    cancel: cancel.clone(),
                },
            );
            peer
        };

        if let Some(peer) = peer {
            self.update_query_on_peer(
                peer,
                query_id,
                content.collection_id.clone(),
                content.selection.clone(),
                version,
                content.sessions.clone(),
                cancel,
            );
        } else if !self.inner.connected_peers.is_empty() {
            self.setup_remote_subscriptions();
        }
    }

    pub fn update_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error> {
        debug!("SubscriptionRelay.update_query() - New query {} needs remote registration", query_id);

        let update = {
            let mut subscriptions = self.inner.subscriptions.lock().expect("poisoned lock");
            match subscriptions.get_mut(&query_id) {
                Some(state) => {
                    // Update the content with new predicate and version
                    let old_content = &state.content;
                    state.content = Arc::new(Content {
                        collection_id: old_content.collection_id.clone(),
                        selection: selection.clone(),
                        sessions: old_content.sessions.clone(),
                        query_id: old_content.query_id,
                        version,
                    });

                    match state.status {
                        Status::Established(peer_id, _) | Status::Requested(peer_id, _) => {
                            // Update to new version, mark as requested for this peer
                            state.status = Status::Requested(peer_id, version);
                            state.cancel.cancel_and_swap();
                            Some((peer_id, state.content.collection_id.clone(), state.content.sessions.clone(), state.cancel.clone()))
                            // Return the peer_id to send update to
                        }
                        _ => {
                            // Not established yet, just update to PendingRemote and setup
                            state.status = Status::PendingRemote;
                            None
                        }
                    }
                }
                None => return Err(anyhow!("Predicate {} not found", query_id)),
            }
        };

        match update {
            Some((peer_id, collection_id, sessions, cancel)) => {
                self.update_query_on_peer(peer_id, query_id, collection_id, selection, version, sessions, cancel);
            }
            None => {
                // Not established yet - use setup_remote_subscriptions for initial setup
                self.setup_remote_subscriptions();
            }
        };

        Ok(())
    }

    fn update_query_on_peer(
        &self,
        peer_id: proto::EntityId,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
        sessions: SessionSet<CD>,
        cancel: CancelFlag,
    ) {
        let me = self.clone();
        crate::task::spawn(async move {
            if let Some(node) = me.inner.node.get() {
                // Get the livequery for error handling
                let livequery = {
                    me.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).get(&query_id).map(|state| state.livequery.clone())
                };

                // Send the updated predicate to the peer, under the
                // credentials current at send time.
                match node.remote_subscribe(peer_id, query_id, collection_id, selection, sessions.current(), version).await {
                    _ if cancel.canceled() => debug!("Ignoring superseded reply for predicate {}", query_id),
                    Ok(()) => {
                        // Deltas applied successfully, now activate the livequery
                        if let Some(lq) = livequery {
                            lq.subscription_established(version).await;
                        }

                        // Mark as established - subscription succeeded even if livequery activation had issues
                        let mut subscriptions = me.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                        if !cancel.canceled() {
                            if let Some(info) = subscriptions.get_mut(&query_id) {
                                info.status = Status::Established(peer_id, version);
                            }
                        }
                        debug!("Successfully updated predicate {} on peer {} subscription", query_id, peer_id);
                    }
                    Err(e) => {
                        // Handle error with retry logic
                        me.handle_error(query_id, peer_id, e, &cancel);
                    }
                }
            }
        });
    }

    /// Notify the relay that a predicate should be removed from remote peer subscriptions
    ///
    /// This will clean up all tracking state and send unsubscribe requests to any
    /// remote peers that have this predicate registered.
    pub fn unsubscribe_predicate(&self, query_id: proto::QueryId) {
        debug!("Unregistering predicate {}", query_id);

        // If subscription was established with a peer, send unsubscribe request
        {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(info) = subscriptions.remove(&query_id) {
                info.cancel.cancel();
                if let Status::Established(peer_id, _) | Status::Requested(peer_id, _) = &info.status {
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

    /// Handle peer disconnection - mark all predicates for that peer as needing re-registration
    ///
    /// This should be called when a durable peer disconnects. All predicates registered
    /// with that peer will be marked as pending and will be automatically re-registered
    /// when the peer reconnects or another suitable peer becomes available.
    pub fn notify_peer_disconnected(&self, peer_id: proto::EntityId) {
        debug!("Peer {} disconnected, orphaning predicate registrations", peer_id);

        // Remove from connected peers
        self.inner.connected_peers.remove(&peer_id);

        for info in self.inner.subscriptions.lock().expect("poisoned lock").values_mut() {
            if let Status::Established(established_peer_id, _) | Status::Requested(established_peer_id, _) = &info.status {
                if *established_peer_id == peer_id {
                    // Update state to pending
                    info.status = Status::PendingRemote;
                    info.cancel.cancel();
                    warn!("Predicate {} orphaned due to peer {} disconnect", info.content.query_id, peer_id);
                }
            }
        }

        // Resubscribe any orphaned subscriptions
        self.setup_remote_subscriptions();
    }

    /// Handle peer connection - trigger predicate registration on the new peer subscription
    ///
    /// This should be called when a new durable peer connects. The relay will automatically
    /// attempt to register any pending predicates on the newly connected peer's subscription.
    pub fn notify_peer_connected(&self, peer_id: proto::EntityId) {
        debug!("SubscriptionRelay.notify_peer_connected() - Peer {} connected, registering predicates on peer subscription", peer_id);

        // Add to connected peers
        self.inner.connected_peers.insert(peer_id);

        // Trigger setup with all connected peers
        self.setup_remote_subscriptions();
    }

    /// Whether a query is established with, or being sent to, this peer.
    pub(crate) fn has_subscription_with_peer(&self, peer_id: &proto::EntityId) -> bool {
        self.inner
            .subscriptions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|state| matches!(&state.status, Status::Established(peer, _) | Status::Requested(peer, _) if peer == peer_id))
    }

    /// Get the current state of a predicate registration
    pub fn get_status(&self, query_id: proto::QueryId) -> Option<Status> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.get(&query_id).map(|info| info.status.clone())
    }

    /// Get all unique contexts for predicates established or requested with a specific peer
    /// TODO: update the data structure to do this via a direct lookup rather than having to scan the entire map
    pub fn get_contexts_for_peer(&self, peer_id: &proto::EntityId) -> std::collections::HashSet<CD> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        let mut contexts = std::collections::HashSet::new();

        for (_, state) in subscriptions.iter() {
            match &state.status {
                Status::Established(established_peer, _) | Status::Requested(established_peer, _) => {
                    if established_peer == peer_id {
                        contexts.extend(state.content.sessions.current());
                    }
                }
                _ => {}
            }
        }

        contexts
    }

    /// Register predicates on available durable peer subscriptions
    fn setup_remote_subscriptions(&self) {
        let node = match self.inner.node.get() {
            Some(node) => node,
            None => {
                warn!("No node configured for remote subscription setup");
                return;
            }
        };

        // For now, use the first available peer (could be made smarter)
        let connected_peers = self.inner.connected_peers.to_vec();
        if connected_peers.is_empty() {
            warn!("No durable peers available for remote subscription setup");
            return;
        }

        let target_peer = connected_peers[0];

        // Atomically get pending subscriptions and mark them as requested
        let pending: Vec<_> = {
            self.inner
                .subscriptions
                .lock()
                .expect("poisoned lock")
                .values_mut()
                .filter_map(|info| {
                    if let Status::PendingRemote = info.status {
                        info.status = Status::Requested(target_peer, info.content.version);
                        info.cancel.cancel_and_swap();
                        Some((info.content.clone(), info.cancel.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if pending.is_empty() {
            return;
        }

        debug!("Registering {} predicates on {} peer subscriptions", pending.len(), self.inner.connected_peers.len());

        for (content, cancel) in pending {
            crate::task::spawn(self.clone().attempt_subscribe(node.clone(), target_peer, content, cancel));
        }
    }

    async fn attempt_subscribe(self, node: Arc<dyn TNode<CD>>, target_peer: EntityId, content: Arc<Content<CD>>, cancel: CancelFlag) {
        let query_id = content.query_id;
        let predicate = content.selection.clone();
        // Credentials read at send time from the live source.
        let cdatas = content.sessions.current();
        let version = content.version;

        // Get the livequery for error handling
        let livequery =
            { self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).get(&query_id).map(|state| state.livequery.clone()) };

        // Call remote_subscribe which fetches known matches, subscribes, applies deltas, and stores events
        match node.remote_subscribe(target_peer, query_id, content.collection_id.clone(), predicate, cdatas, version).await {
            _ if cancel.canceled() => debug!("Ignoring superseded reply for predicate {}", query_id),
            Ok(()) => {
                // Deltas applied successfully, now activate the livequery
                // The livequery handles its own errors internally
                if let Some(lq) = livequery {
                    lq.subscription_established(version).await;
                }

                // Mark as established - subscription succeeded even if livequery activation had issues
                let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                if !cancel.canceled() {
                    if let Some(info) = subscriptions.get_mut(&query_id) {
                        info.status = Status::Established(target_peer, version);
                    }
                }
                debug!("Successfully registered predicate {} on peer {} subscription", query_id, target_peer);
            }
            Err(e) => {
                // Handle error with retry logic
                self.handle_error(query_id, target_peer, e, &cancel);
            }
        }
    }

    /// Start background task that periodically retries pending subscriptions
    fn start_retry_task(&self, mut shutdown_rx: tokio::sync::mpsc::Receiver<()>) {
        let me = self.weak();
        crate::task::spawn(async move {
            loop {
                let delay = futures_timer::Delay::new(std::time::Duration::from_secs(5));
                tokio::select! {
                    _ = delay => {
                        // Attempt to setup any pending subscriptions
                        let Some(relay) = me.upgrade() else { break };
                        relay.setup_remote_subscriptions();
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Subscription relay retry task stopped");
                        break;
                    }
                }
            }
        });
    }

    /// Handle errors with retry logic
    fn handle_error(&self, query_id: proto::QueryId, target_peer: proto::EntityId, error: RetrievalError, cancel: &CancelFlag) {
        let error_msg = error.to_string();

        // Evaluate retriability at failure time
        let is_retryable = match &error {
            // Retrieval errors from fetching are generally not retryable
            RetrievalError::RequestError(req_err) => match req_err {
                RequestError::PeerNotConnected => true,
                RequestError::ConnectionLost => true,
                RequestError::SystemNotReady => true,
                RequestError::SendError(_) => true,
                RequestError::InternalChannelClosed => true,
                RequestError::ServerError(_) => false,
                RequestError::UnexpectedResponse(_) => false,
                RequestError::AccessDenied(_) => false,
                RequestError::NodeNotReady | RequestError::NodeHalted(_) => false,
            },
            // Other retrieval errors are not retryable
            _ => false,
        };

        // Update state based on retriability
        let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        if !cancel.canceled() {
            if let Some(info) = subscriptions.get_mut(&query_id) {
                if is_retryable {
                    // Retryable errors go back to pending for retry by background task
                    info.status = Status::PendingRemote;
                    warn!("Retryable failure for predicate {} with peer {}: {} - will retry", query_id, target_peer, error_msg);
                } else {
                    // Non-retryable errors are permanently failed
                    info.status = Status::Failed;
                    tracing::error!("Permanent failure for predicate {} with peer {}: {} - no retry", query_id, target_peer, error_msg);

                    // Error listeners may reenter the relay.
                    let (query, version) = (info.livequery.clone(), info.content.version);
                    drop(subscriptions);
                    query.set_last_error(version, error);
                }
            }
        }
    }
}

/// Trait for communicating with remote peers (abstraction over WeakNode for testing)
#[async_trait]
pub trait TNode<CD: ContextData>: Send + Sync {
    /// Send a predicate registration request to a remote peer, fetch known matches,
    /// apply received deltas, and store used events.
    /// Returns Ok(()) if subscription was established and deltas applied successfully.
    async fn remote_subscribe(
        &self,
        peer_id: proto::EntityId,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        context_data: Vec<CD>,
        version: u32,
    ) -> Result<(), RetrievalError>;

    /// Send a predicate unregistration message to a remote peer
    /// This is a one-way message, no response expected
    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error>;
}

/// Implementation of TNode for WeakNode
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
    ) -> Result<(), RetrievalError> {
        let node = self.upgrade().ok_or(NodeDropped)?;
        node.system.system_epoch().ok_or(RequestError::SystemNotReady)?;

        // 1. Pre-fetch known_matches from local storage
        let known_matches: Vec<ankurah_proto::KnownEntity> = node
            .fetch_entities_from_local(&collection_id, &selection)
            .await?
            .into_iter()
            .map(|entity| ankurah_proto::KnownEntity { entity_id: entity.id(), head: entity.head() })
            .collect();

        // 2. Send subscribe request with known_matches
        let response = node
            .request(
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
            .await?;
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
        // 3. Apply deltas to local node using NodeApplier
        let collection = node.collections.get(&collection_id).await?;
        let event_getter = crate::retrieval::CachedEventGetter::new(collection_id, collection.clone(), &node, &context_data);
        let state_getter = crate::retrieval::LocalStateGetter::new(collection);
        crate::node::applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &event_getter, &state_getter).await?;

        Ok(())
    }

    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error> {
        let node = self.upgrade().ok_or(NodeDropped)?;

        // Use the existing request_remote_unsubscribe method
        node.request_remote_unsubscribe(query_id, vec![peer_id]).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EntityId;
    use ankurah_signals::Mut;
    use std::sync::{Arc, Mutex};

    // Note: Some tests call setup_remote_subscriptions() directly to test the core
    // subscription setup logic in isolation, while others use notify_peer_connected()
    // to test the full event-driven flow. Both approaches are valuable:
    // - Direct calls test the setup mechanism itself (error handling, state transitions)
    // - Event-driven calls test the integration and user-facing API

    // For testing, we'll use CollectionId as our ContextData
    impl ContextData for CollectionId {}

    /// Mock message sender for testing
    #[derive(Debug)]
    struct MockMessageSender<CD: ContextData> {
        next_error: Arc<Mutex<Option<RequestError>>>,
        sent_requests: Arc<Mutex<Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection<Resolved>)>>>,
        should_fail: Arc<Mutex<bool>>,
        failure_message: Arc<Mutex<String>>,
        held_reply: Mutex<Option<tokio::sync::oneshot::Receiver<Result<(), RequestError>>>>,
        _phantom: std::marker::PhantomData<CD>,
    }

    impl<CD: ContextData> MockMessageSender<CD> {
        fn new() -> Self {
            Self {
                sent_requests: Arc::new(Mutex::new(Vec::new())),
                next_error: Arc::new(Mutex::new(None)),
                should_fail: Arc::new(Mutex::new(false)),
                failure_message: Arc::new(Mutex::new(String::new())),
                held_reply: Mutex::new(None),
                _phantom: std::marker::PhantomData,
            }
        }

        fn set_fail_next(&self, error: RequestError) { *self.next_error.lock().unwrap() = Some(error); }

        fn get_sent_requests(&self) -> Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection<Resolved>)> {
            self.sent_requests.lock().unwrap().clone()
        }

        fn clear_sent_requests(&self) { self.sent_requests.lock().unwrap().clear(); }

        /// Parks the next remote_subscribe until the returned sender delivers its reply.
        fn hold_next_reply(&self) -> tokio::sync::oneshot::Sender<Result<(), RequestError>> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            *self.held_reply.lock().unwrap() = Some(rx);
            tx
        }
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
        ) -> Result<(), RetrievalError> {
            self.sent_requests.lock().unwrap().push((peer_id, query_id, collection_id.clone(), selection.clone()));

            let held = self.held_reply.lock().unwrap().take();
            if let Some(reply) = held {
                return reply.await.expect("held reply dropped").map_err(RetrievalError::RequestError);
            }
            // Check if there's an error to fail with
            if let Some(error) = self.next_error.lock().unwrap().take() {
                Err(RetrievalError::RequestError(error))
            } else {
                // Mock successful subscription (fetch, subscribe, apply, store all succeeded)
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

            // Check if there's an error to fail with
            if let Some(error) = self.next_error.lock().unwrap().take() {
                Err(anyhow!(error.to_string()))
            } else {
                Ok(())
            }
        }
    }

    // Mock implementation of RemoteQuerySubscriber for tests
    #[derive(Clone)]
    struct MockLiveQuery;

    thread_local! {
        /// Versions handed to `subscription_established` on this thread; the test runtime is current-thread.
        static ESTABLISHED: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    #[async_trait::async_trait]
    impl RemoteQuerySubscriber for MockLiveQuery {
        async fn subscription_established(&self, version: u32) { ESTABLISHED.with(|e| e.borrow_mut().push(version)) }

        fn set_last_error(&self, _version: u32, _error: RetrievalError) {
            // For tests, we don't track errors
        }
    }

    fn create_test_selection() -> ankql::ast::Selection<Resolved> {
        // Create a simple test predicate
        ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }
    }

    fn create_test_collection_id() -> CollectionId { CollectionId::from("test_collection") }

    #[tokio::test]
    async fn stopping_run_cancels_registrations_and_stops_retries() {
        let run = Mut::new(true);
        let relay = SubscriptionRelay::<CollectionId, MockLiveQuery>::new(&run);
        let query_id = proto::QueryId::new();
        relay.subscribe_query(query_id, create_test_collection_id(), create_test_selection(), SessionSet::new(), 1, MockLiveQuery);
        let cancel = relay.inner.subscriptions.lock().unwrap().get(&query_id).unwrap().cancel.clone();
        assert!(!cancel.canceled());

        run.set(false);
        assert!(cancel.canceled());
        assert!(relay.inner.subscriptions.lock().unwrap().is_empty());
        tokio::time::timeout(std::time::Duration::from_secs(2), relay.inner.shutdown_tx.closed()).await.unwrap();
    }

    fn connected_relay() -> (SubscriptionRelay<CollectionId, MockLiveQuery>, Arc<MockMessageSender<CollectionId>>, EntityId) {
        let relay = SubscriptionRelay::new_test();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");
        let peer_id = EntityId::random();
        relay.notify_peer_connected(peer_id);
        (relay, mock_sender, peer_id)
    }

    fn subscribe(relay: &SubscriptionRelay<CollectionId, MockLiveQuery>, query_id: proto::QueryId) {
        let collection_id = create_test_collection_id();
        relay.subscribe_query(query_id, collection_id.clone(), create_test_selection(), collection_id.into(), 1, MockLiveQuery);
    }

    /// Gives the relay's spawned tasks scheduler turns before the next assertion. The tests use the
    /// current-thread runtime and mocks that suspend only at explicit reply gates.
    async fn settle() {
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn test_new_subscription_setup() {
        let relay = SubscriptionRelay::new_test();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Notify of new subscription
        let reply = mock_sender.hold_next_reply();
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);

        settle().await;
        assert!(matches!(relay.get_status(query_id), Some(Status::Requested(peer, 0)) if peer == peer_id));

        // Verify request was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, query_id);
        assert_eq!(sent_requests[0].2, collection_id);

        // Verify subscription is marked as established
        reply.send(Ok(())).unwrap();
        settle().await;
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));
    }

    #[tokio::test]
    async fn test_peer_disconnection_orphans_subscriptions() {
        let relay = SubscriptionRelay::new_test();

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

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));

        // Simulate peer disconnection
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is marked as pending again
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));
    }

    #[tokio::test]
    async fn test_peer_connection_triggers_setup() {
        let relay = SubscriptionRelay::new_test();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Add pending subscription (no peers connected yet)
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

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
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));
    }

    #[tokio::test]
    async fn test_failed_subscription_retry() {
        let relay = SubscriptionRelay::new_test();
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
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));

        // Now test the retry behavior by disconnecting the peer (puts subscription back to PendingRemote)
        // then setting up the mock to fail, and reconnecting to trigger the retry
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is now in pending state
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

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
        let relay = SubscriptionRelay::new_test();
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
                info.status = Status::PendingRemote; // Retryable errors go back to pending
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
            matches!(relay.get_status(retryable_query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id)
        );
        assert!(matches!(relay.get_status(non_retryable_query_id), Some(Status::Failed)));
    }

    #[tokio::test]
    async fn test_subscription_removal() {
        let relay = SubscriptionRelay::new_test();
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

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));

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
    async fn test_edge_cases() {
        let relay = SubscriptionRelay::new_test();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::random();

        // Test setup without message sender - should not crash
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone().into(), 0, MockLiveQuery);
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should still be pending since no sender
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Now set sender and test with no connected peers
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should still be pending since no peers available
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Verify no requests were sent
        assert_eq!(mock_sender.get_sent_requests().len(), 0);

        // Now connect a peer (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Should now be established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));
        assert_eq!(mock_sender.get_sent_requests().len(), 1);
    }

    #[tokio::test]
    async fn test_notify_unsubscribe_with_no_established_subscription() {
        let relay = SubscriptionRelay::new_test();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();

        // Add subscription but don't establish it
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone().into(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

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

    /// A version-1 reply that arrives after version 2 was requested must not overwrite version 2's state,
    /// whether the late reply is a success or a permanent failure.
    #[tokio::test]
    async fn stale_reply_does_not_overwrite_newer_version() {
        for stale_reply in [Ok(()), Err(RequestError::ServerError("rejected".into()))] {
            let (relay, mock_sender, peer_id) = connected_relay();
            let query_id = proto::QueryId::new();
            let stale = mock_sender.hold_next_reply();
            subscribe(&relay, query_id);
            settle().await;
            assert!(matches!(relay.get_status(query_id), Some(Status::Requested(_, 1))));

            relay.update_query(query_id, create_test_selection(), 2).unwrap();
            settle().await;
            assert!(matches!(relay.get_status(query_id), Some(Status::Established(p, 2)) if p == peer_id));

            ESTABLISHED.take();
            stale.send(stale_reply).unwrap();
            settle().await;
            assert!(matches!(relay.get_status(query_id), Some(Status::Established(_, 2))), "{:?}", relay.get_status(query_id));
            assert_eq!(ESTABLISHED.take(), Vec::<u32>::new(), "a superseded reply must not activate the livequery");
        }
    }

    #[tokio::test]
    async fn replacing_a_registration_cancels_its_old_reply_and_keeps_its_peer() {
        let (relay, sender, peer) = connected_relay();
        let query_id = proto::QueryId::new();
        let stale = sender.hold_next_reply();
        subscribe(&relay, query_id);
        settle().await;
        relay.inner.connected_peers.remove(&peer);
        relay.notify_peer_connected(EntityId::random());

        relay.subscribe_query(
            query_id,
            create_test_collection_id(),
            create_test_selection(),
            create_test_collection_id().into(),
            2,
            MockLiveQuery,
        );
        settle().await;
        stale.send(Err(RequestError::ServerError("stale".into()))).unwrap();
        settle().await;

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(p, 2)) if p == peer));
        assert!(sender.get_sent_requests().iter().all(|(p, _, _, _)| *p == peer));
    }

    /// After a disconnect and reconnect, the reply to the pre-disconnect request must not disturb the
    /// re-registration, even though both carry the same version.
    #[tokio::test]
    async fn stale_reply_after_reconnect_is_ignored() {
        let (relay, mock_sender, peer_id) = connected_relay();
        let query_id = proto::QueryId::new();
        let stale = mock_sender.hold_next_reply();
        subscribe(&relay, query_id);
        settle().await;

        relay.notify_peer_disconnected(peer_id);
        relay.notify_peer_connected(peer_id);
        settle().await;
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(p, 1)) if p == peer_id));

        stale.send(Err(RequestError::ConnectionLost)).unwrap();
        settle().await;
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(_, 1))), "{:?}", relay.get_status(query_id));
    }

    /// While a registration request is in flight on a peer, an update and an unsubscribe go to that peer
    /// rather than to whichever peer the generic setup path would pick.
    #[tokio::test]
    async fn update_and_unsubscribe_while_requested_use_the_recorded_peer() {
        let (relay, mock_sender, peer_id) = connected_relay();
        let query_id = proto::QueryId::new();
        let _first_in_flight = mock_sender.hold_next_reply();
        subscribe(&relay, query_id);
        settle().await;
        // Leave only another peer in the connected set, so the generic setup path would not pick the recorded one
        relay.inner.connected_peers.remove(&peer_id);
        relay.notify_peer_connected(EntityId::random());

        let _second_in_flight = mock_sender.hold_next_reply();
        relay.update_query(query_id, create_test_selection(), 2).unwrap();
        settle().await;
        assert!(matches!(relay.get_status(query_id), Some(Status::Requested(p, 2)) if p == peer_id), "{:?}", relay.get_status(query_id));

        relay.unsubscribe_predicate(query_id);
        settle().await;
        let sent = mock_sender.get_sent_requests();
        assert_eq!(sent.len(), 3, "{sent:?}");
        assert!(sent.iter().all(|(peer, _, _, _)| *peer == peer_id), "{sent:?}");
        assert_eq!(sent[2].2, CollectionId::from("unsubscribe"));
    }

    /// The retry task must not own the relay, or dropping the last handle could never stop it.
    #[tokio::test]
    async fn retry_task_does_not_keep_relay_alive() {
        let relay = SubscriptionRelay::<CollectionId, MockLiveQuery>::new_test();
        let weak = relay.weak();
        drop(relay);
        assert!(weak.upgrade().is_none());
    }
}
