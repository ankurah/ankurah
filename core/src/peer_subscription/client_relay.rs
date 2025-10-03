// TODO: Rename this module from client_relay to remote_subscription for clarity
use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tracing::{debug, warn};

use crate::error::{RequestError, RetrievalError};
use crate::node::ContextData;
use crate::util::safeset::SafeSet;

/// Trait for query initialization that can be driven by SubscriptionRelay
/// Abstracts the relay's interaction with LiveQuery
#[async_trait::async_trait]
pub trait RemoteQuerySubscriber: Clone + Send + Sync + 'static {
    /// Called after remote subscription deltas have been applied
    /// Dispatches to initialize (version 1) or update_selection_init (version >1) internally
    /// Handles marking initialization as complete and setting last_error on failure
    async fn subscription_established(&self, version: u32);

    /// Set the last error for this subscription
    fn set_last_error(&self, error: RetrievalError);
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
    pub selection: ankql::ast::Selection,
    pub context_data: CD,
    pub version: u32,
}

pub struct RemoteQueryState<CD: ContextData, Q: RemoteQuerySubscriber> {
    pub content: Arc<Content<CD>>,
    pub status: Status,
    pub livequery: Q,
}

struct SubscriptionRelayInner<CD: ContextData, Q: RemoteQuerySubscriber> {
    // All subscription information in one place
    subscriptions: std::sync::Mutex<HashMap<proto::QueryId, RemoteQueryState<CD, Q>>>,
    // Track connected durable peers
    connected_peers: SafeSet<proto::EntityId>,
    // Node for communicating with remote peers
    node: OnceLock<Arc<dyn TNode<CD>>>,
    // Shutdown signal for retry task - when dropped, the task will stop
    _shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

/// Manages predicate registration on remote peer reactor subscriptions.
///
/// The SubscriptionRelay provides a resilient, event-driven approach to managing which predicates
/// are registered with remote durable peers. It automatically handles:
/// - Registering predicates on peer reactor subscriptions when peers connect
/// - Re-registering predicates when peers disconnect and reconnect
/// - Retrying failed predicate registration attempts
/// - Clean teardown when predicates are removed
/// - Storing ContextData for each predicate to enable proper authorization
///
/// This design separates predicate management concerns from the main Node implementation,
/// making it easier to test and reason about predicate lifecycle management.
///
/// # Public API (for Node integration)
///
/// - `subscribe_predicate()` - Call when local subscriptions are created (parallel to reactor.subscribe)
/// - `unsubscribe_predicate()` - Call when local subscriptions are removed (parallel to reactor.unsubscribe)
/// - `notify_peer_connected()` - Call when durable peers connect (triggers automatic predicate registration)
/// - `notify_peer_disconnected()` - Call when durable peers disconnect (orphans predicate registrations)
/// - `get_status()` - Query current state of a predicate registration
///
/// # Internal/Testing API
///
/// - `setup_remote_subscriptions()` - Internal method for triggering predicate registration with specific peers
///   (called automatically by notify_peer_connected, but exposed for testing)
///
/// The relay will automatically handle predicate registration/teardown asynchronously.
#[derive(Clone)]
pub struct SubscriptionRelay<CD: ContextData, Q: RemoteQuerySubscriber> {
    inner: Arc<SubscriptionRelayInner<CD, Q>>,
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> Default for SubscriptionRelay<CD, Q> {
    fn default() -> Self { Self::new() }
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> SubscriptionRelay<CD, Q> {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);

        let relay = Self {
            inner: Arc::new(SubscriptionRelayInner {
                subscriptions: std::sync::Mutex::new(HashMap::new()),
                connected_peers: SafeSet::new(),
                node: OnceLock::new(),
                _shutdown_tx: shutdown_tx,
            }),
        };

        // Start background retry task
        relay.start_retry_task(shutdown_rx);

        relay
    }

    /// Inject the node (typically a WeakNode for production)
    ///
    /// This should be called once during initialization. Returns an error if
    /// the node has already been set.
    pub fn set_node(&self, node: Arc<dyn TNode<CD>>) -> Result<(), ()> { self.inner.node.set(node).map_err(|_| ()) }

    /// Notify the relay that a new predicate needs to be registered on remote peer subscriptions
    ///
    /// This should be called whenever a local subscription is established. The relay will
    /// track this predicate and automatically attempt to register it with available durable peers.
    pub fn subscribe_query(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection,
        context_data: CD,
        version: u32,
        livequery: Q,
    ) {
        debug!("SubscriptionRelay.subscribe_predicate() - New predicate {} needs remote registration", query_id);
        {
            self.inner.subscriptions.lock().expect("poisoned lock").insert(
                query_id,
                RemoteQueryState {
                    content: Arc::new(Content { collection_id, selection, context_data, query_id, version }),
                    status: Status::PendingRemote,
                    livequery,
                },
            );
        }

        // Immediately attempt setup with available peers
        if !self.inner.connected_peers.is_empty() {
            self.setup_remote_subscriptions()
        }
    }
    pub fn update_query(&self, query_id: proto::QueryId, selection: ankql::ast::Selection, version: u32) -> Result<(), anyhow::Error> {
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
                        context_data: old_content.context_data.clone(),
                        query_id: old_content.query_id,
                        version,
                    });

                    match state.status {
                        Status::Established(peer_id, _old_version) => {
                            // Update to new version, mark as requested for this peer
                            state.status = Status::Requested(peer_id, version);
                            Some((peer_id, state.content.collection_id.clone(), state.content.context_data.clone()))
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
            Some((peer_id, collection_id, context_data)) => {
                self.update_query_on_peer(peer_id, query_id, collection_id, selection, version, context_data);
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
        selection: ankql::ast::Selection,
        version: u32,
        context_data: CD,
    ) {
        let me = self.clone();
        crate::task::spawn(async move {
            if let Some(node) = me.inner.node.get() {
                // Get the livequery for error handling
                let livequery = { me.inner.subscriptions.lock().unwrap().get(&query_id).map(|state| state.livequery.clone()) };

                // Send the updated predicate to the peer
                match node.remote_subscribe(peer_id, query_id, collection_id, selection, &context_data, version).await {
                    Ok(()) => {
                        // Deltas applied successfully, now activate the livequery
                        if let Some(lq) = livequery {
                            lq.subscription_established(version).await;
                        }

                        // Mark as established - subscription succeeded even if livequery activation had issues
                        let mut subscriptions = me.inner.subscriptions.lock().unwrap();
                        if let Some(info) = subscriptions.get_mut(&query_id) {
                            info.status = Status::Established(peer_id, version);
                        }
                        debug!("Successfully updated predicate {} on peer {} subscription", query_id, peer_id);
                    }
                    Err(e) => {
                        // Handle error with retry logic
                        me.handle_error(query_id, peer_id, e, livequery).await;
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
            let mut subscriptions = self.inner.subscriptions.lock().unwrap();
            if let Some(info) = subscriptions.remove(&query_id) {
                if let Status::Established(peer_id, _version) = &info.status {
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

    /// Get the current state of a predicate registration
    pub fn get_status(&self, query_id: proto::QueryId) -> Option<Status> {
        let subscriptions = self.inner.subscriptions.lock().unwrap();
        subscriptions.get(&query_id).map(|info| info.status.clone())
    }

    /// Get all unique contexts for predicates established or requested with a specific peer
    /// TODO: update the data structure to do this via a direct lookup rather than having to scan the entire map
    pub fn get_contexts_for_peer(&self, peer_id: &proto::EntityId) -> std::collections::HashSet<CD> {
        let subscriptions = self.inner.subscriptions.lock().unwrap();
        let mut contexts = std::collections::HashSet::new();

        for (_, state) in subscriptions.iter() {
            match &state.status {
                Status::Established(established_peer, _) | Status::Requested(established_peer, _) => {
                    if established_peer == peer_id {
                        contexts.insert(state.content.context_data.clone());
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
                        Some(info.content.clone())
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

        for content in pending {
            crate::task::spawn(self.clone().attempt_subscribe(node.clone(), target_peer, content));
        }
    }

    async fn attempt_subscribe(self, node: Arc<dyn TNode<CD>>, target_peer: proto::EntityId, content: Arc<Content<CD>>) {
        let query_id = content.query_id;
        let predicate = content.selection.clone();
        let context_data = content.context_data.clone();
        let version = content.version;

        // Get the livequery for error handling
        let livequery = { self.inner.subscriptions.lock().unwrap().get(&query_id).map(|state| state.livequery.clone()) };

        // Call remote_subscribe which fetches known matches, subscribes, applies deltas, and stores events
        match node.remote_subscribe(target_peer, query_id, content.collection_id.clone(), predicate, &context_data, version).await {
            Ok(()) => {
                // Deltas applied successfully, now activate the livequery
                // The livequery handles its own errors internally
                if let Some(lq) = livequery {
                    lq.subscription_established(version).await;
                }

                // Mark as established - subscription succeeded even if livequery activation had issues
                let mut subscriptions = self.inner.subscriptions.lock().unwrap();
                if let Some(info) = subscriptions.get_mut(&query_id) {
                    info.status = Status::Established(target_peer, version);
                }
                debug!("Successfully registered predicate {} on peer {} subscription", query_id, target_peer);
            }
            Err(e) => {
                // Handle error with retry logic
                self.handle_error(query_id, target_peer, e, livequery).await;
            }
        }
    }

    /// Start background task that periodically retries pending subscriptions
    fn start_retry_task(&self, mut shutdown_rx: tokio::sync::mpsc::Receiver<()>) {
        let me = self.clone();
        crate::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Attempt to setup any pending subscriptions
                        me.setup_remote_subscriptions();
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("Retry task shutting down - SubscriptionRelay dropped");
                        break;
                    }
                }
            }
        });
    }

    /// Handle errors with retry logic
    async fn handle_error(&self, query_id: proto::QueryId, target_peer: proto::EntityId, error: RetrievalError, livequery: Option<Q>) {
        let error_msg = error.to_string();

        // Evaluate retriability at failure time
        let is_retryable = match &error {
            // Retrieval errors from fetching are generally not retryable
            RetrievalError::RequestError(req_err) => match req_err {
                RequestError::PeerNotConnected => true,
                RequestError::ConnectionLost => true,
                RequestError::SendError(_) => true,
                RequestError::InternalChannelClosed => true,
                RequestError::ServerError(_) => false,
                RequestError::UnexpectedResponse(_) => false,
            },
            // Other retrieval errors are not retryable
            _ => false,
        };

        // Update state based on retriability
        let mut subscriptions = self.inner.subscriptions.lock().unwrap();
        if let Some(info) = subscriptions.get_mut(&query_id) {
            if is_retryable {
                // Retryable errors go back to pending for retry by background task
                info.status = Status::PendingRemote;
                warn!("Retryable failure for predicate {} with peer {}: {} - will retry", query_id, target_peer, error_msg);
            } else {
                // Non-retryable errors are permanently failed
                info.status = Status::Failed;
                tracing::error!("Permanent failure for predicate {} with peer {}: {} - no retry", query_id, target_peer, error_msg);

                // Set error on livequery
                if let Some(lq) = livequery {
                    lq.set_last_error(error);
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
        selection: ankql::ast::Selection,
        context_data: &CD,
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
        selection: ankql::ast::Selection,
        context_data: &PA::ContextData,
        version: u32,
    ) -> Result<(), RetrievalError> {
        let node = self.upgrade().ok_or_else(|| RetrievalError::Other("Node has been dropped".to_string()))?;

        // 1. Pre-fetch known_matches from local storage
        let known_matches: Vec<ankurah_proto::KnownEntity> = node
            .fetch_entities_from_local(&collection_id, &selection)
            .await?
            .into_iter()
            .map(|entity| ankurah_proto::KnownEntity { entity_id: entity.id(), head: entity.head() })
            .collect();

        // 2. Send subscribe request with known_matches
        let deltas = match node
            .request(
                peer_id,
                context_data,
                ankurah_proto::NodeRequestBody::SubscribeQuery {
                    query_id,
                    collection: collection_id.clone(),
                    selection: selection.clone(),
                    version,
                    known_matches,
                },
            )
            .await
            .map_err(|e| RetrievalError::RequestError(e))?
        {
            ankurah_proto::NodeResponseBody::QuerySubscribed { query_id: _response_query_id, deltas } => deltas,
            ankurah_proto::NodeResponseBody::Error(e) => return Err(RetrievalError::RequestError(RequestError::ServerError(e))),
            other => return Err(RetrievalError::RequestError(RequestError::UnexpectedResponse(other))),
        };

        // 3. Apply deltas to local node using NodeApplier
        let retriever = crate::retrieval::EphemeralNodeRetriever::new(collection_id, &node, context_data);
        let apply_result = crate::node_applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &retriever).await;
        let event_store_result = retriever.store_used_events().await;

        apply_result?; // apply result is more important than event store result
        event_store_result?;

        Ok(())
    }

    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error> {
        let node = self.upgrade().ok_or_else(|| anyhow!("Node has been dropped"))?;

        // Use the existing request_remote_unsubscribe method
        node.request_remote_unsubscribe(query_id, vec![peer_id]).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::Predicate;
    use ankurah_proto::EntityId;
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
        sent_requests: Arc<Mutex<Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection)>>>,
        should_fail: Arc<Mutex<bool>>,
        failure_message: Arc<Mutex<String>>,
        _phantom: std::marker::PhantomData<CD>,
    }

    impl<CD: ContextData> MockMessageSender<CD> {
        fn new() -> Self {
            Self {
                sent_requests: Arc::new(Mutex::new(Vec::new())),
                next_error: Arc::new(Mutex::new(None)),
                should_fail: Arc::new(Mutex::new(false)),
                failure_message: Arc::new(Mutex::new(String::new())),
                _phantom: std::marker::PhantomData,
            }
        }

        fn set_fail_next(&self, error: RequestError) { *self.next_error.lock().unwrap() = Some(error); }

        fn get_sent_requests(&self) -> Vec<(EntityId, proto::QueryId, CollectionId, ankql::ast::Selection)> {
            self.sent_requests.lock().unwrap().clone()
        }

        fn clear_sent_requests(&self) { self.sent_requests.lock().unwrap().clear(); }
    }

    #[async_trait]
    impl<CD: ContextData> TNode<CD> for MockMessageSender<CD> {
        async fn remote_subscribe(
            &self,
            peer_id: EntityId,
            query_id: proto::QueryId,
            collection_id: CollectionId,
            selection: ankql::ast::Selection,
            _context_data: &CD,
            _version: u32,
        ) -> Result<(), RetrievalError> {
            self.sent_requests.lock().unwrap().push((peer_id, query_id, collection_id.clone(), selection.clone()));

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

    #[async_trait::async_trait]
    impl RemoteQuerySubscriber for MockLiveQuery {
        async fn subscription_established(&self, _version: u32) {
            // Mock - no-op
        }

        fn set_last_error(&self, _error: RetrievalError) {
            // For tests, we don't track errors
        }
    }

    fn create_test_selection() -> ankql::ast::Selection {
        // Create a simple test predicate
        ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }
    }

    fn create_test_collection_id() -> CollectionId { CollectionId::from("test_collection") }

    #[tokio::test]
    async fn test_new_subscription_setup() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Notify of new subscription
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);

        // Check initial state - subscription should immediately go to Requested state since peer is connected
        assert!(matches!(relay.get_status(query_id), Some(Status::Requested(_, _))));

        // Give async task time to complete (setup should happen automatically)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify request was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, query_id);
        assert_eq!(sent_requests[0].2, collection_id);

        // Verify subscription is marked as established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));
    }

    #[tokio::test]
    async fn test_peer_disconnection_orphans_subscriptions() {
        let relay = SubscriptionRelay::new();

        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Setup established subscription by going through the full flow
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone(), 0, MockLiveQuery);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));

        // Simulate peer disconnection
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is marked as pending again
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));
    }

    #[tokio::test]
    async fn test_peer_connection_triggers_setup() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Add pending subscription (no peers connected yet)
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Clear any previous requests
        mock_sender.clear_sent_requests();

        // Simulate peer connection (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Connect peer and add subscription (should succeed initially)
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        let peer_id = EntityId::new();

        // Add subscriptions
        relay.subscribe_query(retryable_query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);
        relay.subscribe_query(non_retryable_query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);

        // Manually set different failure types - retryable goes back to pending, non-retryable stays failed
        {
            let mut subscriptions = relay.inner.subscriptions.lock().unwrap();
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
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Connect peer and setup established subscription
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone(), 0, MockLiveQuery);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));

        // Clear previous requests to focus on unsubscribe
        mock_sender.clear_sent_requests();

        // Remove subscription
        relay.unsubscribe_predicate(query_id);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

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
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Test setup without message sender - should not crash
        relay.subscribe_query(query_id, collection_id.clone(), predicate.clone(), collection_id.clone(), 0, MockLiveQuery);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should still be pending since no sender
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Now set sender and test with no connected peers
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should still be pending since no peers available
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Verify no requests were sent
        assert_eq!(mock_sender.get_sent_requests().len(), 0);

        // Now connect a peer (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should now be established
        assert!(matches!(relay.get_status(query_id), Some(Status::Established(established_peer_id, _)) if established_peer_id == peer_id));
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
        relay.subscribe_query(query_id, collection_id.clone(), predicate, collection_id.clone(), 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Unsubscribe from pending subscription
        relay.unsubscribe_predicate(query_id);

        // Give async task time to complete (though no request should be sent)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify no unsubscribe message was sent (since it wasn't established)
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 0);

        // Verify subscription is gone
        assert!(matches!(relay.get_status(query_id), None));
    }
}
