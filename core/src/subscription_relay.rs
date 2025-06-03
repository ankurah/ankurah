use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use async_trait::async_trait;
use std::sync::{Arc, OnceLock};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::entity::Entity;
use crate::error::{RequestError, RetrievalError};
use crate::node::ContextData;
use crate::subscription::Subscription;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;

#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionState {
    PendingRemote,                // Waiting for remote setup
    Established(proto::EntityId), // Successfully established with peer
    Failed(String),               // Failed to establish, needs retry
}

#[derive(Clone)]
pub struct SubscriptionInfo<CD: ContextData> {
    pub collection_id: CollectionId,
    pub predicate: ankql::ast::Predicate,
    pub context_data: CD,
    pub state: SubscriptionState,
    /// Signal when first Resultset is received from remote peer
    pub first_resultset_signal: Arc<std::sync::Mutex<Option<oneshot::Sender<Vec<proto::EntityId>>>>>,
}

/// Abstracted Node interface for subscription relay integration
#[async_trait]
pub trait TNode<CD: ContextData>: Send + Sync {
    /// Send a subscription request to a remote peer
    /// Returns Ok(()) if the subscription was successfully established (NodeResponseBody::Subscribed),
    /// Err(RequestError) for any error or non-success response
    async fn peer_subscribe(
        &self,
        peer_id: proto::EntityId,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
        context_data: &CD,
    ) -> Result<(), RequestError>;

    /// Send an unsubscribe message to a remote peer
    /// This is a one-way message, no response expected
    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, sub_id: proto::SubscriptionId) -> Result<(), anyhow::Error>;

    /// Get entities from peer to refresh stale data
    async fn get_from_peer(
        &self,
        collection_id: &CollectionId,
        entity_ids: Vec<proto::EntityId>,
        context_data: &CD,
    ) -> Result<(), RetrievalError>;
}

/// Abstracted Reactor interface for subscription relay integration
pub trait TReactor<R>: Send + Sync {
    /// Get subscription by ID for stale entity detection
    fn get_subscription(&self, sub_id: proto::SubscriptionId) -> Option<Subscription<R>>;
}

struct SubscriptionRelayInner<R, CD: ContextData> {
    // All subscription information in one place
    subscriptions: SafeMap<proto::SubscriptionId, SubscriptionInfo<CD>>,
    // Track connected durable peers
    connected_peers: SafeSet<proto::EntityId>,
    // Node interface for communicating with remote peers
    node: OnceLock<Arc<dyn TNode<CD>>>,
    // Reactor interface for managing local subscriptions
    reactor: Arc<dyn TReactor<R>>,
}

/// Manages subscription state and handles remote subscription setup/teardown for ephemeral nodes.
///
/// The SubscriptionRelay provides a resilient, event-driven approach to managing subscriptions
/// with remote durable peers. It automatically handles:
/// - Setting up remote subscriptions when peers connect
/// - Orphaning subscriptions when peers disconnect (marking them for re-setup)
/// - Retrying failed subscription attempts
/// - Clean teardown when subscriptions are removed
/// - Storing ContextData for each subscription to enable proper authorization
/// - Stale entity detection and refresh
///
/// This design separates subscription management concerns from the main Node implementation,
/// making it easier to test and reason about subscription lifecycle management.
///
/// # Public API (for Node integration)
///
/// - `register()` - Call when local subscriptions are created (parallel to reactor.subscribe)
/// - `notify_unsubscribe()` - Call when local subscriptions are removed (parallel to reactor.unsubscribe)
/// - `notify_peer_connected()` - Call when durable peers connect (triggers automatic setup)
/// - `notify_peer_disconnected()` - Call when durable peers disconnect (orphans subscriptions)
/// - `get_subscription_state()` - Query current state of a subscription
///
/// # Internal/Testing API
///
/// - `setup_remote_subscriptions()` - Internal method for triggering setup with specific peers
///   (called automatically by notify_peer_connected, but exposed for testing)
///
/// The relay will automatically handle remote setup/teardown asynchronously.

pub struct SubscriptionRelay<R, CD: ContextData> {
    inner: Arc<SubscriptionRelayInner<R, CD>>,
}
impl<R, CD: ContextData> Clone for SubscriptionRelay<R, CD> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<R: 'static, CD: ContextData> SubscriptionRelay<R, CD> {
    pub fn new(reactor: Arc<dyn TReactor<R>>) -> Self {
        Self {
            inner: Arc::new(SubscriptionRelayInner {
                subscriptions: SafeMap::new(),
                connected_peers: SafeSet::new(),
                node: OnceLock::new(),
                reactor,
            }),
        }
    }

    /// Inject the node interface (typically a WeakNode for production)
    ///
    /// This should be called once during initialization. Returns an error if
    /// the node has already been set.
    pub fn set_node(&self, node: Arc<dyn TNode<CD>>) -> Result<(), ()> { self.inner.node.set(node).map_err(|_| ()) }

    /// Register a new subscription and return a channel to wait for first remote data
    ///
    /// This method will:
    /// 1. Register the subscription with remote peers
    /// 2. Returns a oneshot receiver that will be signaled when first update is received from remote peer
    ///
    /// The caller can use this receiver to wait for remote data before proceeding (or optionally just ignore it).
    pub fn register(
        &self,
        subscription: Subscription<R>,
        context_data: CD,
    ) -> Result<tokio::sync::oneshot::Receiver<Vec<proto::EntityId>>, RetrievalError> {
        let sub_id = subscription.id;
        let collection_id = subscription.collection_id.clone();
        let predicate = subscription.predicate.clone();

        debug!("Registering subscription {}", sub_id);

        let (tx, rx) = oneshot::channel();
        self.inner.subscriptions.insert(
            sub_id,
            SubscriptionInfo {
                collection_id,
                predicate,
                context_data,
                state: SubscriptionState::PendingRemote,
                first_resultset_signal: Arc::new(std::sync::Mutex::new(Some(tx))),
            },
        );

        // Immediately attempt setup with available peers
        if !self.inner.connected_peers.is_empty() {
            self.setup_remote_subscriptions();
        }

        Ok(rx)
    }

    // TODO move this to subscription handle.loaded().await
    pub async fn register_and_wait_first_update(&self, subscription: Subscription<R>, context_data: CD) -> Result<(), RetrievalError> {
        let sub_id = subscription.id;
        let collection_id = subscription.collection_id.clone();
        let predicate = subscription.predicate.clone();

        let (tx, rx) = oneshot::channel();
        self.inner.subscriptions.insert(
            sub_id,
            SubscriptionInfo {
                collection_id,
                predicate,
                context_data,
                state: SubscriptionState::PendingRemote,
                first_resultset_signal: Arc::new(std::sync::Mutex::new(Some(tx))),
            },
        );

        // Immediately attempt setup with available peers
        if !self.inner.connected_peers.is_empty() {
            self.setup_remote_subscriptions();
        }

        // Wait for first data to arrive
        if let Err(_) = rx.await {
            warn!("Failed to receive first remote update for subscription {}", sub_id);
        }

        Ok(())
    }

    /// Signal that first remote data has been applied for a subscription
    /// This is called by the node when initial subscription updates are processed
    pub async fn notify_applied_initial_state(
        &self,
        sub_id: proto::SubscriptionId,
        initial_entity_ids: Vec<proto::EntityId>,
    ) -> Result<(), RetrievalError> {
        println!("🏁 SubscriptionRelay: Applied initial state for subscription {} with {} entities", sub_id, initial_entity_ids.len());

        let Some(info) = self.inner.subscriptions.get(&sub_id) else {
            return Err(RetrievalError::Other(format!("Subscription {} not found", sub_id)));
        };

        // Signal first data arrival with the entity IDs
        if let Some(signal) = info.first_resultset_signal.lock().unwrap().take() {
            // Send the initial entity IDs to whoever is waiting
            let _ = signal.send(initial_entity_ids);
        }
        Ok(())
    }

    /// Notify the relay that a subscription has been removed locally
    ///
    /// This will clean up all tracking state and send unsubscribe requests to any
    /// remote peers that have this subscription established.
    pub fn notify_unsubscribe(&self, sub_id: proto::SubscriptionId) {
        debug!("Unsubscribing from subscription {}", sub_id);

        // If subscription was established with a peer, send unsubscribe request
        if let Some(info) = self.inner.subscriptions.get(&sub_id) {
            if let SubscriptionState::Established(peer_id) = info.state {
                if let Some(sender) = self.inner.node.get() {
                    let sender = sender.clone();
                    crate::task::spawn(async move {
                        if let Err(e) = sender.peer_unsubscribe(peer_id, sub_id).await {
                            warn!("Failed to send unsubscribe message for {}: {}", sub_id, e);
                        } else {
                            debug!("Successfully sent unsubscribe message for {}", sub_id);
                        }
                    });
                }
            }
        }

        // Clean up all state
        debug!("Removing subscription {} from relay", sub_id);
        self.inner.subscriptions.remove(&sub_id);
    }

    /// Get all subscriptions that need remote setup
    pub fn get_pending_subscriptions(&self) -> Vec<(proto::SubscriptionId, SubscriptionInfo<CD>)> {
        self.inner
            .subscriptions
            .to_vec()
            .into_iter()
            .filter(|(_, info)| matches!(info.state, SubscriptionState::PendingRemote | SubscriptionState::Failed(_)))
            .collect()
    }

    /// Handle peer disconnection - mark all subscriptions for that peer as needing setup
    ///
    /// This should be called when a durable peer disconnects. All subscriptions established
    /// with that peer will be marked as pending and will be automatically re-established
    /// when the peer reconnects or another suitable peer becomes available.
    pub fn notify_peer_disconnected(&self, peer_id: proto::EntityId) {
        debug!("Peer {} disconnected, orphaning subscriptions", peer_id);

        // Remove from connected peers
        self.inner.connected_peers.remove(&peer_id);

        for (sub_id, info) in self.inner.subscriptions.to_vec() {
            if let SubscriptionState::Established(established_peer_id) = info.state {
                if established_peer_id == peer_id {
                    // Update state to pending while preserving existing data
                    if let Some(mut info) = self.inner.subscriptions.get(&sub_id) {
                        info.state = SubscriptionState::PendingRemote;
                        self.inner.subscriptions.insert(sub_id, info);
                        debug!("Subscription {} orphaned due to peer {} disconnect", sub_id, peer_id);
                    }
                }
            }
        }
    }

    /// Handle peer connection - trigger remote subscription setup
    ///
    /// This should be called when a new durable peer connects. The relay will automatically
    /// attempt to establish any pending subscriptions with the newly connected peer.
    pub fn notify_peer_connected(&self, peer_id: proto::EntityId) {
        debug!("Peer {} connected, setting up remote subscriptions", peer_id);

        // Add to connected peers
        self.inner.connected_peers.insert(peer_id);

        // Trigger setup with all connected peers
        self.setup_remote_subscriptions();
    }

    /// Get the current state of a subscription
    pub fn get_subscription_state(&self, sub_id: proto::SubscriptionId) -> Option<SubscriptionState> {
        self.inner.subscriptions.get(&sub_id).map(|info| info.state.clone())
    }

    /// Setup remote subscriptions with available durable peers (internal/testing use)
    ///
    /// This method is called automatically by `notify_peer_connected()` and should not
    /// normally be called directly in production code. It's exposed as `pub(crate)` to
    /// allow testing of the subscription setup logic with specific peer lists.
    ///
    /// The method spawns an async task to attempt establishing pending subscriptions
    /// with the provided list of available peers. It's non-blocking and will handle
    /// failures gracefully by marking subscriptions as failed for later retry.
    ///
    /// # Arguments
    /// * `available_peers` - List of peer IDs to attempt subscription setup with
    pub(crate) fn setup_remote_subscriptions(&self) {
        let relay = (*self).clone();
        crate::task::spawn(async move {
            let sender = match relay.inner.node.get() {
                Some(sender) => sender,
                None => {
                    warn!("No node configured for remote subscription setup");
                    return;
                }
            };

            if relay.inner.connected_peers.is_empty() {
                debug!("No durable peers available for remote subscription setup");
                return;
            }

            let pending = relay.get_pending_subscriptions();
            if pending.is_empty() {
                debug!("No pending subscriptions to set up remotely");
                return;
            }

            debug!("Setting up {} remote subscriptions with {} peers", pending.len(), relay.inner.connected_peers.len());

            // For now, use the first available peer (could be made smarter)
            let connected_peers = relay.inner.connected_peers.to_vec();
            let target_peer = connected_peers[0];

            for (sub_id, info) in pending {
                match sender
                    .peer_subscribe(target_peer, sub_id, info.collection_id.clone(), info.predicate.clone(), &info.context_data)
                    .await
                {
                    Ok(()) => {
                        // Mark as established - update the state while preserving existing data
                        if let Some(mut updated_info) = relay.inner.subscriptions.get(&sub_id) {
                            updated_info.state = SubscriptionState::Established(target_peer);
                            relay.inner.subscriptions.insert(sub_id, updated_info);
                        }
                        debug!("Successfully established remote subscription {} with peer {}", sub_id, target_peer);
                    }
                    Err(e) => {
                        // Mark as failed - update the state while preserving existing data
                        if let Some(mut updated_info) = relay.inner.subscriptions.get(&sub_id) {
                            updated_info.state = SubscriptionState::Failed(e.to_string());
                            relay.inner.subscriptions.insert(sub_id, updated_info);
                        }
                        warn!("Failed to establish remote subscription {} with peer {}: {}", sub_id, target_peer, e);
                    }
                }
            }
        });
    }

    /// Get the node interface for making remote calls (used by RemoteEntityRetriever)
    pub(crate) fn get_node(&self) -> Option<Arc<dyn TNode<CD>>> { self.inner.node.get().cloned() }
}

/// Implementation of TReactor for the real Reactor
impl<SE, PA> TReactor<Entity> for crate::reactor::Reactor<SE, PA>
where
    SE: crate::storage::StorageEngine + Send + Sync + 'static,
    PA: crate::policy::PolicyAgent + Send + Sync + 'static,
{
    fn get_subscription(&self, sub_id: proto::SubscriptionId) -> Option<Subscription<Entity>> { self.get_subscription(sub_id) }
}

/// Implementation of TNode for WeakNode to enable subscription relay integration
#[async_trait]
impl<SE, PA> TNode<PA::ContextData> for crate::node::WeakNode<SE, PA>
where
    SE: crate::storage::StorageEngine + Send + Sync + 'static,
    PA: crate::policy::PolicyAgent + Send + Sync + 'static,
{
    async fn peer_subscribe(
        &self,
        peer_id: proto::EntityId,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
        context_data: &PA::ContextData,
    ) -> Result<(), RequestError> {
        let node = self.upgrade().ok_or(RequestError::InternalChannelClosed)?;

        match node
            .request(
                peer_id,
                context_data,
                ankurah_proto::NodeRequestBody::Subscribe { subscription_id: sub_id, collection: collection_id, predicate },
            )
            .await?
        {
            ankurah_proto::NodeResponseBody::Subscribed { subscription_id: _ } => Ok(()),
            ankurah_proto::NodeResponseBody::Error(_) => Err(RequestError::ConnectionLost),
            _ => Err(RequestError::ConnectionLost),
        }
    }

    async fn peer_unsubscribe(&self, peer_id: proto::EntityId, sub_id: proto::SubscriptionId) -> Result<(), anyhow::Error> {
        let node = self.upgrade().ok_or_else(|| anyhow!("Node has been dropped"))?;

        // Use the existing request_remote_unsubscribe method
        node.request_remote_unsubscribe(sub_id, vec![peer_id]).await?;

        Ok(())
    }

    async fn get_from_peer(
        &self,
        collection_id: &CollectionId,
        entity_ids: Vec<proto::EntityId>,
        context_data: &PA::ContextData,
    ) -> Result<(), RetrievalError> {
        let node = self.upgrade().ok_or_else(|| RetrievalError::Other("node has been dropped".to_string()))?;

        node.get_from_peer(collection_id, entity_ids, context_data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::Subscription;
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

    /// Mock reactor for testing
    struct MockReactor;

    impl TReactor<()> for MockReactor {
        fn get_subscription(&self, _sub_id: proto::SubscriptionId) -> Option<Subscription<()>> { None }
    }

    #[async_trait::async_trait]
    impl crate::retrieve::Fetch<()> for MockReactor {
        async fn fetch(
            self: Self,
            _collection_id: &CollectionId,
            _predicate: &ankql::ast::Predicate,
        ) -> Result<Vec<()>, crate::error::RetrievalError> {
            Ok(vec![])
        }
    }

    /// Mock message sender for testing
    #[derive(Debug)]
    struct MockMessageSender<CD: ContextData> {
        sent_requests: Arc<Mutex<Vec<(EntityId, proto::SubscriptionId, CollectionId, Predicate)>>>,
        should_fail: Arc<Mutex<bool>>,
        failure_message: Arc<Mutex<String>>,
        _phantom: std::marker::PhantomData<CD>,
    }

    impl<CD: ContextData> MockMessageSender<CD> {
        fn new() -> Self {
            Self {
                sent_requests: Arc::new(Mutex::new(Vec::new())),
                should_fail: Arc::new(Mutex::new(false)),
                failure_message: Arc::new(Mutex::new("Mock failure".to_string())),
                _phantom: std::marker::PhantomData,
            }
        }

        fn set_should_fail(&self, should_fail: bool, message: Option<String>) {
            *self.should_fail.lock().unwrap() = should_fail;
            if let Some(msg) = message {
                *self.failure_message.lock().unwrap() = msg;
            }
        }

        fn get_sent_requests(&self) -> Vec<(EntityId, proto::SubscriptionId, CollectionId, Predicate)> {
            self.sent_requests.lock().unwrap().clone()
        }

        fn clear_sent_requests(&self) { self.sent_requests.lock().unwrap().clear(); }
    }

    #[async_trait]
    impl<CD: ContextData> TNode<CD> for MockMessageSender<CD> {
        async fn peer_subscribe(
            &self,
            peer_id: EntityId,
            sub_id: proto::SubscriptionId,
            collection_id: CollectionId,
            predicate: Predicate,
            _context_data: &CD,
        ) -> Result<(), RequestError> {
            self.sent_requests.lock().unwrap().push((peer_id, sub_id, collection_id, predicate));

            if *self.should_fail.lock().unwrap() {
                Err(RequestError::ConnectionLost)
            } else {
                Ok(())
            }
        }

        async fn peer_unsubscribe(&self, peer_id: EntityId, sub_id: proto::SubscriptionId) -> Result<(), anyhow::Error> {
            self.sent_requests.lock().unwrap().push((peer_id, sub_id, CollectionId::from("unsubscribe"), Predicate::True));
            Ok(())
        }

        async fn get_from_peer(
            &self,
            _collection_id: &CollectionId,
            _entity_ids: Vec<proto::EntityId>,
            _context_data: &CD,
        ) -> Result<(), RetrievalError> {
            // Mock implementation - just succeed
            Ok(())
        }
    }
    fn create_test_predicate() -> Predicate {
        // Create a simple test predicate
        Predicate::True
    }

    fn create_test_collection_id() -> CollectionId { CollectionId::from("test_collection") }

    #[tokio::test]
    async fn test_new_subscription_setup() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Create mock subscription and register it
        let sub = Subscription::new(sub_id, collection_id.clone(), predicate, Arc::new(Box::new(|_| {})));
        let _rx = relay.register(sub, collection_id.clone())?;

        // Check initial state
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

        // Give async task time to complete (setup should happen automatically)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify request was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, sub_id);
        assert_eq!(sent_requests[0].2, collection_id);

        // Verify subscription is marked as established
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_disconnection_orphans_subscriptions() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Connect the peer first
        relay.notify_peer_connected(peer_id);

        // Setup established subscription by going through the full flow
        let subscription =
            crate::subscription::Subscription::new(sub_id, collection_id.clone(), predicate.clone(), Arc::new(Box::new(|_| {})));
        let _rx = relay.register(subscription, collection_id.clone())?;

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));

        // Simulate peer disconnection
        relay.notify_peer_disconnected(peer_id);

        // Verify subscription is marked as pending again
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_connection_triggers_setup() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Add pending subscription (no peers connected yet)
        let subscription =
            crate::subscription::Subscription::new(sub_id, collection_id.clone(), predicate.clone(), Arc::new(Box::new(|_| {})));
        let _rx = relay.register(subscription, collection_id.clone())?;
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

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
        assert_eq!(sent_requests[0].1, sub_id);

        // Verify subscription is established
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_subscription_retry() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Configure mock to fail
        mock_sender.set_should_fail(true, Some("Connection lost".to_string()));

        // Connect peer and add subscription
        relay.notify_peer_connected(peer_id);
        let sub = Subscription::new(sub_id, collection_id.clone(), predicate, Arc::new(Box::new(|_| {})));
        let _rx = relay.register(sub, collection_id.clone())?;

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify subscription is marked as failed
        match relay.get_subscription_state(sub_id) {
            Some(SubscriptionState::Failed(msg)) => {
                assert!(msg.contains("Connection lost"));
            }
            other => panic!("Expected Failed state, got {:?}", other),
        }

        // Clear requests and configure mock to succeed
        mock_sender.clear_sent_requests();
        mock_sender.set_should_fail(false, None);

        // TODO BEFORE MERGE - I don't think this should be pub. Are we even retrying automatically on failure?
        // Retry setup
        relay.setup_remote_subscriptions();

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify retry was attempted
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);

        // Verify subscription is now established
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));

        Ok(())
    }

    #[tokio::test]
    async fn test_subscription_removal() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Connect peer and setup established subscription
        relay.notify_peer_connected(peer_id);
        let subscription =
            crate::subscription::Subscription::new(sub_id, collection_id.clone(), predicate.clone(), Arc::new(Box::new(|_| {})));
        let _rx = relay.register(subscription, collection_id.clone())?;

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));

        // Clear previous requests to focus on unsubscribe
        mock_sender.clear_sent_requests();

        // Remove subscription
        relay.notify_unsubscribe(sub_id);

        // Give async task time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify unsubscribe message was sent
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 1);
        assert_eq!(sent_requests[0].0, peer_id);
        assert_eq!(sent_requests[0].1, sub_id);

        // Verify subscription is gone
        assert_eq!(relay.get_subscription_state(sub_id), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_edge_cases() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();
        let peer_id = EntityId::new();

        // Test setup without node - should not crash
        let subscription = Subscription::new(sub_id, collection_id.clone(), predicate, Arc::new(Box::new(|_| {})));
        let _rx = relay.register(subscription, collection_id.clone())?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should still be pending since no node
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

        // Now set node and test with no connected peers
        relay.set_node(mock_sender.clone()).expect("Failed to set node");
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should still be pending since no peers available
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

        // Verify no requests were sent
        assert_eq!(mock_sender.get_sent_requests().len(), 0);

        // Now connect a peer (should trigger automatic setup)
        relay.notify_peer_connected(peer_id);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should now be established
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::Established(peer_id)));
        assert_eq!(mock_sender.get_sent_requests().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_notify_unsubscribe_with_no_established_subscription() -> anyhow::Result<()> {
        let mock_reactor = Arc::new(MockReactor);
        let relay: SubscriptionRelay<(), CollectionId> = SubscriptionRelay::new(mock_reactor);
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set node");

        let sub_id = proto::SubscriptionId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_predicate();

        // Add subscription but don't establish it
        let subscription =
            crate::subscription::Subscription::new(sub_id, collection_id.clone(), predicate.clone(), Arc::new(Box::new(|_| {})));
        let _rx = relay.register(subscription, collection_id.clone())?;
        assert_eq!(relay.get_subscription_state(sub_id), Some(SubscriptionState::PendingRemote));

        // Unsubscribe from pending subscription
        relay.notify_unsubscribe(sub_id);

        // Give async task time to complete (though no request should be sent)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify no unsubscribe message was sent (since it wasn't established)
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 0);

        // Verify subscription is gone
        assert_eq!(relay.get_subscription_state(sub_id), None);

        Ok(())
    }
}
