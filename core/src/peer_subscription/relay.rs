use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tracing::{debug, warn};

use crate::error::{RequestError, RetrievalError};
use crate::node::ContextData;
use crate::util::request_fence::RequestValidity;
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
    /// Optional owner lifecycle fence (for example, a catalog-warm
    /// generation). It is combined with relay attempt generation before a
    /// response may mutate local state.
    pub validity: Option<RequestValidity>,
}

pub struct RemoteQueryState<CD: ContextData, Q: RemoteQuerySubscriber> {
    pub content: Arc<Content<CD>>,
    pub status: Status,
    pub livequery: Q,
    /// Monotonic request attempt. Peer id + selection version are not enough
    /// to reject a late response after disconnect/reconnect to the same peer:
    /// the reconnect re-requests the SAME selection version from the SAME
    /// peer, so a straggler response to the pre-disconnect attempt matches on
    /// both fields and would be applied as if it answered the new attempt.
    /// Each attempt captures the value at send time and its validity check
    /// requires the stored generation to still equal it, so once a newer
    /// attempt bumps the counter, the straggler fails the check and is
    /// dropped.
    request_generation: u64,
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
    /// Wakes readiness waiters whenever a query status or peer availability
    /// changes. The status map remains the source of truth.
    status_changed: tokio::sync::Notify,
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
                status_changed: tokio::sync::Notify::new(),
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
    pub(crate) fn set_node(&self, node: Arc<dyn TNode<CD>>) -> Result<(), ()> { self.inner.node.set(node).map_err(|_| ()) }

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
        self.subscribe_query_with_validity(query_id, collection_id, selection, context_data, version, None, livequery);
    }

    pub(crate) fn subscribe_query_with_validity(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection,
        context_data: CD,
        version: u32,
        validity: Option<RequestValidity>,
        livequery: Q,
    ) {
        debug!("SubscriptionRelay.subscribe_predicate() - New predicate {} needs remote registration", query_id);
        {
            self.inner.subscriptions.lock().expect("poisoned lock").insert(
                query_id,
                RemoteQueryState {
                    content: Arc::new(Content { collection_id, selection, context_data, query_id, version, validity }),
                    status: Status::PendingRemote,
                    livequery,
                    request_generation: 0,
                },
            );
        }
        self.inner.status_changed.notify_waiters();

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
                        validity: old_content.validity.clone(),
                    });

                    match state.status {
                        Status::Established(peer_id, _old_version) => {
                            // Update to new version, mark as requested for this peer
                            state.request_generation = state.request_generation.wrapping_add(1);
                            state.status = Status::Requested(peer_id, version);
                            Some((
                                peer_id,
                                state.content.collection_id.clone(),
                                state.content.context_data.clone(),
                                state.request_generation,
                                state.content.validity.clone(),
                            ))
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
        self.inner.status_changed.notify_waiters();

        match update {
            Some((peer_id, collection_id, context_data, request_generation, validity)) => {
                self.update_query_on_peer(peer_id, query_id, collection_id, selection, version, context_data, request_generation, validity);
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
        request_generation: u64,
        owner_validity: Option<RequestValidity>,
    ) {
        let me = self.clone();
        crate::task::spawn(async move {
            if let Some(node) = me.inner.node.get() {
                // Get the livequery for error handling
                let livequery = {
                    me.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).get(&query_id).map(|state| state.livequery.clone())
                };

                // Send the updated predicate to the peer
                let validity = me.request_validity(query_id, peer_id, version, request_generation, owner_validity);
                match node.remote_subscribe(peer_id, query_id, collection_id, selection, &context_data, version, validity.clone()).await {
                    Ok(()) => {
                        if !validity.is_current() {
                            me.cleanup_invalid_attempt(query_id, peer_id, version, request_generation);
                            debug!("Ignoring stale predicate {} update response from peer {}", query_id, peer_id);
                            return;
                        }
                        // Deltas applied successfully, now activate the livequery
                        if let Some(lq) = livequery {
                            lq.subscription_established(version).await;
                        }

                        // Mark as established - subscription succeeded even if livequery activation had issues
                        let mut subscriptions = me.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                        let removed = !subscriptions.contains_key(&query_id);
                        let current = subscriptions.get(&query_id).is_some_and(|info| {
                            info.request_generation == request_generation
                                && info.content.version == version
                                && matches!(info.status, Status::Requested(current_peer, current_version) if current_peer == peer_id && current_version == version)
                        });
                        if current {
                            let info = subscriptions.get_mut(&query_id).expect("current subscription exists");
                            info.status = Status::Established(peer_id, version);
                        }
                        drop(subscriptions);
                        me.inner.status_changed.notify_waiters();
                        if removed {
                            // Teardown raced the response. The first
                            // best-effort unsubscribe may have reached the
                            // peer before this subscription was installed;
                            // repeat it after successful establishment.
                            me.unsubscribe_from_peer(peer_id, query_id);
                            debug!("Cleaning up late predicate {} update completion from peer {}", query_id, peer_id);
                        } else if current {
                            debug!("Successfully updated predicate {} on peer {} subscription", query_id, peer_id);
                        } else {
                            debug!("Ignoring stale predicate {} update completion from peer {}", query_id, peer_id);
                        }
                    }
                    Err(e) => {
                        if !validity.is_current() {
                            me.cleanup_invalid_attempt(query_id, peer_id, version, request_generation);
                            debug!("Ignoring stale predicate {} update failure from peer {}", query_id, peer_id);
                            return;
                        }
                        // Handle error with retry logic
                        me.handle_error(query_id, peer_id, version, request_generation, e, livequery).await;
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

        // A Requested subscription may already exist remotely by the time
        // teardown is observed, even though its response has not arrived.
        // Send the best-effort unsubscribe now; a late successful response
        // performs the same cleanup again after establishment, closing the
        // subscribe/unsubscribe processing race on the peer.
        let peer = {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            subscriptions.remove(&query_id).and_then(|info| match info.status {
                Status::Requested(peer_id, _) | Status::Established(peer_id, _) | Status::PendingUpdate(peer_id, _) => Some(peer_id),
                Status::PendingRemote | Status::Failed => None,
            })
        };
        if let Some(peer_id) = peer {
            self.unsubscribe_from_peer(peer_id, query_id);
        }
        self.inner.status_changed.notify_waiters();
    }

    fn unsubscribe_from_peer(&self, peer_id: proto::EntityId, query_id: proto::QueryId) {
        let Some(node) = self.inner.node.get().cloned() else { return };
        crate::task::spawn(async move {
            if let Err(e) = node.peer_unsubscribe(peer_id, query_id).await {
                warn!("Failed to send unsubscribe message for {}: {}", query_id, e);
            } else {
                debug!("Successfully sent unsubscribe message for {}", query_id);
            }
        });
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
        self.inner.status_changed.notify_waiters();

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
        self.inner.status_changed.notify_waiters();
    }

    /// Get the current state of a predicate registration
    pub fn get_status(&self, query_id: proto::QueryId) -> Option<Status> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.get(&query_id).map(|info| info.status.clone())
    }

    /// Resolve every streaming item to at least one query that is still
    /// current on `peer_id`. Returned owner validities are acquired by
    /// `Node::handle_message` before schema ingestion and held through body
    /// application; unguarded queries need no returned validity.
    pub(crate) fn validities_for_stream_update(
        &self,
        peer_id: &proto::EntityId,
        items: &[proto::SubscriptionUpdateItem],
    ) -> Option<Vec<RequestValidity>> {
        if items.is_empty() {
            return None;
        }

        let candidates: Vec<Vec<Option<RequestValidity>>> = {
            let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
            items
                .iter()
                .map(|item| {
                    item.source_queries
                        .iter()
                        .filter_map(|query_id| {
                            let state = subscriptions.get(query_id)?;
                            let current_peer = match state.status {
                                Status::Requested(peer, _) | Status::Established(peer, _) | Status::PendingUpdate(peer, _) => peer,
                                Status::PendingRemote | Status::Failed => return None,
                            };
                            (current_peer == *peer_id).then(|| state.content.validity.clone())
                        })
                        .collect()
                })
                .collect()
        };

        let mut validities = Vec::new();
        for item_candidates in candidates {
            let candidate = item_candidates.into_iter().find(|validity| validity.as_ref().is_none_or(RequestValidity::is_current))?;
            if let Some(validity) = candidate {
                validities.push(validity);
            }
        }
        Some(validities)
    }

    fn request_validity(
        &self,
        query_id: proto::QueryId,
        peer_id: proto::EntityId,
        version: u32,
        request_generation: u64,
        owner_validity: Option<RequestValidity>,
    ) -> RequestValidity {
        let relay = self.clone();
        let relay_current = move || {
            relay.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner()).get(&query_id).is_some_and(|info| {
                info.request_generation == request_generation
                    && info.content.version == version
                    && matches!(info.status, Status::Requested(current_peer, current_version) if current_peer == peer_id && current_version == version)
            })
        };
        match owner_validity {
            Some(owner) => owner.and(relay_current),
            None => RequestValidity::new(relay_current),
        }
    }

    /// A response can become invalid because its query was removed or because
    /// the owner lifecycle was cancelled. Repeat remote cleanup after that
    /// response so an earlier best-effort unsubscribe cannot be overtaken by
    /// server-side installation. Do not clean up a superseded attempt for the
    /// same query id: that could remove the newer request.
    fn cleanup_invalid_attempt(&self, query_id: proto::QueryId, peer_id: proto::EntityId, version: u32, request_generation: u64) {
        let cleanup = {
            let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
            match subscriptions.get(&query_id) {
                None => true,
                Some(info) => {
                    let same_attempt = info.request_generation == request_generation
                        && info.content.version == version
                        && matches!(info.status, Status::Requested(current_peer, current_version) if current_peer == peer_id && current_version == version);
                    same_attempt && info.content.validity.as_ref().is_some_and(|validity| !validity.is_current())
                }
            }
        };
        if cleanup {
            self.unsubscribe_from_peer(peer_id, query_id);
        }
    }

    /// Wait until the query has applied its remote snapshot, or until no
    /// remote snapshot can currently arrive. Returning `false` means the
    /// caller should use its local cache: either the relay is offline, the
    /// query failed permanently, or it was removed. Status changes and peer
    /// availability are checked after registering the notification future so
    /// a transition cannot be lost between the check and the await.
    pub(crate) async fn wait_established_or_offline(&self, query_id: proto::QueryId) -> bool {
        loop {
            let notified = self.inner.status_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let status = self.get_status(query_id);
            match status {
                Some(Status::Established(_, _)) => return true,
                Some(Status::Failed) | None => return false,
                Some(Status::PendingRemote) => {
                    if !self.inner.connected_peers.is_empty() {
                        notified.await;
                        continue;
                    }
                    // Recheck both pieces after the first snapshot. A peer
                    // connecting between the status and availability reads
                    // either changes the status or makes availability true;
                    // in either case the enabled notification remains a
                    // backstop and we loop instead of prematurely falling
                    // back to local cache.
                    if !matches!(self.get_status(query_id), Some(Status::PendingRemote)) || !self.inner.connected_peers.is_empty() {
                        continue;
                    }
                    return false;
                }
                Some(Status::Requested(_, _) | Status::PendingUpdate(_, _)) => notified.await,
            }
        }
    }

    /// The collection a registered query belongs to. Used by the ephemeral
    /// update path to RESOLVE a new selection (RFC 5.5 in specs/model-property-metadata/rfc.md) before forwarding, as
    /// the trait method carries no collection parameter.
    pub fn collection_for_query(&self, query_id: proto::QueryId) -> Option<CollectionId> {
        let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subscriptions.get(&query_id).map(|state| state.content.collection_id.clone())
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
                        info.request_generation = info.request_generation.wrapping_add(1);
                        info.status = Status::Requested(target_peer, info.content.version);
                        Some((info.content.clone(), info.request_generation))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if pending.is_empty() {
            return;
        }
        self.inner.status_changed.notify_waiters();

        debug!("Registering {} predicates on {} peer subscriptions", pending.len(), self.inner.connected_peers.len());

        for (content, request_generation) in pending {
            crate::task::spawn(self.clone().attempt_subscribe(node.clone(), target_peer, content, request_generation));
        }
    }

    async fn attempt_subscribe(
        self,
        node: Arc<dyn TNode<CD>>,
        target_peer: proto::EntityId,
        content: Arc<Content<CD>>,
        request_generation: u64,
    ) {
        let query_id = content.query_id;
        let predicate = content.selection.clone();
        let context_data = content.context_data.clone();
        let version = content.version;

        // Get the livequery for error handling
        let livequery =
            { self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner()).get(&query_id).map(|state| state.livequery.clone()) };

        // Call remote_subscribe which fetches known matches, subscribes, applies deltas, and stores events
        let validity = self.request_validity(query_id, target_peer, version, request_generation, content.validity.clone());
        match node
            .remote_subscribe(target_peer, query_id, content.collection_id.clone(), predicate, &context_data, version, validity.clone())
            .await
        {
            Ok(()) => {
                if !validity.is_current() {
                    self.cleanup_invalid_attempt(query_id, target_peer, version, request_generation);
                    debug!("Ignoring stale predicate {} response from peer {}", query_id, target_peer);
                    return;
                }
                // Deltas applied successfully, now activate the livequery
                // The livequery handles its own errors internally
                if let Some(lq) = livequery {
                    lq.subscription_established(version).await;
                }

                // Mark as established - subscription succeeded even if livequery activation had issues
                let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                let removed = !subscriptions.contains_key(&query_id);
                let current = subscriptions.get(&query_id).is_some_and(|info| {
                    info.request_generation == request_generation
                        && info.content.version == version
                        && matches!(info.status, Status::Requested(current_peer, current_version) if current_peer == target_peer && current_version == version)
                });
                if current {
                    let info = subscriptions.get_mut(&query_id).expect("current subscription exists");
                    info.status = Status::Established(target_peer, version);
                }
                drop(subscriptions);
                self.inner.status_changed.notify_waiters();
                if removed {
                    // Teardown raced the response. Repeat the unsubscribe
                    // after establishment so server-side processing order
                    // cannot leave an orphaned subscription.
                    self.unsubscribe_from_peer(target_peer, query_id);
                    debug!("Cleaning up late predicate {} establishment from peer {}", query_id, target_peer);
                } else if current {
                    debug!("Successfully registered predicate {} on peer {} subscription", query_id, target_peer);
                } else {
                    debug!("Ignoring stale predicate {} establishment from peer {}", query_id, target_peer);
                }
            }
            Err(e) => {
                if !validity.is_current() {
                    self.cleanup_invalid_attempt(query_id, target_peer, version, request_generation);
                    debug!("Ignoring stale predicate {} failure from peer {}", query_id, target_peer);
                    return;
                }
                // Handle error with retry logic
                self.handle_error(query_id, target_peer, version, request_generation, e, livequery).await;
            }
        }
    }

    /// Start background task that periodically retries pending subscriptions
    fn start_retry_task(&self, mut shutdown_rx: tokio::sync::mpsc::Receiver<()>) {
        let me = self.clone();
        crate::task::spawn(async move {
            loop {
                let delay = futures_timer::Delay::new(std::time::Duration::from_secs(5));
                tokio::select! {
                    _ = delay => {
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
    async fn handle_error(
        &self,
        query_id: proto::QueryId,
        target_peer: proto::EntityId,
        version: u32,
        request_generation: u64,
        error: RetrievalError,
        livequery: Option<Q>,
    ) {
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
                RequestError::AccessDenied(_) => false,
            },
            // Other retrieval errors are not retryable
            _ => false,
        };

        // Update state based on retriability
        let mut deliver_error = None;
        let mut changed = false;
        let mut removed = false;
        {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(info) = subscriptions.get_mut(&query_id) {
                if info.request_generation != request_generation
                    || info.content.version != version
                    || !matches!(info.status, Status::Requested(current_peer, current_version) if current_peer == target_peer && current_version == version)
                {
                    debug!("Ignoring stale predicate {} failure from peer {}", query_id, target_peer);
                    return;
                }
                changed = true;
                if is_retryable {
                    // Retryable errors go back to pending for retry by background task
                    info.status = Status::PendingRemote;
                    warn!("Retryable failure for predicate {} with peer {}: {} - will retry", query_id, target_peer, error_msg);
                } else {
                    // Non-retryable errors are permanently failed
                    info.status = Status::Failed;
                    tracing::error!("Permanent failure for predicate {} with peer {}: {} - no retry", query_id, target_peer, error_msg);

                    // Set error on livequery
                    deliver_error = livequery;
                }
            } else {
                removed = true;
            }
        }
        if removed {
            // `remote_subscribe` can fail after the peer installed the
            // subscription (for example while applying its returned deltas).
            // Teardown already removed local state, so repeat remote cleanup
            // just as the late-success path does.
            self.unsubscribe_from_peer(target_peer, query_id);
            return;
        }
        if let Some(lq) = deliver_error {
            lq.set_last_error(error);
        }
        if changed {
            self.inner.status_changed.notify_waiters();
        }
    }
}

/// Trait for communicating with remote peers (abstraction over WeakNode for testing)
#[async_trait]
pub(crate) trait TNode<CD: ContextData>: Send + Sync {
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
        validity: RequestValidity,
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
        validity: RequestValidity,
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
        let guarded_response = node
            .request_if_current(
                peer_id,
                context_data,
                ankurah_proto::NodeRequestBody::SubscribeQuery {
                    query_id,
                    collection: collection_id.clone(),
                    selection: selection.clone(),
                    version,
                    known_matches,
                },
                validity.clone(),
            )
            .await
            .map_err(RetrievalError::RequestError)?;
        // Keep the owner-fence lease alive through NodeApplier. Reset first
        // invalidates the fence and then waits for this lease before deleting
        // storage, so a response admitted at schema ingress cannot apply
        // deltas after the reset clear.
        let (response, _request_lease) = guarded_response.into_parts();
        let deltas = match response {
            ankurah_proto::NodeResponseBody::QuerySubscribed { query_id: _response_query_id, deltas } => deltas,
            ankurah_proto::NodeResponseBody::Error(e) => return Err(RetrievalError::RequestError(RequestError::ServerError(e))),
            other => return Err(RetrievalError::RequestError(RequestError::UnexpectedResponse(other))),
        };

        // The response handler performed the same check before ingesting its
        // attached schema. Recheck at the application boundary in case the
        // attempt was superseded while the response future was being woken.
        if !validity.is_current() {
            debug!("Node.remote_subscribe: discarding stale response for query {} version {}", query_id, version);
            return Ok(());
        }

        tracing::debug!(
            "Node.remote_subscribe: query_id: {}, collection_id: {}, received deltas: {}",
            query_id,
            collection_id,
            deltas.len()
        );
        // 3. Apply deltas to local node using NodeApplier
        let collection = node.collections.get(&collection_id).await?;
        let event_getter = crate::retrieval::CachedEventGetter::new(collection_id, collection.clone(), &node, context_data);
        let state_getter = crate::retrieval::LocalStateGetter::new(collection);
        crate::node_applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &event_getter, &state_getter).await?;
        drop(_request_lease);

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
    use crate::util::request_fence::RequestFence;
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
            _validity: RequestValidity,
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

    /// Holds a successful subscribe response until the test releases it, so
    /// teardown can deterministically race an in-flight Requested state.
    #[derive(Debug, Default)]
    struct BlockingMessageSender {
        subscribe_started: tokio::sync::Notify,
        release_subscribe: tokio::sync::Notify,
        unsubscribes: Mutex<Vec<(EntityId, proto::QueryId)>>,
    }

    #[async_trait]
    impl TNode<CollectionId> for BlockingMessageSender {
        async fn remote_subscribe(
            &self,
            _peer_id: EntityId,
            _query_id: proto::QueryId,
            _collection_id: CollectionId,
            _selection: ankql::ast::Selection,
            _context_data: &CollectionId,
            _version: u32,
            _validity: RequestValidity,
        ) -> Result<(), RetrievalError> {
            self.subscribe_started.notify_one();
            self.release_subscribe.notified().await;
            Ok(())
        }

        async fn peer_unsubscribe(&self, peer_id: EntityId, query_id: proto::QueryId) -> Result<(), anyhow::Error> {
            self.unsubscribes.lock().unwrap().push((peer_id, query_id));
            Ok(())
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

    fn stream_item(source_queries: Vec<proto::QueryId>) -> proto::SubscriptionUpdateItem {
        proto::SubscriptionUpdateItem {
            entity_id: EntityId::new(),
            model: proto::ModelId::Entity(EntityId::new()),
            content: proto::UpdateContent::EventOnly(vec![]),
            predicate_relevance: vec![],
            source_queries,
        }
    }

    fn establish_for_peer(
        relay: &SubscriptionRelay<CollectionId, MockLiveQuery>,
        peer_id: EntityId,
        query_id: proto::QueryId,
        validity: Option<RequestValidity>,
    ) {
        let collection = create_test_collection_id();
        relay.subscribe_query_with_validity(query_id, collection.clone(), create_test_selection(), collection, 1, validity, MockLiveQuery);
        relay.inner.subscriptions.lock().unwrap().get_mut(&query_id).unwrap().status = Status::Established(peer_id, 1);
    }

    #[tokio::test]
    async fn stream_admission_requires_a_current_source_for_every_item() {
        let relay = SubscriptionRelay::new();
        let peer_id = EntityId::new();
        let first = proto::QueryId::new();
        let second = proto::QueryId::new();
        establish_for_peer(&relay, peer_id, first, None);
        establish_for_peer(&relay, peer_id, second, None);

        let honest = [stream_item(vec![first]), stream_item(vec![second])];
        assert!(matches!(relay.validities_for_stream_update(&peer_id, &honest), Some(validities) if validities.is_empty()));

        let unknown = proto::QueryId::new();
        let partly_stale = [stream_item(vec![first]), stream_item(vec![unknown])];
        assert!(relay.validities_for_stream_update(&peer_id, &partly_stale).is_none());
        assert!(relay.validities_for_stream_update(&peer_id, &[]).is_none());

        let other_peer = EntityId::new();
        assert!(relay.validities_for_stream_update(&other_peer, &[stream_item(vec![first])]).is_none());
    }

    #[tokio::test]
    async fn stream_admission_rejects_an_invalidated_owner_before_application() {
        let relay = SubscriptionRelay::new();
        let peer_id = EntityId::new();
        let query_id = proto::QueryId::new();
        let fence = RequestFence::new();
        establish_for_peer(&relay, peer_id, query_id, Some(RequestValidity::fenced(fence.clone())));

        let items = [stream_item(vec![query_id])];
        let validities = relay.validities_for_stream_update(&peer_id, &items).expect("current owner admits its stream");
        assert_eq!(validities.len(), 1);
        drop(validities.into_iter().next().unwrap().try_acquire().expect("current fence grants a lease"));

        fence.invalidate();
        assert!(relay.validities_for_stream_update(&peer_id, &items).is_none());
    }

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
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

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
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

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
    async fn wait_pending_with_connected_peer_observes_establishment() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        relay.subscribe_query(query_id, collection_id.clone(), create_test_selection(), collection_id, 0, MockLiveQuery);
        assert!(matches!(relay.get_status(query_id), Some(Status::PendingRemote)));

        // Reproduce the mixed snapshot in notify_peer_connected: peer
        // availability becomes visible just before setup changes the query
        // from PendingRemote to Requested. Readiness must not linearize this
        // as offline and miss the imminent initial snapshot.
        let peer_id = EntityId::new();
        relay.inner.connected_peers.insert(peer_id);
        let waiter = {
            let relay = relay.clone();
            tokio::spawn(async move { relay.wait_established_or_offline(query_id).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "a connected PendingRemote query must wait for setup instead of falling back offline");

        {
            let mut subscriptions = relay.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
            subscriptions.get_mut(&query_id).expect("subscription present").status = Status::Established(peer_id, 0);
        }
        relay.inner.status_changed.notify_waiters();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("establishment must wake the mixed-snapshot waiter")
            .expect("mixed-snapshot waiter task must complete"));
    }

    #[tokio::test]
    async fn wait_established_observes_requested_terminal_transitions() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender).expect("Failed to set message sender");
        let peer_id = EntityId::new();
        relay.inner.connected_peers.insert(peer_id);

        let make_requested = || {
            relay.inner.connected_peers.remove(&peer_id);
            let query_id = proto::QueryId::new();
            let collection_id = create_test_collection_id();
            relay.subscribe_query(query_id, collection_id.clone(), create_test_selection(), collection_id, 0, MockLiveQuery);
            {
                let mut subscriptions = relay.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
                subscriptions.get_mut(&query_id).expect("subscription present").status = Status::Requested(peer_id, 0);
            }
            relay.inner.connected_peers.insert(peer_id);
            query_id
        };

        let established_query = make_requested();
        let established_waiter = {
            let relay = relay.clone();
            tokio::spawn(async move { relay.wait_established_or_offline(established_query).await })
        };
        tokio::task::yield_now().await;
        {
            let mut subscriptions = relay.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
            subscriptions.get_mut(&established_query).expect("subscription present").status = Status::Established(peer_id, 0);
        }
        relay.inner.status_changed.notify_waiters();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), established_waiter)
            .await
            .expect("established transition must wake the waiter")
            .expect("established waiter task must complete"));

        let failed_query = make_requested();
        let failed_waiter = {
            let relay = relay.clone();
            tokio::spawn(async move { relay.wait_established_or_offline(failed_query).await })
        };
        tokio::task::yield_now().await;
        {
            let mut subscriptions = relay.inner.subscriptions.lock().unwrap_or_else(|error| error.into_inner());
            subscriptions.get_mut(&failed_query).expect("subscription present").status = Status::Failed;
        }
        relay.inner.status_changed.notify_waiters();
        assert!(!tokio::time::timeout(std::time::Duration::from_secs(1), failed_waiter)
            .await
            .expect("failed transition must wake the waiter")
            .expect("failed waiter task must complete"));
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
    async fn requested_teardown_repeats_cleanup_after_late_success() {
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(BlockingMessageSender::default());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let peer_id = EntityId::new();
        relay.notify_peer_connected(peer_id);

        let subscribe_started = mock_sender.subscribe_started.notified();
        tokio::pin!(subscribe_started);
        subscribe_started.as_mut().enable();
        relay.subscribe_query(query_id, collection_id.clone(), create_test_selection(), collection_id, 0, MockLiveQuery);
        tokio::time::timeout(std::time::Duration::from_secs(1), subscribe_started).await.expect("the subscribe request must start");
        assert!(matches!(relay.get_status(query_id), Some(Status::Requested(requested_peer, 0)) if requested_peer == peer_id));

        relay.unsubscribe_predicate(query_id);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while mock_sender.unsubscribes.lock().unwrap().len() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Requested teardown must send an immediate best-effort unsubscribe");

        // Let the subscribe succeed after the first cleanup. The completion
        // sees that local state was removed and sends a second unsubscribe,
        // which is ordered after remote establishment and prevents an orphan.
        mock_sender.release_subscribe.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while mock_sender.unsubscribes.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late successful establishment must repeat remote cleanup");

        assert!(matches!(relay.get_status(query_id), None));
        assert_eq!(mock_sender.unsubscribes.lock().unwrap().as_slice(), &[(peer_id, query_id), (peer_id, query_id)]);
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
        futures_timer::Delay::new(std::time::Duration::from_millis(10)).await;

        // Verify no unsubscribe message was sent (since it wasn't established)
        let sent_requests = mock_sender.get_sent_requests();
        assert_eq!(sent_requests.len(), 0);

        // Verify subscription is gone
        assert!(matches!(relay.get_status(query_id), None));
    }
}
