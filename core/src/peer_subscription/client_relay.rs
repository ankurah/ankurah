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

    /// Latch a relay-leg error, floored on the same report sequence as
    /// the status transition it accompanies: a superseded failure's late
    /// latch must not overwrite a newer attempt's recovery.
    fn set_last_error(&self, seq: u64, error: RetrievalError);

    /// Report a remote-leg transition; the livequery folds it into its
    /// public status signal. `seq` is drawn under the relay's lock at the
    /// transition and floors delivery: reports dispatch outside the lock,
    /// so a delayed older report must never overwrite a newer one.
    fn remote_status(&self, seq: u64, status: crate::livequery::RemoteStatus);
}

#[derive(Debug, Clone)]
pub enum Status {
    PendingRemote,
    Requested(proto::EntityId, u32),   // peer_id, version
    Established(proto::EntityId, u32), // peer_id, version
    /// Non-retryable
    Failed,
}

/// The public projection of a [`Status`] transition, for the derivable
/// arms; `Failed` carries no error here, so its reports are built where
/// the error is in hand (`handle_error`).
fn projected(status: &Status) -> Option<crate::livequery::RemoteStatus> {
    match status {
        Status::PendingRemote => Some(crate::livequery::RemoteStatus::Pending),
        Status::Requested(_, version) => Some(crate::livequery::RemoteStatus::Requested { version: *version }),
        Status::Established(_, version) => Some(crate::livequery::RemoteStatus::Established { version: *version }),
        Status::Failed => None,
    }
}

#[derive(Debug)]
pub struct Content<CD: ContextData> {
    pub query_id: proto::QueryId,
    pub collection_id: CollectionId,
    pub selection: ankql::ast::Selection,
    /// The live credential source; every send reads its then-current
    /// values, so a reconnect re-registration after a refresh carries the
    /// fresh ones. A set-backed query sends every live credential.
    pub sessions: crate::session::Sessions<CD>,
    pub version: u32,
}

pub struct RemoteQueryState<CD: ContextData, Q: RemoteQuerySubscriber> {
    pub content: Arc<Content<CD>>,
    pub status: Status,
    pub livequery: Q,
    /// Monotonic stamp for status reports, drawn under the subscriptions
    /// lock at each transition; the livequery floors on it.
    pub report_seq: u64,
    /// Monotonic per-entry attempt id, bumped under the lock every time
    /// a send is dispatched. `(peer, version)` alone is REUSABLE (a
    /// reconnect of the same peer re-sends the same version), so reports
    /// must also match the attempt that produced them or two attempts
    /// alias and a dead one can resolve its successor's state.
    pub attempt: u64,
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> RemoteQueryState<CD, Q> {
    /// Stamp the CURRENT status for reporting: bump the sequence under
    /// the lock and project. Dispatch the returned report after the lock
    /// is released.
    fn stamp(&mut self) -> Option<(u64, crate::livequery::RemoteStatus, Q)> {
        let report = projected(&self.status)?;
        self.report_seq += 1;
        Some((self.report_seq, report, self.livequery.clone()))
    }

    /// Stamp with an explicit report (the non-derivable Failed arms).
    fn stamp_with(&mut self, report: crate::livequery::RemoteStatus) -> (u64, crate::livequery::RemoteStatus, Q) {
        self.report_seq += 1;
        (self.report_seq, report, self.livequery.clone())
    }
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
        sessions: crate::session::Sessions<CD>,
        version: u32,
        livequery: Q,
    ) {
        debug!("SubscriptionRelay.subscribe_predicate() - New predicate {} needs remote registration", query_id);
        let stamped = {
            let mut state = RemoteQueryState {
                content: Arc::new(Content { collection_id, selection, sessions, query_id, version }),
                status: Status::PendingRemote,
                livequery,
                report_seq: 0,
                attempt: 0,
            };
            let stamped = state.stamp();
            self.inner.subscriptions.lock().expect("poisoned lock").insert(query_id, state);
            stamped
        };
        if let Some((seq, report, livequery)) = stamped {
            livequery.remote_status(seq, report);
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
                    // Racing reissues dispatch unordered after minting under
                    // the livequery's lock. A stale write here would be
                    // re-sent verbatim by the next reconnect or retry, so
                    // the stored content gets the same floor the completion
                    // transitions and the server upsert enforce.
                    if version < state.content.version {
                        debug!("Ignoring stale update_query for {} (version {} < {})", query_id, version, state.content.version);
                        return Ok(());
                    }
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
                        Status::Established(peer_id, _old_version) => {
                            // Update to new version, mark as requested for this peer
                            state.attempt += 1;
                            state.status = Status::Requested(peer_id, version);
                            (
                                state.stamp(),
                                Some((peer_id, state.content.collection_id.clone(), state.content.sessions.snapshot(), state.attempt)),
                            )
                            // Return the peer_id to send update to
                        }
                        _ => {
                            // Not established yet, just update to PendingRemote and setup
                            state.status = Status::PendingRemote;
                            (state.stamp(), None)
                        }
                    }
                }
                None => return Err(anyhow!("Predicate {} not found", query_id)),
            }
        };

        let (stamped, target) = update;
        if let Some((seq, report, livequery)) = stamped {
            livequery.remote_status(seq, report);
        }
        match target {
            Some((peer_id, collection_id, context_data, attempt)) => {
                self.update_query_on_peer(peer_id, query_id, collection_id, selection, version, context_data, attempt);
            }
            None => {
                // Not established yet - use setup_remote_subscriptions for initial setup
                self.setup_remote_subscriptions();
            }
        };

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update_query_on_peer(
        &self,
        peer_id: proto::EntityId,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection,
        version: u32,
        context_data: Vec<CD>,
        attempt: u64,
    ) {
        let me = self.clone();
        crate::task::spawn(async move {
            if let Some(node) = me.inner.node.get() {
                // Send the updated predicate to the peer
                match node.remote_subscribe(peer_id, query_id, collection_id, selection, context_data, version).await {
                    Ok(()) => {
                        // Claim the completion FIRST, then activate: only
                        // the attempt that wins the transition may run
                        // activation, through the livequery its own stamp
                        // carries (activating before the claim would let a
                        // dead attempt's late completion activate the local
                        // leg and clear an error a live attempt latched).
                        // Acceptance is by attempt identity; the full rule
                        // lives at attempt_subscribe's completion arm.
                        let (stamped, kick) = {
                            let mut subscriptions = me.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                            match subscriptions.get_mut(&query_id) {
                                Some(info) => match info.status {
                                    Status::Requested(peer, v) if peer == peer_id && v == version && info.attempt == attempt => {
                                        info.status = Status::Established(peer_id, version);
                                        (info.stamp(), false)
                                    }
                                    Status::PendingRemote => (None, true),
                                    _ => (None, false),
                                },
                                None => (None, false),
                            }
                        };
                        if let Some((seq, report, lq)) = stamped {
                            lq.subscription_established(version).await;
                            lq.remote_status(seq, report);
                        }
                        if kick {
                            me.setup_remote_subscriptions();
                        }
                        debug!("Successfully updated predicate {} on peer {} subscription", query_id, peer_id);
                    }
                    Err(e) => {
                        // Handle error with retry logic
                        me.handle_error(query_id, peer_id, e, version, attempt).await;
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

        let orphaned: Vec<_> = {
            let mut subscriptions = self.inner.subscriptions.lock().expect("poisoned lock");
            subscriptions
                .values_mut()
                .filter_map(|info| {
                    if let Status::Established(established_peer_id, _) | Status::Requested(established_peer_id, _) = &info.status {
                        if *established_peer_id == peer_id {
                            // Update state to pending
                            info.status = Status::PendingRemote;
                            warn!("Predicate {} orphaned due to peer {} disconnect", info.content.query_id, peer_id);
                            return info.stamp();
                        }
                    }
                    None
                })
                .collect()
        };
        for (seq, report, livequery) in orphaned {
            livequery.remote_status(seq, report);
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
                        contexts.extend(state.content.sessions.snapshot());
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
                        info.attempt += 1;
                        info.status = Status::Requested(target_peer, info.content.version);
                        Some((info.content.clone(), info.attempt, info.stamp()))
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

        for (content, attempt, stamped) in pending {
            if let Some((seq, report, livequery)) = stamped {
                livequery.remote_status(seq, report);
            }
            crate::task::spawn(self.clone().attempt_subscribe(node.clone(), target_peer, content, attempt));
        }
    }

    async fn attempt_subscribe(self, node: Arc<dyn TNode<CD>>, target_peer: proto::EntityId, content: Arc<Content<CD>>, attempt: u64) {
        let query_id = content.query_id;
        let predicate = content.selection.clone();
        let context_data = content.sessions.snapshot();
        let version = content.version;

        // Call remote_subscribe which fetches known matches, subscribes, applies deltas, and stores events
        match node.remote_subscribe(target_peer, query_id, content.collection_id.clone(), predicate, context_data, version).await {
            Ok(()) => {
                // Claim the completion FIRST, then activate. A report is
                // acted on ONLY when it matches the outstanding attempt:
                // every send stamps Requested(peer, version) and bumps the
                // attempt, so a report that finds anything else is a dead
                // attempt's late echo (superseded by a newer send, re-routed
                // after its peer disconnected, or already resolved). A
                // matching Requested also implies the version IS the newest
                // content (a newer mint would have re-stamped the entry,
                // breaking the match). Dead echoes never mutate state; an
                // entry sitting at PendingRemote gets a kick so a lost
                // dispatch cannot strand it. Only the claiming attempt runs
                // activation, through the livequery its own stamp carries:
                // activating before the claim would let a dead attempt's
                // late completion activate the local leg and clear an error
                // a live attempt latched. (remote_subscribe already merged
                // the response deltas into storage; that data was attested
                // by the server regardless of which attempt fetched it.)
                let (stamped, kick) = {
                    let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                    match subscriptions.get_mut(&query_id) {
                        Some(info) => match info.status {
                            Status::Requested(peer, v) if peer == target_peer && v == version && info.attempt == attempt => {
                                info.status = Status::Established(target_peer, version);
                                (info.stamp(), false)
                            }
                            Status::PendingRemote => (None, true),
                            _ => (None, false),
                        },
                        None => (None, false),
                    }
                };
                if let Some((seq, report, lq)) = stamped {
                    lq.subscription_established(version).await;
                    lq.remote_status(seq, report);
                }
                if kick {
                    self.setup_remote_subscriptions();
                }
                debug!("Successfully registered predicate {} on peer {} subscription", query_id, target_peer);
            }
            Err(e) => {
                // Handle error with retry logic
                self.handle_error(query_id, target_peer, e, version, attempt).await;
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
        error: RetrievalError,
        version: u32,
        attempt: u64,
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

        // A failure is acted on ONLY when it matches the outstanding
        // attempt (Requested with this peer and version) — the same rule
        // as the completion sites; see the comment there. A matching
        // retryable failure re-queues for the retry task; a matching
        // terminal failure holds Failed (and latches) until a credential
        // change or selection update re-kicks. A dead attempt's failure
        // mutates nothing and latches nothing: the entry is either being
        // driven by a live attempt or sits at PendingRemote, which gets a
        // kick so a lost dispatch cannot strand it.
        let (stamped, kick, latch_error) = {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            match subscriptions.get_mut(&query_id) {
                Some(info) => match info.status {
                    Status::Requested(peer, v) if peer == target_peer && v == version && info.attempt == attempt => {
                        if is_retryable {
                            // Retryable errors go back to pending for retry by background task
                            info.status = Status::PendingRemote;
                            warn!("Retryable failure for predicate {} with peer {}: {} - will retry", query_id, target_peer, error_msg);
                            (info.stamp(), false, false)
                        } else {
                            // Non-retryable: report Denied for a local refusal
                            // to sign, the error text otherwise.
                            info.status = Status::Failed;
                            tracing::error!(
                                "Failure for predicate {} with peer {}: {} - awaiting re-kick",
                                query_id,
                                target_peer,
                                error_msg
                            );
                            let report = if let RetrievalError::RequestError(RequestError::AccessDenied(denied)) = &error {
                                crate::livequery::RemoteStatus::Denied { reason: denied.to_string() }
                            } else {
                                crate::livequery::RemoteStatus::Error { message: error_msg.clone() }
                            };
                            (Some(info.stamp_with(report)), false, true)
                        }
                    }
                    Status::PendingRemote => {
                        debug!(
                            "Failure for predicate {} from a dead attempt (v{} via {}); entry pending re-drive",
                            query_id, version, target_peer
                        );
                        (None, true, false)
                    }
                    _ => {
                        debug!("Ignoring failure for predicate {} from a dead attempt (v{} via {})", query_id, version, target_peer);
                        (None, false, false)
                    }
                },
                None => (None, false, false),
            }
        };
        if let Some((seq, report, lq)) = stamped {
            if latch_error {
                lq.set_last_error(seq, error);
            }
            lq.remote_status(seq, report);
        }
        if kick {
            self.setup_remote_subscriptions();
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
        selection: ankql::ast::Selection,
        context_data: Vec<PA::ContextData>,
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
                &context_data,
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
        crate::node_applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &event_getter, &state_getter).await?;

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
            _context_data: Vec<CD>,
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

        fn set_last_error(&self, _seq: u64, _error: RetrievalError) {
            // For tests, we don't track errors
        }

        fn remote_status(&self, _seq: u64, _status: crate::livequery::RemoteStatus) {
            // For tests, we don't track status transitions
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
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );

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
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate,
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );

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
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );
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
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );

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
        relay.subscribe_query(
            retryable_query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );
        relay.subscribe_query(
            non_retryable_query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
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
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());
        relay.set_node(mock_sender.clone()).expect("Failed to set message sender");

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Connect peer and setup established subscription
        relay.notify_peer_connected(peer_id);
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate,
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );

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
        let relay = SubscriptionRelay::new();
        let mock_sender = Arc::new(MockMessageSender::<CollectionId>::new());

        let query_id = proto::QueryId::new();
        let collection_id = create_test_collection_id();
        let predicate = create_test_selection();
        let peer_id = EntityId::new();

        // Test setup without message sender - should not crash
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate.clone(),
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );
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
        relay.subscribe_query(
            query_id,
            collection_id.clone(),
            predicate,
            crate::session::Session::detached(collection_id.clone()).into(),
            0,
            MockLiveQuery,
        );
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
