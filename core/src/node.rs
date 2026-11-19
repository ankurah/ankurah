use crate::{internal::prelude::*, schema::registration::RegistrationError};
use ankql::ast::Resolved;
use ankurah_proto::Attested;
use ankurah_signals::{Calculated, Get, Mut, Read, Wait};
use anyhow::anyhow;
use futures::{FutureExt, TryFutureExt};

use rand::prelude::*;
use rand::rngs::SmallRng;
use std::{
    fmt,
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::sync::oneshot;

use crate::connector::{ConnectionId, PeerConnectionError, PeerSender};
use crate::context::{Context, ContextAuth, ContextInner};
use crate::entity::WeakEntitySet;
use crate::error::{NodeHaltReason, NodeReadinessError, RequestError};
use crate::peer_subscription::{SubscriptionHandler, SubscriptionRelay};
use crate::policy::ContextPolicy;
use crate::reactor::Reactor;
use crate::retrieval::{CachedEventGetter, LocalEventGetter, LocalStateGetter};
use crate::schema::catalog::register::RegistrationAuth;
use crate::system::SystemManager;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use crate::util::Iterable;
use crate::{action_info, notice_info};
use itertools::Itertools;
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, warn};

pub mod applier;
mod comparison;
mod erased;
pub(crate) mod event_admissibility;
pub(crate) mod handler;
pub mod handles;
mod key;
mod key_lookup;
mod match_args;
mod peer_state;
mod replication;
mod schema_registration;
mod state;
#[cfg(test)]
mod tests;
mod trees;

pub(crate) use erased::NodeErased;
pub use handles::{NodeHandle, NodeRef};
pub use key::{InvalidNodeKey, NodeKey};
pub use match_args::{nocache, CachePolicy, MatchArgs};
pub use peer_state::PeerState;
pub use replication::{Replication, ReplicationState};
use schema_registration::WireRegistrant;
pub use state::NodeState;

/// A participant in the Ankurah network, and primary place where queries are initiated

pub struct Node<SE, PA>(pub(crate) Arc<NodeInner<SE, PA>>)
where PA: PolicyAgent;
impl<SE, PA> Clone for Node<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct WeakNode<SE, PA>
where PA: PolicyAgent
{
    node: Weak<NodeInner<SE, PA>>,
    /// The node's keypair, readable after the node drops so that a weak handle still gives the node id.
    keypair: Read<Option<NodeKey>>,
}
impl<SE, PA> Clone for WeakNode<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self { node: self.node.clone(), keypair: self.keypair.clone() } }
}

impl<SE, PA> WeakNode<SE, PA>
where PA: PolicyAgent
{
    pub fn upgrade(&self) -> Option<Node<SE, PA>> { self.node.upgrade().map(Node) }

    /// The node's id, or `None` while the node is unkeyed.
    pub fn node_id(&self) -> Option<proto::NodeId> { self.keypair.value().map(|key| key.public_key().node_id()) }
}

impl<SE, PA> Deref for Node<SE, PA>
where PA: PolicyAgent
{
    type Target = Arc<NodeInner<SE, PA>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

pub use crate::session::ContextData;

pub struct NodeInner<SE, PA>
where PA: PolicyAgent
{
    /// The node's keypair, which gives its node id and signs its presence: `None` while the node is unkeyed, until
    /// the lookup that [`Node::new`] and [`Node::new_durable`] start finds the stored keypair or stores a new one.
    keypair: Mut<Option<NodeKey>>,
    pub durable: bool,
    pub storage: Arc<SE>,

    /// Digest trees created by this node's queries and maintained from canonical state.
    /// Shared independently so the maintenance worker never retains the Node.
    pub(crate) trees: Arc<crate::sync::TreeManager<SE>>,
    tree_maintenance: Arc<tokio::sync::Notify>,
    captures: comparison::serve::Captures<SE, PA::ContextData>,

    pub(crate) entities: WeakEntitySet,
    /// Hold across storage commit and WeakEntitySet publication: register a new resident entity
    /// or update one already registered there. Otherwise a competing writer could retry
    /// from stale in-memory state after storage advances. Reactor notification follows later.
    pub(crate) commit_publication_lock: Arc<tokio::sync::Mutex<()>>,
    peer_connections: SafeMap<ConnectionId, Arc<PeerState<PA::ContextData>>>,
    next_connection: AtomicU64,
    durable_peers: SafeSet<ConnectionId>,

    /// Per-node source of randomness for peer selection. Seeded from entropy in production;
    /// an explicit seed can be injected at construction so the simulation harness and tests
    /// can reproduce an identical selection sequence. Held behind a Mutex because draws mutate
    /// RNG state and Node is shared across tasks; the lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,

    /// The continuous superset of every session backing a context on
    /// this node: each context attaches its credential source here at
    /// construction, so any state the node acts under is enumerable
    /// from one place.
    pub(crate) sessions: SessionSet<PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Reactor,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE>,

    pub catalog: Arc<CatalogManager>,
    alive: Mut<bool>,
    /// Services may run during startup, but stop on halt or node drop.
    pub(crate) run: Calculated<bool>,

    /// Weak bookkeeping for this node's live queries.
    pub(crate) live_queries: crate::livequery::LiveQueryRegistry,

    pub(crate) subscription_relay: Option<SubscriptionRelay<PA::ContextData, crate::livequery::WeakEntityLiveQuery>>,
}

impl<SE, PA> NodeInner<SE, PA>
where PA: PolicyAgent
{
    /// This node's id, the public key of its keypair: `None` while the node is unkeyed.
    pub fn id(&self) -> Option<proto::NodeId> { self.keypair.with(|key| key.as_ref().map(|key| key.public_key().node_id())) }

    /// This node's id, on a path that runs only on a connection: [`Node::register_peer`] waits for the keypair,
    /// so a node with a connection is keyed.
    fn keyed_id(&self) -> proto::NodeId { self.id().expect("a node with a connection is keyed") }
}

impl<SE, PA> NodeErased for NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn check_not_halted(&self) -> Result<(), NodeHaltReason> { self.system.check_not_halted() }

    fn reactor(&self) -> &Reactor { &self.reactor }
    fn live_queries(&self) -> &crate::livequery::LiveQueryRegistry { &self.live_queries }

    fn unsubscribe_remote_query(&self, query_id: proto::QueryId) {
        if let Some(relay) = &self.subscription_relay {
            relay.unsubscribe_predicate(query_id);
        }
    }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// An ephemeral node with the keypair its storage engine keeps, so its node id stays the same for as long as the
    /// store keeps that keypair. The node starts unkeyed, and looks the keypair up, or stores a new one, as it initializes.
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self { Self::build(engine, policy_agent, false, None, SmallRng::from_entropy()) }

    /// A durable node with the keypair its storage engine keeps, so its node id stays the same for as long as the
    /// store keeps that keypair. The node starts unkeyed, and looks the keypair up, or stores a new one, as it initializes.
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self::build(engine, policy_agent, true, None, SmallRng::from_entropy())
    }

    /// An ephemeral node, keyed from the start with a keypair supplied by the application; its storage engine's keypair
    /// is not read. Reusing the same keypair preserves the node id across runs.
    pub fn new_with_key(engine: Arc<SE>, policy_agent: PA, key: NodeKey) -> Self {
        Self::build(engine, policy_agent, false, Some(key), SmallRng::from_entropy())
    }

    /// A durable node, keyed from the start with a keypair supplied by the application; its storage engine's keypair
    /// is not read. Reusing the same keypair preserves the node id across runs.
    pub fn new_durable_with_key(engine: Arc<SE>, policy_agent: PA, key: NodeKey) -> Self {
        Self::build(engine, policy_agent, true, Some(key), SmallRng::from_entropy())
    }

    /// Wipe local storage, the stored keypair included, when a different system is offered, so a new node can adopt it.
    /// Disabled by default. A different system always halts this node; it never recovers in place.
    pub fn set_allow_system_replacement(&self, allow: bool) { self.system.set_allow_system_replacement(allow); }

    /// Initialization and terminal halt state; independent of peer connectivity.
    pub fn state(&self) -> ankurah_signals::Read<NodeState> { self.system.node_state() }

    /// Require completed keypair, system, catalog, and policy initialization, or return why the node is unavailable.
    pub fn check_ready(&self) -> Result<(), NodeReadinessError> { self.state().value().check_ready() }

    /// Wait for keypair, system, catalog, and policy initialization without retaining the node; return an error if it halts.
    pub fn wait_ready(&self) -> impl std::future::Future<Output = Result<(), NodeReadinessError>> + Send + 'static {
        let state = self.state();
        async move {
            state
                .wait_for(|state| match state.check_ready() {
                    Err(NodeReadinessError::NotReady) => None,
                    result => Some(result),
                })
                .await
        }
    }

    /// Construct an ephemeral node whose peer-selection RNG is seeded explicitly, keyed from the start with a keypair
    /// generated for this run. Intended for the simulation harness and deterministic tests; production paths use
    /// [`Node::new`].
    pub fn new_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, false, Some(NodeKey::generate()), SmallRng::seed_from_u64(rng_seed))
    }

    /// Construct a durable node whose peer-selection RNG is seeded explicitly, keyed from the start with a keypair
    /// generated for this run. Intended for the simulation harness and deterministic tests; production paths use
    /// [`Node::new_durable`].
    pub fn new_durable_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, true, Some(NodeKey::generate()), SmallRng::seed_from_u64(rng_seed))
    }

    fn build(engine: Arc<SE>, policy_agent: PA, durable: bool, key: Option<NodeKey>, rng: SmallRng) -> Self {
        let entityset = WeakEntitySet::new(SystemEpoch::allocate());
        let unkeyed = key.is_none();
        let system_manager = SystemManager::new(engine.clone(), entityset.clone(), durable);
        let alive = Mut::new(true);
        let state = system_manager.node_state();
        let run = {
            let alive = alive.read();
            let state = state.clone();
            Calculated::new(move || alive.get() && !matches!(state.get(), NodeState::Halted(_)))
        };
        let reactor = Reactor::new(run.clone());

        let live_queries = crate::livequery::LiveQueryRegistry::new();

        // Only ephemeral nodes relay subscriptions upstream to a durable peer.
        let subscription_relay = if durable { None } else { Some(SubscriptionRelay::new(&run)) };

        let commit_publication_lock = Arc::new(tokio::sync::Mutex::new(()));
        let catalog = Arc::new(CatalogManager::default());
        let trees = crate::sync::TreeManager::new(
            engine.clone(),
            commit_publication_lock.clone(),
            crate::sync::CacheSettings { bytes: 8 * 1024 * 1024 },
            64,
        )
        .with_catalog(&catalog);

        let node = Node(Arc::new(NodeInner {
            keypair: Mut::new(key),
            storage: engine,
            trees: Arc::new(trees),
            tree_maintenance: Arc::new(tokio::sync::Notify::new()),
            captures: comparison::serve::Captures::default(),
            entities: entityset,
            commit_publication_lock,
            peer_connections: SafeMap::new(),
            next_connection: AtomicU64::new(0),
            durable_peers: SafeSet::new(),
            rng: Mutex::new(rng),
            reactor,
            durable,
            policy_agent,
            system: system_manager,
            catalog,
            alive,
            run,
            sessions: SessionSet::new(),
            live_queries,
            subscription_relay,
        }));
        notice_info!("{node} created as {}", if durable { "durable" } else { "ephemeral" });

        let resolver: Arc<dyn crate::storage::CatalogResolver> = node.catalog.clone();
        node.storage.set_catalog_resolver(Arc::downgrade(&resolver));
        node.start_tree_maintenance();

        if let Some(ref relay) = node.subscription_relay {
            let weak_node = node.weak();
            if relay.set_node(Arc::new(weak_node)).is_err() {
                warn!("Failed to set message sender for subscription relay");
            }
        }

        let catalog = node.catalog.clone();
        let weak_node = node.weak();
        let system = node.system.clone();
        let run = node.run.clone();
        let agent = node.policy_agent.clone();
        let storage = node.storage.clone();
        crate::task::spawn(async move {
            let keyed = {
                let weak_node = weak_node.clone();
                async move {
                    if unkeyed {
                        let key = key_lookup::look_up(&*storage).await?;
                        if let Some(node) = weak_node.upgrade() {
                            node.keypair.set(Some(key));
                            notice_info!("{node} has its keypair");
                        }
                    }
                    Ok::<_, NodeHaltReason>(())
                }
            };
            let initialized = futures::future::try_join3(system.wait_system_ready(), catalog.start(weak_node.clone()).err_into(), keyed);
            let initialized = async move {
                initialized.await?;
                agent.start(weak_node).await.map_err(|error| NodeHaltReason::PolicyAgentStartFailed(error.to_string()))
            };
            tokio::select! {
                biased;
                _ = run.wait_value(false) => {},
                result = initialized => match result {
                    Ok(_) => system.mark_running(),
                    Err(reason) => { system.halt(reason); }
                },
            }
        });

        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode { node: Arc::downgrade(&self.0), keypair: self.0.keypair.read() } }

    /// Wait for this node's keypair and return the node id it gives, or the reason the node halted unkeyed.
    pub async fn wait_id(&self) -> Result<proto::NodeId, NodeHaltReason> { Ok(self.wait_keypair().await?.public_key().node_id()) }

    /// This node's keypair once it has one, or the reason the node halted unkeyed.
    async fn wait_keypair(&self) -> Result<NodeKey, NodeHaltReason> {
        tokio::select! {
            biased;
            key = self.keypair.wait_for(|key: &Option<NodeKey>| key.clone()) => Ok(key),
            reason = self.system.wait_halted() => Err(reason),
        }
    }

    /// A signed presence for the peer's `challenge`, using this node's `side` of the connection.
    /// The node waits for its keypair, and for system storage to load so its presence includes any stored system root.
    pub async fn presence(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
    ) -> Result<proto::SignedPresence, NodeHaltReason> {
        self.system.wait_loaded().await?;
        let key = self.wait_keypair().await?;
        let presence = proto::Presence {
            key: key.public_key(),
            durable: self.durable,
            system_root: self.system.root(),
            protocol_version: proto::PROTOCOL_VERSION,
        };
        Ok(presence.sign(side, challenge, |bytes| key.sign(bytes)))
    }

    /// The node accepts a peer connection only if its presence passes signature, protocol version and system checks.
    /// `side` is this node's side; the peer's signature must cover the opposite side and the `challenge` this node sent.
    /// It waits for this node's keypair first, so a node with a connection is keyed.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %signed.presence.node_id().to_base64_short(), durable = %signed.presence.durable)))]
    pub async fn register_peer(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
        signed: proto::SignedPresence,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionId, PeerConnectionError> {
        let id = self.wait_id().await?;
        action_info!(self, "register_peer", "{}", &signed.presence);
        let node_id = signed.presence.node_id();

        if !proto::protocol_compatible(signed.presence.protocol_version) {
            let rejection = proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: signed.presence.protocol_version };
            warn!("Node({}) refusing peer {}: {}", id, node_id, rejection);
            return Err(rejection.into());
        }
        if !signed.answers(side.opposite(), challenge) {
            warn!("Node({}) refusing peer {}: {}", id, node_id, PeerConnectionError::UnprovenKey);
            return Err(PeerConnectionError::UnprovenKey);
        }
        let presence = signed.presence;

        self.system.wait_loaded().await?;
        if presence.durable && !self.durable {
            self.system.adopt_system(presence.system_root.clone().ok_or(PeerConnectionError::MissingSystem)?).await?;
        } else if let (Some(current), Some(offered)) = (self.system.root_id(), &presence.system_root) {
            if current != offered.payload.entity_id {
                return Err(PeerConnectionError::SystemMismatch { current, offered: offered.payload.entity_id });
            }
        }
        let connection = ConnectionId(self.next_connection.fetch_add(1, Ordering::Relaxed));
        notice_info!("Node({:#}) register_peer {:#} as {}", id, node_id, connection);
        let subscription_handler = SubscriptionHandler::new(connection, self);
        self.peer_connections.insert(connection, Arc::new(PeerState::new(node_id, sender, presence.durable, subscription_handler)));
        if presence.durable {
            self.durable_peers.insert(connection);
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_connected(connection);
            }
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
        Ok(connection)
    }
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(connection = %connection)))]
    pub fn deregister_peer(&self, connection: ConnectionId) {
        notice_info!("{self} deregister_peer {connection}");

        self.durable_peers.remove(&connection);
        self.captures.release_peer(connection);
        // Get and cleanup subscriptions before removing the peer
        if let Some(peer_state) = self.peer_connections.remove(&connection) {
            peer_state.clear_pending();
            action_info!(
                self,
                "unsubscribing",
                "subscription {} for peer {}",
                peer_state.subscription_handler.subscription_id(),
                peer_state.node_id
            );
            // ReactorSubscription is automatically unsubscribed on drop
        }

        // Notify subscription relay of peer disconnection (unconditional - relay handles filtering)
        if let Some(ref relay) = self.subscription_relay {
            relay.notify_peer_disconnected(connection);
        }
    }
    /// The node id the peer registered for `connection`.
    pub(crate) fn peer_node_id(&self, connection: ConnectionId) -> Result<proto::NodeId, RequestError> {
        Ok(self.peer_connections.get(&connection).ok_or(RequestError::PeerNotConnected)?.node_id)
    }

    /// The connections to peers with `node_id`, in registration order.
    pub fn connections_of(&self, node_id: proto::NodeId) -> Vec<ConnectionId> {
        let mut connections: Vec<_> = self
            .peer_connections
            .to_vec()
            .into_iter()
            .filter(|(_, peer)| peer.node_id == node_id)
            .map(|(connection, _)| connection)
            .collect();
        connections.sort();
        connections
    }

    /// The node sends a request on the most recently registered connection for `node_id` and waits for the response.
    pub async fn request<'a, C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        let connection = self.connections_of(node_id).pop().ok_or(RequestError::PeerNotConnected)?;
        self.request_on(connection, cdata, request_body).await
    }

    /// The node sends a request on `connection` and accepts its response only on that connection.
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(connection = %connection, request_body = %request_body)))]
    pub(crate) async fn request_on<'a, C>(
        &self,
        connection: ConnectionId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.system.check_not_halted()?;
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        let request =
            proto::NodeRequest { id: request_id.clone(), to: self.peer_node_id(connection)?, from: self.keyed_id(), body: request_body };

        // Temporary credentialless catalog reads. Remove this bypass when the #425 catalog
        // follow-on uses #426's node session union for catalog queries.
        let auth = if crate::schema::request_bypasses_policy(&request.body) {
            Vec::new()
        } else {
            self.policy_agent.sign_request(self, cdata, &request)?
        };

        let _pending = {
            let peer = self.peer_connections.get(&connection).ok_or(RequestError::PeerNotConnected)?;
            let pending = peer_state::PendingRequest { connection: Arc::downgrade(&peer), id: request_id.clone() };
            peer.pending_requests.insert(request_id, response_tx);
            peer.send_message(proto::NodeMessage::Request { auth, request })?;
            pending
        };

        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    // TODO LATER: rework this to be retried in the background some number of times
    pub fn send_update(&self, connection: ConnectionId, notification: proto::NodeUpdateBody) {
        if self.system.check_not_halted().is_err() {
            return;
        }
        // same as request, minus cdata and the sign_request step
        debug!("{self}.send_update({connection}, {notification})");
        let (response_tx, _response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        // Get the peer connection
        let Some(peer) = self.peer_connections.get(&connection) else {
            warn!("Failed to send update on {}: {}", connection, RequestError::PeerNotConnected);
            return;
        };

        // Store the response channel
        peer.pending_updates.insert(id.clone(), response_tx);

        let notification =
            proto::NodeMessage::Update(proto::NodeUpdate { id, from: self.keyed_id(), to: peer.node_id, body: notification });

        match peer.send_message(notification) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to send update to peer {}: {}", peer.node_id, e);
            }
        };

        // response_rx.await.map_err(|_| RequestError::InternalChannelClosed)??;
    }

    /// The node accepts a message only if its sender node id matches the peer registered for `connection`.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, connection: ConnectionId, message: proto::NodeMessage) -> anyhow::Result<()> {
        self.system.check_not_halted()?;
        let peer_node_id = self.peer_node_id(connection)?;
        if message.sender() != peer_node_id {
            return Err(anyhow!("peer {} sent a message as {}", peer_node_id, message.sender()));
        }
        let own_id = self.keyed_id();
        match message {
            proto::NodeMessage::Update(update) => {
                debug!("Node({}) received update {}", own_id, update);

                if let Some(sender) = { self.peer_connections.get(&connection).map(|c| c.sender.cloned()) } {
                    let _from = update.from;
                    let _id = update.id.clone();
                    if update.to != own_id {
                        warn!("{} received message from {} but is not the intended recipient", own_id, update.from);
                        return Ok(());
                    }

                    // take down the return address
                    let id = update.id.clone();
                    let to = update.from;
                    let from = own_id;

                    let body = match self.handle_update(connection, update).await {
                        Ok(_) => proto::NodeUpdateAckBody::Success,
                        Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                    };

                    sender.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
                }
            }
            proto::NodeMessage::UpdateAck(ack) => {
                debug!("Node({}) received ack notification {} {}", own_id, ack.id, ack.body);
                // let connection = self.peer_connections.get(&ack.from).ok_or(RequestError::PeerNotConnected)?;
                // if let Some(tx) = connection.pending_updates.remove(&ack.id) {
                //     tx.send(Ok(proto::NodeResponseBody::Success)).unwrap();
                // }
            }
            proto::NodeMessage::Request { auth, request } => {
                debug!("Node({}) received request {}", own_id, request);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer
                if let Some(sender) = { self.peer_connections.get(&connection).map(|c| c.sender.cloned()) } {
                    let from = request.from;
                    let request_id = request.id.clone();
                    if request.to != own_id {
                        warn!("{} received message from {} but is not the intended recipient", own_id, request.from);
                        return Ok(());
                    }

                    // Temporary credentialless catalog reads. Remove this bypass when the #425 catalog
                    // follow-on uses #426's node session union for catalog queries.
                    let cdata = if crate::schema::request_bypasses_policy(&request.body) {
                        Ok(Vec::new())
                    } else {
                        self.policy_agent.check_request(self, &auth, &request).await
                    };
                    let body = match cdata {
                        Ok(cdata) => match self.handle_request(connection, &cdata, request).await {
                            Ok(result) => result,
                            Err(e) => comparison::response_error(e),
                        },
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let _result =
                        sender.send_message(proto::NodeMessage::Response(proto::NodeResponse { request_id, from: own_id, to: from, body }));
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", own_id, response);
                if response.to != own_id {
                    return Err(anyhow!("response addressed to another node"));
                }
                let peer = self.peer_connections.get(&connection).ok_or(RequestError::PeerNotConnected)?;
                if let Some(tx) = peer.pending_requests.remove(&response.request_id) {
                    tx.send(Ok(response.body)).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
            proto::NodeMessage::ReleaseComparison { comparison, .. } => {
                self.captures.release(connection, comparison);
            }
            proto::NodeMessage::UnsubscribeQuery { query_id, .. } => {
                // Remove predicate from the peer's subscription
                if let Some(peer_state) = self.peer_connections.get(&connection) {
                    peer_state.subscription_handler.remove_predicate(query_id).await?;
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(request = %request)))]
    async fn handle_request<C>(
        &self,
        connection: ConnectionId,
        cdata: &C,
        request: proto::NodeRequest,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        C: Iterable<PA::ContextData>,
    {
        match request.body {
            proto::NodeRequestBody::CommitTransaction { id, events } => handler::commit_transaction(self, cdata, id, events).await,
            proto::NodeRequestBody::RegisterSchema { model } => {
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for RegisterSchema"))?;
                if !self.durable {
                    return Ok(proto::NodeResponseBody::Error(RegistrationError::NotDurable.to_string()));
                }
                let mut registrant = WireRegistrant::new(model);
                match self.catalog.resolve_or_register(self, &mut registrant, RegistrationAuth::Credential(cdata.clone())).await {
                    Ok(()) => Ok(proto::NodeResponseBody::SchemaRegistered {
                        model: registrant.into_response().expect("successful registration must bind its response"),
                    }),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { selection, comparison, key_spec, known_state } => {
                let credentials = cdata.iterable().cloned().collect();
                let target = self.begin_comparison(connection, comparison, selection, key_spec, known_state, &credentials).await?;
                Ok(proto::NodeResponseBody::Fetch(target))
            }
            proto::NodeRequestBody::Compare { comparison, selection, request: exchange } => {
                let credentials = cdata.iterable().cloned().collect();
                let result = self.handle_comparison(connection, comparison, &selection, exchange, &credentials).await;
                match result {
                    Ok(reply) => Ok(proto::NodeResponseBody::Compared(Ok(reply))),
                    Err(error) => Ok(comparison::response_error(error.into())),
                }
            }
            proto::NodeRequestBody::Get { ids } => {
                let policy = ContextPolicy::from_credentials(&self.policy_agent, cdata);
                let mut results: Vec<_> =
                    self.storage.get_states(ids, &policy.retrieval_predicate()).await?.into_iter().map(Into::into).collect();
                policy.check_reads(&mut results);
                Ok(proto::NodeResponseBody::Get(results))
            }
            proto::NodeRequestBody::GetEvents { event_ids } => Ok(handler::get_events(self, cdata, event_ids).await?),
            proto::NodeRequestBody::SubscribeQuery { query_id, selection, version, comparison, key_spec, known_state } => {
                let peer_state = self.peer_connections.get(&connection).ok_or(RequestError::PeerNotConnected)?;
                // Catalog subscriptions carry no credential; others carry exactly one.
                let cdata = match cdata.iterable().at_most_one() {
                    Ok(cdata) => cdata,
                    Err(_) => {
                        return Err(anyhow!(
                            "SubscribeQuery currently requires at most one cdata (this server does not yet accept several per subscribe)"
                        ));
                    }
                };
                peer_state
                    .subscription_handler
                    .subscribe_query(self, query_id, selection, cdata, version, comparison, key_spec, known_state)
                    .await
            }
        }
    }

    async fn handle_update(&self, connection: ConnectionId, notification: proto::NodeUpdate) -> anyhow::Result<()> {
        match notification.body {
            proto::NodeUpdateBody::Comparison { comparison, deltas } => {
                self.receive_comparison_repair(connection, comparison, notification.id, deltas).boxed().await?;
                Ok(())
            }
            proto::NodeUpdateBody::SubscriptionUpdate { items } => {
                tracing::debug!("Node({}) received subscription update from peer {}", self.keyed_id(), notification.from);
                self.system.require_system_ready()?;
                crate::node::applier::NodeApplier::apply_updates(self, &connection, &notification.from, items).await?;
                Ok(())
            }
        }
    }

    pub(crate) async fn relay_to_required_peers(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        events: &[Attested<proto::Event>],
    ) -> Result<(), MutationError> {
        // TODO determine how many durable peers need to respond before we can proceed. The others should continue in the background.
        // as of this writing, we only have one durable peer, so we can just await the response from "all" of them
        for connection in self.get_durable_peers() {
            match self
                .request_on(connection, cdata, proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.to_vec() })
                .await
            {
                Ok(proto::NodeResponseBody::CommitComplete { .. }) => (),
                Err(error) => return Err(RetrievalError::from(error).into()),
                Ok(proto::NodeResponseBody::Error(e)) => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!("Peer {} rejected: {}", connection, e)))));
                }
                _ => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                        "Peer {} returned unexpected response",
                        connection
                    )))));
                }
            }
        }
        Ok(())
    }

    /// Return the resident entity if a live handle keeps it in memory.
    pub fn get_resident_entity(&self, id: proto::EntityId) -> Option<crate::entity::Entity> { self.entities.get(&id) }

    /// Store events created outside this durable node, committing all of them or none.
    /// The node uses its own authority without policy checks; the caller must authorize the events.
    /// Supply parents before their children. Importing an event again leaves its entity unchanged.
    pub async fn import_events(&self, events: &[proto::Attested<proto::Event>]) -> Result<(), MutationError> {
        if !self.durable {
            return Err(MutationError::NotDurable);
        }
        self.system.require_system_ready()?;
        // Prevent registration from creating duplicate definitions while the import updates the catalog.
        let _allocator = self.catalog.allocator.lock().await;
        let policy = ContextPolicy::new(&self.policy_agent, ContextAuth::<Mut<Vec<PA::ContextData>>>::Privileged);
        crate::util::retry::retry_on!(MutationError::WriteConflict, {
            let mut trx = crate::transaction::remote::RemoteTransaction::new(self, &policy);
            for event in events {
                trx.add_event(event).await?;
            }
            trx.commit().await
        })
    }

    /// Build a context over its credential source: bare ContextData, an
    /// existing session handle, or a whole [`SessionSet`] —
    /// attached to the node's registry at construction.
    ///
    /// [`SessionSet`]: crate::session::SessionSet
    pub fn context(&self, sessions: impl Into<SessionSet<PA::ContextData>>) -> Result<Context, anyhow::Error> {
        self.check_ready()?;
        Ok(Context::new(Node::clone(self), sessions))
    }

    pub async fn context_async(&self, sessions: impl Into<SessionSet<PA::ContextData>>) -> Result<Context, NodeReadinessError> {
        self.wait_ready().await?;
        Ok(Context::new(Node::clone(self), sessions))
    }

    /// An uncredentialed context for local system work.
    pub(crate) fn privileged_context(&self) -> Context {
        Context(Arc::new(ContextInner { node: NodeHandle::Strong(Node::clone(self)), auth: ContextAuth::Privileged }))
    }

    /// Retrieve peer states and persist them in this node.
    pub(crate) async fn get_from_peer(&self, ids: Vec<proto::EntityId>, cdata: &Vec<PA::ContextData>) -> Result<(), RetrievalError> {
        self.system.require_system_ready().map_err(MutationError::from)?;
        let connection = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;
        let peer_node_id = self.peer_node_id(connection)?;

        match self.request_on(connection, cdata, proto::NodeRequestBody::Get { ids: ids.clone() }).await? {
            proto::NodeResponseBody::Get(results) => {
                if results.len() != ids.len() || results.iter().zip(&ids).any(|(result, id)| result.entity_id() != *id) {
                    return Err(RetrievalError::Other("Peer Get did not answer the requested identities".into()));
                }
                let deltas = results.into_iter().map(proto::EntityDelta::try_from).collect::<Result<Vec<_>, _>>()?;
                let event_getter = CachedEventGetter::new(self, cdata);
                applier::NodeApplier::apply_deltas(self, &peer_node_id, deltas, &event_getter, self.storage.as_ref()).await?;
                Ok(())
            }
            proto::NodeResponseBody::Error(e) => {
                debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                debug!("Unexpected response type from peer get");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }

    /// Reconcile with a durable peer before reading the current local result.
    pub(crate) async fn fetch_from_peer(
        &self,
        selection: ankql::ast::Selection<Resolved>,
        cdata: &Vec<PA::ContextData>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.system.require_system_ready().map_err(MutationError::from)?;
        let peer = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;
        self.reconcile_query(peer, selection.clone(), cdata, None).await?;
        self.fetch_entities_from_local(&selection).await
    }

    /// A randomly selected connection to a durable peer.
    ///
    /// Draws from the node's seeded RNG (not `thread_rng`) so the simulation harness can reproduce
    /// selections. The candidate slice is sorted first: `choose` indexes by position, so a stable
    /// candidate order is required for a given seed to yield a given peer.
    pub fn get_durable_peer_random(&self) -> Option<ConnectionId> {
        let peers = self.get_durable_peers();
        let mut rng = self.rng.lock().expect("node rng mutex poisoned");
        peers.choose(&mut *rng).copied()
    }

    /// Connections to durable peers in registration order, so the node sends requests in a reproducible order.
    /// The underlying set is unordered; callers that emit per peer (relay, random selection)
    /// depend on this stable order for the C1 determinism audit.
    pub fn get_durable_peers(&self) -> Vec<ConnectionId> {
        let mut peers = self.durable_peers.to_vec();
        peers.sort();
        peers
    }

    /// TEST ONLY: Create a phantom entity with a specific ID.
    ///
    /// This creates an entity that was never properly created via Transaction::create(),
    /// has no creation event, and has an empty state. Used for adversarial testing to
    /// verify that commit paths properly reject such phantom entities.
    ///
    /// WARNING: This bypasses all normal entity creation validation. Only use in tests.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn conjure_evil_phantom(&self, id: proto::EntityId, initial_model_membership: ModelId) -> crate::entity::Entity {
        self.entities.conjure_evil_phantom(id, initial_model_membership)
    }

    /// TEST ONLY: Register a durable peer connection directly, bypassing the connection handshake.
    ///
    /// Lets tests populate the durable-peer set to exercise fan-out ordering and seeded random
    /// selection without standing up real peer connections.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn insert_durable_peer_for_test(&self) -> ConnectionId {
        let connection = ConnectionId(self.next_connection.fetch_add(1, Ordering::Relaxed));
        self.durable_peers.insert(connection);
        connection
    }

    /// TEST ONLY: Observe the session registry — the current credential
    /// of every session backing a context on this node, in the
    /// registry's union order. Values, not handles: observation cannot
    /// become mutation.
    ///
    /// Lets tests assert membership (a context's source joining at
    /// construction, its members leaving when the last holder drops)
    /// while the registry itself stays core's, unreachable from outside.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn session_registry(&self) -> Vec<PA::ContextData> { self.sessions.current() }
}

impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub async fn request_remote_unsubscribe(&self, query_id: proto::QueryId, connections: Vec<ConnectionId>) -> anyhow::Result<()> {
        for (connection, peer) in self.peer_connections.get_list(connections) {
            if let Some(peer) = peer {
                peer.send_message(proto::NodeMessage::UnsubscribeQuery { from: self.keyed_id(), query_id })?;
            } else {
                warn!("Peer {} not connected", connection);
            }
        }

        Ok(())
    }
}

impl<SE, PA> Drop for NodeInner<SE, PA>
where PA: PolicyAgent
{
    fn drop(&mut self) {
        self.alive.set(false);
        notice_info!("{self} dropped");
    }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub(crate) fn subscribe_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        sessions: SessionSet<PA::ContextData>,
        version: u32,
        livequery: crate::livequery::WeakEntityLiveQuery,
    ) {
        if let Some(ref relay) = self.subscription_relay {
            relay.subscribe_query(query_id, selection, sessions, version, livequery);
        }
    }

    /// Load matching states from local storage into resident entities; does not apply read policy.
    pub async fn fetch_entities_from_local(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        self.system.check_not_halted()?;
        let initial_states = self.storage.fetch_states(selection).await?;
        let state_getter = LocalStateGetter::new(self.storage.clone());
        let event_getter = LocalEventGetter::new(self.storage.clone(), self.durable);
        let mut entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) = self.entities.with_state(&state_getter, &event_getter, state.payload.entity_id, state.payload.state).await?;
            entities.push(entity);
        }
        Ok(entities)
    }
}
impl<SE, PA> fmt::Display for NodeInner<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.id().map_or_else(|| "unkeyed".to_owned(), |id| id.to_base64_short());
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{id}\x1b[2m]\x1b[0m")
    }
}
impl<SE, PA> fmt::Display for Node<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { fmt::Display::fmt(&*self.0, f) }
}
