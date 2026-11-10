use crate::internal::prelude::*;
use ankql::ast::Resolved;
use ankurah_signals::{Calculated, Get, Mut, Wait};

use rand::prelude::*;
use rand::rngs::SmallRng;
use std::{
    fmt,
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

use crate::connector::{Handshake, PeerConnectionError, PeerProvenance, PeerSender};
use crate::context::{Context, ContextAuth, ContextInner};
use crate::entity::WeakEntitySet;
use crate::error::{NodeHaltReason, NodeReadinessError, RequestError};
use crate::peer_subscription::{SubscriptionHandler, SubscriptionRelay};
use crate::policy::PeerTrust;
use crate::reactor::Reactor;
use crate::retrieval::{LocalEventGetter, LocalStateGetter};
use crate::util::Iterable;
use crate::{action_info, notice_info};
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, info, warn};

mod builder;
pub(crate) mod delta;
pub(crate) mod durable_peer;
mod erased;
pub(crate) mod event_admissibility;
pub mod handles;
pub(crate) mod identity;
pub(crate) mod inbound;
mod lifecycle;
mod match_args;
mod peers;
#[cfg(test)]
mod tests;

pub use builder::NodeBuilder;
pub(crate) use erased::NodeErased;
pub use handles::{NodeHandle, NodeRef};
pub use identity::key::NodeKey;
pub use lifecycle::state::NodeState;
use lifecycle::NodeLifecycle;
pub use match_args::{nocache, CachePolicy, MatchArgs};
pub use peers::peer_state::PeerState;
use peers::plugin::NodePlugins;
pub use peers::plugin::{AnnouncementFailure, DuplicatePluginName, NodePlugin, PeerRefusal, PeerRegistration};
pub use peers::ConnectionRegistration;
use peers::{PeerRegistry, RequiredTrust};

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
    node_id: proto::NodeId,
}
impl<SE, PA> Clone for WeakNode<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self { node: self.node.clone(), node_id: self.node_id } }
}

impl<SE, PA> WeakNode<SE, PA>
where PA: PolicyAgent
{
    pub fn upgrade(&self) -> Option<Node<SE, PA>> { self.node.upgrade().map(Node) }
    pub fn node_id(&self) -> proto::NodeId { self.node_id }
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
    pub id: proto::NodeId,
    pub durable: bool,
    pub storage: Arc<SE>,
    /// Signs this node's presence; never handed to the policy agent.
    key: NodeKey,
    plugins: NodePlugins<SE, PA>,

    pub(crate) entities: WeakEntitySet,
    /// Hold across storage commit and WeakEntitySet publication: register a new resident entity
    /// or update one already registered there. Otherwise a competing writer could retry
    /// from stale in-memory state after storage advances. Reactor notification follows later.
    pub(crate) commit_publication_lock: tokio::sync::Mutex<()>,
    peers: PeerRegistry<PA::ContextData>,

    /// The continuous superset of every session backing a context on
    /// this node: each context attaches its credential source here at
    /// construction, so any state the node acts under is enumerable
    /// from one place.
    pub(crate) sessions: SessionSet<PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Reactor,
    pub(crate) policy_agent: PA,
    pub(crate) lifecycle: NodeLifecycle<SE>,

    pub catalog: Arc<CatalogManager>,
    alive: Mut<bool>,
    /// Services may run during startup, but stop on halt or node drop.
    pub(crate) run: Calculated<bool>,

    /// Weak bookkeeping for this node's live queries.
    pub(crate) live_queries: crate::livequery::LiveQueryRegistry,

    pub(crate) subscription_relay: Option<SubscriptionRelay<PA::ContextData, crate::livequery::WeakEntityLiveQuery>>,
}

impl<SE, PA> NodeErased for NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn check_not_halted(&self) -> Result<(), NodeHaltReason> { self.lifecycle.check_not_halted() }

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
    /// Construct an ephemeral node with a fresh identity and no plugins.
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self::start(engine, policy_agent, false, SmallRng::from_entropy(), None, NodePlugins::none())
    }
    /// Construct a durable node with a fresh identity, which does not survive a restart, and no
    /// plugins; [`Node::builder`] gives a node a key.
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self::start(engine, policy_agent, true, SmallRng::from_entropy(), None, NodePlugins::none())
    }
    /// Configure a node's role, key and plugins before it starts.
    pub fn builder(engine: Arc<SE>, policy_agent: PA) -> NodeBuilder<SE, PA> { NodeBuilder::new(engine, policy_agent) }

    /// Halt this node with [`NodeHaltReason::Reset`] (a node already halted keeps its reason) and wipe
    /// its storage. The wipe starts on the first poll, finishes without the caller, and waits for any
    /// storage commit in flight; none begins a commit after the halt. A call during or after another
    /// reset waits for that wipe rather than wiping again, and retries one that failed. `Ok` means
    /// the store is wiped; the host then builds a fresh node on it.
    pub async fn reset(&self) -> Result<(), MutationError> { self.lifecycle.reset().await }

    /// Initialization and terminal halt state; independent of peer connectivity.
    pub fn state(&self) -> ankurah_signals::Read<NodeState> { self.lifecycle.node_state() }

    /// This node's catalog epoch, or the reason it halted: a halted node, reset or not, yields no epoch.
    pub fn catalog_epoch(&self) -> Result<CatalogEpoch, NodeHaltReason> {
        self.lifecycle.check_not_halted()?;
        Ok(self.entities.catalog_epoch())
    }

    /// Require completed catalog and policy initialization, or return why the node is unavailable.
    pub fn check_ready(&self) -> Result<(), NodeReadinessError> { self.state().value().check_ready() }

    /// Wait for catalog and policy initialization without retaining the node; return an error if it halts.
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

    /// Wait until the catalog holds the definitions this node stores, without waiting for a durable
    /// peer as [`Node::wait_ready`] does; return the halt reason if the node halts first. Lets a
    /// plugin read stored entities of a model before the node's first durable answer.
    pub async fn wait_catalog_loaded(&self) -> Result<(), NodeHaltReason> {
        tokio::select! {
            biased;
            reason = self.lifecycle.wait_halted() => Err(reason),
            () = self.catalog.wait_loaded() => Ok(()),
        }
    }

    /// Construct an ephemeral node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new`].
    pub fn new_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::start(engine, policy_agent, false, SmallRng::seed_from_u64(rng_seed), None, NodePlugins::none())
    }

    /// Construct a durable node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new_durable`].
    pub fn new_durable_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::start(engine, policy_agent, true, SmallRng::seed_from_u64(rng_seed), None, NodePlugins::none())
    }

    fn start(engine: Arc<SE>, policy_agent: PA, durable: bool, rng: SmallRng, key: Option<NodeKey>, plugins: NodePlugins<SE, PA>) -> Self {
        let entityset = WeakEntitySet::new(CatalogEpoch::allocate());
        let key = key.unwrap_or_else(|| {
            let key = NodeKey::generate();
            if durable {
                notice_info!("Durable node {:#} was given no persisted key; its identity will not survive a restart", key.node_id());
            }
            key
        });
        let id = key.node_id();
        let lifecycle = NodeLifecycle::new(engine.clone());
        let alive = Mut::new(true);
        let state = lifecycle.node_state();
        let run = {
            let alive = alive.read();
            let state = state.clone();
            Calculated::new(move || alive.get() && !matches!(state.get(), NodeState::Halted(_)))
        };
        let reactor = Reactor::new(run.clone());
        notice_info!("Node {id:#} created as {}", if durable { "durable" } else { "ephemeral" });

        let live_queries = crate::livequery::LiveQueryRegistry::new();

        // Only ephemeral nodes relay subscriptions upstream to a durable peer.
        let subscription_relay = if durable { None } else { Some(SubscriptionRelay::new(&run)) };

        let node = Node(Arc::new(NodeInner {
            id,
            key,
            plugins,
            storage: engine,
            entities: entityset,
            commit_publication_lock: tokio::sync::Mutex::new(()),
            peers: PeerRegistry::new(rng),
            reactor,
            durable,
            policy_agent,
            lifecycle,
            catalog: Arc::new(CatalogManager::default()),
            alive,
            run,
            sessions: SessionSet::new(),
            live_queries,
            subscription_relay,
        }));

        let resolver: Arc<dyn crate::storage::CatalogResolver> = node.catalog.clone();
        node.storage.set_catalog_resolver(Arc::downgrade(&resolver));

        if let Some(ref relay) = node.subscription_relay {
            let weak_node = node.weak();
            if relay.set_node(Arc::new(weak_node)).is_err() {
                warn!("Failed to set message sender for subscription relay");
            }
        }

        let catalog = node.catalog.clone();
        let weak_node = node.weak();
        let lifecycle = node.lifecycle.clone();
        let run = node.run.clone();
        let agent = node.policy_agent.clone();
        crate::task::spawn(async move {
            let initialized = async move {
                catalog.start(weak_node.clone()).await?;
                agent.start(weak_node).await.map_err(|error| NodeHaltReason::PolicyAgentStartFailed(error.to_string()))
            };
            tokio::select! {
                biased;
                _ = run.wait_value(false) => {},
                result = initialized => match result {
                    Ok(_) => lifecycle.mark_running(),
                    Err(reason) => { lifecycle.halt(reason); }
                },
            }
        });

        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode { node: Arc::downgrade(&self.0), node_id: self.0.id } }

    /// The identity this node mints and signs under.
    pub fn identity(&self) -> proto::Identity { self.id.identity() }

    /// Begin this node's side of one new connection's handshake, reached as `provenance` says.
    pub fn begin_handshake(&self, provenance: PeerProvenance) -> Handshake { Handshake::new(self.id, provenance) }

    /// Sign this node's presence answering the peer on `handshake`'s connection, carrying each
    /// plugin's announcement; a halted node signs none, nor one whose plugin cannot announce, and
    /// none answers before the peer's opening arrives, or on a connection this node accepted
    /// before it registered the dialer.
    pub async fn presence(&self, handshake: &Handshake) -> Result<proto::Presence, PeerConnectionError> {
        self.lifecycle.check_not_halted()?;
        handshake.check_may_present()?;
        let mut claims = handshake.presence_claims(self.id, self.durable)?;
        claims.announcements = self.plugins.announcements(self).await.map_err(PeerConnectionError::AnnouncementFailed)?;
        Ok(self.key.sign_presence(claims))
    }

    /// Register a peer connection whose presence completes `handshake`, this node's side of it,
    /// once per handshake. How the connection was reached decides, through the policy agent's
    /// [`PolicyAgent::peer_trust`], whether the peer is durable. Then each plugin is asked, in the
    /// order plugged, and the first refusal leaves the peer unregistered. The connector holds the
    /// registration returned while the connection is open.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.claims.node_id.to_base64_short(), durable = %presence.claims.durable)))]
    pub async fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: &mut Handshake,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionRegistration, PeerConnectionError> {
        action_info!(self, "register_peer", "{}", &presence);

        if let Err(refusal) = handshake.authenticate(self.id, &presence) {
            warn!("Node({}) refusing peer: {}", self.id, refusal);
            return Err(refusal);
        }
        let provenance = handshake.provenance();
        let claims = &presence.claims;
        let peer = claims.node_id;
        let trust = self.policy_agent.peer_trust(&peer, provenance, claims.durable);
        if claims.durable && !trust.durable {
            info!("Node({}) serves {:?} peer {} as not durable despite its durable claim", self.id, provenance, peer);
        }

        self.lifecycle.check_not_halted()?;
        if let Err(refusal) = self.plugins.register_peer(self, claims, provenance, trust).await {
            warn!("Node({}) refusing peer {}: {}", self.id, peer, refusal);
            return Err(PeerConnectionError::Refused(refusal));
        }
        // A plugin may have halted this node.
        self.lifecycle.check_not_halted()?;
        let subscription_handler = SubscriptionHandler::new(peer, self);
        let registration = self.peers.register(peer, sender, trust, subscription_handler, self.subscription_relay.as_ref());
        handshake.record_registration();
        // TODO send hello message to the peer, including present head state for all relevant collections
        Ok(registration)
    }
    /// Remove `registration`, which [`Node::register_peer`] returned; a newer registration of the
    /// same peer stays.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %registration.peer().to_base64_short())))]
    pub fn deregister_peer(&self, registration: ConnectionRegistration) {
        let node_id = registration.peer();
        notice_info!("Node({:#}) deregister_peer {:#}", self.id, node_id);

        if let Some(peer_state) = self.peers.deregister(registration, self.subscription_relay.as_ref()) {
            action_info!(self, "unsubscribing", "subscription {} for peer {}", peer_state.subscription_handler.subscription_id(), node_id);
            // ReactorSubscription is automatically unsubscribed on drop
        }
    }
    /// Send a request to a connected peer and wait for its response.
    pub async fn request<'a, C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.send_request(node_id, RequiredTrust::Any, cdata, request_body).await
    }

    /// Send a request to a peer chosen because it is durable, and wait for its response. It is
    /// refused with [`RequestError::NotTrustedAsDurable`] unless the connection now registered
    /// for that peer is trusted as durable.
    pub async fn request_durable<C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.send_request(node_id, RequiredTrust::Durable, cdata, request_body).await
    }

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id, request_body = %request_body)))]
    async fn send_request<C>(
        &self,
        node_id: proto::NodeId,
        required: RequiredTrust,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.lifecycle.check_not_halted()?;
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        // Temporary credentialless catalog reads. Remove this bypass when the #425 catalog
        // follow-on uses #426's node session union for catalog queries.
        let bypasses_policy = crate::schema::request_bypasses_policy(&request_body);
        let request =
            proto::NodeRequest { id: request_id.clone(), to: node_id, from: self.id, body: proto::CarriedBody::encode(&request_body) };
        let auth = if bypasses_policy { Vec::new() } else { self.policy_agent.sign_request(self, cdata, &request)? };

        {
            // Checked and sent with no await between, through the connection that was checked.
            let connection = self.peers.trusted_connection(&node_id, required)?;
            connection.pending_requests.insert(request_id, response_tx);
            connection.send_message(proto::NodeMessage::Request { auth, request })?;
        }

        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    // TODO LATER: rework this to be retried in the background some number of times
    pub fn send_update(&self, node_id: proto::NodeId, notification: proto::NodeUpdateBody) {
        if self.lifecycle.check_not_halted().is_err() {
            return;
        }
        // same as request, minus cdata and the sign_request step
        debug!("{self}.send_update({node_id:#}, {notification})");
        let (response_tx, _response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        // Get the peer connection
        let Some(connection) = self.peers.connection(&node_id) else {
            warn!("Failed to send update to peer {}: {}", node_id, RequestError::PeerNotConnected);
            return;
        };

        // Store the response channel
        connection.pending_updates.insert(id.clone(), response_tx);

        let body = proto::CarriedBody::encode(&notification);
        let notification = proto::NodeMessage::Update(proto::NodeUpdate { id, from: self.id, to: node_id, body });

        match connection.send_message(notification) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to send update to peer {}: {}", node_id, e);
            }
        };

        // response_rx.await.map_err(|_| RequestError::InternalChannelClosed)??;
    }

    /// Handle a frame that arrived on the connection `registration` registered. A frame naming
    /// another node as its sender, or arriving once a newer registration of the peer replaced
    /// this one, is refused and logged, and the connection stays up.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, registration: ConnectionRegistration, message: proto::NodeMessage) -> anyhow::Result<()> {
        inbound::handle_message(self, registration, message).await
    }

    /// Return the resident entity if a live handle keeps it in memory.
    pub fn get_resident_entity(&self, id: proto::EntityId) -> Option<crate::entity::Entity> { self.entities.get(&id) }

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

    /// A durable peer drawn from the node's seeded source of randomness.
    pub fn get_durable_peer_random(&self) -> Option<proto::NodeId> { self.peers.random_durable() }

    /// The trust this node granted a connected peer.
    pub fn granted_trust(&self, peer: &proto::NodeId) -> Option<PeerTrust> { self.peers.granted_trust(peer) }

    /// The durable peers' ids, sorted by id for a stable fan-out order.
    pub fn get_durable_peers(&self) -> Vec<proto::NodeId> { self.peers.durable_peers() }

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

    /// TEST ONLY: Register a durable peer id directly, bypassing the connection handshake.
    ///
    /// Lets tests populate the durable-peer set to exercise fan-out ordering and seeded random
    /// selection without standing up real peer connections.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn insert_durable_peer_for_test(&self, peer_id: proto::NodeId) { self.peers.insert_durable_for_test(peer_id); }

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
    pub async fn request_remote_unsubscribe(&self, query_id: proto::QueryId, peers: Vec<proto::NodeId>) -> anyhow::Result<()> {
        for (peer_id, item) in self.peers.connections(peers) {
            if let Some(connection) = item {
                connection.send_message(proto::NodeMessage::UnsubscribeQuery { from: self.id, query_id })?;
            } else {
                warn!("Peer {} not connected", peer_id);
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
        notice_info!("Node({}) dropped", self.id);
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
        self.lifecycle.check_not_halted()?;
        let initial_states = self.storage.fetch_states(selection).await?;
        let state_getter = LocalStateGetter::new(self.storage.clone());
        let event_getter = LocalEventGetter::new(self.storage.clone(), self.durable);
        let mut entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) = self.entities.with_state(&state_getter, &event_getter, state.entity_id, state.state).await?;
            entities.push(entity);
        }
        Ok(entities)
    }
}
impl<SE, PA> fmt::Display for Node<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{}\x1b[2m]\x1b[0m", self.id.to_base64_short())
    }
}
