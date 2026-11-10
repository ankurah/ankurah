use crate::{internal::prelude::*, schema::registration::RegistrationError};
use ankql::ast::Resolved;
use ankurah_proto::Attested;
use ankurah_signals::{Calculated, Get, Mut, Wait};
use anyhow::anyhow;

use rand::prelude::*;
use rand::rngs::SmallRng;
use std::{
    fmt,
    ops::Deref,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::oneshot;

use crate::connector::{Handshake, PeerConnectionError, PeerProvenance, PeerSender};
use crate::context::{Context, ContextAuth, ContextInner};
use crate::entity::WeakEntitySet;
use crate::error::{NodeHaltReason, NodeReadinessError, RequestError};
use crate::peer_subscription::{SubscriptionHandler, SubscriptionRelay};
use crate::policy::{ContextPolicy, PeerTrust};
use crate::reactor::Reactor;
use crate::retrieval::{CachedEventGetter, LocalEventGetter, LocalStateGetter};
use crate::schema::catalog::register::RegistrationAuth;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use crate::util::Iterable;
use crate::{action_info, notice_info};
use itertools::Itertools;
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, info, warn};

pub mod applier;
pub(crate) mod attestation;
mod builder;
mod erased;
pub(crate) mod event_admissibility;
pub(crate) mod handler;
pub mod handles;
mod key;
mod lifecycle;
mod match_args;
mod peer_state;
mod plugin;
mod schema_registration;
mod state;
#[cfg(test)]
mod tests;

pub use builder::NodeBuilder;
pub(crate) use erased::NodeErased;
pub use handles::{NodeHandle, NodeRef};
pub use key::NodeKey;
use lifecycle::NodeLifecycle;
pub use match_args::{nocache, CachePolicy, MatchArgs};
pub use peer_state::PeerState;
use plugin::NodePlugins;
pub use plugin::{DuplicatePluginName, NodePlugin, PeerRefusal, PeerRegistration};
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

/// What a request needs from the trust its recipient holds when it is sent.
#[derive(Clone, Copy)]
enum RequiredTrust {
    /// Any registered peer.
    Any,
    /// A peer this node trusts as durable.
    Durable,
}

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
    peer_connections: SafeMap<proto::NodeId, Arc<PeerState<PA::ContextData>>>,
    durable_peers: SafeSet<proto::NodeId>,
    /// Held while a registration or deregistration updates `durable_peers`, the subscription
    /// relay and `peer_connections`, so all three describe the same registration.
    registration: Mutex<()>,

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
    /// storage commit in flight; none commits after the halt. A call during or after another reset
    /// waits for that wipe rather than wiping again, and retries one that failed. `Ok` means the
    /// store is wiped; the host then builds a fresh node on it.
    pub async fn reset(&self) -> Result<(), MutationError> { self.lifecycle.reset().await }

    /// Initialization and terminal halt state; independent of peer connectivity.
    pub fn state(&self) -> ankurah_signals::Read<NodeState> { self.lifecycle.node_state() }

    /// This node's system epoch, or the reason it halted: a halted node, reset or not, yields no epoch.
    pub fn system_epoch(&self) -> Result<SystemEpoch, NodeHaltReason> {
        self.lifecycle.check_not_halted()?;
        Ok(self.entities.system_epoch())
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
        let entityset = WeakEntitySet::new(SystemEpoch::allocate());
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
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            registration: Mutex::new(()),
            rng: Mutex::new(rng),
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

    /// Begin this node's side of one new connection's handshake. Send its opening to the peer
    /// first, take the peer's opening into it, then pass it to [`Node::register_peer`] with the
    /// peer's presence.
    pub fn begin_handshake(&self) -> Handshake { Handshake::new(self.id) }

    /// Sign this node's presence answering the peer on `handshake`'s connection, carrying each
    /// plugin's announcement; a halted node signs none, and none answers before the peer's opening arrives.
    pub async fn presence(&self, handshake: &Handshake) -> Result<proto::Presence, PeerConnectionError> {
        self.lifecycle.check_not_halted()?;
        let mut claims = handshake.presence_claims(self.id, self.durable)?;
        claims.announcements = self.plugins.announcements(self).await;
        Ok(self.key.sign_presence(claims))
    }

    /// Register a peer connection whose presence completes `handshake`, this node's side of it.
    /// `provenance` says how the connection was made; the policy agent's [`PolicyAgent::peer_trust`]
    /// decides from it whether the peer is durable. Then each plugin is asked, in the order
    /// plugged, and the first refusal leaves the peer unregistered.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.claims.node_id.to_base64_short(), durable = %presence.claims.durable)))]
    pub async fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: Handshake,
        provenance: PeerProvenance,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), PeerConnectionError> {
        action_info!(self, "register_peer", "{}", &presence);

        if let Err(refusal) = handshake.authenticate(self.id, &presence) {
            warn!("Node({}) refusing peer: {}", self.id, refusal);
            return Err(refusal);
        }
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
        let _registration = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // The latest registration decides trust: an id registering again without durable trust
        // leaves durable routing before its new sender is reachable.
        if !trust.durable && self.durable_peers.remove(&peer) {
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_disconnected(peer);
            }
        }
        self.peer_connections.insert(peer, Arc::new(PeerState::new(sender, trust, subscription_handler)));
        if trust.durable {
            self.durable_peers.insert(peer);
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_connected(peer);
            }
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
        Ok(())
    }
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %node_id.to_base64_short())))]
    pub fn deregister_peer(&self, node_id: proto::NodeId) {
        notice_info!("Node({:#}) deregister_peer {:#}", self.id, node_id);

        let _registration = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.durable_peers.remove(&node_id);
        // Get and cleanup subscriptions before removing the peer
        if let Some(peer_state) = self.peer_connections.remove(&node_id) {
            action_info!(self, "unsubscribing", "subscription {} for peer {}", peer_state.subscription_handler.subscription_id(), node_id);
            // ReactorSubscription is automatically unsubscribed on drop
        }

        // Notify subscription relay of peer disconnection (unconditional - relay handles filtering)
        if let Some(ref relay) = self.subscription_relay {
            relay.notify_peer_disconnected(node_id);
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
            let connection = self.trusted_connection(&node_id, required)?;
            connection.pending_requests.insert(request_id, response_tx);
            connection.send_message(proto::NodeMessage::Request { auth, request })?;
        }

        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    /// The connection to `peer`, if the trust this node grants that connection now meets `required`.
    fn trusted_connection(&self, peer: &proto::NodeId, required: RequiredTrust) -> Result<Arc<PeerState<PA::ContextData>>, RequestError> {
        let connection = self.peer_connections.get(peer).ok_or(RequestError::PeerNotConnected)?;
        match required {
            RequiredTrust::Durable if !connection.trust.durable => Err(RequestError::NotTrustedAsDurable(*peer)),
            RequiredTrust::Any | RequiredTrust::Durable => Ok(connection),
        }
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
        let Some(connection) = self.peer_connections.get(&node_id) else {
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

    // TODO add a node id argument to this function rather than getting it from the message
    // (does this actually make it more secure? or just move the place they could lie to us to the handshake?)
    // Not if its signed by a node key.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        self.lifecycle.check_not_halted()?;
        match message {
            proto::NodeMessage::Update(update) => {
                debug!("Node({}) received update {}", self.id, update);

                if let Some(sender) = { self.peer_connections.get(&update.from).map(|c| c.sender.cloned()) } {
                    let _from = update.from;
                    let _id = update.id.clone();
                    if update.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, update.from);
                        return Ok(());
                    }

                    // take down the return address
                    let id = update.id.clone();
                    let to = update.from;
                    let from = self.id;

                    // TODO - validate the from node id is the one we're connected to
                    let body = match update.body.decode() {
                        Ok(body) => match self.handle_update(update.from, body).await {
                            Ok(_) => proto::NodeUpdateAckBody::Success,
                            Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                        },
                        Err(unreadable) => {
                            debug!("Node({}) cannot read update {} from {}: {}", self.id, id, update.from, unreadable);
                            proto::NodeUpdateAckBody::Unsupported
                        }
                    };

                    sender.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
                }
            }
            proto::NodeMessage::UpdateAck(ack) => {
                debug!("Node({}) received ack notification {} {}", self.id, ack.id, ack.body);
                // let connection = self.peer_connections.get(&ack.from).ok_or(RequestError::PeerNotConnected)?;
                // if let Some(tx) = connection.pending_updates.remove(&ack.id) {
                //     tx.send(Ok(proto::NodeResponseBody::Success)).unwrap();
                // }
            }
            proto::NodeMessage::Request { auth, request } => {
                debug!("Node({}) received request {}", self.id, request);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = { self.peer_connections.get(&request.from).map(|c| c.sender.cloned()) } {
                    let from = request.from;
                    let request_id = request.id.clone();
                    if request.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                        return Ok(());
                    }

                    let body = match request.body.decode() {
                        Ok(body) => {
                            // Temporary credentialless catalog reads. Remove this bypass when the #425 catalog
                            // follow-on uses #426's node session union for catalog queries.
                            let cdata = if crate::schema::request_bypasses_policy(&body) {
                                Ok(Vec::new())
                            } else {
                                self.policy_agent.check_request(self, &auth, &request).await
                            };
                            match cdata {
                                Ok(cdata) => match self.handle_request(&cdata, from, body).await {
                                    Ok(result) => result,
                                    Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                                },
                                Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                            }
                        }
                        Err(unreadable) => {
                            debug!("Node({}) cannot read request {} from {}: {}", self.id, request_id, from, unreadable);
                            proto::NodeResponseBody::Unsupported
                        }
                    };
                    let _result = sender.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                        request_id,
                        from: self.id,
                        to: from,
                        body: proto::CarriedBody::encode(&body),
                    }));
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                let connection = self.peer_connections.get(&response.from).ok_or(RequestError::PeerNotConnected)?;
                if let Some(tx) = connection.pending_requests.remove(&response.request_id) {
                    tx.send(response.body.decode().map_err(RequestError::UnreadableResponse))
                        .map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
            proto::NodeMessage::UnsubscribeQuery { from, query_id } => {
                // Remove predicate from the peer's subscription
                if let Some(peer_state) = self.peer_connections.get(&from) {
                    peer_state.subscription_handler.remove_predicate(query_id).await?;
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(body = %body)))]
    async fn handle_request<C>(
        &self,
        cdata: &C,
        from: proto::NodeId,
        body: proto::NodeRequestBody,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        C: Iterable<PA::ContextData>,
    {
        match body {
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
            proto::NodeRequestBody::Fetch { mut selection, known_matches } => {
                let policy = ContextPolicy::from_credentials(&self.policy_agent, cdata);
                selection.predicate = policy.filter_predicate(selection.predicate)?;

                let expanded_states = crate::util::expand_states::expand_states(
                    self.storage.fetch_states(&selection).await?,
                    known_matches.iter().map(|k| k.entity_id).collect::<Vec<_>>(),
                    self.storage.as_ref(),
                )
                .await?;

                let known_map: std::collections::HashMap<_, _> = known_matches.into_iter().map(|k| (k.entity_id, k.head)).collect();

                let mut deltas = Vec::new();
                for state in expanded_states {
                    if policy.check_read(&state.payload.entity_id, &state.payload.state).is_err() {
                        continue;
                    }

                    if let Some(delta) = self.generate_entity_delta(&known_map, state, cdata).await? {
                        deltas.push(delta);
                    }
                }
                Ok(proto::NodeResponseBody::Fetch(deltas))
            }
            proto::NodeRequestBody::Get { ids } => {
                let policy = ContextPolicy::from_credentials(&self.policy_agent, cdata);
                let mut results: Vec<_> =
                    self.storage.get_states(ids, &policy.retrieval_predicate()).await?.into_iter().map(Into::into).collect();
                policy.check_reads(&mut results);
                let results = results
                    .into_iter()
                    .map(|result| match result {
                        proto::GetResult::Found(state) => proto::GetResult::Found(attestation::served_state(self, state.payload)),
                        unserved => unserved,
                    })
                    .collect();
                Ok(proto::NodeResponseBody::Get(results))
            }
            proto::NodeRequestBody::GetEvents { event_ids } => Ok(handler::get_events(self, cdata, event_ids).await?),
            proto::NodeRequestBody::SubscribeQuery { query_id, selection, version, known_matches } => {
                let peer_state = self.peer_connections.get(&from).ok_or_else(|| anyhow!("Peer {} not connected", from))?;
                // Catalog subscriptions carry no credential; others carry exactly one.
                let cdata = match cdata.iterable().at_most_one() {
                    Ok(cdata) => cdata,
                    Err(_) => {
                        return Err(anyhow!(
                            "SubscribeQuery currently requires at most one cdata (this server does not yet accept several per subscribe)"
                        ));
                    }
                };
                peer_state.subscription_handler.subscribe_query(self, query_id, selection, cdata, version, known_matches).await
            }
        }
    }

    async fn handle_update(&self, from: proto::NodeId, body: proto::NodeUpdateBody) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", from));
        };

        match body {
            proto::NodeUpdateBody::SubscriptionUpdate { items } => {
                tracing::debug!("Node({}) received subscription update from peer {}", self.id, from);
                crate::node::applier::NodeApplier::apply_updates(self, &from, items).await?;
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
        for peer_id in self.get_durable_peers() {
            let body = proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.to_vec() };
            match self.request_durable(peer_id, cdata, body).await {
                Ok(proto::NodeResponseBody::CommitComplete { .. }) => (),
                Err(error) => return Err(RetrievalError::from(error).into()),
                Ok(proto::NodeResponseBody::Error(e)) => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!("Peer {} rejected: {}", peer_id, e)))));
                }
                _ => {
                    return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                        "Peer {} returned unexpected response",
                        peer_id
                    )))));
                }
            }
        }
        Ok(())
    }

    /// Generate EntityDelta for an entity state, using known_matches to decide between StateSnapshot and EventBridge
    /// Returns None if the entity is in known_matches with equal heads (client already has current state)
    pub(crate) async fn generate_entity_delta<C>(
        &self,
        known_map: &std::collections::HashMap<proto::EntityId, proto::Clock>,
        entity_state: proto::Attested<proto::EntityState>,
        cdata: &C,
    ) -> anyhow::Result<Option<proto::EntityDelta>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        C: crate::util::Iterable<PA::ContextData>,
    {
        // Destructure to take ownership and avoid clones
        let proto::EntityState { entity_id, state } = entity_state.payload;
        let current_head = &state.head;

        // Entity is in known_matches - try to optimize the response
        if let Some(known_head) = known_map.get(&entity_id) {
            // Case 1: Heads equal → return None (omit entity, client already has current state) ✓
            if known_head == current_head {
                return Ok(None);
            }

            // Case 2: Heads differ → try to build EventBridge (cheaper than full state) ✓
            let policy = ContextPolicy::from_credentials(&self.policy_agent, cdata);
            match self.collect_event_bridge(known_head, current_head, &state, &policy).await {
                Ok(attested_events) if !attested_events.is_empty() => {
                    // Convert Attested<Event> to EventFragments (strips entity_id)
                    let event_fragments: Vec<proto::EventFragment> = attested_events.into_iter().map(|e| e.into()).collect();

                    return Ok(Some(proto::EntityDelta {
                        entity_id,
                        content: proto::DeltaContent::EventBridge { events: event_fragments },
                    }));
                }
                _ => {
                    // Fall through to StateSnapshot if bridge building failed or returned empty
                }
            }
        }

        // Case 3: Entity not in known_matches OR bridge building failed → send full StateSnapshot ✓
        let state_fragment = attestation::served_state(self, proto::EntityState { entity_id, state }).into();
        Ok(Some(proto::EntityDelta { entity_id, content: proto::DeltaContent::StateSnapshot { state: state_fragment } }))
    }

    /// Collect events between known_head and current_head using event_dag comparison.
    /// Returns events needed to advance from known_head to current_head.
    pub(crate) async fn collect_event_bridge<C>(
        &self,
        known_head: &proto::Clock,
        current_head: &proto::Clock,
        state: &proto::State,
        policy: &ContextPolicy<'_, PA, C>,
    ) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        C: ankurah_signals::Signal + ankurah_signals::Peek<Vec<PA::ContextData>>,
    {
        use crate::event_dag::{compare, AbstractCausalRelation};
        use crate::retrieval::LocalEventGetter;
        use std::collections::HashSet;

        let event_getter = LocalEventGetter::new(self.storage.clone(), self.durable);

        // First check the causal relationship
        let comparison_result = compare(&event_getter, current_head, known_head, 100000).await?;

        match comparison_result.relation {
            AbstractCausalRelation::Equal => {
                // Heads are equal - no events needed
                Ok(vec![])
            }
            AbstractCausalRelation::StrictDescends { chain: _ } => {
                // Current descends from known - collect events by walking backward
                // TODO: Optimize with forward chain from relation (GitHub #200)
                let known_set: HashSet<_> = known_head.ids().collect();
                let mut events = Vec::new();
                let mut frontier: Vec<proto::EventId> = current_head.ids().cloned().collect();
                let mut visited: HashSet<proto::EventId> = HashSet::new();

                while !frontier.is_empty() {
                    let batch = std::mem::take(&mut frontier);
                    let fetched = self.storage.get_events(batch).await?;

                    for event in fetched {
                        let id = event.payload.id();
                        if visited.insert(id.clone()) && !known_set.contains(&id) {
                            // Add parents to frontier (walking backward)
                            for parent in event.payload.parent.ids() {
                                if !visited.contains(parent) && !known_set.contains(parent) {
                                    frontier.push(parent.clone());
                                }
                            }
                            events.push(event);
                        }
                    }
                }

                // Receivers must not learn events the read policy hides.
                // One unreadable event makes the whole bridge unusable (a
                // chain with a hole loses operations downstream), so give up
                // entirely and let the caller fall back to a state snapshot,
                // which passes its own read check.
                for event in &events {
                    // FIXME: should we really pass state here? Ideally we'd pass the Entity
                    // but we don't necessarily have it here. It's possible the PolicyAgent
                    // could fetch the Entity itself for a given clock (though this might require)
                    // checkpointing. I think we're not even using this anyway
                    // in favor of PolicyAgent-generated predicates anyway. If that's the case
                    // maybe we omit state here and handwave about future specific-clock (snapshot) retrievals
                    // as a way to discharge this theoretical requirement in the future?
                    match policy.check_read_event(event, state) {
                        Ok(()) => {}
                        Err(AccessDenied::ByPolicy(_)) => return Ok(vec![]),
                        Err(e) => return Err(anyhow!("check_read_event failed while building event bridge: {}", e)),
                    }
                }

                // Backward BFS discovery order is not a causal order (uneven
                // branch lengths interleave); sort parents-first so the wire
                // carries a sane order. Receivers sort again and must not
                // trust this.
                let events = crate::event_dag::ordering::topo_sort_events(events)?;
                Ok(events)
            }
            _ => {
                // Other relationships (NotDescends, Incomparable, etc.) - can't build simple bridge
                Ok(vec![])
            }
        }
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

    /// Retrieve peer states and persist them in this node.
    pub(crate) async fn get_from_peer(&self, ids: Vec<proto::EntityId>, cdata: &Vec<PA::ContextData>) -> Result<(), RetrievalError> {
        self.lifecycle.check_not_halted()?;
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self.request_durable(peer_id, cdata, proto::NodeRequestBody::Get { ids: ids.clone() }).await? {
            proto::NodeResponseBody::Get(results) => {
                if results.len() != ids.len() || results.iter().zip(&ids).any(|(result, id)| result.entity_id() != *id) {
                    return Err(RetrievalError::Other("Peer Get did not answer the requested identities".into()));
                }
                let deltas = results.into_iter().map(proto::EntityDelta::try_from).collect::<Result<Vec<_>, _>>()?;
                let event_getter = CachedEventGetter::new(self, cdata);
                applier::NodeApplier::apply_deltas(self, &peer_id, deltas, &event_getter, self.storage.as_ref()).await?;
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

    /// Fetch matching entities from a durable peer using local heads as known matches.
    /// Apply its deltas locally before returning.
    pub(crate) async fn fetch_from_peer(
        &self,
        selection: ankql::ast::Selection<Resolved>,
        cdata: &Vec<PA::ContextData>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.lifecycle.check_not_halted()?;
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        let known_matched_entities = self.fetch_entities_from_local(&selection).await?;

        let known_matches = known_matched_entities
            .iter()
            .map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() })
            .collect();

        let selection_clone = selection.clone();
        match self.request_durable(peer_id, cdata, proto::NodeRequestBody::Fetch { selection, known_matches }).await? {
            proto::NodeResponseBody::Fetch(deltas) => {
                let event_getter = CachedEventGetter::new(self, cdata);
                let state_getter = LocalStateGetter::new(self.storage.clone());

                applier::NodeApplier::apply_deltas(self, &peer_id, deltas, &event_getter, &state_getter).await?;
                // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

                self.fetch_entities_from_local(&selection_clone).await
            }
            proto::NodeResponseBody::Error(e) => {
                debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }

    /// Get a random durable peer node ID.
    ///
    /// Draws from the node's seeded RNG (not `thread_rng`) so the simulation harness can reproduce
    /// selections. The candidate slice is sorted first: `choose` indexes by position, so a stable
    /// candidate order is required for a given seed to yield a given peer.
    pub fn get_durable_peer_random(&self) -> Option<proto::NodeId> {
        let peers = self.get_durable_peers();
        let mut rng = self.rng.lock().expect("node rng mutex poisoned");
        peers.choose(&mut *rng).copied()
    }

    /// The trust this node granted a connected peer.
    pub fn peer_trust(&self, peer: &proto::NodeId) -> Option<PeerTrust> { self.peer_connections.get(peer).map(|state| state.trust) }

    /// Get all durable peer node IDs, sorted by id for a stable fan-out order.
    /// The underlying set is unordered; callers that emit per peer (relay, random selection)
    /// depend on this stable order for the C1 determinism audit.
    pub fn get_durable_peers(&self) -> Vec<proto::NodeId> {
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

    /// TEST ONLY: Register a durable peer id directly, bypassing the connection handshake.
    ///
    /// Lets tests populate the durable-peer set to exercise fan-out ordering and seeded random
    /// selection without standing up real peer connections.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn insert_durable_peer_for_test(&self, peer_id: proto::NodeId) { self.durable_peers.insert(peer_id); }

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
        for (peer_id, item) in self.peer_connections.get_list(peers) {
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
            let (_, entity) = self.entities.with_state(&state_getter, &event_getter, state.payload.entity_id, state.payload.state).await?;
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
