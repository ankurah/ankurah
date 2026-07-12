use crate::selection::filter::Filterable;
use ankurah_proto::{self as proto, Attested, CollectionId, EntityState};
use anyhow::anyhow;
use ed25519_dalek::{Signer, SigningKey};

use rand::prelude::*;
use rand::rngs::SmallRng;
use std::{
    fmt,
    hash::Hash,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::sync::oneshot;

use crate::{
    action_error, action_info,
    changes::EntityChange,
    collectionset::CollectionSet,
    connector::{PeerFrameError, PeerHandshake, PeerSender, SendError, VerifiedPeerMessage},
    context::Context,
    entity::{Entity, WeakEntitySet},
    error::{MutationError, RequestError, RetrievalError},
    notice_info,
    peer_subscription::{SubscriptionHandler, SubscriptionRelay},
    policy::{AccessDenied, PolicyAgent},
    reactor::{AbstractEntity, Reactor},
    retrieval::{CachedEventGetter, GetEvents, LocalEventGetter, LocalStateGetter, SuspenseEvents},
    schema::catalog::CatalogManager,
    storage::{StorageCollectionWrapper, StorageEngine},
    system::{RootReservation, SystemManager},
    util::request_fence::{RequestLease, RequestValidity},
    util::{safemap::SafeMap, safeset::SafeSet, Iterable},
};
use itertools::Itertools;
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, error, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    signing_key: SigningKey,
    incoming_session: proto::HandshakeChallenge,
    /// Entity/system generation in which frames on this session are allowed
    /// to dispatch. A preserved first-join session is rebound only after its
    /// root switch completes; already-verified old frames retain the old
    /// token and fail closed.
    system_generation: std::sync::RwLock<Arc<AtomicBool>>,
    outgoing_session: proto::HandshakeChallenge,
    incoming_replay_window: Mutex<ReplayWindow>,
    next_outgoing_sequence: Mutex<u64>,
    durable: AtomicBool,
    ready: AtomicBool,
    /// The root whose persistence must complete before this peer can carry
    /// traffic. Shared across reconnects from the same pending founder.
    pending_root: Option<proto::EntityId>,
    subscription_handler: SubscriptionHandler,
    pending_requests: SafeMap<proto::RequestId, PendingRequest>,
    pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    /// Model-definition ids whose catalog defs were already shipped on this
    /// connection (#330 once-per-connection descriptor shipping). A
    /// reconnection builds a fresh `PeerState`, so the peer is re-announced.
    announced_models: std::sync::Mutex<std::collections::BTreeSet<proto::EntityId>>,
}

const PEER_REPLAY_WINDOW: u64 = 4096;

#[derive(Default)]
struct ReplayWindow {
    highest: Option<u64>,
    seen: std::collections::BTreeSet<u64>,
}

impl ReplayWindow {
    fn accept(&mut self, peer: proto::NodeId, sequence: u64) -> Result<(), PeerFrameError> {
        if self.seen.contains(&sequence) {
            return Err(PeerFrameError::ReplayedSequence { peer, sequence });
        }
        if self.highest.is_some_and(|highest| sequence < highest.saturating_sub(PEER_REPLAY_WINDOW - 1)) {
            return Err(PeerFrameError::StaleSequence { peer, sequence });
        }

        self.highest = Some(self.highest.map_or(sequence, |highest| highest.max(sequence)));
        self.seen.insert(sequence);
        let floor = self.highest.unwrap().saturating_sub(PEER_REPLAY_WINDOW - 1);
        self.seen.retain(|seen| *seen >= floor);
        Ok(())
    }
}

struct PendingRequest {
    response: oneshot::Sender<Result<GuardedResponse, RequestError>>,
    validity: Option<RequestValidity>,
}

#[derive(Debug)]
pub(crate) struct GuardedResponse {
    body: proto::NodeResponseBody,
    lease: RequestLease,
}

impl GuardedResponse {
    pub(crate) fn into_parts(self) -> (proto::NodeResponseBody, RequestLease) { (self.body, self.lease) }
}

impl PeerState {
    fn generation_alive(&self) -> bool { self.system_generation.read().unwrap().load(Ordering::Acquire) }

    fn close_pending(&self) {
        for pending in self.pending_requests.drain_values() {
            let _ = pending.response.send(Err(RequestError::ConnectionLost));
        }
        for sender in self.pending_updates.drain_values() {
            let _ = sender.send(Err(RequestError::ConnectionLost));
        }
    }

    fn sign_message_for_delivery(&self, message: proto::NodeMessage) -> proto::SignedPeerMessage {
        let mut next_sequence = self.next_outgoing_sequence.lock().unwrap_or_else(|e| e.into_inner());
        let sequence = *next_sequence;
        let signature = self.signing_key.sign(&proto::SignedPeerMessage::signable_bytes(self.outgoing_session, sequence, &message)).into();
        *next_sequence += 1;
        proto::SignedPeerMessage { session: self.outgoing_session, sequence, message, signature }
    }

    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        if !self.generation_alive() {
            return Err(SendError::ConnectionClosed);
        }
        // Signing, enqueue, and sequence advancement are one serialized step.
        // Concurrent callers therefore cannot enqueue N+1 before N, and a
        // failed bounded-channel enqueue does not burn a sequence number.
        let mut next_sequence = self.next_outgoing_sequence.lock().unwrap_or_else(|e| e.into_inner());
        if !self.generation_alive() {
            return Err(SendError::ConnectionClosed);
        }
        let sequence = *next_sequence;
        let signature = self.signing_key.sign(&proto::SignedPeerMessage::signable_bytes(self.outgoing_session, sequence, &message)).into();
        self.sender.send_message(proto::SignedPeerMessage { session: self.outgoing_session, sequence, message, signature })?;
        *next_sequence += 1;
        Ok(())
    }
}

pub struct MatchArgs {
    pub selection: ankql::ast::Selection,
    pub cached: bool,
}

impl TryInto<MatchArgs> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> { Ok(MatchArgs { selection: ankql::parser::parse_selection(self)?, cached: true }) }
}
impl TryInto<MatchArgs> for String {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> {
        Ok(MatchArgs { selection: ankql::parser::parse_selection(&self)?, cached: true })
    }
}

impl From<ankql::ast::Predicate> for MatchArgs {
    fn from(val: ankql::ast::Predicate) -> Self {
        MatchArgs { selection: ankql::ast::Selection { predicate: val, order_by: None, limit: None }, cached: true }
    }
}

impl From<ankql::ast::Selection> for MatchArgs {
    fn from(val: ankql::ast::Selection) -> Self { MatchArgs { selection: val, cached: true } }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

pub fn nocache<T: TryInto<ankql::ast::Selection, Error = ankql::error::ParseError>>(s: T) -> Result<MatchArgs, ankql::error::ParseError> {
    MatchArgs::nocache(s)
}
impl MatchArgs {
    pub fn nocache<T>(s: T) -> Result<Self, ankql::error::ParseError>
    where T: TryInto<ankql::ast::Selection, Error = ankql::error::ParseError> {
        Ok(Self { selection: s.try_into()?, cached: false })
    }
}

/// A participant in the Ankurah network, and primary place where queries are initiated

pub struct Node<SE, PA>(pub(crate) Arc<NodeInner<SE, PA>>)
where PA: PolicyAgent;
impl<SE, PA> Clone for Node<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct WeakNode<SE, PA>(Weak<NodeInner<SE, PA>>)
where PA: PolicyAgent;
impl<SE, PA> Clone for WeakNode<SE, PA>
where PA: PolicyAgent
{
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<SE, PA> WeakNode<SE, PA>
where PA: PolicyAgent
{
    pub fn upgrade(&self) -> Option<Node<SE, PA>> { self.0.upgrade().map(Node) }
}

impl<SE, PA> Deref for Node<SE, PA>
where PA: PolicyAgent
{
    type Target = Arc<NodeInner<SE, PA>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

/// Represents the user session - or whatever other context the PolicyAgent
/// Needs to perform it's evaluation.
pub trait ContextData: Send + Sync + Clone + Hash + Eq + 'static {}

pub struct NodeInner<SE, PA>
where PA: PolicyAgent
{
    pub id: proto::NodeId,
    /// The private half of this node's cryptographic identity. Core keeps
    /// custody so presence and durable attestations can be signed without
    /// handing key material to policy agents or transports.
    pub(crate) signing_key: SigningKey,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    pub(crate) entities: WeakEntitySet,
    peer_connections: SafeMap<proto::NodeId, Arc<PeerState>>,
    /// Serializes replacement, promotion, and teardown of the current
    /// registration for a stable NodeId. The PeerState itself is the routing
    /// authority; keeping transitions under one lock prevents a stale session
    /// from changing the role of its replacement.
    peer_registry_lock: Mutex<()>,
    #[cfg(any(test, feature = "test-helpers"))]
    test_durable_peers: SafeSet<proto::NodeId>,

    /// Per-node source of randomness for peer selection. Seeded from entropy in production;
    /// an explicit seed can be injected at construction so the simulation harness and tests
    /// can reproduce an identical selection sequence. Held behind a Mutex because draws mutate
    /// RNG state and Node is shared across tasks; the lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,

    pub(crate) predicate_context: SafeMap<proto::QueryId, PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Reactor,
    pub(crate) policy_agent: PA,
    pub(crate) invalid_attestations: AtomicU64,
    pub system: SystemManager<SE, PA>,

    /// The metadata catalog map (RFC section 5.2 in specs/model-property-metadata/rfc.md). Warmed from storage on
    /// durable nodes and via the subscription relay on ephemeral nodes.
    pub catalog: CatalogManager<SE, PA>,

    pub(crate) subscription_relay: Option<SubscriptionRelay<PA::ContextData, crate::livequery::WeakEntityLiveQuery>>,

    /// Type resolver for AST preparation (temporary heuristic until Phase 3 schema)
    pub(crate) type_resolver: crate::TypeResolver,

    /// Keeps this node's sibling-teardown callback alive in the shared
    /// storage fence (the fence holds only a Weak). Invoked when ANOTHER
    /// manager on the same engine advances the storage epoch; see
    /// `CollectionSet::set_epoch_participant`.
    _epoch_participant: std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>,
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Attach the live catalog resolver to a user entity at each assembly
    /// path (create/load/apply). Property access can then resolve display
    /// names to stable property ids while backends continue to operate on
    /// `PropertyKey`. System and metadata-catalog entities are the bootstrap
    /// exemption and remain name-addressed without a resolver.
    pub(crate) fn stamp_resolver(&self, entity: &Entity) {
        let collection = entity.collection();
        // System + catalog collections are name-keyed (the bootstrap
        // exemption): they need no resolver, and stamping one would be inert.
        if collection.as_str() == crate::system::SYSTEM_COLLECTION_ID || crate::schema::is_catalog_collection(collection) {
            return;
        }
        // Stamp the live catalog resolver so the entity's sync read path can
        // resolve display names to property ids (the PropertyKey amendment,
        // #289, replacing the old id-keyed backend binding flip). The backend
        // stays dumb; identity is carried by the PropertyKey.
        entity.set_resolver(self.catalog.resolver_weak());
    }

    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self::build(engine, policy_agent, false, SmallRng::from_entropy(), Self::generate_signing_key())
    }
    /// Construct a durable node with a fresh identity key.
    ///
    /// Use this only with fresh/temporary storage. A persistent durable node
    /// must load the same key on every restart and call
    /// [`Self::new_durable_with_signing_key`], otherwise its stored system root
    /// names a different founder and the node cannot become ready.
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        Self::build(engine, policy_agent, true, SmallRng::from_entropy(), Self::generate_signing_key())
    }

    /// Construct an ephemeral node with a caller-provided identity key.
    ///
    /// Durable embedders should persist this key and pass it back through
    /// [`Node::new_durable_with_signing_key`] when restarting the node.
    pub fn new_with_signing_key(engine: Arc<SE>, policy_agent: PA, signing_key: SigningKey) -> Self {
        Self::build(engine, policy_agent, false, SmallRng::from_entropy(), signing_key)
    }

    /// Construct a durable node with a caller-provided, persistable identity
    /// key.
    pub fn new_durable_with_signing_key(engine: Arc<SE>, policy_agent: PA, signing_key: SigningKey) -> Self {
        Self::build(engine, policy_agent, true, SmallRng::from_entropy(), signing_key)
    }

    /// Construct an ephemeral node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new`].
    pub fn new_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, false, SmallRng::seed_from_u64(rng_seed), Self::generate_signing_key())
    }

    /// Construct a durable node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new_durable`].
    pub fn new_durable_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, true, SmallRng::seed_from_u64(rng_seed), Self::generate_signing_key())
    }

    fn generate_signing_key() -> SigningKey {
        let mut rng = rand::rngs::OsRng;
        SigningKey::generate(&mut rng)
    }

    fn build(engine: Arc<SE>, policy_agent: PA, durable: bool, rng: SmallRng, signing_key: SigningKey) -> Self {
        let collections = CollectionSet::new(engine);
        let entityset = WeakEntitySet::with_system_generation(collections.storage_generation());
        let id = proto::NodeId::from(signing_key.verifying_key());
        let reactor = Reactor::new();
        notice_info!("Node {id:#} created as {}", if durable { "durable" } else { "ephemeral" });

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), durable, id);
        let catalog_manager = CatalogManager::new(collections.clone(), entityset.clone(), reactor.clone(), durable);

        // Only ephemeral nodes relay subscriptions upstream to a durable peer.
        let subscription_relay = if durable { None } else { Some(SubscriptionRelay::new()) };

        let node = Node(Arc::new(NodeInner {
            id,
            signing_key,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            peer_registry_lock: Mutex::new(()),
            #[cfg(any(test, feature = "test-helpers"))]
            test_durable_peers: SafeSet::new(),
            rng: Mutex::new(rng),
            reactor,
            durable,
            policy_agent,
            invalid_attestations: AtomicU64::new(0),
            system: system_manager,
            catalog: catalog_manager,
            predicate_context: SafeMap::new(),
            subscription_relay,
            type_resolver: crate::TypeResolver::new(),
            _epoch_participant: std::sync::OnceLock::new(),
        }));

        // Set up the message sender for the subscription relay
        if let Some(ref relay) = node.subscription_relay {
            let weak_node = node.weak();
            if relay.set_node(Arc::new(weak_node)).is_err() {
                warn!("Failed to set message sender for subscription relay");
            }
        }

        node.catalog.start(node.weak());
        // Storage engines seed their durable id-to-name maps (table and column
        // naming) from the catalog resolver at materialization time. Injected
        // here, not at engine construction, because the engine is built before
        // the node/catalog exist.
        node.collections.set_catalog_resolver(node.catalog.resolver_weak());
        // The reactor types ORDER BY sort keys from the same catalog resolver
        // (canonical value_type collation).
        node.reactor.set_catalog_resolver(node.catalog.resolver_weak());
        // Assembly-time choke point (the PropertyKey amendment, #289): every
        // entity handed out by the entity set gets the catalog resolver
        // stamped, so no assembly path can forget it and the sync read path can
        // always resolve names to ids.
        node.entities.set_bind_hook(Box::new({
            let weak = node.weak();
            move |entity| {
                if let Some(node) = weak.upgrade() {
                    node.stamp_resolver(entity);
                }
            }
        }));
        // SystemManager owns the destructive reset sequence but the Node owns
        // connection routing. Bridge them with a weak hook so reset can drain
        // old-system PeerStates and their pending waiters without creating an
        // Arc cycle. A pending peer for the newly Reserved root is preserved
        // during an automatic system replacement and promoted after its proof
        // is durable; hard_reset preserves none.
        node.system.set_peer_reset_hook(Arc::new({
            let weak = node.weak();
            move |mode| {
                if let Some(node) = weak.upgrade() {
                    match mode {
                        crate::system::PeerSessionsReset::Drain { preserved_pending_root } => {
                            node.reset_peer_sessions(preserved_pending_root)
                        }
                        crate::system::PeerSessionsReset::Rebind { fresh_generation } => node.rebind_peer_sessions(&fresh_generation),
                    }
                }
            }
        }));
        // Sibling teardown for shared-engine resets: when a DIFFERENT Node
        // built over this exact Arc<StorageEngine> advances the storage
        // epoch, this node's handles are already fenced by the shared
        // generation token, but its open transports and pending request
        // oneshots must be closed and woken eagerly (they would otherwise
        // wait on a session the reset made permanently unanswerable).
        let epoch_participant: Arc<dyn Fn() + Send + Sync> = Arc::new({
            let weak = node.weak();
            move || {
                if let Some(node) = weak.upgrade() {
                    node.reset_peer_sessions(None);
                }
            }
        });
        node.collections.set_epoch_participant(&epoch_participant);
        let _ = node.0._epoch_participant.set(epoch_participant);
        node.policy_agent.on_node_ready(node.weak());

        node
    }
    /// TEST/INTROSPECTION: how many reactor queries the CURRENT registered
    /// session for `node_id` carries. None when no session is registered.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn peer_subscription_query_count(&self, node_id: &proto::NodeId) -> Option<usize> {
        let subscription_id = self.peer_connections.get(node_id).map(|state| state.subscription_handler.subscription_id())?;
        self.reactor.subscription_queries_len(subscription_id)
    }

    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }

    /// Build the signed Presence every connector sends during its handshake.
    /// Keeping construction here ensures transports never handle the private
    /// node key or accidentally sign different claim projections.
    pub fn begin_peer_handshake(&self) -> PeerHandshake {
        let mut nonce = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        PeerHandshake::new(proto::HandshakeChallenge::new(self.id, nonce))
    }

    pub fn presence(&self, challenge: proto::HandshakeChallenge) -> proto::Presence {
        // Never advertise a root while it is being replaced/reset. In
        // particular, hard_reset marks the system unready before the async
        // storage deletion reaches `root = None`; exposing the old proof in
        // that window lets reconnecting transports re-register a stale
        // founder on remote nodes.
        let system_root = (self.system.is_system_ready() && !self.system.is_resetting()).then(|| self.system.root_proof()).flatten();
        let timestamp = proto::time::unix_ms_now();
        let claims = proto::PresenceClaims {
            node_id: self.id,
            durable: self.durable,
            system_root: system_root.as_ref().map(proto::SystemRootProof::entity_id),
            challenge,
            timestamp,
            protocol_version: proto::PROTOCOL_VERSION,
        };
        let signature = self.signing_key.sign(&proto::Presence::signable_bytes(&claims)).into();
        proto::Presence {
            node_id: self.id,
            durable: self.durable,
            system_root,
            challenge,
            timestamp,
            signature,
            protocol_version: proto::PROTOCOL_VERSION,
        }
    }

    /// Number of invalid, mismatched, or unrecognized attestation envelopes
    /// stripped by core before a PolicyAgent saw the payload.
    pub fn invalid_attestation_count(&self) -> u64 { self.invalid_attestations.load(Ordering::Relaxed) }

    fn recognize_durable_presence(
        &self,
        presence: &proto::Presence,
    ) -> Result<(bool, Option<proto::SystemRootProof>), proto::PresenceRefusal> {
        if !presence.durable {
            return Ok((false, None));
        }
        let Some(proof) = presence.system_root.as_ref() else {
            return Ok((false, None));
        };
        match self.system.reserve_root_from_presence(proof, presence.node_id) {
            Ok(RootReservation::StartJoin(proof)) => Ok((true, Some(proof))),
            Ok(RootReservation::AlreadyPinned) => Ok((true, None)),
            Ok(RootReservation::Conflict) if self.system.founder() == Some(presence.node_id) => {
                warn!("Node({}) refusing founder {} with a conflicting system root", self.id, presence.node_id);
                Err(proto::PresenceRefusal::InvalidSystemRoot(presence.node_id))
            }
            Ok(RootReservation::Conflict) => Ok((false, None)),
            Err(error) => {
                warn!("Node({}) refusing peer {}: invalid system-root proof: {}", self.id, presence.node_id, error);
                Err(proto::PresenceRefusal::InvalidSystemRoot(presence.node_id))
            }
        }
    }

    /// Register a peer connection after its Presence handshake.
    ///
    /// Refuses (without registering anything) when the peer's protocol
    /// version is incompatible; the connector should relay the returned
    /// rejection best-effort and close the connection. Enforced here, not
    /// in connectors, so every transport inherits it.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.node_id.to_base64_short(), durable = %presence.durable)))]
    pub fn register_peer(
        &self,
        presence: proto::Presence,
        handshake: PeerHandshake,
        outgoing_session: proto::HandshakeChallenge,
        sender: Box<dyn PeerSender>,
    ) -> Result<(), proto::PresenceRefusal> {
        action_info!(self, "register_peer", "{}", &presence);
        let registration_generation = self.entities.system_generation();

        if self.system.is_destructive_resetting() {
            warn!("Node({}) refusing peer {} while system reset is active", self.id, presence.node_id);
            return Err(proto::PresenceRefusal::InvalidSystemRoot(presence.node_id));
        }

        if !proto::protocol_compatible(presence.protocol_version) {
            let rejection = proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: presence.protocol_version };
            warn!("Node({}) refusing peer {}: {}", self.id, presence.node_id, rejection);
            return Err(rejection.into());
        }
        if presence.node_id == self.id {
            warn!("Node({}) refusing reflected/self Presence", self.id);
            return Err(proto::PresenceRefusal::SelfConnection(self.id));
        }
        if outgoing_session.issuer() != presence.node_id {
            warn!("Node({}) refusing peer {}: peer-issued challenge names another node", self.id, presence.node_id);
            return Err(proto::PresenceRefusal::UnexpectedChallenge(presence.node_id));
        }
        if presence.challenge != handshake.challenge() || presence.challenge.issuer() != self.id {
            warn!("Node({}) refusing peer {}: Presence answered another connection challenge", self.id, presence.node_id);
            return Err(proto::PresenceRefusal::UnexpectedChallenge(presence.node_id));
        }
        if !presence.verify_signature() {
            warn!("Node({}) refusing peer {}: invalid presence signature", self.id, presence.node_id);
            return Err(proto::PresenceRefusal::InvalidSignature(presence.node_id));
        }

        let (recognized_durable, root_to_join) = self.recognize_durable_presence(&presence)?;
        if presence.durable && !recognized_durable {
            warn!("Node({}) treating unrecognized durable claimant {} as non-durable", self.id, presence.node_id);
        }
        // A first-join founder is authenticated but not yet usable: accepting
        // its schema/application traffic before the root proof is durable can
        // contaminate a later system if persistence fails. Reconnects for the
        // same Reserved root remain pending and are promoted together.
        let system_ready = self.system.is_system_ready();
        let pending_root =
            if recognized_durable && !system_ready { presence.system_root.as_ref().map(proto::SystemRootProof::entity_id) } else { None };
        // Once a root is Reserved but not ready, no unrelated connection may
        // carry traffic into that join window. Keep non-winners registered but
        // inert so concurrent handshakes retain their historical `Ok` result;
        // successful root publication promotes them as non-durable peers.
        // A root-less node before initial create/join remains permissive --
        // every durable mutation path independently requires system readiness.
        let ready = system_ready || self.system.root_id().is_none();
        let subscription_handler = SubscriptionHandler::new(presence.node_id, self);
        let new_state = Arc::new(PeerState {
            sender,
            signing_key: self.signing_key.clone(),
            incoming_session: handshake.challenge(),
            system_generation: std::sync::RwLock::new(registration_generation.clone()),
            outgoing_session,
            incoming_replay_window: Mutex::new(ReplayWindow::default()),
            next_outgoing_sequence: Mutex::new(0),
            durable: AtomicBool::new(recognized_durable && ready),
            ready: AtomicBool::new(ready),
            pending_root,
            subscription_handler,
            pending_requests: SafeMap::new(),
            pending_updates: SafeMap::new(),
            announced_models: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        });
        {
            let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
            // Re-check under the same lock reset uses for draining. A
            // registration that passed the optimistic check just before reset
            // cannot publish itself behind the drain.
            if self.system.is_destructive_resetting() {
                new_state.sender.close();
                return Err(proto::PresenceRefusal::InvalidSystemRoot(presence.node_id));
            }
            if !self.entities.is_current_generation(&registration_generation) {
                // The shared generation advanced while this handshake was
                // being verified. For a session that was granted nothing
                // root-dependent (not durable, no pending reservation) the
                // swap is benign -- a concurrent first create/join published
                // a new owner -- so rebind the fresh session to the current
                // generation instead of refusing a racing loser. Anything
                // root-dependent was verified against the OLD root state and
                // must not publish itself behind the transition.
                if recognized_durable || new_state.pending_root.is_some() {
                    new_state.sender.close();
                    return Err(proto::PresenceRefusal::InvalidSystemRoot(presence.node_id));
                }
                *new_state.system_generation.write().unwrap() = self.entities.system_generation();
            }
            let old_state = self.peer_connections.replace(presence.node_id, new_state);
            let old_was_durable =
                old_state.as_ref().is_some_and(|state| state.ready.load(Ordering::Acquire) && state.durable.load(Ordering::Acquire));
            if let Some(old_state) = old_state {
                old_state.sender.close();
                old_state.close_pending();
            }
            let new_is_durable = recognized_durable && ready;
            if old_was_durable != new_is_durable {
                if let Some(ref relay) = self.subscription_relay {
                    if new_is_durable {
                        relay.notify_peer_connected(presence.node_id);
                    } else {
                        relay.notify_peer_disconnected(presence.node_id);
                    }
                }
            }
        }

        if recognized_durable && !ready {
            if let Some(system_root) = root_to_join {
                let root_id = system_root.entity_id();
                let peer_id = presence.node_id;
                action_info!(self, "received verified system root", "{}", &system_root.state.payload);
                let me = self.clone();
                crate::task::spawn(async move {
                    if let Err(e) = me.system.finish_reserved_join(system_root).await {
                        action_error!(me, "failed to join system", "{}", &e);
                        // Close the reservation synchronously before removing
                        // the peer, so a reconnect cannot attach to a doomed
                        // root during the async storage reset.
                        if let Some(abort) = me.system.begin_abort_reserved_join(root_id) {
                            me.deregister_peer_pending_root(peer_id, root_id);
                            if let Err(reset_error) = me.system.finish_abort_reserved_join(abort).await {
                                action_error!(me, "failed to clean up aborted system join", "{}", &reset_error);
                            }
                        }
                    } else {
                        me.promote_peers_for_root(peer_id, root_id);
                        action_info!(me, "successfully joined system");
                    }
                });
            } else if presence.system_root.is_none() {
                error!("Node({}) durable peer {} has no system root", self.id, presence.node_id);
            }
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
        Ok(())
    }

    fn promote_peers_for_root(&self, node_id: proto::NodeId, root_id: proto::EntityId) -> bool {
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        if self.system.root_id() != Some(root_id) || !self.system.is_system_ready() {
            return false;
        }

        let mut founder_promoted = false;
        for (peer_id, peer_state) in self.peer_connections.to_vec() {
            if peer_state.ready.load(Ordering::Acquire) {
                continue;
            }
            match peer_state.pending_root {
                Some(pending) if pending == root_id => {
                    *peer_state.system_generation.write().unwrap() = self.entities.system_generation();
                    peer_state.durable.store(true, Ordering::Release);
                    peer_state.ready.store(true, Ordering::Release);
                    founder_promoted |= peer_id == node_id;
                    if let Some(ref relay) = self.subscription_relay {
                        relay.notify_peer_connected(peer_id);
                    }
                }
                None => {
                    // This connection lost the first-root race. It remains a
                    // valid authenticated peer, but never enters durable
                    // routing for the winner's system.
                    *peer_state.system_generation.write().unwrap() = self.entities.system_generation();
                    peer_state.ready.store(true, Ordering::Release);
                }
                Some(_) => {}
            }
        }
        founder_promoted
    }

    fn deregister_peer_pending_root(&self, node_id: proto::NodeId, root_id: proto::EntityId) -> bool {
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        let Some(peer_state) =
            self.peer_connections.remove_if(&node_id, |state| state.pending_root == Some(root_id) && !state.ready.load(Ordering::Acquire))
        else {
            return false;
        };
        self.cleanup_removed_peer(node_id, peer_state);
        true
    }
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %node_id.to_base64_short())))]
    fn cleanup_removed_peer(&self, node_id: proto::NodeId, peer_state: Arc<PeerState>) {
        let was_durable = peer_state.ready.load(Ordering::Acquire) && peer_state.durable.load(Ordering::Acquire);
        peer_state.sender.close();
        peer_state.close_pending();
        action_info!(self, "unsubscribing", "subscription {} for peer {}", peer_state.subscription_handler.subscription_id(), node_id);
        // ReactorSubscription is automatically unsubscribed on drop.
        if was_durable {
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_disconnected(node_id);
            }
        }
    }

    /// A first owner was published over previously EMPTY storage: nothing a
    /// live session asserted has been invalidated, so instead of draining,
    /// rebind every READY session to the fresh generation. Not-yet-ready
    /// sessions (including the pending founder) keep their old token;
    /// `promote_peers_for_root` re-arms them once the root is ready, exactly
    /// as its race-loser arm already promises ("remains a valid
    /// authenticated peer").
    fn rebind_peer_sessions(&self, fresh_generation: &Arc<AtomicBool>) {
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        for (_, state) in self.peer_connections.to_vec() {
            if state.ready.load(Ordering::Acquire) {
                *state.system_generation.write().unwrap() = fresh_generation.clone();
            }
        }
    }

    /// Drop every old-system connection and wake all of its pending request
    /// and update waiters. During an automatic root replacement, keep only a
    /// not-yet-ready connection whose pending root is the newly Reserved root;
    /// it is the authenticated channel that triggered the switch.
    fn reset_peer_sessions(&self, preserved_pending_root: Option<proto::EntityId>) {
        let removed = {
            let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
            let mut removed = Vec::new();
            for (node_id, state) in self.peer_connections.drain() {
                let preserve =
                    preserved_pending_root.is_some_and(|root| state.pending_root == Some(root) && !state.ready.load(Ordering::Acquire));
                if preserve {
                    self.peer_connections.insert(node_id, state);
                } else {
                    removed.push((node_id, state));
                }
            }
            #[cfg(any(test, feature = "test-helpers"))]
            self.test_durable_peers.clear();
            self.predicate_context.clear();
            removed
        };

        for (node_id, state) in removed {
            self.cleanup_removed_peer(node_id, state);
        }
    }

    /// Tear down only the registration created for `incoming_session`.
    /// Stable NodeIds can reconnect before an older transport observes close;
    /// that stale close must not delete the replacement PeerState.
    pub fn deregister_peer_session(&self, node_id: proto::NodeId, incoming_session: proto::HandshakeChallenge) -> bool {
        notice_info!("Node({:#}) deregister_peer_session {:#}", self.id, node_id);
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        let Some(peer_state) = self.peer_connections.remove_if(&node_id, |state| state.incoming_session == incoming_session) else {
            debug!("Node({}) ignored stale disconnect for peer {}", self.id, node_id);
            return false;
        };
        self.cleanup_removed_peer(node_id, peer_state);
        true
    }

    /// Catalog definition states for the `models` not yet announced on
    /// `connection` (#330 once-per-connection descriptor shipping). Well-known
    /// system/catalog ids need no defs (they are static). The defs are the
    /// stored, attested catalog entities themselves -- model, memberships,
    /// properties -- so the receiver validates and ingests them exactly like
    /// any served state. Marks the models announced up front; a lost message
    /// costs one reconnection's worth of re-announcement, never a wrong def.
    async fn state_with_genesis(
        collection: &StorageCollectionWrapper,
        entity_id: proto::EntityId,
    ) -> Result<proto::StateWithGenesis, RetrievalError> {
        let state = collection.get_state(entity_id).await?;
        let genesis_id = proto::EventId::from_bytes(entity_id.to_bytes());
        let genesis = collection
            .get_events(vec![genesis_id.clone()])
            .await?
            .into_iter()
            .find(|event| event.payload.id() == genesis_id)
            .ok_or_else(|| RetrievalError::EventNotFound(genesis_id))?;
        Ok(proto::StateWithGenesis { genesis, state })
    }

    async fn schema_states_for_models(
        &self,
        connection: &PeerState,
        models: std::collections::BTreeSet<proto::EntityId>,
    ) -> Vec<proto::StateWithGenesis> {
        let fresh: Vec<proto::EntityId> = {
            let mut announced = connection.announced_models.lock().unwrap();
            models
                .into_iter()
                .filter(|model| crate::schema::well_known_collection(model).is_none())
                .filter(|model| announced.insert(*model))
                .collect()
        };
        if fresh.is_empty() {
            return Vec::new();
        }
        let mut states = std::collections::BTreeMap::<proto::EntityId, proto::StateWithGenesis>::new();
        let (Ok(model_col), Ok(property_col), Ok(membership_col)) = (
            self.collections.get(&crate::schema::model_collection()).await,
            self.collections.get(&crate::schema::property_collection()).await,
            self.collections.get(&crate::schema::model_property_collection()).await,
        ) else {
            warn!("Node({}) could not open catalog collections to ship schema defs", self.id);
            // Nothing was shipped: roll these back out of `announced` so a
            // later update retries them (otherwise a transient catalog-open
            // failure would strand every future body referencing these models).
            let mut announced = connection.announced_models.lock().unwrap();
            for model in &fresh {
                announced.remove(model);
            }
            return Vec::new();
        };
        for model in &fresh {
            let mut model_states = Vec::new();
            let mut failure = None;
            match Self::state_with_genesis(&model_col, *model).await {
                Ok(state) => model_states.push(state),
                Err(error) => failure = Some(error),
            }
            for membership in self.catalog.memberships_of(model) {
                if failure.is_none() {
                    match Self::state_with_genesis(&property_col, membership.property).await {
                        Ok(state) => model_states.push(state),
                        Err(error) => failure = Some(error),
                    }
                }
                if failure.is_none() {
                    match Self::state_with_genesis(&membership_col, membership.id).await {
                        Ok(state) => model_states.push(state),
                        Err(error) => failure = Some(error),
                    }
                }
            }

            if let Some(error) = failure {
                warn!("Node({}) cannot ship complete schema proof for model {}: {}", self.id, model.to_base64_short(), error);
                connection.announced_models.lock().unwrap().remove(model);
                continue;
            }

            let mut conflict = false;
            for proof in &model_states {
                let entity_id = proof.state.payload.entity_id;
                if states.get(&entity_id).is_some_and(|existing| {
                    existing.genesis.payload != proof.genesis.payload || existing.state.payload != proof.state.payload
                }) {
                    conflict = true;
                    break;
                }
            }
            if conflict {
                warn!("Node({}) found conflicting snapshots while assembling schema proof for model {}", self.id, model.to_base64_short());
                connection.announced_models.lock().unwrap().remove(model);
                continue;
            }
            for proof in model_states {
                states.entry(proof.state.payload.entity_id).or_insert(proof);
            }
        }
        states.into_values().collect()
    }

    /// Receiver side of descriptor shipping (#330): policy-validate and ingest
    /// catalog defs attached to a message envelope BEFORE its body is
    /// processed, so model-id resolution and property naming see a warm map.
    fn ingest_schema(&self, from: &proto::NodeId, schema: &[proto::StateWithGenesis]) -> anyhow::Result<()> {
        if schema.is_empty() {
            return Ok(());
        }
        if self.system.founder() != Some(*from) {
            return Err(anyhow!("only the recognized system founder may ship catalog descriptor states"));
        }
        let mut accepted = std::collections::BTreeMap::<proto::EntityId, proto::StateWithGenesis>::new();
        for proof in schema.iter().cloned() {
            let genesis = self.validate_incoming_event(from, proof.genesis)?;
            let state = self.validate_incoming_state(from, proof.state)?;
            let genesis_id = genesis.payload.id();
            if !genesis.payload.is_entity_create()
                || proto::EntityId::from(genesis_id) != state.payload.entity_id
                || genesis.payload.entity_id != state.payload.entity_id
                || genesis.payload.model != state.payload.model
            {
                return Err(anyhow!("shipped schema state {} lacks its matching genesis identity proof", state.payload.entity_id));
            }

            let entity_id = state.payload.entity_id;
            let verified = proto::StateWithGenesis { genesis, state };
            if let Some(existing) = accepted.get(&entity_id) {
                if existing.genesis.payload != verified.genesis.payload || existing.state.payload != verified.state.payload {
                    return Err(anyhow!("conflicting shipped schema proofs for entity {entity_id}"));
                }
            } else {
                accepted.insert(entity_id, verified);
            }
        }
        let states: Vec<_> = accepted.into_values().map(|proof| proof.state).collect();
        self.catalog.ingest_wire_states(&states)?;
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id, request_body = %request_body)))]
    pub async fn request<'a, C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.request_inner(node_id, cdata, request_body, None).await.map(|response| response.into_parts().0)
    }

    /// Send a request whose response is useful only while `validity` remains
    /// current. Response handling checks it before ingesting attached schema;
    /// the caller may recheck it before applying the response body.
    pub(crate) async fn request_if_current<C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
        validity: RequestValidity,
    ) -> Result<GuardedResponse, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        self.request_inner(node_id, cdata, request_body, Some(validity)).await
    }

    async fn request_inner<C>(
        &self,
        node_id: proto::NodeId,
        cdata: &C,
        request_body: proto::NodeRequestBody,
        validity: Option<RequestValidity>,
    ) -> Result<GuardedResponse, RequestError>
    where
        C: Iterable<PA::ContextData>,
    {
        let (response_tx, response_rx) = oneshot::channel::<Result<GuardedResponse, RequestError>>();
        let request_id = proto::RequestId::new();

        let request = proto::NodeRequest { id: request_id.clone(), to: node_id, from: self.id, body: request_body };
        let auth = self.policy_agent.sign_request(self, cdata, &request)?;

        // Lookup, pending registration, and synchronous enqueue are one
        // registry-critical step. Otherwise replacement can drain the old
        // PeerState just before this request inserts its waiter, stranding it
        // forever on a retired Arc.
        {
            let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
            let connection = self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?;
            if !connection.ready.load(Ordering::Acquire) {
                return Err(RequestError::PeerNotConnected);
            }
            connection.pending_requests.insert(request_id.clone(), PendingRequest { response: response_tx, validity });
            if let Err(error) = connection.send_message(proto::NodeMessage::Request { auth, request }) {
                connection.pending_requests.remove(&request_id);
                return Err(error.into());
            }
        }

        // Wait for response
        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    // TODO LATER: rework this to be retried in the background some number of times
    pub fn send_update(&self, node_id: proto::NodeId, notification: proto::NodeUpdateBody) {
        // same as request, minus cdata and the sign_request step
        debug!("{self}.send_update({node_id:#}, {notification})");
        let (response_tx, _response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        let connection = {
            let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
            let Some(connection) = self.peer_connections.get(&node_id) else {
                warn!("Failed to send update to peer {}: {}", node_id, RequestError::PeerNotConnected);
                return;
            };
            if !connection.ready.load(Ordering::Acquire) {
                warn!("Failed to send update to peer {} before registration became ready", node_id);
                return;
            }
            connection.pending_updates.insert(id.clone(), response_tx);
            connection
        };

        // Descriptor shipping (#330) needs async collection of the catalog
        // states, and this fn is called from sync reactor callbacks, so the
        // send moves into a task. Wire descriptor states only bootstrap
        // missing catalog entries, so reordered tasks cannot roll mutable
        // schema fields backward; the ordinary catalog stream owns updates.
        let node = self.clone();
        crate::task::spawn(async move {
            let schema = node.schema_states_for_models(&connection, notification.referenced_models()).await;
            let message = proto::NodeMessage::Update(proto::NodeUpdate { id, from: node.id, to: node_id, body: notification, schema });
            let _registry_guard = node.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
            let still_current = node.peer_connections.get(&node_id).is_some_and(|current| Arc::ptr_eq(&current, &connection));
            if !still_current {
                return;
            }
            if let Err(e) = connection.send_message(message) {
                warn!("Failed to send update to peer {}: {}", node_id, e);
            }
        });
    }

    /// Admit a message from a connector connection whose peer was
    /// authenticated by a signed Presence handshake.
    ///
    /// The connection identity, not the envelope, is authoritative. This
    /// check deliberately happens before dispatch so no message variant can
    /// look up another peer's return channel or mutate state under a spoofed
    /// identity.
    pub fn verify_peer_message(
        &self,
        authenticated_peer: proto::NodeId,
        frame: proto::SignedPeerMessage,
    ) -> Result<VerifiedPeerMessage, PeerFrameError> {
        let Some(connection) = self.peer_connections.get(&authenticated_peer) else {
            return Err(PeerFrameError::PeerNotRegistered { peer: authenticated_peer });
        };
        if !connection.ready.load(Ordering::Acquire) {
            return Err(PeerFrameError::PeerNotReady { peer: authenticated_peer });
        }
        // Hold the session's generation read lock through synchronous frame
        // verification. A preserved first-join session cannot be rebound by
        // promotion between verification and token capture.
        let connection_generation = connection.system_generation.read().unwrap();
        let system_generation = connection_generation.clone();
        if frame.session != connection.incoming_session {
            return Err(PeerFrameError::WrongSession { peer: authenticated_peer });
        }
        if !frame.verify(authenticated_peer) {
            return Err(PeerFrameError::InvalidSignature { peer: authenticated_peer });
        }
        let declared_sender = frame.message.declared_sender();
        if declared_sender != authenticated_peer {
            return Err(crate::connector::PeerIdentityMismatch { authenticated_peer, declared_sender }.into());
        }
        connection.incoming_replay_window.lock().unwrap_or_else(|e| e.into_inner()).accept(authenticated_peer, frame.sequence)?;

        if !self.entities.is_current_generation(&system_generation) {
            return Err(PeerFrameError::PeerNotReady { peer: authenticated_peer });
        }
        drop(connection_generation);

        Ok(VerifiedPeerMessage {
            message: frame.message,
            authenticated_peer,
            incoming_session: connection.incoming_session,
            system_generation,
        })
    }

    pub async fn handle_verified_peer_message(&self, message: VerifiedPeerMessage) -> anyhow::Result<()> {
        // Revalidate immediately before dispatch, but do not hold the reset
        // lease across arbitrary peer/policy/subscription I/O. The exact
        // token is propagated into every downstream mutation boundary.
        self.peer_for_dispatch(message.authenticated_peer, Some(message.incoming_session), &message.system_generation)?;
        self.dispatch_message(message.message, message.system_generation, Some(message.incoming_session)).await
    }

    pub async fn handle_peer_message(&self, authenticated_peer: proto::NodeId, frame: proto::SignedPeerMessage) -> anyhow::Result<()> {
        let message = self.verify_peer_message(authenticated_peer, frame)?;
        self.handle_verified_peer_message(message).await
    }

    /// Unbound message injection seam used by protocol/adversarial tests and
    /// the deterministic simulator. It is absent from production builds;
    /// connectors must call [`Self::handle_peer_message`] with the identity
    /// authenticated for that connection.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        self.dispatch_message(message, self.entities.system_generation(), None).await
    }

    /// Sign a frame without enqueueing it. The deterministic simulator owns
    /// delivery itself, but still exercises the production session/signature/
    /// sequence verifier.
    #[cfg(feature = "test-helpers")]
    pub fn sign_peer_message(
        &self,
        recipient: proto::NodeId,
        message: proto::NodeMessage,
    ) -> Result<proto::SignedPeerMessage, RequestError> {
        let connection = self.peer_connections.get(&recipient).ok_or(RequestError::PeerNotConnected)?;
        Ok(connection.sign_message_for_delivery(message))
    }

    fn peer_for_dispatch(
        &self,
        peer: proto::NodeId,
        verified_session: Option<proto::HandshakeChallenge>,
        expected_generation: &Arc<AtomicBool>,
    ) -> Result<Arc<PeerState>, PeerFrameError> {
        if !self.entities.is_current_generation(expected_generation) {
            return Err(PeerFrameError::PeerNotReady { peer });
        }
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        let current = self.peer_connections.get(&peer).ok_or(PeerFrameError::PeerNotRegistered { peer })?;
        if verified_session.is_some_and(|session| current.incoming_session != session) {
            return Err(PeerFrameError::WrongSession { peer });
        }
        if !current.ready.load(Ordering::Acquire) {
            return Err(PeerFrameError::PeerNotReady { peer });
        }
        let current_generation = current.system_generation.read().unwrap();
        if !Arc::ptr_eq(&current_generation, expected_generation) {
            return Err(PeerFrameError::PeerNotReady { peer });
        }
        drop(current_generation);
        Ok(current)
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    async fn dispatch_message(
        &self,
        message: proto::NodeMessage,
        expected_generation: Arc<AtomicBool>,
        verified_session: Option<proto::HandshakeChallenge>,
    ) -> anyhow::Result<()> {
        match message {
            proto::NodeMessage::Update(update) => {
                debug!("Node({}) received update {}", self.id, update);
                self.peer_for_dispatch(update.from, verified_session, &expected_generation)?;
                if update.to != self.id {
                    warn!("{} received message from {} but is not the intended recipient", self.id, update.from);
                    return Ok(());
                }

                // take down the return address
                let id = update.id.clone();
                let to = update.from;
                let from = self.id;

                // A streamed item is admissible only while at least one
                // query that caused it is still current on this peer. For
                // reset-sensitive catalog queries, retain the owner fence
                // lease through schema ingestion, persistence, and the
                // acknowledgement so hard reset can quiesce the whole
                // mutation boundary before deleting storage.
                let _stream_leases: Vec<RequestLease> = match (&self.subscription_relay, &update.body) {
                    (Some(relay), proto::NodeUpdateBody::SubscriptionUpdate { items }) => {
                        let leases = relay
                            .validities_for_stream_update(&update.from, items)
                            .and_then(|validities| validities.into_iter().map(|validity| validity.try_acquire()).collect());
                        match leases {
                            Some(leases) => leases,
                            None => {
                                debug!(
                                    "Node({}) discarded stale subscription update {} from {} before schema ingestion",
                                    self.id, update.id, update.from
                                );
                                let connection = self.peer_for_dispatch(to, verified_session, &expected_generation)?;
                                connection.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
                                    id,
                                    from,
                                    to,
                                    body: proto::NodeUpdateAckBody::Error("subscription update has no current source query".to_string()),
                                }))?;
                                return Ok(());
                            }
                        }
                    }
                    (None, proto::NodeUpdateBody::SubscriptionUpdate { .. }) => {
                        let connection = self.peer_for_dispatch(to, verified_session, &expected_generation)?;
                        connection.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
                            id,
                            from,
                            to,
                            body: proto::NodeUpdateAckBody::Error("subscription update has no current source query".to_string()),
                        }))?;
                        return Ok(());
                    }
                };

                // Descriptor shipping (#330): warm the catalog map from the
                // attached defs BEFORE applying the body. Hold the exact
                // generation lease only across this synchronous map fold;
                // the update applier carries the token to its own bounded
                // durable-write fences.
                let schema_result = {
                    let _guard = self.system.guard_generation(&expected_generation).await?;
                    self.peer_for_dispatch(update.from, verified_session, &expected_generation)?;
                    self.ingest_schema(&update.from, &update.schema)
                };
                let body = match schema_result {
                    Ok(()) => match self.handle_update(update, &expected_generation).await {
                        Ok(_) => proto::NodeUpdateAckBody::Success,
                        Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                    },
                    Err(error) => proto::NodeUpdateAckBody::Error(error.to_string()),
                };

                let connection = self.peer_for_dispatch(to, verified_session, &expected_generation)?;
                connection.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
            }
            proto::NodeMessage::UpdateAck(ack) => {
                debug!("Node({}) received ack notification {} {}", self.id, ack.id, ack.body);
                // Drain the pending entry so the map does not leak (the send
                // side no longer blocks on it).
                let connection = self.peer_for_dispatch(ack.from, verified_session, &expected_generation)?;
                connection.pending_updates.remove(&ack.id);
                // Surface a rejected update instead of dropping it silently.
                // Automatic retry (e.g. re-announcing the model and re-sending)
                // is still a TODO -- see send_update.
                if let proto::NodeUpdateAckBody::Error(e) = &ack.body {
                    warn!("Node({}) update {} rejected by peer {}: {}", self.id, ack.id, ack.from, e);
                }
            }
            proto::NodeMessage::Request { auth, request } => {
                debug!("Node({}) received request {}", self.id, request);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                self.peer_for_dispatch(request.from, verified_session, &expected_generation)?;
                let from = request.from;
                let request_id = request.id.clone();
                if request.to != self.id {
                    warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                    return Ok(());
                }

                // Policy may perform arbitrary async work, so carry the
                // expected generation through it rather than holding reset.
                let body = match self.policy_agent.check_request(self, &auth, &request).await {
                    Ok(cdata) => match self.handle_request(&cdata, request, &expected_generation, verified_session).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    },
                    Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                };
                let connection = self.peer_for_dispatch(from, verified_session, &expected_generation)?;
                // Descriptor shipping (#330): catalog defs for any model id
                // this response references that the connection has not seen.
                let schema = self.schema_states_for_models(&connection, body.referenced_models()).await;
                let connection = self.peer_for_dispatch(from, verified_session, &expected_generation)?;
                let _result = connection.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                    request_id,
                    from: self.id,
                    to: from,
                    body,
                    schema,
                }));
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                let connection = self.peer_for_dispatch(response.from, verified_session, &expected_generation)?;
                // Descriptor shipping (#330): ingest attached defs before the
                // requester consumes the body -- but only for a response that
                // matches a request we actually sent, so an unsolicited or
                // misattributed response cannot poison the catalog map.
                if let Some(pending) = connection.pending_requests.remove(&response.request_id) {
                    // Owner-fence admission (epoch hardening): a response for
                    // a reset-sensitive request must acquire its owner lease
                    // BEFORE schema ingestion, and hold it until the caller
                    // finishes applying the body.
                    let lease = match &pending.validity {
                        Some(validity) => match validity.try_acquire() {
                            Some(lease) => lease,
                            None => {
                                debug!(
                                    "Node({}) discarded stale response {} from {} before schema ingestion",
                                    self.id, response.request_id, response.from
                                );
                                return Ok(());
                            }
                        },
                        None => RequestLease::unguarded(),
                    };
                    let body = {
                        let _guard = self.system.guard_generation(&expected_generation).await?;
                        self.peer_for_dispatch(response.from, verified_session, &expected_generation)?;
                        self.ingest_schema(&response.from, &response.schema)
                            .map(|()| GuardedResponse { body: response.body, lease })
                            .map_err(|error| RequestError::ServerError(error.to_string()))
                    };
                    pending.response.send(body).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
            proto::NodeMessage::UnsubscribeQuery { from, query_id } => {
                // Remove predicate from the peer's subscription
                let peer_state = self.peer_for_dispatch(from, verified_session, &expected_generation)?;
                peer_state.subscription_handler.remove_predicate(query_id)?;
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(request = %request)))]
    async fn handle_request<C>(
        &self,
        cdata: &C,
        request: proto::NodeRequest,
        expected_generation: &Arc<AtomicBool>,
        verified_session: Option<proto::HandshakeChallenge>,
    ) -> anyhow::Result<proto::NodeResponseBody>
    where
        C: Iterable<PA::ContextData>,
    {
        match request.body {
            proto::NodeRequestBody::CommitTransaction { id, events } => {
                // Protected collections (the system collection and the metadata
                // catalog) are not mutable through ordinary transactions,
                // regardless of the sender's software version; the catalog's
                // only mutation path is the registration operation (RFC 4).
                //
                // The write target is the collection the model id RESOLVES to,
                // not the literal id on the wire: a static well-known-id check
                // would miss a non-reserved model id that the catalog map
                // routes to a protected collection. Resolve every event up
                // front so a protected target aborts the whole transaction
                // before any event is written. (Registration writes the catalog
                // through a direct commit_remote_transaction call, bypassing
                // this ingress guard, so this does not block it.)
                for event in &events {
                    let collection_id = self.resolve_model_wait(&event.payload.model).await?;
                    if crate::schema::is_protected_collection(&collection_id) {
                        return Ok(proto::NodeResponseBody::Error(format!(
                            "collection '{}' is protected and not writable by transactions",
                            collection_id
                        )));
                    }
                }
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for CommitTransaction"))?;
                match self.commit_remote_transaction_in_generation(cdata, id.clone(), events, expected_generation).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete { id }),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { collection, mut selection, known_matches } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;
                selection.predicate = self.policy_agent.filter_predicate(cdata, &collection, selection.predicate)?;

                // Expand initial_states to include entities from known_matches that weren't in the predicate results
                let expanded_states = crate::util::expand_states::expand_states(
                    storage_collection.fetch_states(&selection).await?,
                    known_matches.iter().map(|k| k.entity_id).collect::<Vec<_>>(),
                    &storage_collection,
                )
                .await?;

                let known_map: std::collections::HashMap<_, _> = known_matches.into_iter().map(|k| (k.entity_id, k.head)).collect();

                let mut deltas = Vec::new();
                for state in expanded_states {
                    if self
                        .policy_agent
                        .check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state, Some(self.catalog.resolver_weak()))
                        .is_err()
                    {
                        continue;
                    }

                    // Generate delta based on known_matches (returns None if heads are equal)
                    // No need to reconstruct Entity - work directly with EntityState
                    if let Some(delta) = self.generate_entity_delta(&known_map, state, &storage_collection, cdata).await? {
                        deltas.push(delta);
                    }
                }
                let _guard = self.system.guard_generation(expected_generation).await?;
                Ok(proto::NodeResponseBody::Fetch(deltas))
            }
            proto::NodeRequestBody::Get { collection, ids } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;

                // filter out any that the policy agent says we don't have access to
                let mut states = Vec::new();
                for state in storage_collection.get_states(ids).await? {
                    match self.policy_agent.check_read(
                        cdata,
                        &state.payload.entity_id,
                        &collection,
                        &state.payload.state,
                        Some(self.catalog.resolver_weak()),
                    ) {
                        Ok(_) => states.push(state),
                        Err(AccessDenied::ByPolicy(_)) => {}
                        // TODO: we need to have a cleaner delineation between actual access denied versus processing errors
                        Err(e) => return Err(anyhow!("Error from peer get: {}", e)),
                    }
                }

                let _guard = self.system.guard_generation(expected_generation).await?;
                Ok(proto::NodeResponseBody::Get(states))
            }
            proto::NodeRequestBody::GetEvents { collection, event_ids } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;

                // filter out any that the policy agent says we don't have access to
                let mut events = Vec::new();
                for event in storage_collection.get_events(event_ids).await? {
                    match self.policy_agent.check_read_event(cdata, &collection, &event) {
                        Ok(_) => events.push(event),
                        Err(AccessDenied::ByPolicy(_)) => {}
                        // TODO: we need to have a cleaner delineation between actual access denied versus processing errors
                        Err(e) => return Err(anyhow!("Error from peer subscription: {}", e)),
                    }
                }

                let _guard = self.system.guard_generation(expected_generation).await?;
                Ok(proto::NodeResponseBody::GetEvents(events))
            }
            proto::NodeRequestBody::RegisterSchema { models, properties, memberships } => {
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for RegisterSchema"))?;
                match self.execute_schema_registration_in_generation(cdata, models, properties, memberships, expected_generation).await {
                    // The resolved definitions ARE the response (RFC 5.2):
                    // the requester folds them into its catalog map on ack.
                    Ok((models, properties, memberships)) => {
                        Ok(proto::NodeResponseBody::SchemaRegistered { models, properties, memberships })
                    }
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::SubscribeQuery { query_id, collection, selection, version, known_matches } => {
                let peer_state = self.peer_for_dispatch(request.from, verified_session, expected_generation)?;
                // only one cdata is permitted for SubscribePredicate
                use itertools::Itertools;
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for SubscribePredicate"))?;
                let response = peer_state
                    .subscription_handler
                    .subscribe_query(self, query_id.clone(), collection, selection, cdata, version, known_matches)
                    .await;
                match self.system.guard_generation(expected_generation).await {
                    Ok(_guard) => Ok(response?),
                    Err(error) => {
                        // The exact old handler may have resumed its reactor
                        // upsert after reset. Remove only its query; never
                        // look the peer up again by stable NodeId, which could
                        // target a replacement session's handler.
                        let _ = peer_state.subscription_handler.remove_predicate(query_id);
                        Err(error.into())
                    }
                }
            }
        }
    }

    async fn handle_update(&self, notification: proto::NodeUpdate, expected_generation: &Arc<AtomicBool>) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::NodeUpdateBody::SubscriptionUpdate { items } => {
                tracing::debug!("Node({}) received subscription update from peer {}", self.id, notification.from);
                crate::node_applier::NodeApplier::apply_updates(self, &notification.from, items, expected_generation.clone()).await?;
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
            match self.request(peer_id, cdata, proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.to_vec() }).await
            {
                Ok(proto::NodeResponseBody::CommitComplete { .. }) => (),
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

    /// INGRESS resolution for the wire envelope (#330): the collection a
    /// received model id routes to -- well-known system/catalog ids answer on
    /// a stone-cold node, user models come from the catalog map. A miss is
    /// rejected loudly: once descriptor shipping guarantees delivery, an
    /// unresolvable model id is a protocol violation, and rejection (unlike
    /// synthesizing a collection) is retryable.
    pub(crate) fn resolve_model(&self, model: &proto::EntityId) -> Result<proto::CollectionId, RetrievalError> {
        self.catalog.collection_for_model(model).ok_or_else(|| {
            RetrievalError::Other(format!("unknown model id {}: no well-known or catalog entry (protocol violation?)", model.to_base64()))
        })
    }

    /// [`Self::resolve_model`] for ingress paths on a durable node, where a
    /// miss can mean the startup storage warm has not finished yet (a
    /// restarted node receiving traffic immediately): await catalog
    /// readiness once and retry before rejecting. Ephemeral nodes never
    /// wait here -- their defs arrive inline with the message (descriptor
    /// shipping runs before body processing), so a miss is already final.
    pub(crate) async fn resolve_model_wait(&self, model: &proto::EntityId) -> Result<proto::CollectionId, RetrievalError> {
        match self.resolve_model(model) {
            Ok(collection) => Ok(collection),
            Err(e) => {
                if self.durable && !self.catalog.is_catalog_ready() {
                    self.catalog.wait_catalog_ready().await;
                    self.resolve_model(model)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Prove that a state-only payload is anchored to the genesis named by
    /// its self-certifying EntityId. State snapshots do not carry that proof
    /// inline, so ingress retrieves the event whose EventId has the same 32
    /// bytes and validates it before the state can materialize an entity.
    pub(crate) async fn validate_state_identity<E>(&self, state: &proto::EntityState, event_getter: &E) -> Result<(), MutationError>
    where E: GetEvents + Send + Sync {
        let genesis_id = proto::EventId::from_bytes(state.entity_id.to_bytes());
        let genesis = event_getter.get_event(&genesis_id).await?;
        self.validate_event_scope(&genesis)?;
        if !genesis.is_entity_create() || genesis.id() != genesis_id || genesis.entity_id != state.entity_id || genesis.model != state.model
        {
            return Err(MutationError::InvalidEvent);
        }
        Ok(())
    }

    /// Does all the things necessary to commit a remote transaction
    pub async fn commit_remote_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        let system_generation = self.entities.system_generation();
        self.commit_remote_transaction_in_generation(cdata, id, events, &system_generation).await
    }

    pub(crate) async fn commit_remote_transaction_in_generation(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<Attested<proto::Event>>,
        system_generation: &Arc<AtomicBool>,
    ) -> Result<(), MutationError> {
        let _system_generation_guard = self.system.guard_generation(system_generation).await?;
        debug!("{self} commiting transaction {id} with {} events", events.len());
        let mut changes = Vec::new();

        for event in events.iter_mut() {
            self.validate_event_scope(&event.payload)?;
            // Submitted events may carry portable attestations from earlier
            // hops. Retain only envelopes that are structurally bound,
            // correctly signed, and issued by the recognized durable before
            // anything reaches storage.
            self.verify_event_attestations(event);

            // INGRESS (#330): the envelope carries a model id; resolve it to
            // the local collection (well-knowns, then catalog) or reject.
            let collection_id = self.resolve_model_wait(&event.payload.model).await?;
            let collection = self.collections.get(&collection_id).await?;

            // When applying an event, we should only look at the local storage for the lineage
            let event_getter = LocalEventGetter::new(collection.clone(), self.durable);
            let state_getter = LocalStateGetter::new(collection.clone());
            let entity = self
                .entities
                .get_retrieve_or_create_in_generation(
                    &state_getter,
                    &event_getter,
                    &collection_id,
                    &event.payload.entity_id,
                    system_generation.clone(),
                )
                .await?;
            let admission: Result<_, MutationError> = async {
                // Stage the event so BFS can discover it while validating on
                // an isolated fork. In particular, a denied genesis must not
                // mutate a newly materialized resident entity.
                event_getter.stage_event(event.payload.clone());
                use std::sync::atomic::AtomicBool;
                let trx_alive = Arc::new(AtomicBool::new(true));
                let entity_before = entity.clone();
                let entity_after = entity.snapshot(trx_alive);
                entity_after.apply_event(&event_getter, &event.payload).await?;

                Ok(self.policy_agent.check_event(self, cdata, &entity_before, &entity_after, &event.payload)?)
            }
            .await;
            let admission = match admission {
                Ok(admission) => admission,
                Err(error) => {
                    // Retrieval creates an empty resident placeholder when a
                    // remote genesis names a new entity. A denial must evict
                    // that placeholder as well as avoid storage mutation.
                    self.entities.remove_if_phantom_in_generation(&event.payload.entity_id, system_generation);
                    return Err(error);
                }
            };
            let locally_attested = self.attest_event(event.payload.clone(), admission);
            event.attestations.0.extend(locally_attested.attestations.0);

            // Commit the staged event to permanent storage
            event_getter.commit_event(event).await?;

            // Event is now in storage, so BFS will find it while applying to
            // the real entity.
            let applied = entity.apply_event(&event_getter, &event.payload).await?;

            if applied {
                let state = entity.to_state()?;
                let entity_state = EntityState { entity_id: entity.id(), model: entity.model_id()?, state };
                let admission = self.policy_agent.attest_state(self, &entity_state);
                let attested = self.attest_state(entity_state, admission);
                collection.set_state(attested).await?;
                changes.push(EntityChange::new(entity.clone(), vec![event.clone()])?);
            }
        }

        self.reactor.notify_change(changes).await;

        Ok(())
    }

    /// Generate EntityDelta for an entity state, using known_matches to decide between StateSnapshot and EventBridge
    /// Returns None if the entity is in known_matches with equal heads (client already has current state)
    pub(crate) async fn generate_entity_delta<C>(
        &self,
        known_map: &std::collections::HashMap<proto::EntityId, proto::Clock>,
        entity_state: proto::Attested<proto::EntityState>,
        storage_collection: &crate::storage::StorageCollectionWrapper,
        cdata: &C,
    ) -> anyhow::Result<Option<proto::EntityDelta>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        C: crate::util::Iterable<PA::ContextData>,
    {
        // Destructure to take ownership and avoid clones
        let proto::Attested { payload: proto::EntityState { entity_id, model, state }, attestations } = entity_state;
        let current_head = &state.head;

        // Entity is in known_matches - try to optimize the response
        if let Some(known_head) = known_map.get(&entity_id) {
            // Case 1: Heads equal → return None (omit entity, client already has current state) ✓
            if known_head == current_head {
                return Ok(None);
            }

            // Case 2: Heads differ → try to build EventBridge (cheaper than full state) ✓
            match self
                .collect_event_bridge(&self.resolve_model_wait(&model).await?, storage_collection, known_head, current_head, cdata)
                .await
            {
                Ok(attested_events) if !attested_events.is_empty() => {
                    // Convert Attested<Event> to EventFragments (strips entity_id and collection)
                    let event_fragments: Vec<proto::EventFragment> = attested_events.into_iter().map(|e| e.into()).collect();

                    return Ok(Some(proto::EntityDelta {
                        entity_id,
                        model,
                        content: proto::DeltaContent::EventBridge { events: event_fragments },
                    }));
                }
                _ => {
                    // Fall through to StateSnapshot if bridge building failed or returned empty
                }
            }
        }

        // Case 3: Entity not in known_matches OR bridge building failed → send full StateSnapshot ✓
        let state_fragment = proto::StateFragment { state, attestations };
        Ok(Some(proto::EntityDelta { entity_id, model, content: proto::DeltaContent::StateSnapshot { state: state_fragment } }))
    }

    /// Collect events between known_head and current_head using event_dag comparison.
    /// Returns events needed to advance from known_head to current_head.
    pub(crate) async fn collect_event_bridge<C>(
        &self,
        collection: &proto::CollectionId,
        storage_collection: &crate::storage::StorageCollectionWrapper,
        known_head: &proto::Clock,
        current_head: &proto::Clock,
        cdata: &C,
    ) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        C: crate::util::Iterable<PA::ContextData>,
    {
        use crate::event_dag::{compare, AbstractCausalRelation};
        use crate::retrieval::LocalEventGetter;
        use std::collections::HashSet;

        let event_getter = LocalEventGetter::new(storage_collection.clone(), self.durable);

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
                let known_set: HashSet<_> = known_head.as_slice().iter().collect();
                let mut events = Vec::new();
                let mut frontier: Vec<proto::EventId> = current_head.as_slice().to_vec();
                let mut visited: HashSet<proto::EventId> = HashSet::new();

                while !frontier.is_empty() {
                    let batch = std::mem::take(&mut frontier);
                    let fetched = storage_collection.get_events(batch).await?;

                    for event in fetched {
                        let id = event.payload.id();
                        if visited.insert(id.clone()) && !known_set.contains(&id) {
                            // Add parents to frontier (walking backward)
                            for parent in event.payload.parent.as_slice() {
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
                    match self.policy_agent.check_read_event(cdata, collection, event) {
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

    /// The currently-resident (in-memory) entity for `id`, if one is held.
    ///
    /// The resident entity carries the authoritative materialized state, which
    /// can be ahead of the persisted state buffer: the EventOnly apply path
    /// commits events and advances the in-memory entity but does not rewrite the
    /// state buffer in storage (state is a rebuildable cache of the event log).
    /// A test or tool that must observe a node's true materialized state
    /// (e.g. the phase 2 simulation harness checking cross-node convergence)
    /// needs this resident view; reading the storage state buffer alone
    /// under-reports EventOnly progress. Returns `None` if no strong reference
    /// keeps the entity resident, in which case the caller falls back to the
    /// persisted state.
    pub fn get_resident_entity(&self, id: proto::EntityId) -> Option<crate::entity::Entity> { self.entities.get(&id) }

    pub fn context(&self, data: PA::ContextData) -> Result<Context, anyhow::Error> {
        if !self.system.is_system_ready() {
            return Err(anyhow!("System is not ready"));
        }
        // NOTE: unlike `context_async`, the synchronous `context` does NOT
        // kick off the ephemeral catalog subscription (RFC 5.2). Doing so
        // requires a spawned, fire-and-forget task (this method cannot
        // await), and that concurrent subscription setup perturbs the
        // precise entity-watcher teardown timing that some existing
        // subscription-lifecycle tests assert. Ephemeral consumers that need
        // the catalog resolve it by creating their context via
        // `context_async`, or by calling `node.catalog.ensure_subscribed`
        // directly; `wait_catalog_ready` gates readiness either way. Durable
        // nodes are unaffected (they warm from storage at startup).
        Ok(Context::new(Node::clone(self), data))
    }

    pub async fn context_async(&self, data: PA::ContextData) -> Context {
        self.system.wait_system_ready().await;
        let node = Node::clone(self);
        // Ephemeral nodes warm the catalog map by subscribing to the catalog
        // collections through the relay (RFC 5.2); first call wins, the rest
        // are no-ops. Durable nodes short-circuit (they warm from storage).
        node.catalog.ensure_subscribed(data.clone(), &node).await;
        Context::new(node, data)
    }

    pub(crate) async fn get_from_peer(
        &self,
        collection_id: &CollectionId,
        ids: Vec<proto::EntityId>,
        cdata: &PA::ContextData,
    ) -> Result<(), RetrievalError> {
        let system_generation = self.entities.system_generation();
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(peer_id, cdata, proto::NodeRequestBody::Get { collection: collection_id.clone(), ids })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Get(states) => {
                let collection = self.collections.get(collection_id).await?;
                let event_getter = CachedEventGetter::new(collection_id.clone(), collection.clone(), self, cdata);

                for state in states {
                    let state = self.validate_incoming_state(&peer_id, state)?;
                    self.validate_state_identity(&state.payload, &event_getter).await.map_err(|e| RetrievalError::Other(e.to_string()))?;
                    let _guard = self.system.guard_generation(&system_generation).await?;
                    collection.set_state(state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
                }
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

    /// Get all currently registered, ready durable peer NodeIds, sorted for a
    /// stable fan-out order. Routing derives from the current PeerState rather
    /// than a second role set, so replacement/teardown cannot skew identity
    /// and role across two independently mutated structures.
    pub fn get_durable_peers(&self) -> Vec<proto::NodeId> {
        let _registry_guard = self.peer_registry_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut peers: Vec<_> = self
            .peer_connections
            .to_vec()
            .into_iter()
            .filter_map(|(id, state)| {
                (state.ready.load(Ordering::Acquire)
                    && state.durable.load(Ordering::Acquire)
                    && state.system_generation.read().unwrap().load(Ordering::Acquire))
                .then_some(id)
            })
            .collect();
        #[cfg(any(test, feature = "test-helpers"))]
        peers.extend(self.test_durable_peers.to_vec());
        peers.sort();
        peers.dedup();
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
    pub fn conjure_evil_phantom(&self, id: proto::EntityId, collection: proto::CollectionId) -> crate::entity::Entity {
        self.entities.conjure_evil_phantom(id, collection)
    }

    /// TEST ONLY: Register a durable peer id directly, bypassing the connection handshake.
    ///
    /// Lets tests populate the durable-peer set to exercise fan-out ordering and seeded random
    /// selection without standing up real peer connections.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn insert_durable_peer_for_test(&self, peer_id: proto::NodeId) { self.test_durable_peers.insert(peer_id); }
}

impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub async fn request_remote_unsubscribe(&self, query_id: proto::QueryId, peers: Vec<proto::NodeId>) -> anyhow::Result<()> {
        for (peer_id, item) in self.peer_connections.get_list(peers) {
            if let Some(connection) = item {
                // `from` identifies the UNSUBSCRIBING node: the receiver looks it
                // up in its own peer_connections to find the subscription handler
                // holding this query. Addressing it to the target peer's id (as
                // this did historically) made the receiver look up itself, miss,
                // and silently leak the registration -- masked pre-rev-4 by the
                // no-subscriptions "invalid update" rejection, which the always-on
                // catalog subscriptions now keep from ever engaging.
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
        collection_id: CollectionId,
        schema: Option<&'static crate::schema::ModelSchema>,
        selection: ankql::ast::Selection,
        cdata: PA::ContextData,
        version: u32,
        request_validity: Option<RequestValidity>,
        livequery: crate::livequery::WeakEntityLiveQuery,
    ) {
        if self.subscription_relay.is_none() {
            return;
        }
        // The selection this ephemeral node forwards to its durable peer must
        // be resolved from PathExpr to stable Identifier, and
        // resolution can await (catalog warm-up, first-use registration).
        // This path is sync (called from the LiveQuery constructor), so
        // resolve + forward inside a spawned task. The relay registration is
        // already asynchronous with respect to activation, so deferring it
        // one hop is safe. The exact compiled model registers inside the
        // resolve; if neither the catalog cache nor registration can resolve
        // the reference (policy denial, no durable
        // peer), the query fails LOUD on the livequery's `error` -- there is
        // nothing truthful a subscription could ask for.
        let node = self.clone();
        crate::task::spawn(async move {
            let resolved = match node.catalog.resolve_selection_deferred(&node, Some(&cdata), &collection_id, schema, &selection).await {
                Ok(resolved) => resolved,
                Err(e) => {
                    debug!("subscribe_remote_query resolution failed for {query_id}: {e}");
                    if let Some(lq) = livequery.upgrade() {
                        lq.set_error_for_version(version, e.into());
                        lq.mark_initial_query_failed();
                    }
                    return;
                }
            };
            // Resolve types in the AST (converts literals for JSON path comparisons).
            let selection = node.type_resolver.resolve_selection_types(resolved);
            // The remote peer and the local reactor must consume the SAME
            // resolved selection. Store it before relay registration, whose
            // completion callback activates the local reactor.
            let Some(lq) = livequery.upgrade() else { return };
            // A newer selection may already be resolving. The initial relay
            // entry is still required as its serialization point; that newer
            // task waits for this registration and then updates it.
            lq.set_resolved_selection(selection.clone(), version);
            // Resolution may outlive the catalog generation that requested
            // it. Refuse a late relay entry after reset invalidation; local
            // cached activation holds the same fence through its storage
            // fetch, while relay responses acquire it at schema ingress.
            if request_validity.as_ref().is_some_and(|validity| !validity.is_current()) {
                lq.mark_initial_query_failed();
                return;
            }
            node.predicate_context.insert(query_id, cdata.clone());
            if let Some(ref relay) = node.subscription_relay {
                relay.subscribe_query_with_validity(query_id, collection_id, selection, cdata, version, request_validity, livequery);
                lq.mark_initial_query_ready();
            }
        });
    }

    pub async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let storage_collection = self.collections.get(collection_id).await?;
        let initial_states = storage_collection.fetch_states(selection).await?;
        let state_getter = LocalStateGetter::new(storage_collection.clone());
        let event_getter = LocalEventGetter::new(storage_collection, self.durable);
        let mut entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) = self
                .entities
                .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state)
                .await?;
            entities.push(entity);
        }
        Ok(entities)
    }
}
#[async_trait::async_trait]
pub trait TNodeErased<E: AbstractEntity + Filterable + Send + 'static = Entity>: Send + Sync + 'static {
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId);
    fn update_query_selection(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection,
        version: u32,
        livequery: crate::livequery::WeakEntityLiveQuery,
    ) -> Result<(), anyhow::Error>;
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection,
    ) -> Result<Vec<E>, RetrievalError>;
    fn reactor(&self) -> &Reactor<E>;
}

#[async_trait::async_trait]
impl<SE, PA> TNodeErased<Entity> for Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId) {
        // Clean up subscription context
        self.predicate_context.remove(&query_id);

        // Notify subscription relay for remote cleanup
        if let Some(ref relay) = self.subscription_relay {
            relay.unsubscribe_predicate(query_id);
        }
    }

    fn update_query_selection(
        &self,
        query_id: proto::QueryId,
        collection_id: CollectionId,
        selection: ankql::ast::Selection,
        version: u32,
        livequery: crate::livequery::WeakEntityLiveQuery,
    ) -> Result<(), anyhow::Error> {
        // Both durable and ephemeral updates resolve here. This trait method
        // is sync, so resolve + store + activate/forward in a spawned task.
        // Keeping that sequence in one place guarantees that the local
        // reactor and remote peer see the same resolved AST.
        let node = self.clone();
        crate::task::spawn(async move {
            let Some(lq) = livequery.upgrade() else { return };
            if !lq.wait_initial_query_ready().await {
                lq.set_error_for_version(version, RetrievalError::Other("initial query resolution failed".to_string()));
                return;
            }
            // This sync trait path does not carry context data, but an
            // ephemeral query retained its original context before marking
            // the initial subscription ready. Reuse it so a schema-less
            // update can warm a cold catalog instead of waiting on readiness
            // that no task started. Durable queries have no relay context;
            // their startup catalog warm is independently bounded.
            let schema = lq.schema();
            let resolve_cdata = schema.is_none().then(|| node.predicate_context.get(&query_id)).flatten();
            let selection =
                match node.catalog.resolve_selection_deferred(&node, resolve_cdata.as_ref(), &collection_id, schema, &selection).await {
                    Ok(resolved) => resolved,
                    Err(e) => {
                        debug!("update_query_selection resolution failed for {query_id}: {e}");
                        lq.set_error_for_version(version, e.into());
                        return;
                    }
                };
            // Resolve types in the AST (converts literals for JSON path comparisons).
            let selection = node.type_resolver.resolve_selection_types(selection);
            if !lq.set_resolved_selection(selection.clone(), version) {
                return;
            }
            if let Some(ref relay) = node.subscription_relay {
                if let Err(e) = relay.update_query(query_id, selection, version) {
                    debug!("update_query_selection forward failed for {query_id}: {e}");
                    lq.set_error_for_version(version, e.into());
                }
            } else if let Err(e) = lq.activate(version).await {
                tracing::error!("LiveQuery update failed for predicate {query_id}: {e}");
                lq.set_error_for_version(version, e);
            }
        });
        Ok(())
    }

    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection,
    ) -> Result<Vec<Entity>, RetrievalError> {
        Node::fetch_entities_from_local(self, collection_id, selection).await
    }

    fn reactor(&self) -> &Reactor<Entity> { &self.0.reactor }
}

impl<SE, PA> fmt::Display for Node<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{}\x1b[2m]\x1b[0m", self.id.to_base64_short())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        property::{backend::lww::LWWBackend, backend::PropertyBackend, PropertyKey},
        storage::{StorageCollection, StorageEngine},
        value::Value,
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicBool, Ordering},
    };

    struct EmptyEngine;
    struct EmptyCollection;

    #[async_trait::async_trait]
    impl StorageEngine for EmptyEngine {
        type Value = ();

        async fn collection(&self, _id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
            Ok(Arc::new(EmptyCollection))
        }

        async fn delete_all_collections(&self) -> Result<bool, MutationError> { Ok(true) }

        async fn claim_system_root(&self, _candidate: &proto::SystemRootProof) -> Result<crate::storage::SystemRootClaim, MutationError> {
            Ok(crate::storage::SystemRootClaim::Claimed)
        }

        async fn system_root_claim(&self) -> Result<Option<proto::SystemRootProof>, RetrievalError> { Ok(None) }

        async fn release_system_root_claim(&self, _expected: &proto::SystemRootProof) -> Result<bool, MutationError> { Ok(true) }
    }

    #[async_trait::async_trait]
    impl StorageCollection for EmptyCollection {
        async fn set_state(&self, _state: Attested<EntityState>) -> Result<bool, MutationError> { Ok(true) }

        async fn get_state(&self, id: proto::EntityId) -> Result<Attested<EntityState>, RetrievalError> {
            Err(RetrievalError::EntityNotFound(id))
        }

        async fn fetch_states(&self, _selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
            Ok(Vec::new())
        }

        async fn add_event(&self, _event: &Attested<proto::Event>) -> Result<bool, MutationError> { Ok(true) }

        async fn get_events(&self, _event_ids: Vec<proto::EventId>) -> Result<Vec<Attested<proto::Event>>, RetrievalError> {
            Ok(Vec::new())
        }

        async fn dump_entity_events(&self, _id: proto::EntityId) -> Result<Vec<Attested<proto::Event>>, RetrievalError> { Ok(Vec::new()) }
    }

    #[derive(Clone)]
    struct CapturingSender {
        peer: proto::NodeId,
        sent: Arc<Mutex<Vec<proto::NodeMessage>>>,
    }

    impl PeerSender for CapturingSender {
        fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError> {
            self.sent.lock().unwrap().push(message.message);
            Ok(())
        }

        fn close(&self) {}

        fn recipient_node_id(&self) -> proto::NodeId { self.peer }

        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }

    fn forged_model_state(collection: &str) -> proto::StateWithGenesis {
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Name("collection".to_owned()), Some(Value::String(collection.to_owned())));
        backend.set(PropertyKey::Name("name".to_owned()), Some(Value::String("Stale model".to_owned())));
        let lww = backend.to_operations().unwrap().expect("catalog state has fields");
        let model = crate::schema::well_known_model_id(crate::schema::MODEL_COLLECTION_ID).unwrap();
        let genesis = proto::Event::genesis(
            model,
            Some(proto::EntityId::from_bytes([0x51; 32])),
            proto::OperationSet(BTreeMap::from([("lww".to_owned(), lww.clone())])),
        );
        let event_id = genesis.id();
        let entity_id = genesis.entity_id;
        let backend = LWWBackend::new();
        backend.apply_operations_with_event(&lww, event_id.clone()).unwrap();
        let state = Attested::opt(
            EntityState {
                entity_id,
                model,
                state: proto::State {
                    state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                    head: proto::Clock::from(vec![event_id]),
                },
            },
            None,
        );
        proto::StateWithGenesis { genesis: Attested::opt(genesis, None), state }
    }

    #[tokio::test]
    async fn invalid_request_response_is_dropped_before_schema_ingestion() {
        let node = Node::new(Arc::new(EmptyEngine), PermissiveAgent::new());
        // A real second node signs the presence so the production handshake
        // verification admits the capturing transport.
        let peer_node = Node::new(Arc::new(EmptyEngine), PermissiveAgent::new());
        let peer = peer_node.id;
        let sent = Arc::new(Mutex::new(Vec::new()));
        let node_handshake = node.begin_peer_handshake();
        let peer_handshake = peer_node.begin_peer_handshake();
        node.register_peer(
            peer_node.presence(node_handshake.challenge()),
            node_handshake,
            peer_handshake.challenge(),
            Box::new(CapturingSender { peer, sent: sent.clone() }),
        )
        .unwrap();

        let owner_current = Arc::new(AtomicBool::new(true));
        let validity = RequestValidity::new({
            let owner_current = owner_current.clone();
            move || owner_current.load(Ordering::Acquire)
        });
        let request = {
            let node = node.clone();
            tokio::spawn(async move {
                node.request_if_current(
                    peer,
                    &DEFAULT_CONTEXT,
                    proto::NodeRequestBody::Get { collection: CollectionId::fixed_name("ignored"), ids: Vec::new() },
                    validity,
                )
                .await
            })
        };

        let request_id = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Some(proto::NodeMessage::Request { request, .. }) = sent.lock().unwrap().first() {
                    break request.id.clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("guarded request must be sent");

        owner_current.store(false, Ordering::Release);
        node.handle_message(proto::NodeMessage::Response(proto::NodeResponse {
            request_id,
            from: peer,
            to: node.id,
            body: proto::NodeResponseBody::Get(Vec::new()),
            schema: vec![forged_model_state("must_not_ingest")],
        }))
        .await
        .unwrap();

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), request)
            .await
            .expect("stale response must release its requester")
            .expect("request task must not panic")
            .expect_err("stale response body must not be delivered");
        assert!(matches!(error, RequestError::InternalChannelClosed));
        assert!(node.catalog.model_by_collection("must_not_ingest").is_none(), "stale attached schema must not enter the catalog");
    }
}
