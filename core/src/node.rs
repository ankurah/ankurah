use crate::selection::filter::Filterable;
use ankurah_proto::{self as proto, Attested, CollectionId};
use anyhow::anyhow;

use rand::prelude::*;
use rand::rngs::SmallRng;
use std::{
    fmt,
    hash::Hash,
    ops::Deref,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::oneshot;

use crate::{
    action_error, action_info,
    changes::EntityChange,
    collectionset::CollectionSet,
    connector::{PeerSender, SendError},
    context::Context,
    entity::{Entity, WeakEntitySet},
    error::{MutationError, RequestError, RetrievalError},
    notice_info,
    peer_subscription::{SubscriptionHandler, SubscriptionRelay},
    policy::{AccessDenied, PolicyAgent},
    reactor::{AbstractEntity, Reactor},
    retrieval::{CachedEventGetter, LocalEventGetter, LocalStateGetter},
    schema::catalog::CatalogManager,
    storage::StorageEngine,
    system::SystemManager,
    util::request_fence::{RequestLease, RequestValidity},
    util::{safemap::SafeMap, safeset::SafeSet, Iterable},
};
use itertools::Itertools;
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, error, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    _durable: bool,
    subscription_handler: SubscriptionHandler,
    pending_requests: SafeMap<proto::RequestId, PendingRequest>,
    pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    /// Model-definition ids whose catalog defs were already shipped on this
    /// connection (#330 once-per-connection descriptor shipping). A
    /// reconnection builds a fresh `PeerState`, so the peer is re-announced.
    announced_models: std::sync::Mutex<std::collections::BTreeSet<proto::EntityId>>,
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
    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
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
    pub id: proto::EntityId,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    pub(crate) entities: WeakEntitySet,
    peer_connections: SafeMap<proto::EntityId, Arc<PeerState>>,
    durable_peers: SafeSet<proto::EntityId>,

    /// Per-node source of randomness for peer selection. Seeded from entropy in production;
    /// an explicit seed can be injected at construction so the simulation harness and tests
    /// can reproduce an identical selection sequence. Held behind a Mutex because draws mutate
    /// RNG state and Node is shared across tasks; the lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,

    pub(crate) predicate_context: SafeMap<proto::QueryId, PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Reactor,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE, PA>,

    /// The metadata catalog map (RFC section 5.2 in specs/model-property-metadata/rfc.md). Warmed from storage on
    /// durable nodes and via the subscription relay on ephemeral nodes.
    pub catalog: CatalogManager<SE, PA>,

    pub(crate) subscription_relay: Option<SubscriptionRelay<PA::ContextData, crate::livequery::WeakEntityLiveQuery>>,

    /// Node-held staging, one area per collection (the D1 2.8 substrate):
    /// staged-but-unapplied events survive across applier calls here, which
    /// is what makes NeedsState/NeedsEvents buffering and descendant
    /// re-drive real. The commit lanes keep per-call areas (Atomic mode
    /// retains nothing); the PerItem ingest lanes draw from this map.
    /// Boundary consequence: re-drive is a node-held-area property, so a
    /// parent arriving through a COMMIT lane does not drain a buffered
    /// PerItem orphan; it integrates on that entity's next PerItem or
    /// state-adoption touch, or after cap eviction and redelivery.
    /// Tightening that (a post-commit re-plan against this map) is D3
    /// lifecycle territory.
    pub(crate) staging: SafeMap<CollectionId, Arc<crate::ingest::StagingArea>>,

    /// Events admitted WITHOUT generation verification (the adopted-history
    /// lanes, D2-3): bounded, in-memory, ids only. Consumed by the M5
    /// eligibility rule (an unverified generation never feeds an
    /// acceleration); loss on restart or eviction degrades to
    /// default-eligible, which the suppress-only discipline keeps safe.
    pub(crate) unverified_events: crate::ingest::UnverifiedEvents,

    /// Type resolver for AST preparation (temporary heuristic until Phase 3 schema)
    pub(crate) type_resolver: crate::TypeResolver,
}

/// One entity's planned, policy-checked commit group: the phase-one output
/// of the Atomic commit lanes (M6 remote, M7 local), consumed by their
/// phase two. The staging area holds the group's events, check_event
/// attestations attached; nothing durable has happened yet.
pub(crate) struct PlannedEntityGroup {
    pub(crate) entity: Entity,
    pub(crate) staging: Arc<crate::ingest::StagingArea>,
    pub(crate) getter: LocalEventGetter,
    pub(crate) collection: crate::storage::StorageCollectionWrapper,
    pub(crate) plan: crate::ingest::IngestPlan,
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

    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self { Self::build(engine, policy_agent, false, SmallRng::from_entropy()) }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self { Self::build(engine, policy_agent, true, SmallRng::from_entropy()) }

    /// Construct an ephemeral node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new`].
    pub fn new_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, false, SmallRng::seed_from_u64(rng_seed))
    }

    /// Construct a durable node whose peer-selection RNG is seeded explicitly.
    /// Intended for the simulation harness and deterministic tests; production paths use [`Node::new_durable`].
    pub fn new_durable_with_seed(engine: Arc<SE>, policy_agent: PA, rng_seed: u64) -> Self {
        Self::build(engine, policy_agent, true, SmallRng::seed_from_u64(rng_seed))
    }

    fn build(engine: Arc<SE>, policy_agent: PA, durable: bool, rng: SmallRng) -> Self {
        let collections = CollectionSet::new(engine);
        let entityset: WeakEntitySet = Default::default();
        let id = proto::EntityId::new();
        let reactor = Reactor::new();
        notice_info!("Node {id:#} created as {}", if durable { "durable" } else { "ephemeral" });

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), durable);
        let catalog_manager = CatalogManager::new(collections.clone(), entityset.clone(), reactor.clone(), durable);

        // Only ephemeral nodes relay subscriptions upstream to a durable peer.
        let subscription_relay = if durable { None } else { Some(SubscriptionRelay::new()) };

        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            rng: Mutex::new(rng),
            reactor,
            durable,
            policy_agent,
            system: system_manager,
            catalog: catalog_manager,
            predicate_context: SafeMap::new(),
            subscription_relay,
            staging: SafeMap::new(),
            unverified_events: crate::ingest::UnverifiedEvents::default(),
            type_resolver: crate::TypeResolver::new(),
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
        node.policy_agent.on_node_ready(node.weak());

        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }

    /// Register a peer connection after its Presence handshake.
    ///
    /// Refuses (without registering anything) when the peer's protocol
    /// version is incompatible; the connector should relay the returned
    /// rejection best-effort and close the connection. Enforced here, not
    /// in connectors, so every transport inherits it.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.node_id.to_base64_short(), durable = %presence.durable)))]
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) -> Result<(), proto::PresenceRejection> {
        action_info!(self, "register_peer", "{}", &presence);

        if !proto::protocol_compatible(presence.protocol_version) {
            let rejection = proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: presence.protocol_version };
            warn!("Node({}) refusing peer {}: {}", self.id, presence.node_id, rejection);
            return Err(rejection);
        }

        let subscription_handler = SubscriptionHandler::new(presence.node_id, self);
        self.peer_connections.insert(
            presence.node_id,
            Arc::new(PeerState {
                sender,
                _durable: presence.durable,
                subscription_handler,
                pending_requests: SafeMap::new(),
                pending_updates: SafeMap::new(),
                announced_models: std::sync::Mutex::new(std::collections::BTreeSet::new()),
            }),
        );
        if presence.durable {
            self.durable_peers.insert(presence.node_id);

            // Notify subscription relay of new durable peer connection
            if let Some(ref relay) = self.subscription_relay {
                relay.notify_peer_connected(presence.node_id);
            }

            if !self.durable {
                if let Some(system_root) = presence.system_root {
                    action_info!(self, "received system root", "{}", &system_root.payload);
                    let me = self.clone();
                    crate::task::spawn(async move {
                        if let Err(e) = me.system.join_system(system_root).await {
                            action_error!(me, "failed to join system", "{}", &e);
                        } else {
                            action_info!(me, "successfully joined system");
                        }
                    });
                } else {
                    error!("Node({}) durable peer {} has no system root", self.id, presence.node_id);
                }
            }
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
        Ok(())
    }
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %node_id.to_base64_short())))]
    pub fn deregister_peer(&self, node_id: proto::EntityId) {
        notice_info!("Node({:#}) deregister_peer {:#}", self.id, node_id);

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
    /// Catalog definition states for the `models` not yet announced on
    /// `connection` (#330 once-per-connection descriptor shipping). Well-known
    /// system/catalog ids need no defs (they are static). The defs are the
    /// stored, attested catalog entities themselves -- model, memberships,
    /// properties -- so the receiver validates and ingests them exactly like
    /// any served state. Marks the models announced up front; a lost message
    /// costs one reconnection's worth of re-announcement, never a wrong def.
    async fn schema_states_for_models(
        &self,
        connection: &PeerState,
        models: std::collections::BTreeSet<proto::EntityId>,
    ) -> Vec<proto::Attested<proto::EntityState>> {
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
        let (states, failed) = self.catalog.definition_states_for_models(&fresh).await;
        if !failed.is_empty() {
            let mut announced = connection.announced_models.lock().unwrap();
            for model in failed {
                announced.remove(&model);
            }
        }
        states
    }

    /// Receiver side of descriptor shipping (#330): policy-validate and ingest
    /// catalog defs attached to a message envelope BEFORE its body is
    /// processed, so model-id resolution and property naming see a warm map.
    fn ingest_schema(&self, from: &proto::EntityId, schema: &[proto::Attested<proto::EntityState>]) {
        if schema.is_empty() {
            return;
        }
        let accepted: Vec<proto::Attested<proto::EntityState>> = schema
            .iter()
            .filter(|state| match self.policy_agent.validate_received_state(self, from, state) {
                Ok(()) => true,
                Err(e) => {
                    warn!("Node({}) rejecting shipped schema def from {}: {}", self.id, from.to_base64_short(), e);
                    false
                }
            })
            .cloned()
            .collect();
        self.catalog.ingest_wire_states(&accepted);
    }

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id, request_body = %request_body)))]
    pub async fn request<'a, C>(
        &self,
        node_id: proto::EntityId,
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
        node_id: proto::EntityId,
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
        node_id: proto::EntityId,
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

        // Get the peer connection
        let connection = self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?;

        connection.pending_requests.insert(request_id, PendingRequest { response: response_tx, validity });
        connection.send_message(proto::NodeMessage::Request { auth, request })?;

        // Wait for response
        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    // TODO LATER: rework this to be retried in the background some number of times
    pub fn send_update(&self, node_id: proto::EntityId, notification: proto::NodeUpdateBody) {
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

        // Descriptor shipping (#330) needs async collection of the catalog
        // states, and this fn is called from sync reactor callbacks, so the
        // send moves into a task. Update order across tasks is not
        // guaranteed, which the receiver already tolerates: wire order is
        // untrusted (events topo-sort, snapshots compare heads).
        let node = self.clone();
        crate::task::spawn(async move {
            let schema = node.schema_states_for_models(&connection, notification.referenced_models()).await;
            let message = proto::NodeMessage::Update(proto::NodeUpdate { id, from: node.id, to: node_id, body: notification, schema });
            if let Err(e) = connection.send_message(message) {
                warn!("Failed to send update to peer {}: {}", node_id, e);
            }
        });
    }

    // TODO add a node id argument to this function rather than getting it from the message
    // (does this actually make it more secure? or just move the place they could lie to us to the handshake?)
    // Not if its signed by a node key.
    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
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

                    // A streamed item is admissible only while at least one
                    // query that caused it is still current on this peer. For
                    // reset-sensitive catalog queries, retain the owner fence
                    // lease through schema ingestion, persistence, and the
                    // acknowledgement so hard reset can quiesce the whole
                    // mutation boundary before deleting storage.
                    let reject_stream = || {
                        sender.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck {
                            id: id.clone(),
                            from,
                            to,
                            body: proto::NodeUpdateAckBody::Error("subscription update has no current source query".to_string()),
                        }))
                    };
                    let _stream_leases: Vec<RequestLease> = match (&self.subscription_relay, &update.body) {
                        (Some(relay), proto::NodeUpdateBody::SubscriptionUpdate { items }) => {
                            let Some(validities) = relay.validities_for_stream_update(&update.from, items) else {
                                debug!(
                                    "Node({}) discarded stale subscription update {} from {} before schema ingestion",
                                    self.id, update.id, update.from
                                );
                                reject_stream()?;
                                return Ok(());
                            };
                            let Some(leases) = validities.into_iter().map(|validity| validity.try_acquire()).collect() else {
                                debug!(
                                    "Node({}) discarded invalidated subscription update {} from {} before schema ingestion",
                                    self.id, update.id, update.from
                                );
                                reject_stream()?;
                                return Ok(());
                            };
                            leases
                        }
                        (None, proto::NodeUpdateBody::SubscriptionUpdate { .. }) => {
                            reject_stream()?;
                            return Ok(());
                        }
                    };

                    // Descriptor shipping (#330): warm the catalog map from the
                    // attached defs BEFORE applying the body, so model routing
                    // and property naming resolve. Ingest only AFTER the
                    // connection + recipient checks: a misaddressed or
                    // unsolicited envelope must not mutate our catalog map.
                    self.ingest_schema(&update.from, &update.schema);

                    // TODO - validate the from node id is the one we're connected to
                    let body = match self.handle_update(update).await {
                        Ok(_) => proto::NodeUpdateAckBody::Success,
                        Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                    };

                    sender.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
                }
            }
            proto::NodeMessage::UpdateAck(ack) => {
                debug!("Node({}) received ack notification {} {}", self.id, ack.id, ack.body);
                // Drain the pending entry so the map does not leak (the send
                // side no longer blocks on it).
                if let Some(connection) = self.peer_connections.get(&ack.from) {
                    connection.pending_updates.remove(&ack.id);
                }
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

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(connection) = { self.peer_connections.get(&request.from) } {
                    let sender = connection.sender.cloned();
                    let from = request.from;
                    let request_id = request.id.clone();
                    if request.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                        return Ok(());
                    }

                    // Validate the request auth first, converting errors to error responses
                    let body = match self.policy_agent.check_request(self, &auth, &request).await {
                        Ok(cdata) => match self.handle_request(&cdata, request).await {
                            Ok(result) => result,
                            Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                        },
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    // Descriptor shipping (#330): catalog defs for any model id
                    // this response references that the connection has not seen.
                    let schema = self.schema_states_for_models(&connection, body.referenced_models()).await;
                    let _result = sender.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                        request_id,
                        from: self.id,
                        to: from,
                        body,
                        schema,
                    }));
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                let connection = self.peer_connections.get(&response.from).ok_or(RequestError::PeerNotConnected)?;
                // Descriptor shipping (#330): ingest attached defs before the
                // requester consumes the body -- but only for a response that
                // matches a request we actually sent, so an unsolicited or
                // misattributed response cannot poison the catalog map.
                if let Some(pending) = connection.pending_requests.remove(&response.request_id) {
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
                    self.ingest_schema(&response.from, &response.schema);
                    pending
                        .response
                        .send(Ok(GuardedResponse { body: response.body, lease }))
                        .map_err(|_| anyhow!("Failed to send guarded response"))?;
                }
            }
            proto::NodeMessage::UnsubscribeQuery { from, query_id } => {
                // Remove predicate from the peer's subscription
                if let Some(peer_state) = self.peer_connections.get(&from) {
                    peer_state.subscription_handler.remove_predicate(query_id)?;
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(request = %request)))]
    async fn handle_request<C>(&self, cdata: &C, request: proto::NodeRequest) -> anyhow::Result<proto::NodeResponseBody>
    where C: Iterable<PA::ContextData> {
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
                match self.commit_remote_transaction(cdata, id.clone(), events).await {
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

                Ok(proto::NodeResponseBody::GetEvents(events))
            }
            proto::NodeRequestBody::RegisterSchema { models, properties, memberships } => {
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for RegisterSchema"))?;
                match self.execute_schema_registration(cdata, models, properties, memberships).await {
                    // The resolved definitions ARE the response (RFC 5.2):
                    // the requester folds them into its catalog map on ack.
                    Ok((models, properties, memberships)) => {
                        Ok(proto::NodeResponseBody::SchemaRegistered { models, properties, memberships })
                    }
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::SubscribeQuery { query_id, collection, selection, version, known_matches } => {
                let peer_state = self.peer_connections.get(&request.from).ok_or_else(|| anyhow!("Peer {} not connected", request.from))?;
                // only one cdata is permitted for SubscribePredicate
                use itertools::Itertools;
                let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for SubscribePredicate"))?;
                peer_state.subscription_handler.subscribe_query(self, query_id, collection, selection, cdata, version, known_matches).await
            }
        }
    }

    async fn handle_update(&self, notification: proto::NodeUpdate) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::NodeUpdateBody::SubscriptionUpdate { items } => {
                tracing::debug!("Node({}) received subscription update from peer {}", self.id, notification.from);
                crate::node_applier::NodeApplier::apply_updates(self, &notification.from, items).await?;
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

    /// Phase one of the Atomic commit lanes (M6 remote, M7 local): stage one
    /// entity's events into per-call staging, plan them, typed-fail anything
    /// the plan cannot resolve, and preview every scheduled event on a fork
    /// for the policy check. Nothing durable happens here; the caller owns
    /// phase two and failure cleanup.
    pub(crate) async fn plan_and_check_entity_group(
        &self,
        cdata: &PA::ContextData,
        entity: &Entity,
        group: &[Attested<proto::Event>],
    ) -> Result<PlannedEntityGroup, MutationError> {
        let collection = self.collections.get(entity.collection()).await?;
        let staging = Arc::new(crate::ingest::StagingArea::with_default_cap());
        // Lineage lookups stay local-plus-staged on the commit lanes, as
        // before.
        let getter = LocalEventGetter::with_staging(collection.clone(), self.durable, staging.clone());

        for attested in group {
            staging.stage(attested.clone());
        }

        let batch: Vec<proto::EventId> = group.iter().map(|e| e.payload.id()).collect();
        let plan = crate::ingest::plan_entity(&entity.head(), &batch, &staging, &getter).await?;

        // NeedsState on the commit lanes fails typed, fast, and atomically;
        // the sender's retry recovers once state arrives through the
        // normal lanes. The plan originally wired an inline Get
        // fetch here (2.4); amended by the maintainer 2026-07-06: a
        // nested round-trip inside a request handler deadlocks the
        // sim's inline delivery, amplifies requests on the server
        // (unknown-entity commits would fan out Gets while holding
        // the inbound request open), and is futile on the common
        // single-durable topology, where the receiving node has no
        // durable peer to ask. Active deep-gap recovery is
        // revisited with the attested-comparison work (D5,
        // #199/#323). The local lane cannot reach these outcomes (a
        // transaction's parents are its own resident's lineage); the
        // guards are shared defense.
        for (eid, outcome) in &plan.preresolved {
            match outcome {
                crate::ingest::IngestOutcome::NeedsState { .. } => {
                    return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::NonCreationOverEmptyHead).into());
                }
                crate::ingest::IngestOutcome::NeedsEvents { missing } => {
                    // Atomic mode cannot partially apply: a missing
                    // parent fails the whole transaction before
                    // anything commits. Retryable retrieval truth,
                    // same surface as the PerItem arms (M5).
                    let missing = missing.first().cloned().unwrap_or_else(|| eid.clone());
                    return Err(RetrievalError::EventNotFound(missing).into());
                }
                _ => {}
            }
        }

        // Fork-policy phase over the planned schedule: every event
        // previews on a fork (before and after states), the real
        // entity untouched until phase two. check_event attestations
        // are attached by REPLACING the staged envelope (restage;
        // plain stage is an idempotent no-op for an already-staged id,
        // which silently discarded these attestations before the codex
        // review caught it).
        use std::sync::atomic::AtomicBool;
        let fork = entity.snapshot(Arc::new(AtomicBool::new(true)));
        for event_id in &plan.schedule {
            let Some(attested) = staging.get_attested(event_id) else { continue };
            let fork_before = fork.snapshot(Arc::new(AtomicBool::new(true)));
            fork.apply_event(&getter, &attested.payload).await.map_err(crate::ingest::type_comparison_error)?;
            match self.policy_agent.check_event(self, cdata, &fork_before, &fork, &attested.payload) {
                Ok(Some(attestation)) => {
                    let mut updated = attested.clone();
                    updated.attestations.push(attestation);
                    staging.restage(updated);
                }
                Ok(None) => {}
                Err(denied) => {
                    return Err(crate::error::IngestError::PolicyDenied(denied).into());
                }
            }
        }

        Ok(PlannedEntityGroup { entity: entity.clone(), staging, getter, collection, plan })
    }

    /// Does all the things necessary to commit a remote transaction
    pub async fn commit_remote_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        debug!("{self} commiting transaction {id} with {} events", events.len());

        // Group by entity, preserving first-appearance order. Cross-entity
        // order is not semantic (per-entity order comes from parent edges);
        // first-appearance keeps the walk deterministic. The envelope
        // carries a MODEL id (INGRESS, #330); phase one resolves it to the
        // local collection per group.
        let mut groups: Vec<(proto::EntityId, proto::EntityId, Vec<Attested<proto::Event>>)> = Vec::new();
        for event in events {
            match groups.iter_mut().find(|(eid, _, _)| *eid == event.payload.entity_id) {
                Some((_, _, list)) => list.push(event),
                None => groups.push((event.payload.entity_id, event.payload.model, vec![event])),
            }
        }

        // Atomic mode, phase one (D1 M6): stage, plan, and run the
        // fork-policy phase for EVERY event, creations included (closing
        // the C4-19 asymmetry), BEFORE anything commits. Any denial or
        // unappliable event means nothing durable. Speculative residents
        // materialized along the way are evicted on failure.
        let mut touched: Vec<proto::EntityId> = Vec::new();
        let mut ready: Vec<PlannedEntityGroup> = Vec::new();
        let phase_one = async {
            for (entity_id, model, group) in &groups {
                // INGRESS (#330): the envelope carries a model id; resolve
                // it to the local collection (well-knowns, then catalog) or
                // reject the transaction.
                let collection_id = self.resolve_model_wait(model).await?;
                let collection = self.collections.get(&collection_id).await?;
                let event_getter = LocalEventGetter::new(collection.clone(), self.durable);
                let state_getter = LocalStateGetter::new(collection);

                let entity = self.entities.get_retrieve_or_create(&state_getter, &event_getter, &collection_id, entity_id).await?;
                touched.push(*entity_id);

                ready.push(self.plan_and_check_entity_group(cdata, &entity, group).await?);
            }
            Ok::<(), MutationError>(())
        }
        .await;

        if let Err(e) = phase_one {
            // Nothing durable; a speculative empty-head resident must not
            // survive the denial (C4-12).
            for entity_id in &touched {
                self.entities.remove_if_phantom(entity_id);
            }
            return Err(e);
        }

        // Phase two: execute. Policy has passed for every event; failures
        // from here are storage-class, reported per event, not rolled back
        // (same as local commit today). The applied prefix is persisted and
        // notified.
        let mut changes = Vec::new();
        let mut failure: Option<MutationError> = None;
        for PlannedEntityGroup { entity, staging, getter, collection, plan } in ready {
            let persist = crate::node_applier::NodePersist { node: self, collection: &collection };
            let outcome =
                crate::ingest::execute_plan(plan, &entity, &self.entities, &staging, &getter, &persist, &self.unverified_events).await;
            // One change per entity carrying its applied events in
            // application order. The old per-event shape was an artifact of
            // building each change mid-loop while the head sat at that
            // event; built after execution, an ancestor is only
            // constructible inside a multi-event batch (EntityChange's own
            // containment rule), which is the sanctioned shape bridges and
            // multi-event subscription items already use (V4).
            if !outcome.applied.is_empty() {
                changes.push(EntityChange::new(entity.clone(), outcome.applied.clone())?);
            }
            if let Some(e) = outcome.failure {
                failure = Some(e);
                break;
            }
            for (eid, o) in &outcome.outcomes {
                if let crate::ingest::IngestOutcome::NeedsEvents { missing } = o {
                    failure = Some(RetrievalError::EventNotFound(missing.first().cloned().unwrap_or_else(|| eid.clone())).into());
                    break;
                }
            }
            if failure.is_some() {
                break;
            }
        }

        self.reactor.notify_change(changes).await;

        if let Some(e) = failure {
            return Err(e);
        }
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

    pub fn next_entity_id(&self) -> proto::EntityId { proto::EntityId::new() }

    /// The currently-resident (in-memory) entity for `id`, if one is held.
    ///
    /// The resident entity carries the authoritative materialized state. The
    /// ingest executor persists the state buffer after an entity's events on
    /// every arm (uniform state persistence, D1), so buffer and resident rest
    /// at the same head once an apply call returns. The resident view still
    /// matters to a reader that can interleave with an in-flight apply (the
    /// buffer write lands after the events), and after a crash between
    /// commit_event and save_state, the one window where the buffer
    /// legitimately rests behind the event log until the entity's next write
    /// (rehydration-time repair of that window is D5's contract). Returns
    /// `None` if no strong reference keeps the entity resident, in which case
    /// the caller falls back to the persisted state.
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

    /// True when an error bottoms out in EventNotFound through any nesting
    /// of the mutual MutationError/RetrievalError boxing: the lineage
    /// between a fetched state and the local head is unobtainable here.
    /// This lane keeps hand-unwrapping the mutual boxing until deep-gap
    /// recovery is typed with the attested-comparison work (D5, #199/#323).
    fn lineage_unobtainable(e: &MutationError) -> Option<proto::EventId> {
        match e {
            MutationError::RetrievalError(RetrievalError::EventNotFound(id)) => Some(id.clone()),
            MutationError::RetrievalError(RetrievalError::MutationError(inner)) => Self::lineage_unobtainable(inner),
            _ => None,
        }
    }

    /// Fetch entities by id from a durable peer and integrate the returned
    /// states through the shared state-apply.
    ///
    /// Contract (D1 plan 2.9): this must never be called from reactor
    /// evaluation paths. It notifies the reactor after apply returns, and
    /// reactor evaluation holds a non-reentrant lock; a call from inside
    /// evaluation would deadlock. The reactor's own gap-fill uses the Fetch
    /// lane, not this one.
    pub(crate) async fn get_from_peer(
        &self,
        collection_id: &CollectionId,
        ids: Vec<proto::EntityId>,
        cdata: &PA::ContextData,
    ) -> Result<(), RetrievalError> {
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(peer_id, cdata, proto::NodeRequestBody::Get { collection: collection_id.clone(), ids })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Get(states) => {
                let collection = self.collections.get(collection_id).await?;

                // Entity-mediated adoption (D1 M4, replacing the raw
                // set_state this lane carried next to its TODO): with_state
                // comparison decides what each fetched state may do, so a
                // stale or divergent fetch cannot clobber a newer local
                // state, persistence is advance-gated, and matching
                // established queries hear about fetched entities.
                let state_getter = LocalStateGetter::new(collection.clone());
                // The node-held staging area: a fetched state that advances
                // the head may be exactly what a buffered orphan was waiting
                // for, and the feed's re-drive drains it from here.
                let staging = self.staging_for(collection_id);
                let event_getter = CachedEventGetter::with_staging(collection_id.clone(), collection.clone(), self, cdata, staging.clone());
                let persist = crate::node_applier::NodePersist { node: self, collection: &collection };
                // PerItem containment (plan 2.7): one bad state must not
                // abort the batch, and entities that already adopted and
                // persisted must still notify, or an established matching
                // query silently misses an entity this node now holds. The
                // first failure is remembered and returned AFTER the
                // applied subset notifies.
                let mut changes = Vec::new();
                let mut first_failure: Option<RetrievalError> = None;
                for state in states {
                    let result = async {
                        self.policy_agent.validate_received_state(self, &peer_id, &state)?;
                        crate::ingest::apply_state_feed(
                            &self.entities,
                            &state_getter,
                            &event_getter,
                            &staging,
                            state.payload.entity_id,
                            state.payload.collection.clone(),
                            state.payload.state.clone(),
                            &[],
                            &persist,
                            &self.unverified_events,
                        )
                        .await
                    }
                    .await;
                    match result {
                        Ok(applied) => {
                            if applied.advanced {
                                match EntityChange::new(applied.entity, Vec::new()) {
                                    Ok(change) => changes.push(change),
                                    Err(e) => {
                                        first_failure.get_or_insert(RetrievalError::Other(format!("{:?}", e)));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(missing) = Self::lineage_unobtainable(&e) {
                                // The fetched state's lineage cannot be
                                // verified from here: the events between it
                                // and the local head are unobtainable
                                // (possibly read-denied upstream).
                                // Unverifiable state is never adopted;
                                // whatever local state exists keeps serving,
                                // stale but honest. Deep-gap recovery
                                // without event bodies is the
                                // attested-comparison work (D5, #199/#323).
                                tracing::warn!(
                                    "get_from_peer: not adopting unverifiable state for {} (missing event {})",
                                    state.payload.entity_id,
                                    missing
                                );
                            } else {
                                first_failure.get_or_insert(RetrievalError::Other(format!("{:?}", e)));
                            }
                        }
                    }
                }
                if !changes.is_empty() {
                    self.reactor.notify_change(changes).await;
                }
                match first_failure {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
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
    pub fn get_durable_peer_random(&self) -> Option<proto::EntityId> {
        let peers = self.get_durable_peers();
        let mut rng = self.rng.lock().expect("node rng mutex poisoned");
        peers.choose(&mut *rng).copied()
    }

    /// Get all durable peer node IDs, sorted by id for a stable fan-out order.
    /// The underlying set is unordered; callers that emit per peer (relay, random selection)
    /// depend on this stable order for the C1 determinism audit.
    pub fn get_durable_peers(&self) -> Vec<proto::EntityId> {
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
    pub fn insert_durable_peer_for_test(&self, peer_id: proto::EntityId) { self.durable_peers.insert(peer_id); }

    /// The node-held staging area for one collection, created on first use.
    /// Staged-but-unapplied events survive across applier calls here; see
    /// the `staging` field.
    pub(crate) fn staging_for(&self, collection: &CollectionId) -> Arc<crate::ingest::StagingArea> {
        self.staging.get_or_default(collection.clone())
    }

    /// (len, evictions, cap) of one collection's node-held staging area.
    /// R6/R8 observability: buffering and cap eviction must be visible from
    /// outside the crate.
    #[cfg(feature = "test-helpers")]
    pub fn staging_probe_for_test(&self, collection: &CollectionId) -> (usize, u64, usize) {
        let area = self.staging_for(collection);
        (area.len(), area.evictions(), area.cap())
    }

    /// Whether one event is buffered in the collection's node-held area.
    #[cfg(feature = "test-helpers")]
    pub fn staging_contains_for_test(&self, collection: &CollectionId, id: &proto::EventId) -> bool {
        self.staging_for(collection).contains(id)
    }
}

impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub async fn request_remote_unsubscribe(&self, query_id: proto::QueryId, peers: Vec<proto::EntityId>) -> anyhow::Result<()> {
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
        peer: proto::EntityId,
        sent: Arc<Mutex<Vec<proto::NodeMessage>>>,
    }

    impl PeerSender for CapturingSender {
        fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
            self.sent.lock().unwrap().push(message);
            Ok(())
        }

        fn recipient_node_id(&self) -> proto::EntityId { self.peer }

        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }

    fn forged_model_state(collection: &str) -> Attested<EntityState> {
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Name("collection".to_owned()), Some(Value::String(collection.to_owned())));
        backend.set(PropertyKey::Name("name".to_owned()), Some(Value::String("Stale model".to_owned())));
        let operations = backend.to_operations().unwrap().expect("catalog state has fields");
        let event_id = proto::EventId::from_bytes([0x5A; 32]);
        backend.apply_operations_with_event(&operations, event_id.clone()).unwrap();
        Attested::opt(
            EntityState {
                entity_id: proto::EntityId::new(),
                model: crate::schema::well_known_model_id(crate::schema::MODEL_COLLECTION_ID).unwrap(),
                state: proto::State {
                    state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                    head: proto::Clock::from(vec![event_id]),
                },
            },
            None,
        )
    }

    #[tokio::test]
    async fn invalid_request_response_is_dropped_before_schema_ingestion() {
        let node = Node::new(Arc::new(EmptyEngine), PermissiveAgent::new());
        let peer = proto::EntityId::new();
        let sent = Arc::new(Mutex::new(Vec::new()));
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
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
