use crate::selection::filter::Filterable;
use ankurah_proto::{self as proto, Attested, CollectionId, EntityState};
use anyhow::anyhow;

use rand::prelude::*;
use std::{
    fmt,
    hash::Hash,
    ops::Deref,
    sync::{Arc, Weak},
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
    retrieval::LocalRetriever,
    storage::StorageEngine,
    system::SystemManager,
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
    pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
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

    pub(crate) predicate_context: SafeMap<proto::QueryId, PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Reactor,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE, PA>,

    pub(crate) subscription_relay: Option<SubscriptionRelay<PA::ContextData, crate::livequery::WeakEntityLiveQuery>>,
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine.clone());
        let entityset: WeakEntitySet = Default::default();
        let id = proto::EntityId::new();
        let reactor = Reactor::new();
        notice_info!("Node {id:#} created as ephemeral");

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), false);

        // Create subscription relay for ephemeral nodes
        let subscription_relay = Some(SubscriptionRelay::new());

        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            reactor,
            durable: false,
            policy_agent,
            system: system_manager,
            predicate_context: SafeMap::new(),
            subscription_relay,
        }));

        // Set up the message sender for the subscription relay
        if let Some(ref relay) = node.subscription_relay {
            let weak_node = node.weak();
            if relay.set_node(Arc::new(weak_node)).is_err() {
                warn!("Failed to set message sender for subscription relay");
            }
        }

        node
    }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine);
        let entityset: WeakEntitySet = Default::default();
        let id = proto::EntityId::new();
        let reactor = Reactor::new();
        notice_info!("Node {id:#} created as durable");

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), true);

        Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            reactor,
            durable: true,
            policy_agent,
            system: system_manager,
            predicate_context: SafeMap::new(),
            subscription_relay: None,
        }))
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.node_id.to_base64_short(), durable = %presence.durable)))]
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        action_info!(self, "register_peer", "{}", &presence);

        let subscription_handler = SubscriptionHandler::new(presence.node_id, self);
        self.peer_connections.insert(
            presence.node_id,
            Arc::new(PeerState {
                sender,
                _durable: presence.durable,
                subscription_handler,
                pending_requests: SafeMap::new(),
                pending_updates: SafeMap::new(),
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
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        let request = proto::NodeRequest { id: request_id.clone(), to: node_id, from: self.id, body: request_body };
        let auth = self.policy_agent.sign_request(self, cdata, &request)?;

        // Get the peer connection
        let connection = self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?;

        connection.pending_requests.insert(request_id, response_tx);
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

        let notification = proto::NodeMessage::Update(proto::NodeUpdate { id, from: self.id, to: node_id, body: notification });

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
                    let body = match self.handle_update(update).await {
                        Ok(_) => proto::NodeUpdateAckBody::Success,
                        Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
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

                let cdata = self.policy_agent.check_request(self, &auth, &request).await?;

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = { self.peer_connections.get(&request.from).map(|c| c.sender.cloned()) } {
                    let from = request.from;
                    let request_id = request.id.clone();
                    if request.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                        return Ok(());
                    }

                    let body = match self.handle_request(&cdata, request).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let _result = sender.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                        request_id,
                        from: self.id,
                        to: from,
                        body,
                    }));
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                let connection = self.peer_connections.get(&response.from).ok_or(RequestError::PeerNotConnected)?;
                if let Some(tx) = connection.pending_requests.remove(&response.request_id) {
                    tx.send(Ok(response.body)).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
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
                    if self.policy_agent.check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state).is_err() {
                        continue;
                    }

                    // Generate delta based on known_matches (returns None if heads are equal)
                    // No need to reconstruct Entity - work directly with EntityState
                    if let Some(delta) = self.generate_entity_delta(&known_map, state, &storage_collection).await? {
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
                    match self.policy_agent.check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state) {
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
                    match self.policy_agent.check_read_event(cdata, &event) {
                        Ok(_) => events.push(event),
                        Err(AccessDenied::ByPolicy(_)) => {}
                        // TODO: we need to have a cleaner delineation between actual access denied versus processing errors
                        Err(e) => return Err(anyhow!("Error from peer subscription: {}", e)),
                    }
                }

                Ok(proto::NodeResponseBody::GetEvents(events))
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

    /// Does all the things necessary to commit a remote transaction
    pub async fn commit_remote_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        debug!("{self} commiting transaction {id} with {} events", events.len());
        let mut changes = Vec::new();

        for event in events.iter_mut() {
            let collection = self.collections.get(&event.payload.collection).await?;

            // When applying an event, we should only look at the local storage for the lineage
            let retriever = LocalRetriever::new(collection.clone());
            let entity = self.entities.get_retrieve_or_create(&retriever, &event.payload.collection, &event.payload.entity_id).await?;

            // Handle creates vs updates differently for policy validation
            let (entity_before, entity_after, already_applied) = if event.payload.is_entity_create() && entity.head().is_empty() {
                // Create: apply to entity directly, use as both before/after
                entity.apply_event(&retriever, &event.payload).await?;
                (entity.clone(), entity.clone(), true)
            } else {
                // Update: snapshot, apply to fork for validation
                use std::sync::atomic::AtomicBool;
                let trx_alive = Arc::new(AtomicBool::new(true));
                let forked = entity.snapshot(trx_alive);
                forked.apply_event(&retriever, &event.payload).await?;
                (entity.clone(), forked, false)
            };

            // Check policy with before/after states
            if let Some(attestation) = self.policy_agent.check_event(self, cdata, &entity_before, &entity_after, &event.payload)? {
                event.attestations.push(attestation);
            }

            // For updates only: apply event to real entity (creates already applied above)
            let applied = if already_applied { true } else { entity.apply_event(&retriever, &event.payload).await? };

            if applied {
                let state = entity.to_state()?;
                let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                let attestation = self.policy_agent.attest_state(self, &entity_state);
                let attested = Attested::opt(entity_state, attestation);
                collection.add_event(event).await?;
                collection.set_state(attested).await?;
                changes.push(EntityChange::new(entity.clone(), vec![event.clone()])?);
            }
        }

        self.reactor.notify_change(changes).await;

        Ok(())
    }

    /// Generate EntityDelta for an entity state, using known_matches to decide between StateSnapshot and EventBridge
    /// Returns None if the entity is in known_matches with equal heads (client already has current state)
    pub(crate) async fn generate_entity_delta(
        &self,
        known_map: &std::collections::HashMap<proto::EntityId, proto::Clock>,
        entity_state: proto::Attested<proto::EntityState>,
        storage_collection: &crate::storage::StorageCollectionWrapper,
    ) -> anyhow::Result<Option<proto::EntityDelta>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Destructure to take ownership and avoid clones
        let proto::Attested { payload: proto::EntityState { entity_id, collection, state }, attestations } = entity_state;
        let current_head = &state.head;

        // Entity is in known_matches - try to optimize the response
        if let Some(known_head) = known_map.get(&entity_id) {
            // Case 1: Heads equal → return None (omit entity, client already has current state) ✓
            if known_head == current_head {
                return Ok(None);
            }

            // Case 2: Heads differ → try to build EventBridge (cheaper than full state) ✓
            match self.collect_event_bridge(storage_collection, known_head, current_head).await {
                Ok(attested_events) if !attested_events.is_empty() => {
                    // Convert Attested<Event> to EventFragments (strips entity_id and collection)
                    let event_fragments: Vec<proto::EventFragment> = attested_events.into_iter().map(|e| e.into()).collect();

                    return Ok(Some(proto::EntityDelta {
                        entity_id,
                        collection,
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
        Ok(Some(proto::EntityDelta { entity_id, collection, content: proto::DeltaContent::StateSnapshot { state: state_fragment } }))
    }

    /// Collect events between known_head and current_head using lineage comparison
    pub(crate) async fn collect_event_bridge(
        &self,
        storage_collection: &crate::storage::StorageCollectionWrapper,
        known_head: &proto::Clock,
        current_head: &proto::Clock,
    ) -> anyhow::Result<Vec<proto::Attested<proto::Event>>>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        use crate::lineage::{EventAccumulator, Ordering};
        use crate::retrieval::LocalRetriever;

        let retriever = LocalRetriever::new(storage_collection.clone());
        let accumulator = EventAccumulator::new(None); // No limit for Phase 1
        let mut comparison = crate::lineage::Comparison::new_with_accumulator(
            &retriever,
            current_head,
            known_head,
            100000, // TODO: make budget configurable
            Some(accumulator),
        );

        // Run comparison
        loop {
            match comparison.step().await? {
                Some(Ordering::Descends) => {
                    // Current descends from known - perfect for event bridge
                    break;
                }
                Some(Ordering::Equal) => {
                    // Heads are equal - no events needed
                    break;
                }
                Some(_) => {
                    // Other relationships (NotDescends, Incomparable, etc.) - can't build bridge
                    return Ok(vec![]);
                }
                None => {
                    // Continue stepping
                }
            }
        }

        // Extract accumulated events
        Ok(comparison.take_accumulated_events().unwrap_or_default())
    }

    pub fn next_entity_id(&self) -> proto::EntityId { proto::EntityId::new() }

    pub fn context(&self, data: PA::ContextData) -> Result<Context, anyhow::Error> {
        if !self.system.is_system_ready() {
            return Err(anyhow!("System is not ready"));
        }
        Ok(Context::new(Node::clone(self), data))
    }

    pub async fn context_async(&self, data: PA::ContextData) -> Context {
        self.system.wait_system_ready().await;
        Context::new(Node::clone(self), data)
    }

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

                // do we have the ability to merge states?
                // because that's what we have to do I think
                for state in states {
                    self.policy_agent.validate_received_state(self, &peer_id, &state)?;
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

    /// Get a random durable peer node ID
    pub fn get_durable_peer_random(&self) -> Option<proto::EntityId> {
        let mut rng = rand::thread_rng();
        // Convert to Vec since DashSet iterator doesn't support random selection
        let peers: Vec<_> = self.durable_peers.to_vec();
        peers.choose(&mut rng).copied()
    }

    /// Get all durable peer node IDs
    pub fn get_durable_peers(&self) -> Vec<proto::EntityId> { self.durable_peers.to_vec() }
}

impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub async fn request_remote_unsubscribe(&self, query_id: proto::QueryId, peers: Vec<proto::EntityId>) -> anyhow::Result<()> {
        for (peer_id, item) in self.peer_connections.get_list(peers) {
            if let Some(connection) = item {
                connection.send_message(proto::NodeMessage::UnsubscribeQuery { from: peer_id, query_id })?;
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
        selection: ankql::ast::Selection,
        cdata: PA::ContextData,
        version: u32,
        livequery: crate::livequery::WeakEntityLiveQuery,
    ) {
        if let Some(ref relay) = self.subscription_relay {
            self.predicate_context.insert(query_id, cdata.clone());
            relay.subscribe_query(query_id, collection_id, selection, cdata, version, livequery);
        }
    }

    pub async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let storage_collection = self.collections.get(collection_id).await?;
        let initial_states = storage_collection.fetch_states(selection).await?;
        let retriever = crate::retrieval::LocalRetriever::new(storage_collection);
        let mut entities = Vec::with_capacity(initial_states.len());
        for state in initial_states {
            let (_, entity) =
                self.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            entities.push(entity);
        }
        Ok(entities)
    }
}
#[async_trait::async_trait]
pub trait TNodeErased<E: AbstractEntity + Filterable + Send + 'static = Entity>: Send + Sync + 'static {
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId);
    fn update_remote_query(&self, query_id: proto::QueryId, selection: ankql::ast::Selection, version: u32) -> Result<(), anyhow::Error>;
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection,
    ) -> Result<Vec<E>, RetrievalError>;
    fn reactor(&self) -> &Reactor<E>;
    fn has_subscription_relay(&self) -> bool;
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

    fn update_remote_query(&self, query_id: proto::QueryId, selection: ankql::ast::Selection, version: u32) -> Result<(), anyhow::Error> {
        if let Some(ref relay) = self.subscription_relay {
            relay.update_query(query_id, selection, version)?;
        }
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

    fn has_subscription_relay(&self) -> bool { self.subscription_relay.is_some() }
}

impl<SE, PA> fmt::Display for Node<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{}\x1b[2m]\x1b[0m", self.id.to_base64_short())
    }
}
