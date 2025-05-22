use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityState};
use anyhow::anyhow;

use async_trait::async_trait;
use rand::prelude::*;
use std::{
    fmt,
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

use crate::{
    action_debug, action_error, action_info, action_warn,
    changes::{ChangeSet, EntityChange, ItemChange},
    collectionset::CollectionSet,
    connector::{PeerSender, SendError},
    context::{Context, NodeAndContext},
    entity::{Entity, WeakEntitySet},
    error::{MutationError, RequestError, RetrievalError},
    notice_info,
    policy::{AccessDenied, PolicyAgent},
    reactor::Reactor,
    storage::StorageEngine,
    subscription::SubscriptionHandle,
    system::SystemManager,
    task::spawn,
    transaction::Transaction,
    util::{safemap::SafeMap, safeset::SafeSet},
};
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, error, info, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    _durable: bool,
    subscriptions: SafeSet<proto::SubscriptionId>,
    pending_requests: SafeMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
    pending_updates: SafeMap<proto::UpdateId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,
}

impl PeerState {
    pub fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> { self.sender.send_message(message) }
}

pub struct MatchArgs {
    pub predicate: ankql::ast::Predicate,
    pub cached: bool,
}

impl TryInto<MatchArgs> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> {
        Ok(MatchArgs { predicate: ankql::parser::parse_selection(self)?, cached: false })
    }
}
impl TryInto<MatchArgs> for String {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs, Self::Error> {
        Ok(MatchArgs { predicate: ankql::parser::parse_selection(&self)?, cached: false })
    }
}

impl From<ankql::ast::Predicate> for MatchArgs {
    fn from(val: ankql::ast::Predicate) -> Self { MatchArgs { predicate: val, cached: false } }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

/// A participant in the Ankurah network, and primary place where queries are initiated

pub struct Node<SE, PA>(Arc<NodeInner<SE, PA>>)
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
#[async_trait]
pub trait ContextData: Send + Sync + Clone + 'static {}

pub struct NodeInner<SE, PA>
where PA: PolicyAgent
{
    pub id: proto::EntityId,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    pub(crate) entities: WeakEntitySet,
    peer_connections: SafeMap<proto::EntityId, Arc<PeerState>>,
    durable_peers: SafeSet<proto::EntityId>,

    subscription_context: SafeMap<proto::SubscriptionId, PA::ContextData>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Arc<Reactor<SE, PA>>,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE, PA>,
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
        let reactor = Reactor::new(collections.clone(), entityset.clone(), policy_agent.clone(), id.clone());
        notice_info!("Node {id:#} created as ephemeral");

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), reactor.clone(), false);

        Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            reactor,
            durable: false,
            policy_agent,
            system: system_manager,
            subscription_context: SafeMap::new(),
        }))
    }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine);
        let entityset: WeakEntitySet = Default::default();
        let id = proto::EntityId::new();
        let reactor = Reactor::new(collections.clone(), entityset.clone(), policy_agent.clone(), id.clone());
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
            subscription_context: SafeMap::new(),
        }))
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(node_id = %presence.node_id.to_base64_short(), durable = %presence.durable)))]
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        action_info!(self, "register_peer", "{}", &presence);

        self.peer_connections.insert(
            presence.node_id,
            Arc::new(PeerState {
                sender,
                _durable: presence.durable,
                subscriptions: SafeSet::new(),
                pending_requests: SafeMap::new(),
                pending_updates: SafeMap::new(),
            }),
        );
        if presence.durable {
            self.durable_peers.insert(presence.node_id);
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
        info!("Node({:#}) deregister_peer {:#}", self.id, node_id);

        // Get and cleanup subscriptions before removing the peer
        if let Some(peer_state) = self.peer_connections.get(&node_id) {
            // Get all subscription IDs
            let subscriptions = peer_state.subscriptions.to_vec();

            // Unsubscribe each one from the reactor
            for sub_id in subscriptions {
                action_info!(self, "unsubscribing", "subscription {} for peer {}", sub_id, node_id);
                self.reactor.unsubscribe(sub_id);
            }
        }

        // Remove the peer connection and durable status
        self.peer_connections.remove(&node_id);
        self.durable_peers.remove(&node_id);
    }
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id, request_body = %request_body)))]
    pub async fn request(
        &self,
        node_id: proto::EntityId,
        cdata: &PA::ContextData,
        request_body: proto::NodeRequestBody,
    ) -> Result<proto::NodeResponseBody, RequestError> {
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        let request = proto::NodeRequest { id: request_id.clone(), to: node_id, from: self.id, body: request_body };
        let auth = self.policy_agent.sign_request(self, cdata, &request);

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
        info!("{self}.send_update({node_id:#}, {notification})");
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
            proto::NodeMessage::Unsubscribe { from, subscription_id } => {
                self.reactor.unsubscribe(subscription_id);
                // Remove and drop the subscription handle
                if let Some(peer_state) = self.peer_connections.get(&from) {
                    peer_state.subscriptions.remove(&subscription_id);
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(request = %request)))]
    async fn handle_request(&self, cdata: &PA::ContextData, request: proto::NodeRequest) -> anyhow::Result<proto::NodeResponseBody> {
        match request.body {
            proto::NodeRequestBody::CommitTransaction { id, events } => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_remote_transaction(cdata, id.clone(), events).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete { id }),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { collection, predicate } => {
                self.policy_agent.can_access_collection(cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;
                let predicate = self.policy_agent.filter_predicate(cdata, &collection, predicate)?;

                let mut states = Vec::new();
                for state in storage_collection.fetch_states(&predicate).await? {
                    if self.policy_agent.check_read(cdata, &state.payload.entity_id, &collection, &state.payload.state).is_ok() {
                        states.push(state);
                    }
                }
                Ok(proto::NodeResponseBody::Fetch(states))
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
            proto::NodeRequestBody::GetEvents { collection, event_ids, motivation: _ } => {
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
            proto::NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                self.handle_subscribe_request(cdata, request.from, subscription_id, collection, predicate).await
            }
        }
    }

    async fn handle_update(&self, notification: proto::NodeUpdate) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::NodeUpdateBody::SubscriptionUpdate { subscription_id, items } => {
                // TODO check if this is a valid subscription
                action_debug!(self, "received subscription update for {} items", "{}", items.len());
                if let Some(cdata) = self.subscription_context.get(&subscription_id) {
                    let nodeandcontext = NodeAndContext { node: self.clone(), cdata };
                    self.apply_subscription_updates(&notification.from, items, nodeandcontext).await?;
                } else {
                    error!("Received subscription update for unknown subscription {}", subscription_id);
                    return Err(anyhow!("Received subscription update for unknown subscription {}", subscription_id));
                }
                Ok(())
            }
        }
    }

    async fn relay_to_required_peers(
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

    /// Does all the things necessary to commit a local transaction
    /// notably, the application of events to Entities works differently versus remote transactions
    pub async fn commit_local_trx(&self, cdata: &PA::ContextData, trx: Transaction) -> Result<(), MutationError> {
        let (trx_id, entity_events) = trx.into_parts()?;
        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        // Check policy and collect attestations
        for (entity, event) in entity_events {
            let attestation = self.policy_agent.check_event(self, cdata, &entity, &event)?;
            let attested = Attested::opt(event.clone(), attestation);
            attested_events.push(attested.clone());
            entity_attested_events.push((entity, attested));
        }

        // Relay to peers and wait for confirmation
        self.relay_to_required_peers(cdata, trx_id, &attested_events).await?;

        // All peers confirmed, now we can update local state
        let mut changes: Vec<EntityChange> = Vec::new();
        for (entity, attested_event) in entity_attested_events {
            let collection = self.collections.get(&attested_event.payload.collection).await?;
            collection.add_event(&attested_event).await?;
            entity.commit_head(Clock::new([attested_event.payload.id()]));

            // If this entity has an upstream, propagate the changes
            if let Some(ref upstream) = entity.upstream {
                upstream.apply_event(&self.collections.get(&attested_event.payload.collection).await?, &attested_event.payload).await?;
            }

            // Persist

            let state = entity.to_state()?;

            let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
            let attestation = self.policy_agent.attest_state(self, &entity_state);
            let attested = Attested::opt(entity_state, attestation);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(entity.clone(), vec![attested_event])?);
        }

        // Notify reactor of ALL changes
        self.reactor.notify_change(changes);
        Ok(())
    }

    /// Does all the things necessary to commit a remote transaction
    pub async fn commit_remote_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        info!("Node {} committing transaction {} with {} events", self.id, id, events.len());
        let mut changes = Vec::new();

        for event in events.iter_mut() {
            let collection = self.collections.get(&event.payload.collection).await?;
            let entity = self.entities.get_retrieve_or_create(&event.payload.collection, &collection, &event.payload.entity_id).await?;

            // we have the entity, so we can check access, optionally atteste, and apply/save the event;
            if let Some(attestation) = self.policy_agent.check_event(self, cdata, &entity, &event.payload)? {
                event.attestations.push(attestation);
            }
            if entity.apply_event(&collection, &event.payload).await? {
                let state = entity.to_state()?;
                let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                let attestation = self.policy_agent.attest_state(self, &entity_state);
                let attested = Attested::opt(entity_state, attestation);
                collection.add_event(event).await?;
                collection.set_state(attested).await?;
                changes.push(EntityChange::new(entity.clone(), vec![event.clone()])?);
            }
        }

        self.reactor.notify_change(changes);

        Ok(())
    }

    // Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    // we also don't need to fan events out to peers because we're receiving them from a peer
    pub async fn apply_subscription_updates(
        &self,
        from_peer_id: &proto::EntityId,
        updates: Vec<proto::SubscriptionUpdateItem>,
        nodeandcontext: NodeAndContext<SE, PA>,
    ) -> Result<(), MutationError> {
        let mut changes = Vec::new();
        for update in updates {
            match self.apply_subscription_update(from_peer_id, update, &nodeandcontext).await {
                Ok(Some(change)) => {
                    changes.push(change);
                }
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    action_warn!(self, "received invalid update from peer", "{}: {}", from_peer_id.to_base64_short(), e);
                }
            }
        }
        debug!("Node {} notifying reactor of {} changes", self.id, changes.len());
        self.reactor.notify_change(changes);
        Ok(())
    }

    // TODO:
    // in the Initial and Add cases, if this is the first time we're hearing about this entity
    // it's fine that we only have the state, and no events.
    // But if we already have the state and its clock is not identical, we
    // may need to fetch the events that connect the two. If those events are in the
    // collection, the collection, then lineage compare inside with_state -> apply_state
    // will find them. But if there's a gap, it doesn't currently have the ability to request
    // those events from the peer.
    pub async fn apply_subscription_update(
        &self,
        from_peer_id: &proto::EntityId,
        update: proto::SubscriptionUpdateItem,
        nodeandcontext: &NodeAndContext<SE, PA>,
    ) -> Result<Option<EntityChange>, MutationError> {
        info!("MARK apply_subscription_update {} type={:?} from={}", self.id.to_base64_short(), update, from_peer_id.to_base64_short());
        match update {
            proto::SubscriptionUpdateItem::Initial { entity_id, collection, state } => {
                info!("MARK apply_subscription_update.Initial entity={} state={:?}", entity_id.to_base64_short(), state);
                let state = (entity_id, collection, state).into();
                // validate that we trust the state given to us
                self.policy_agent.validate_received_state(self, from_peer_id, &state)?;

                let payload = state.payload;
                let collection = self.collections.get(&payload.collection).await?;
                let getter = (payload.collection.clone(), nodeandcontext);

                info!("MARK apply_subscription_update.with_state entity={entity_id:#} head={}", payload.state.head);
                match self.entities.with_state(&getter, payload.entity_id, payload.collection, payload.state).await? {
                    (Some(true), entity) => {
                        // We had the entity already, and this state is newer than the one we have, so save it to the collection
                        // Not sure if we should reproject the state - discuss
                        // however, if we do reproject, we need to re-attest the state
                        let state = entity.to_state()?;
                        let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                        let attestation = self.policy_agent.attest_state(self, &entity_state);
                        let attested = Attested::opt(entity_state, attestation);
                        collection.set_state(attested).await?;
                        Ok(Some(EntityChange::new(entity, vec![])?))
                    }
                    (Some(false), _entity) => {
                        // We had the entity already, and this state is not newer than the one we have so we drop it to the floor
                        Ok(None)
                    }
                    (None, entity) => {
                        // We did not have the entity yet, so we created it, so save it to the collection
                        // see notes as above regarding reprojecting and re-attesting the state
                        let state = entity.to_state()?;
                        let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                        let attestation = self.policy_agent.attest_state(self, &entity_state);
                        let attested = Attested::opt(entity_state, attestation);
                        collection.set_state(attested).await?;
                        Ok(Some(EntityChange::new(entity, vec![])?))
                    }
                }
            }
            proto::SubscriptionUpdateItem::Add { entity_id, collection: collection_id, state, events } => {
                let collection = self.collections.get(&collection_id).await?;

                // validate and store the events, in case we need them for lineage comparison
                let mut attested_events = Vec::new();
                for event in events.iter() {
                    let event = (entity_id, collection_id.clone(), event.clone()).into();
                    self.policy_agent.validate_received_event(self, from_peer_id, &event)?;
                    // store the validated event in case we need it for lineage comparison
                    collection.add_event(&event).await?;
                    attested_events.push(event);
                }

                let state = (entity_id, collection_id.clone(), state).into();
                self.policy_agent.validate_received_state(self, from_peer_id, &state)?;

                match self.entities.with_state(&collection, entity_id, collection_id, state.payload.state).await? {
                    (Some(false), _entity) => {
                        // had it already, and the state is not newer than the one we have
                        Ok(None)
                    }
                    (Some(true), entity) => {
                        // had it already, and the state is newer than the one we have, and was applied successfully, so save it and return the change
                        // reduce the probability of error by reprojecting the state - is this necessary if we've validated the attestation?
                        // See notes above regarding reprojecting and re-attesting the state
                        let state = entity.to_state()?;
                        let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                        let attestation = self.policy_agent.attest_state(self, &entity_state);
                        let attested = Attested::opt(entity_state, attestation);
                        collection.set_state(attested).await?;
                        Ok(Some(EntityChange::new(entity, attested_events)?))
                    }

                    (None, entity) => {
                        // did not have it, so we created it, so save it and return the change
                        // See notes above regarding attestation/reprojection
                        let state = entity.to_state()?;
                        let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                        let attestation = self.policy_agent.attest_state(self, &entity_state);
                        let attested = Attested::opt(entity_state, attestation);
                        collection.set_state(attested).await?;
                        Ok(Some(EntityChange::new(entity, attested_events)?))
                    }
                }
            }
            proto::SubscriptionUpdateItem::Change { entity_id, collection: collection_id, events } => {
                let collection = self.collections.get(&collection_id).await?;
                let mut attested_events = Vec::new();
                for event in events.iter() {
                    let event = (entity_id, collection_id.clone(), event.clone()).into();

                    self.policy_agent.validate_received_event(self, from_peer_id, &event)?;
                    // store the validated event in case we need it for lineage comparison
                    collection.add_event(&event).await?;
                    attested_events.push(event);
                }

                let entity = self.entities.get_retrieve_or_create(&collection_id, &collection, &entity_id).await?;

                let mut changed = false;
                for event in attested_events.iter() {
                    changed = entity.apply_event(&collection, &event.payload).await?;
                }
                if changed {
                    Ok(Some(EntityChange::new(entity, attested_events)?))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn request_remote_subscribe(
        &self,
        cdata: &PA::ContextData,
        sub: &mut SubscriptionHandle,
        collection_id: &CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<()> {
        // First, find any durable nodes to subscribe to
        let durable_peer_id = self.get_durable_peer_random();

        // If we have a durable node, send a subscription request to it
        if let Some(peer_id) = durable_peer_id {
            match self
                .request(
                    peer_id,
                    cdata,
                    proto::NodeRequestBody::Subscribe { subscription_id: sub.id, collection: collection_id.clone(), predicate },
                )
                .await?
            {
                proto::NodeResponseBody::Subscribed { subscription_id: _ } => {
                    debug!("Node {} subscribed to peer {}", self.id, peer_id);
                    // Add the peer to the subscription handle's peers list
                    sub.peers.push(peer_id);
                }
                proto::NodeResponseBody::Error(e) => {
                    return Err(anyhow!("Error from peer subscription: {}", e));
                }
                _ => {
                    return Err(anyhow!("Unexpected response type from peer subscription"));
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(peer_id = %peer_id.to_base64_short(), sub_id = %sub_id, collection_id = %collection_id, predicate = %predicate)))]
    async fn handle_subscribe_request(
        &self,
        cdata: &PA::ContextData,
        peer_id: proto::EntityId,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        // First fetch initial state
        // let storage_collection = self.collections.get(&collection_id).await?;
        // let states = storage_collection.fetch_states(&predicate).await?;

        self.policy_agent.can_access_collection(cdata, &collection_id)?;
        let predicate = self.policy_agent.filter_predicate(cdata, &collection_id, predicate)?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        {
            let peer_id = peer_id;
            self.reactor
                .subscribe(sub_id, &collection_id, predicate, move |changeset| {
                    // TODO move this into a task being fed by a channel and reorg into a function
                    let mut updates: Vec<proto::SubscriptionUpdateItem> = Vec::new();

                    // When changes occur, collect events and states
                    for change in changeset.changes.into_iter() {
                        match change {
                            ItemChange::Initial { item } => {
                                // For initial state, include both events and state
                                if let Ok(es) = item.to_entity_state() {
                                    let attestation = node.policy_agent.attest_state(&node, &es);

                                    updates.push(proto::SubscriptionUpdateItem::initial(
                                        item.id(),
                                        item.collection.clone(),
                                        Attested::opt(es, attestation),
                                    ));
                                }
                            }
                            ItemChange::Add { item, events } => {
                                // For entities which were not previously matched, state AND events should be included
                                // but it's weird because EntityState and Event redundantly include entity_id and collection
                                // But we want to Attest events independently, and we need to attest State - but we can't just attest a naked state,
                                // it has to have the entity_id

                                let state = match item.to_state() {
                                    Ok(state) => state,
                                    Err(e) => {
                                        warn!("Node {} entity {} state experienced an error - {}", node.id, item.id, e);
                                        continue;
                                    }
                                };

                                let es = EntityState { entity_id: item.id, collection: item.collection.clone(), state };
                                let attestation = node.policy_agent.attest_state(&node, &es);

                                updates.push(proto::SubscriptionUpdateItem::add(
                                    item.id,
                                    item.collection.clone(),
                                    Attested::opt(es, attestation),
                                    events,
                                ));
                            }
                            ItemChange::Update { item, events } | ItemChange::Remove { item, events } => {
                                updates.push(proto::SubscriptionUpdateItem::change(item.id, item.collection.clone(), events));
                            }
                        }
                    }

                    if !updates.is_empty() {
                        node.send_update(peer_id, proto::NodeUpdateBody::SubscriptionUpdate { subscription_id: sub_id, items: updates });
                    }
                })
                .await?;
        };

        // Store the subscription handle
        if let Some(peer_state) = self.peer_connections.get(&peer_id) {
            peer_state.subscriptions.insert(sub_id);
        }

        Ok(proto::NodeResponseBody::Subscribed { subscription_id: sub_id })
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

    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cdata: &PA::ContextData,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.id, id, collection_id);

        if !self.durable {
            // Fetch from peers and commit first response
            match self.get_from_peer(collection_id, vec![id], cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = self.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", self.id);
            return Ok(local);
        }
        debug!("Node({}).get_entity fetching from storage", self.id);

        let collection = self.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                let (_changed, entity) =
                    self.entities.with_state(&collection, id, collection_id.clone(), entity_state.payload.state).await?;
                Ok(entity)
            }
            Err(RetrievalError::EntityNotFound(id)) => {
                let (_, entity) = self.entities.with_state(&collection, id, collection_id.clone(), proto::State::default()).await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch a list of entities based on a predicate
    pub async fn fetch_entities(
        &self,
        collection_id: &CollectionId,
        args: MatchArgs,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Entity>, RetrievalError> {
        // TODO implement cached: true
        if !self.durable {
            // Fetch from peers and commit first response
            match self.fetch_from_peer(collection_id, args.predicate.clone(), cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        self.policy_agent.can_access_collection(cdata, collection_id)?;
        // Fetch raw states from storage
        let storage_collection = self.collections.get(collection_id).await?;
        let filtered_predicate = self.policy_agent.filter_predicate(cdata, collection_id, args.predicate)?;
        let states = storage_collection.fetch_states(&filtered_predicate).await?;

        // Convert states to entities
        let mut entities = Vec::new();
        for state in states {
            let (_, entity) =
                self.entities.with_state(&storage_collection, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            entities.push(entity);
        }
        Ok(entities)
    }

    pub async fn subscribe(
        &self,
        cdata: &PA::ContextData,
        sub_id: proto::SubscriptionId,
        collection_id: &CollectionId,
        mut args: MatchArgs,
        callback: Box<dyn Fn(ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<SubscriptionHandle, RetrievalError> {
        let mut handle = SubscriptionHandle::new(Box::new(Node(self.0.clone())) as Box<dyn TNodeErased>, sub_id);

        self.policy_agent.can_access_collection(cdata, collection_id)?;
        args.predicate = self.policy_agent.filter_predicate(cdata, collection_id, args.predicate)?;

        println!("{self}.subscribe MARK 1 {sub_id} {collection_id}");
        // TODO BEFORE MERGE - this should be after reactor.subscribe but I think LocalProcess is executing the server side subscribe in the same task
        self.subscription_context.insert(sub_id, cdata.clone());
        self.request_remote_subscribe(cdata, &mut handle, collection_id, args.predicate.clone()).await?;
        println!("{self}.subscribe MARK 2 {sub_id} {collection_id}");
        self.reactor.subscribe(handle.id, collection_id, args, callback).await?;
        println!("{self}.subscribe MARK 3 {sub_id} {collection_id}");

        Ok(handle)
    }
    /// Fetch entities from the first available durable peer.
    async fn fetch_from_peer(
        &self,
        collection_id: &CollectionId,
        predicate: ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> anyhow::Result<(), RetrievalError> {
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(peer_id, cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), predicate })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Fetch(states) => {
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
                debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }

    async fn get_from_peer(
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
    pub async fn request_remote_unsubscribe(&self, sub_id: proto::SubscriptionId, peers: Vec<proto::EntityId>) -> anyhow::Result<()> {
        for (peer_id, item) in self.peer_connections.get_list(peers) {
            if let Some(connection) = item {
                let sub_id = sub_id;
                connection.send_message(proto::NodeMessage::Unsubscribe { from: peer_id, subscription_id: sub_id })?;
            } else {
                warn!("Peer {} not connected", peer_id);
            }
        }

        Ok(())
    }

    pub fn unsubscribe(self: &Arc<Self>, handle: &SubscriptionHandle) -> anyhow::Result<()> {
        let node = Node(self.clone());
        let peers = handle.peers.clone();
        let sub_id = handle.id;
        spawn(async move {
            node.subscription_context.remove(&sub_id);
            node.reactor.unsubscribe(sub_id);
            if let Err(e) = node.request_remote_unsubscribe(sub_id, peers).await {
                warn!("Error unsubscribing from peers: {}", e);
            }
        });
        Ok(())
    }
}

impl<SE, PA> Drop for NodeInner<SE, PA>
where PA: PolicyAgent
{
    fn drop(&mut self) {
        info!("Node({}) dropped", self.id);
    }
}

pub trait TNodeErased: Send + Sync + 'static {
    fn unsubscribe(&self, handle: &SubscriptionHandle);
}

impl<SE, PA> TNodeErased for Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn unsubscribe(&self, handle: &SubscriptionHandle) { let _ = self.0.unsubscribe(handle); }
}

impl<SE, PA> fmt::Display for Node<SE, PA>
where PA: PolicyAgent
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bold blue, dimmed brackets
        write!(f, "\x1b[1;34mnode\x1b[2m[\x1b[1;34m{}\x1b[2m]\x1b[0m", self.id.to_base64_short())
    }
}
