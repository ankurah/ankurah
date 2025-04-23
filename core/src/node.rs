use ankurah_proto::{self as proto, Attested, AuthData, CollectionId, EntityState};
use anyhow::anyhow;

use async_trait::async_trait;
use rand::prelude::*;
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::oneshot;

use crate::{
    changes::{ChangeSet, EntityChange, ItemChange},
    collectionset::CollectionSet,
    connector::{PeerSender, SendError},
    context::Context,
    entity::{Entity, WeakEntitySet},
    error::{MutationError, RequestError, RetrievalError},
    policy::PolicyAgent,
    reactor::Reactor,
    storage::StorageEngine,
    subscription::SubscriptionHandle,
    system::SystemManager,
    task::spawn,
    util::{safemap::SafeMap, safeset::SafeSet},
};
#[cfg(feature = "instrument")]
use tracing::instrument;

use tracing::{debug, info, warn};

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

impl Into<MatchArgs> for ankql::ast::Predicate {
    fn into(self) -> MatchArgs { MatchArgs { predicate: self, cached: false } }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

/// A participant in the Ankurah network, and primary place where queries are initiated

pub struct Node<SE, PA>(Arc<NodeInner<SE, PA>>);
impl<SE, PA> Clone for Node<SE, PA> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct WeakNode<SE, PA>(Weak<NodeInner<SE, PA>>);
impl<SE, PA> Clone for WeakNode<SE, PA> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<SE, PA> WeakNode<SE, PA> {
    pub fn upgrade(&self) -> Option<Node<SE, PA>> { self.0.upgrade().map(Node) }
}

impl<SE, PA> Deref for Node<SE, PA> {
    type Target = Arc<NodeInner<SE, PA>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

/// Represents the user session - or whatever other context the PolicyAgent
/// Needs to perform it's evaluation.
#[async_trait]
pub trait ContextData: Send + Sync + 'static {}

pub struct NodeInner<SE, PA> {
    pub id: proto::EntityId,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    pub(crate) entities: WeakEntitySet,
    peer_connections: SafeMap<proto::EntityId, Arc<PeerState>>,
    durable_peers: SafeSet<proto::EntityId>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Arc<Reactor<SE, PA>>,
    pub(crate) policy_agent: PA,
    pub system: SystemManager<SE>,
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine.clone());
        let entityset: WeakEntitySet = Default::default();
        let reactor = Reactor::new(collections.clone(), entityset.clone(), policy_agent.clone());
        let id = proto::EntityId::new();
        info!("Node {id} created as ephemeral");

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), false);

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
        }));

        node
    }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine);
        let entityset: WeakEntitySet = Default::default();
        let reactor = Reactor::new(collections.clone(), entityset.clone(), policy_agent.clone());

        let id = proto::EntityId::new();
        info!("Node {id} created as durable");

        let system_manager = SystemManager::new(collections.clone(), entityset.clone(), true);

        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: entityset,
            peer_connections: SafeMap::new(),
            durable_peers: SafeSet::new(),
            reactor,
            durable: true,
            policy_agent,
            system: system_manager,
        }));

        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %presence.node_id, durable = %presence.durable)))]
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        info!("Node({}).register_peer {}", self.id, presence.node_id);
        self.peer_connections.insert(
            presence.node_id.clone(),
            Arc::new(PeerState {
                sender,
                _durable: presence.durable,
                subscriptions: SafeSet::new(),
                pending_requests: SafeMap::new(),
                pending_updates: SafeMap::new(),
            }),
        );
        if presence.durable {
            self.durable_peers.insert(presence.node_id.clone());
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(node_id = %node_id)))]
    pub fn deregister_peer(&self, node_id: proto::EntityId) {
        info!("Node({}).deregister_peer {}", self.id, node_id);
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

        let request = proto::NodeRequest { id: request_id.clone(), to: node_id.clone(), from: self.id.clone(), body: request_body };
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
        info!("Node({}).send_update to {}", self.id, node_id);
        let (response_tx, _response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        // Get the peer connection
        let Some(connection) = self.peer_connections.get(&node_id) else {
            warn!("Failed to send update to peer {}: {}", node_id, RequestError::PeerNotConnected);
            return;
        };

        // Store the response channel
        connection.pending_updates.insert(id.clone(), response_tx);

        let notification =
            proto::NodeMessage::Update(proto::NodeUpdate { id, from: self.id.clone(), to: node_id.clone(), body: notification });

        info!("Node({}) connection.send_message to {}", self.id, node_id);
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
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(message = %message)))]
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        match message {
            proto::NodeMessage::Update(update) => {
                debug!("Node({}) received update {}", self.id, update);

                if let Some(sender) = { self.peer_connections.get(&update.from).map(|c| c.sender.cloned()) } {
                    let _from = update.from.clone();
                    let _id = update.id.clone();
                    if update.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, update.from);
                        return Ok(());
                    }

                    // take down the return address
                    let id = update.id.clone();
                    let to = update.from.clone();
                    let from = self.id.clone();

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
                    let from = request.from.clone();
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
                        from: self.id.clone(),
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

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(request = %request)))]
    async fn handle_request(&self, cdata: &PA::ContextData, request: proto::NodeRequest) -> anyhow::Result<proto::NodeResponseBody> {
        match request.body {
            proto::NodeRequestBody::CommitTransaction { id, events } => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_transaction(&cdata, id.clone(), events).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete { id }),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { collection, predicate } => {
                self.policy_agent.can_access_collection(&cdata, &collection)?;
                let storage_collection = self.collections.get(&collection).await?;
                let predicate = self.policy_agent.filter_predicate(&cdata, &collection, predicate)?;
                let states: Vec<_> = storage_collection.fetch_states(&predicate).await?.into_iter().collect();
                Ok(proto::NodeResponseBody::Fetch(states))
            }
            proto::NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                self.handle_subscribe_request(&cdata, request.from, subscription_id, collection, predicate).await
            }
        }
    }

    async fn handle_update(&self, notification: proto::NodeUpdate) -> anyhow::Result<()> {
        let Some(_connection) = self.peer_connections.get(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::NodeUpdateBody::SubscriptionUpdate { subscription_id: _, items } => {
                // TODO check if this is a valid subscription
                info!("Node {} received subscription update for {} items", self.id, items.len());
                self.apply_subscription_update(&notification.from, items).await?;
                Ok(())
            }
        }
    }

    /// Commit events associated with a pending write transaction (which may be local or remote)
    pub async fn commit_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        info!("Node {} committing transaction {} with {} events", self.id, id, events.len());

        // first check if all events are allowed by the local policy agent
        let mut updates: Vec<(Entity, Attested<proto::Event>)> = Vec::new();
        for event in events.iter_mut() {
            // Apply Events to the Node's registered Entities first.
            let entity = self.get_entity(&event.payload.collection, event.payload.entity_id.clone()).await?;
            let attestation = self.policy_agent.check_event(self, cdata, &entity, &event.payload)?;

            if let Some(attestation) = attestation {
                event.attestations.push(attestation);
            }
            updates.push((entity, event.clone()));
        }
        // TODO review this
        // If we're not a durable node, send the transaction to a durable peer and wait for confirmation
        // if !self.durable {
        // let peer_id: proto::NodeId = self.get_durable_peers().pop().ok_or(MutationError::NoDurablePeers)?;
        // TODO - these need to race each other, and run concurrently.
        for peer_id in self.get_durable_peers() {
            match self
                .request(peer_id.clone(), cdata, proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.clone() })
                .await
            {
                Ok(proto::NodeResponseBody::CommitComplete { id }) => {
                    info!("Peer {} confirmed transaction {} commit", peer_id, id)
                }
                Ok(proto::NodeResponseBody::Error(e)) => warn!("Peer {} error: {}", peer_id, e),
                Ok(_) => warn!("Peer {} unexpected response type", peer_id),
                Err(_) => warn!("Peer {} internal channel closed", peer_id),
            }
        }

        let mut changes = Vec::new();
        // finally, apply events locally
        for (entity, event) in updates {
            let payload = &event.payload;
            let collection = self.collections.get(&payload.collection).await?;
            let changed = entity.apply_event(&collection, &payload).await?;
            // Push the state buffers to storage.
            let state = entity.to_state()?;
            collection.add_event(&event).await?;
            collection.set_state(payload.entity_id, &state).await?;

            if changed {
                changes.push(EntityChange { entity: entity.clone(), events: vec![event.clone()] });
            }
        }
        self.reactor.notify_change(changes);

        Ok(())
    }

    // Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    // we also don't need to fan events out to peers because we're receiving them from a peer
    pub async fn apply_subscription_update(
        &self,
        from_peer_id: &proto::EntityId,
        updates: Vec<proto::SubscriptionUpdateItem>,
    ) -> Result<(), MutationError> {
        let mut changes = Vec::new();
        for update in updates {
            match update {
                proto::SubscriptionUpdateItem::Initial { state } => {
                    // check with policy agentif this is a valid attested state
                    match self.policy_agent.validate_received_state(self, &from_peer_id, &state) {
                        Ok(()) => {
                            let payload = &state.payload;
                            let entity = self.get_entity(&payload.collection, payload.entity_id.clone()).await?;
                            let collection = self.collections.get(&payload.collection).await?;
                            if entity.apply_state(&collection, &payload.state).await? {
                                collection.set_state(payload.entity_id, &payload.state).await?;
                                // TODO check if this is desirable to send an empty events list
                                changes.push(EntityChange { entity: entity.clone(), events: vec![] });
                            }
                        }
                        Err(e) => {
                            warn!("Node {} received invalid state from peer {} - {}", self.id, from_peer_id, e);
                        }
                    }
                }
                proto::SubscriptionUpdateItem::Add { state, events } => {
                    // check with policy agentif this is a valid attested state
                    if let Err(e) = self.policy_agent.validate_received_state(self, &from_peer_id, &state) {
                        warn!("Node {} received invalid state from peer {} - {}", self.id, from_peer_id, e);
                        continue;
                    }

                    let payload = &state.payload;
                    let entity = self.get_entity(&payload.collection, payload.entity_id.clone()).await?;
                    let collection = self.collections.get(&payload.collection).await?;
                    if entity.apply_state(&collection, &payload.state).await? {
                        collection.set_state(payload.entity_id, &payload.state).await?;
                    }

                    for event in events {
                        if let Err(e) = self.policy_agent.validate_received_event(self, &from_peer_id, &event) {
                            warn!("Node {} received invalid event from peer {} - {}", self.id, from_peer_id, e);
                            continue;
                        }
                    }

                    match self.policy_agent.validate_received_state(self, &from_peer_id, &state) {
                        Ok(()) => {
                            let entity = self.get_entity(&payload.collection, payload.entity_id.clone()).await?;
                        }
                        Err(e) => {
                            warn!("Node {} received invalid state from peer {} - {}", self.id, from_peer_id, e);
                        }
                    }
                }
                proto::SubscriptionUpdateItem::Change { events } => {
                    // check with policy agentif this is a valid attested state
                    unimplemented!()
                    // HERE! - Should we take the aggragation we are given and assume/validate they are are the same entity, emitting a single EntityChange?
                    // match self.policy_agent.validate_received_event(self, from_peer_id, &update) {
                    //     Ok(()) => {
                    //         let entity = self.get_entity(&update.payload.collection, update.payload.entity_id.clone()).await?;
                    //         entity.apply_event(&update.payload).await?;
                    //         let collection = self.collections.get(&update.payload.collection).await?;
                    //         let state = entity.to_state()?;
                    //         collection.add_event(&update).await?;
                    //         info!(
                    //             "Node {} set_state for entity {} in collection {}",
                    //             self.id, update.payload.entity_id, update.payload.collection
                    //         );
                    //         let changed = collection.set_state(update.payload.entity_id, &state).await?;
                    //         if changed {
                    //             changes.push(EntityChange { entity: entity.clone(), events: vec![update.clone()] });
                    //         }
                    //     }
                    //     Err(e) => {
                    //         warn!("Node {} received invalid event from peer {} - {}", self.id, from_peer_id, e);
                    //     }
                    // }
                }
            }
        }

        info!("Node {} notifying reactor of {} changes", self.id, changes.len());
        // AH YES - this is failing because we are no longer sending the initial state to all peers
        // and the new event doesn't seem to be enough to materialize the state. Not sure exactly why YRS doesn't have enough info,
        // but ultimately that's immaterial - the client node needs to either fetch the precursors, or the state needs to be included in the Update from the peer.
        // Ideally both - the peer should track whether this item was previously matched/included previously, and if not, include the state in the update.
        // The client should ALSO be able to retrieve the precursors in the event that this state was omitted.
        // Think about this, but it should be easily reproducible by constucting an initially-non-matching entity, and then updating it to match.
        self.reactor.notify_change(changes);
        Ok(())
    }

    pub async fn request_remote_subscribe(
        &self,
        cdata: &PA::ContextData,
        sub: &mut SubscriptionHandle,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<()> {
        // First, find any durable nodes to subscribe to
        let durable_peer_id = self.get_durable_peer_random();

        // If we have a durable node, send a subscription request to it
        if let Some(peer_id) = durable_peer_id {
            match self
                .request(
                    peer_id,
                    &cdata,
                    proto::NodeRequestBody::Subscribe {
                        subscription_id: sub.id.clone(),
                        collection: collection_id.clone(),
                        predicate: predicate.clone(),
                    },
                )
                .await?
            {
                proto::NodeResponseBody::Subscribed { subscription_id: _ } => {
                    info!("Node {} subscribed to peer {}", self.id, peer_id);
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

    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(peer_id = %peer_id, sub_id = %sub_id, collection_id = %collection_id, predicate = %predicate)))]
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
            let peer_id = peer_id.clone();
            self.reactor
                .subscribe(sub_id, &collection_id, predicate, move |changeset| {
                    // TODO move this into a task being fed by a channel and reorg into a function
                    let mut updates: Vec<proto::SubscriptionUpdateItem> = Vec::new();

                    // When changes occur, collect events and states
                    for change in changeset.changes.into_iter() {
                        match change {
                            ItemChange::Initial { item } => {
                                // For initial state, include both events and state
                                if let Ok(state) = item.to_state() {
                                    let es =
                                        EntityState { entity_id: item.id, collection: item.collection.clone(), state, head: item.head() };
                                    let attestation = node.policy_agent.attest_state(&node, &es);
                                    updates.push(proto::SubscriptionUpdateItem::Initial { state: Attested::opt(es, attestation) });
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

                                let es = EntityState { entity_id: item.id, collection: item.collection.clone(), state, head: item.head() };
                                let attestation = node.policy_agent.attest_state(&node, &es);

                                updates.push(proto::SubscriptionUpdateItem::Add { state: Attested::opt(es, attestation), events });
                            }
                            ItemChange::Update { events, .. } | ItemChange::Remove { events, .. } => {
                                updates.push(proto::SubscriptionUpdateItem::Change { events });
                            }
                        }
                    }

                    if !updates.is_empty() {
                        info!("Node({}) sending update to {}", node.id, peer_id);
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

    pub fn context(&self, data: PA::ContextData) -> Context { Context::new(Node::clone(self), data) }

    /// Retrieve a single entity by id
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        // cdata: &PA::ContextData,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.id, id, collection_id);

        if let Some(local) = self.entities.get(id) {
            debug!("Node({}).get_entity found local entity - returning", self.id);
            return Ok(local);
        }
        debug!("Node({}).get_entity fetching from storage", self.id);

        let collection = self.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                return self.entities.with_state(id, collection_id.clone(), entity_state);
            }
            Err(RetrievalError::EntityNotFound(id)) => {
                // let scoped_entity = Entity::new(id, collection.to_string());
                // let ref_entity = Arc::new(scoped_entity);
                // Revisit this
                let entity = self.entities.with_state(id, collection_id.clone(), proto::State::default())?;
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
        if !self.durable {
            // Fetch from peers and commit first response
            match self.fetch_from_peer(&collection_id, &args.predicate, cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        self.policy_agent.can_access_collection(cdata, collection_id)?;
        // Fetch raw states from storage
        let storage_collection = self.collections.get(&collection_id).await?;
        let filtered_predicate = self.policy_agent.filter_predicate(cdata, collection_id, args.predicate)?;
        let states = storage_collection.fetch_states(&filtered_predicate).await?;

        // Convert states to entities
        let mut entities = Vec::new();
        for (id, state) in states {
            let entity = self.entities.with_state(id, collection_id.clone(), state)?;
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

        // TODO spawn a task for these and make this fn syncrhonous - Pending error handling refinement / retry logic
        // spawn(async move {
        self.request_remote_subscribe(cdata, &mut handle, &collection_id, &args.predicate).await?;
        self.reactor.subscribe(handle.id, &collection_id, args, callback).await?;
        // });

        Ok(handle)
    }
    /// Fetch entities from the first available durable peer.
    async fn fetch_from_peer(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> anyhow::Result<(), RetrievalError> {
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(
                peer_id.clone(),
                cdata,
                proto::NodeRequestBody::Fetch { collection: collection_id.clone(), predicate: predicate.clone() },
            )
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Fetch(states) => {
                let raw_bucket = self.collections.get(collection_id).await?;
                // do we have the ability to merge states?
                // because that's what we have to do I think
                for (id, state) in states {
                    raw_bucket.set_state(id, &state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
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

    /// Get a random durable peer node ID
    pub fn get_durable_peer_random(&self) -> Option<proto::EntityId> {
        let mut rng = rand::thread_rng();
        // Convert to Vec since DashSet iterator doesn't support random selection
        let peers: Vec<_> = self.durable_peers.to_vec();
        peers.choose(&mut rng).map(|i| i.clone())
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
                let sub_id = sub_id.clone();
                connection.send_message(proto::NodeMessage::Unsubscribe { from: peer_id.clone(), subscription_id: sub_id.clone() })?;
            } else {
                warn!("Peer {} not connected", peer_id);
            }
        }

        Ok(())
    }

    pub fn unsubscribe(self: &Arc<Self>, handle: &SubscriptionHandle) -> anyhow::Result<()> {
        let node = Node(self.clone());
        let peers = handle.peers.clone();
        let sub_id = handle.id.clone();
        spawn(async move {
            node.reactor.unsubscribe(sub_id);
            if let Err(e) = node.request_remote_unsubscribe(sub_id, peers).await {
                warn!("Error unsubscribing from peers: {}", e);
            }
        });
        Ok(())
    }
}

impl<SE, PA> Drop for NodeInner<SE, PA> {
    fn drop(&mut self) {
        info!("Node({}) dropped", self.id);
    }
}

pub trait TNodeErased: Send + Sync + 'static {
    fn unsubscribe(&self, handle: &SubscriptionHandle) -> ();
}

impl<SE, PA> TNodeErased for Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn unsubscribe(&self, handle: &SubscriptionHandle) -> () { let _ = self.0.unsubscribe(handle); }
}
