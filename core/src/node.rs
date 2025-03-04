use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use async_trait::async_trait;
use dashmap::{DashMap, DashSet, Entry};
use rand::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;

use tokio::sync::oneshot;

use crate::registry::EntityRegistry;
use crate::{
    changes::{ChangeSet, EntityChange, ItemChange},
    connector::PeerSender,
    context::Context,
    entity::{Entity, WeakEntity},
    error::{MutationError, RequestError, RetrievalError},
    policy::PolicyAgent,
    reactor::Reactor,
    storage::{StorageCollectionWrapper, StorageEngine},
    subscription::SubscriptionHandle,
    task::spawn,
};
use tracing::{debug, info, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    #[allow(unused)]
    durable: bool,
    subscriptions: DashSet<proto::SubscriptionId>,
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

impl<SE, PA> Deref for Node<SE, PA> {
    type Target = Arc<NodeInner<SE, PA>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

/// Represents the user session - or whatever other context the PolicyAgent
/// Needs to perform it's evaluation.
#[async_trait]
pub trait ContextData: Send + Sync + 'static {}

pub struct NodeInner<SE, PA> {
    pub id: proto::NodeId,
    pub durable: bool,
    storage_engine: Arc<SE>,
    collections: DashMap<CollectionId, StorageCollectionWrapper>,

    pub(crate) entities: EntityRegistry,
    // peer_connections: Vec<PeerConnection>,
    peer_connections: DashMap<proto::NodeId, PeerState>,
    durable_peers: DashSet<proto::NodeId>,
    pending_requests: DashMap<proto::RequestId, oneshot::Sender<Result<proto::ResponseBody, RequestError>>>,
    pending_notifications: DashMap<proto::UpdateId, oneshot::Sender<Result<proto::ResponseBody, RequestError>>>,

    /// The reactor for handling subscriptions
    pub(crate) reactor: Arc<Reactor<SE, PA>>,
    pub(crate) policy_agent: PA,
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        let entities = EntityRegistry::new();
        let reactor = Reactor::new(engine.clone(), entities.clone(), policy_agent.clone());
        let id = proto::NodeId::new();
        info!("Node {} created", id);
        let node = Node(Arc::new(NodeInner {
            id,
            storage_engine: engine,
            collections: DashMap::new(),
            entities,
            peer_connections: DashMap::new(),
            durable_peers: DashSet::new(),
            pending_requests: DashMap::new(),
            pending_notifications: DashMap::new(),
            reactor,
            durable: false,
            policy_agent,
        }));
        // reactor.set_node(node.weak());

        node
    }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        let entities = EntityRegistry::new();
        let reactor = Reactor::new(engine.clone(), entities.clone(), policy_agent.clone());
        let id = proto::NodeId::new();
        info!("Node {} created as durable", id);

        let node = Node(Arc::new(NodeInner {
            id,
            storage_engine: engine,
            collections: DashMap::new(),
            entities: EntityRegistry::new(),
            peer_connections: DashMap::new(),
            durable_peers: DashSet::new(),
            pending_requests: DashMap::new(),
            pending_notifications: DashMap::new(),
            reactor,
            durable: true,
            policy_agent,
        }));
        // reactor.set_node(node.weak());
        node
    }
    pub fn weak(&self) -> WeakNode<SE, PA> { WeakNode(Arc::downgrade(&self.0)) }
}

impl<SE, PA> WeakNode<SE, PA> {
    pub fn upgrade(&self) -> Option<Node<SE, PA>> { self.0.upgrade().map(Node) }
}

impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        info!("Node {} register peer {}", self.id, presence.node_id);
        self.peer_connections
            .insert(presence.node_id.clone(), PeerState { sender, durable: presence.durable, subscriptions: DashSet::new() });
        if presence.durable {
            self.durable_peers.insert(presence.node_id.clone());
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    pub fn deregister_peer(&self, node_id: proto::NodeId) {
        info!("Node {} deregister peer {}", self.id, node_id);
        self.peer_connections.remove(&node_id);
        self.durable_peers.remove(&node_id);
    }
    pub async fn request(
        &self,
        node_id: proto::NodeId,
        cdata: &PA::ContextData,
        request_body: proto::RequestBody,
    ) -> Result<proto::ResponseBody, RequestError> {
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::ResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        let request = proto::Request { id: request_id.clone(), to: node_id.clone(), from: self.id.clone(), body: request_body };
        let auth = self.policy_agent.sign_request(self, cdata, &request);
        // Store the response channel
        self.pending_requests.insert(request_id, response_tx);

        {
            // Get the peer connection
            let connection = { self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?.sender.cloned() };
            // Send the request
            connection.send_message(proto::NodeMessage::Request { auth, request }).await?;
        }

        // Wait for response
        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    pub async fn send_update(&self, node_id: proto::NodeId, notification: proto::UpdateBody) -> Result<(), RequestError> {
        // same as request, minus cdata and the sign_request step

        let (response_tx, response_rx) = oneshot::channel::<Result<proto::ResponseBody, RequestError>>();
        let id = proto::UpdateId::new();

        // Store the response channel
        self.pending_notifications.insert(id.clone(), response_tx);

        let notification = proto::NodeMessage::Update(proto::Update { id, from: self.id.clone(), to: node_id.clone(), body: notification });
        {
            // Get the peer connection
            let connection = { self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?.sender.cloned() };
            // Send the request
            connection.send_message(notification).await?;
        }

        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)??;
        Ok(())
    }
}

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // TODO add a node id argument to this function rather than getting it from the message
    // (does this actually make it more secure? or just move the place they could lie to us to the handshake?)
    // Not if its signed by a node key.
    pub async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        match message {
            proto::NodeMessage::Update(update) => {
                info!("Node {} received update {}", self.id, update);

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

                    let body = match self.handle_update(update).await {
                        Ok(_) => proto::UpdateAckBody::Success,
                        Err(e) => proto::UpdateAckBody::Error(e.to_string()),
                    };
                    sender.send_message(proto::NodeMessage::UpdateAck(proto::UpdateAck { id, from, to, body })).await?;
                }
            }
            proto::NodeMessage::UpdateAck(ack) => {
                info!("Node {} received ack notification {} {}", self.id, ack.id, ack.body);
                if let Some((_, tx)) = self.pending_notifications.remove(&ack.id) {
                    tx.send(Ok(proto::ResponseBody::Success)).unwrap();
                }
            }
            proto::NodeMessage::Request { auth, request } => {
                info!("Node {} received request {}", self.id, request);
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
                        Err(e) => proto::ResponseBody::Error(e.to_string()),
                    };
                    let _result = sender
                        .send_message(proto::NodeMessage::Response(proto::Response { request_id, from: self.id.clone(), to: from, body }))
                        .await;
                }
            }
            proto::NodeMessage::Response(response) => {
                info!("Node {} received response {}", self.id, response);
                if let Some((_, tx)) = self.pending_requests.remove(&response.request_id) {
                    tx.send(Ok(response.body)).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
        }
        Ok(())
    }

    async fn handle_request(&self, cdata: &PA::ContextData, request: proto::Request) -> anyhow::Result<proto::ResponseBody> {
        match request.body {
            proto::RequestBody::CommitTransaction { id, events } => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_transaction(&cdata, id, events).await {
                    Ok(_) => Ok(proto::ResponseBody::CommitComplete),
                    Err(e) => Ok(proto::ResponseBody::Error(e.to_string())),
                }
            }
            proto::RequestBody::Fetch { collection, predicate } => {
                self.policy_agent.can_access_collection(&cdata, &collection)?;
                let storage_collection = self.collection(&collection).await;
                let predicate = self.policy_agent.filter_predicate(&cdata, &collection, predicate)?;
                let states: Vec<_> = storage_collection.fetch_states(&predicate).await?.into_iter().collect();
                Ok(proto::ResponseBody::Fetch(states))
            }
            proto::RequestBody::Subscribe { subscription_id, collection, predicate } => {
                self.handle_subscribe_request(&cdata, request.from, subscription_id, collection, predicate).await
            }
        }
    }

    async fn handle_update(&self, notification: proto::Update) -> anyhow::Result<()> {
        let Some(mut peer_state) = self.peer_connections.get_mut(&notification.from) else {
            return Err(anyhow!("Rejected notification from unknown node {}", notification.from));
        };

        match notification.body {
            proto::UpdateBody::SubscriptionUpdate { subscription_id: _, events } => {
                // TODO check if this is a valid subscription
                info!("Node {} received subscription update for {} events", self.id, events.len());
                self.apply_events_from_peer(&notification.from, events).await?;
                Ok(())
            }
            proto::UpdateBody::Unsubscribe { subscription_id } => {
                if let Some(_) = peer_state.subscriptions.remove(&subscription_id) {
                    self.reactor.unsubscribe(subscription_id);

                    Ok(())
                } else {
                    Err(anyhow!("Subscription {} not found (unsubscribe)", subscription_id))
                }
            }
        }
    }

    /// Commit events associated with a pending write transaction (which may be local or remote)
    pub async fn commit_transaction(
        &self,
        cdata: &PA::ContextData,
        id: proto::TransactionId,
        mut events: Vec<proto::Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        info!("Node {} committing transaction {} with {} events", self.id, id, events.len());
        let mut changes = Vec::new();

        // first check if all events are allowed by the local policy agent
        let mut updates: Vec<(Entity, proto::Attested<proto::Event>)> = Vec::new();
        for event in events.iter_mut() {
            // Apply Events to the Node's registered Entities first.
            let entity = self.get_entity(&event.payload.collection, event.payload.entity_id.clone()).await?;
            let attestation = self.policy_agent.check_event(self, cdata, &entity, &event.payload)?;
            if let Some(attestation) = attestation {
                event.attestations.push(attestation);
            }
            updates.push((entity, event.clone()));
        }

        // If we're not a durable node, send the transaction to a durable peer and wait for confirmation
        // if !self.durable {
        // let peer_id: proto::NodeId = self.get_durable_peers().pop().ok_or(MutationError::NoDurablePeers)?;
        // HACK - sendding all events to all peers is wrong, but we were doing it before, and I'm reproducing that behavior here temporarily.
        // TODO - these need to race each other, and run concurrently.
        for peer_id in self.get_durable_peers() {
            match self
                .request(peer_id.clone(), cdata, proto::RequestBody::CommitTransaction { id: id.clone(), events: events.clone() })
                .await
            {
                Ok(proto::ResponseBody::CommitComplete) => {
                    info!("Peer {} confirmed commit", peer_id)
                }
                Ok(proto::ResponseBody::Error(e)) => warn!("Peer {} error: {}", peer_id, e),
                Ok(_) => warn!("Peer {} unexpected response type", peer_id),
                Err(_) => warn!("Peer {} internal channel closed", peer_id),
            }
        }

        // finally, apply events locally
        for (entity, event) in updates {
            entity.apply_event(&event.payload)?;
            // Push the state buffers to storage.
            let collection = self.collection(&event.payload.collection).await;
            let state = entity.to_state()?;
            collection.add_event(&event).await?;
            let changed = collection.set_state(event.payload.entity_id, &state).await?;

            if changed {
                changes.push(EntityChange { entity: entity.clone(), events: vec![event.clone()] });
            }
        }
        self.reactor.notify_change(changes);

        Ok(())
    }

    // Similar to commit_transaction, except that we check event attestations instead of checking write permissions
    // we also don't need to fan events out to peers because we're receiving them from a peer
    pub async fn apply_events_from_peer(
        &self,
        from_peer_id: &proto::NodeId,
        events: Vec<proto::Attested<proto::Event>>,
    ) -> Result<(), MutationError> {
        let mut changes = Vec::new();
        for event in events {
            match self.policy_agent.validate_received_event(self, from_peer_id, &event) {
                Ok(()) => {
                    let entity = self.get_entity(&event.payload.collection, event.payload.entity_id.clone()).await?;
                    entity.apply_event(&event.payload)?;
                    let collection = self.collection(&event.payload.collection).await;
                    let state = entity.to_state()?;
                    collection.add_event(&event).await?;
                    info!("Node {} set_state for entity {} in collection {}", self.id, event.payload.entity_id, event.payload.collection);
                    let changed = collection.set_state(event.payload.entity_id, &state).await?;
                    if changed {
                        changes.push(EntityChange { entity: entity.clone(), events: vec![event.clone()] });
                    }
                }
                Err(e) => {
                    warn!("Node {} received invalid event from peer {} - {}", self.id, from_peer_id, e);
                }
            }
        }
        info!("Node {} notifying reactor of {} changes", self.id, changes.len());
        self.reactor.notify_change(changes);
        Ok(())
    }
}
impl<SE, PA> NodeInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
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
                    proto::RequestBody::Subscribe {
                        subscription_id: sub.id.clone(),
                        collection: collection_id.clone(),
                        predicate: predicate.clone(),
                    },
                )
                .await?
            {
                proto::ResponseBody::Subscribe { initial, subscription_id: _ } => {
                    // Apply initial states to our storage
                    let raw_bucket = self.collection(&collection_id).await;
                    for (id, state) in initial {
                        raw_bucket.set_state(id, &state).await.map_err(|e| anyhow!("Failed to set entity: {:?}", e))?;
                    }
                }
                proto::ResponseBody::Error(e) => {
                    return Err(anyhow!("Error from peer subscription: {}", e));
                }
                _ => {
                    return Err(anyhow!("Unexpected response type from peer subscription"));
                }
            }
        }
        Ok(())
    }

    pub async fn request_remote_unsubscribe(&self, sub_id: proto::SubscriptionId, peers: Vec<proto::NodeId>) -> anyhow::Result<()> {
        // QUESTION: Should we fire and forget these? or do error handling?

        futures::future::join_all(
            peers
                .iter()
                .map(|peer_id| self.send_update(peer_id.clone(), proto::UpdateBody::Unsubscribe { subscription_id: sub_id.clone() })),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
    async fn handle_subscribe_request(
        self: &Arc<Self>,
        cdata: &PA::ContextData,
        peer_id: proto::NodeId,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::ResponseBody> {
        // First fetch initial state
        let storage_collection = self.collection(&collection_id).await;
        let states = storage_collection.fetch_states(&predicate).await?;

        self.policy_agent.can_access_collection(cdata, &collection_id)?;
        let predicate = self.policy_agent.filter_predicate(cdata, &collection_id, predicate)?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        let _handle = {
            let peer_id = peer_id.clone();
            self.reactor
                .subscribe(sub_id, &collection_id, predicate, move |changeset| {
                    // When changes occur, send them to the peer as CommitEvents
                    let events: Vec<_> = changeset
                        .changes
                        .iter()
                        .flat_map(|change| match change {
                            ItemChange::Add { events: updates, .. }
                            | ItemChange::Update { events: updates, .. }
                            | ItemChange::Remove { events: updates, .. } => &updates[..],
                            ItemChange::Initial { .. } => &[],
                        })
                        .cloned()
                        .collect();

                    if !events.is_empty() {
                        let node = node.clone();
                        let peer_id = peer_id.clone();
                        tokio::spawn(async move {
                            let _ = node
                                .send_update(peer_id, proto::UpdateBody::SubscriptionUpdate { subscription_id: sub_id.clone(), events })
                                .await;
                        });
                    }
                })
                .await?
        };

        if let Some(mut peer_state) = self.peer_connections.get_mut(&peer_id) {
            peer_state.subscriptions.insert(sub_id);
        }

        Ok(proto::ResponseBody::Subscribe { initial: states, subscription_id: sub_id })
    }

    pub async fn collection(&self, id: &CollectionId) -> StorageCollectionWrapper {
        if let Some(store) = self.collections.get(id) {
            return store.clone();
        }

        let collection = StorageCollectionWrapper::new(self.storage_engine.collection(id).await.unwrap());

        // We might have raced with another node to create this collection

        if let Entry::Vacant(entry) = self.collections.entry(id.clone()) {
            entry.insert(collection.clone());
        }

        collection
    }

    pub fn next_entity_id(&self) -> proto::ID { proto::ID::new() }

    pub fn context(self: &Arc<Self>, data: PA::ContextData) -> Context { Context::new(Node(self.clone()), data) }

    /// Retrieve a single entity by id
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::ID,
        // cdata: &PA::ContextData,
    ) -> Result<Entity, RetrievalError> {
        info!("fetch_entity {:?}-{:?}", id, collection_id);

        if let Some(entity) = self.entities.get(&id) {
            return Ok(entity);
        }
        info!("fetch_entity 2");

        let collection = self.collection(collection_id).await;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                info!("fetch_entity 3");
                self.entities.assert(collection_id, id, &entity_state)
            }
            Err(RetrievalError::NotFound(id)) => {
                info!("fetch_entity 4");
                self.entities.assert(collection_id, id, &proto::State::default())
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch a list of entities based on a predicate
    pub async fn fetch_entities(
        self: &Arc<Self>,
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
        let storage_collection = self.collection(&collection_id).await;

        let predicate = self.policy_agent.filter_predicate(cdata, collection_id, args.predicate)?;
        let states = storage_collection.fetch_states(&predicate).await?;

        // Convert states to entities
        let mut entities = Vec::new();
        for (id, state) in states {
            let entity = self.entities.assert(&collection_id, id, &state)?;
            entities.push(entity);
        }
        Ok(entities)
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        cdata: &PA::ContextData,
        sub_id: proto::SubscriptionId,
        collection_id: &CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<SubscriptionHandle, RetrievalError> {
        let mut handle = SubscriptionHandle::new(Box::new(Node(self.clone())) as Box<dyn TNodeErased>, sub_id);

        self.policy_agent.can_access_collection(cdata, collection_id)?;

        // TODO spawn a task for these and make this fn syncrhonous - Pending error handling refinement / retry logic
        // spawn(async move {
        self.request_remote_subscribe(cdata, &mut handle, &collection_id, &args.predicate).await?;
        self.reactor.subscribe(handle.id, &collection_id, args, callback).await?;
        // });

        Ok(handle)
    }
    pub fn unsubscribe(self: &Arc<Self>, handle: &SubscriptionHandle) -> anyhow::Result<()> {
        let node = self.clone();
        let peers = handle.peers.clone();
        let sub_id = handle.id.clone();
        spawn(async move {
            node.reactor.unsubscribe(sub_id);
            node.request_remote_unsubscribe(sub_id, peers).await.unwrap();
        });
        Ok(())
    }
    /// Fetch entities from the first available durable peer.
    async fn fetch_from_peer(
        self: &Arc<Self>,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> anyhow::Result<(), RetrievalError> {
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(peer_id.clone(), cdata, proto::RequestBody::Fetch { collection: collection_id.clone(), predicate: predicate.clone() })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::ResponseBody::Fetch(states) => {
                let raw_bucket = self.collection(collection_id).await;
                // do we have the ability to merge states?
                // because that's what we have to do I think
                for (id, state) in states {
                    raw_bucket.set_state(id, &state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
                }
                Ok(())
            }
            proto::ResponseBody::Error(e) => {
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
    pub fn get_durable_peer_random(&self) -> Option<proto::NodeId> {
        let mut rng = rand::thread_rng();
        // Convert to Vec since DashSet iterator doesn't support random selection
        let peers: Vec<_> = self.durable_peers.iter().collect();
        peers.choose(&mut rng).map(|i| i.key().clone())
    }

    /// Get all durable peer node IDs
    pub fn get_durable_peers(&self) -> Vec<proto::NodeId> { self.durable_peers.iter().map(|id| id.clone()).collect() }
    pub fn get_peers(&self) -> Vec<proto::NodeId> { self.peer_connections.iter().map(|e| e.key().clone()).collect() }
}

impl<SE, PA> Drop for NodeInner<SE, PA> {
    fn drop(&mut self) {
        info!("Node {} dropped", self.id);
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
