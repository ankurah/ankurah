use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use dashmap::{DashMap, DashSet};
use rand::prelude::*;
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::{oneshot, RwLock};

use crate::{
    changes::{ChangeSet, EntityChange, ItemChange},
    collectionset::CollectionSet,
    connector::PeerSender,
    context::Context,
    entity::{Entity, WeakEntity},
    error::{RequestError, RetrievalError},
    policy::PolicyAgent,
    reactor::Reactor,
    storage::StorageEngine,
    subscription::SubscriptionHandle,
    task::spawn,
};
use tracing::{debug, info, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    _durable: bool,
    subscriptions: BTreeSet<proto::SubscriptionId>,
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

impl<SE, PA> Deref for Node<SE, PA> {
    type Target = Arc<NodeInner<SE, PA>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

/// Represents the user session - or whatever other context the PolicyAgent
/// Needs to perform it's evaluation. Just a marker trait for now but maybe
/// we'll need to add some methods to it in the future.
pub trait ContextData: Clone + Send + Sync + 'static {}

pub struct NodeInner<SE, PA> {
    pub id: proto::ID,
    pub durable: bool,
    pub collections: CollectionSet<SE>,

    entities: Arc<RwLock<EntityMap>>,
    // peer_connections: Vec<PeerConnection>,
    peer_connections: DashMap<proto::ID, PeerState>,
    durable_peers: DashSet<proto::ID>,
    pending_requests: DashMap<proto::RequestId, oneshot::Sender<Result<proto::NodeResponseBody, RequestError>>>,

    /// The reactor for handling subscriptions
    pub reactor: Arc<Reactor<SE, PA>>,
    _policy_agent: PA,
}

type EntityMap = BTreeMap<(proto::ID, proto::CollectionId), WeakEntity>;

impl<SE, PA> Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine.clone());
        let reactor = Reactor::new(collections.clone(), policy_agent.clone());
        let id = proto::ID::new();
        info!("Node {id} created as ephemeral");
        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: DashMap::new(),
            durable_peers: DashSet::new(),
            pending_requests: DashMap::new(),
            reactor,
            durable: false,
            _policy_agent: policy_agent,
        }));
        // reactor.set_node(node.weak());

        node
    }
    pub fn new_durable(engine: Arc<SE>, policy_agent: PA) -> Self {
        let collections = CollectionSet::new(engine);
        let reactor = Reactor::new(collections.clone(), policy_agent.clone());

        let id = proto::ID::new();
        info!("Node {id} created as durable");
        let node = Node(Arc::new(NodeInner {
            id,
            collections,
            entities: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: DashMap::new(),
            durable_peers: DashSet::new(),
            pending_requests: DashMap::new(),
            reactor,
            durable: true,
            _policy_agent: policy_agent,
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
        info!("Node({}).register_peer {}", self.id, presence.node_id);
        self.peer_connections
            .insert(presence.node_id.clone(), PeerState { sender, _durable: presence.durable, subscriptions: BTreeSet::new() });
        if presence.durable {
            self.durable_peers.insert(presence.node_id.clone());
        }
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    pub fn deregister_peer(&self, node_id: proto::ID) {
        info!("Node({}).deregister_peer {}", self.id, node_id);
        self.peer_connections.remove(&node_id);
        self.durable_peers.remove(&node_id);
    }
    pub async fn request(&self, node_id: proto::ID, request_body: proto::NodeRequestBody) -> Result<proto::NodeResponseBody, RequestError> {
        let (response_tx, response_rx) = oneshot::channel::<Result<proto::NodeResponseBody, RequestError>>();
        let request_id = proto::RequestId::new();

        // Store the response channel
        self.pending_requests.insert(request_id.clone(), response_tx);

        let request = proto::NodeRequest { id: request_id, to: node_id.clone(), from: self.id.clone(), body: request_body };

        {
            // Get the peer connection

            let connection = { self.peer_connections.get(&node_id).ok_or(RequestError::PeerNotConnected)?.sender.cloned() };

            // Send the request
            connection.send_message(proto::NodeMessage::Request(request)).await?;
        }

        // Wait for response
        response_rx.await.map_err(|_| RequestError::InternalChannelClosed)?
    }

    pub async fn handle_message(self: &Arc<Self>, message: proto::NodeMessage) -> anyhow::Result<()> {
        match message {
            proto::NodeMessage::Request(request) => {
                debug!("Node({}) received request {}", self.id, request);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = { self.peer_connections.get(&request.from).map(|c| c.sender.cloned()) } {
                    let from = request.from.clone();
                    let request_id = request.id.clone();
                    if request.to != self.id {
                        warn!("{} received message from {} but is not the intended recipient", self.id, request.from);
                    }

                    let body = match self.handle_request(request).await {
                        Ok(result) => result,
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    };
                    let _result = sender
                        .send_message(proto::NodeMessage::Response(proto::NodeResponse {
                            request_id,
                            from: self.id.clone(),
                            to: from,
                            body,
                        }))
                        .await;
                }
            }
            proto::NodeMessage::Response(response) => {
                debug!("Node {} received response {}", self.id, response);
                if let Some((_, tx)) = self.pending_requests.remove(&response.request_id) {
                    tx.send(Ok(response.body)).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
                }
            }
        }
        Ok(())
    }

    async fn handle_request(self: &Arc<Self>, request: proto::NodeRequest) -> anyhow::Result<proto::NodeResponseBody> {
        match request.body {
            proto::NodeRequestBody::CommitEvents(events) => {
                // TODO - relay to peers in a gossipy/resource-available manner, so as to improve propagation
                // With moderate potential for duplication, while not creating message loops
                // Doing so would be a secondary/tertiary/etc hop for this message
                match self.commit_events_local(&events).await {
                    Ok(_) => Ok(proto::NodeResponseBody::CommitComplete),
                    Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
                }
            }
            proto::NodeRequestBody::Fetch { collection, predicate } => {
                let storage_collection = self.collections.get(&collection).await?;
                let states: Vec<_> = storage_collection.fetch_states(&predicate).await?.into_iter().collect();
                Ok(proto::NodeResponseBody::Fetch(states))
            }
            proto::NodeRequestBody::Subscribe { subscription_id, collection, predicate } => {
                self.handle_subscribe_request(request.from, subscription_id, collection, predicate).await
            }
            proto::NodeRequestBody::Unsubscribe { subscription_id } => {
                self.reactor.unsubscribe(subscription_id);
                // Remove and drop the subscription handle
                if let Some(mut peer_state) = self.peer_connections.get_mut(&request.from) {
                    peer_state.subscriptions.remove(&subscription_id);
                }
                Ok(proto::NodeResponseBody::Success)
            }
        }
    }

    pub async fn request_remote_subscribe(
        &self,
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
                    proto::NodeRequestBody::Subscribe {
                        subscription_id: sub.id.clone(),
                        collection: collection_id.clone(),
                        predicate: predicate.clone(),
                    },
                )
                .await?
            {
                proto::NodeResponseBody::Subscribe { initial, subscription_id: _ } => {
                    // Apply initial states to our storage
                    let raw_bucket = self.collections.get(&collection_id).await?;
                    for (id, state) in initial {
                        raw_bucket.set_state(id, &state).await.map_err(|e| anyhow!("Failed to set entity: {:?}", e))?;
                    }
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
    pub async fn request_remote_unsubscribe(&self, sub_id: proto::SubscriptionId, peers: Vec<proto::ID>) -> anyhow::Result<()> {
        // QUESTION: Should we fire and forget these? or do error handling?

        futures::future::join_all(
            peers
                .iter()
                .map(|peer_id| self.request(peer_id.clone(), proto::NodeRequestBody::Unsubscribe { subscription_id: sub_id.clone() })),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    async fn handle_subscribe_request(
        self: &Arc<Self>,
        peer_id: proto::ID,
        sub_id: proto::SubscriptionId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        // First fetch initial state
        let storage_collection = self.collections.get(&collection_id).await?;
        let states = storage_collection.fetch_states(&predicate).await?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        {
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
                            let _ = node.request(peer_id, proto::NodeRequestBody::CommitEvents(events)).await;
                        });
                    }
                })
                .await?;
        };

        // Store the subscription handle
        if let Some(mut peer_state) = self.peer_connections.get_mut(&peer_id) {
            peer_state.subscriptions.insert(sub_id);
        }

        Ok(proto::NodeResponseBody::Subscribe { initial: states, subscription_id: sub_id })
    }

    pub fn next_entity_id(&self) -> proto::ID { proto::ID::new() }

    pub fn context(self: &Arc<Self>, data: PA::ContextData) -> Context { Context::new(Node(self.clone()), data) }

    async fn commit_events_local(self: &Arc<Self>, events: &Vec<proto::Event>) -> anyhow::Result<()> {
        debug!("Node({}).commit_events_local {}", self.id, events.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(","));
        let mut changes = Vec::new();

        // First apply events locally
        for event in events {
            // Apply Events to the Node's registered Entities first.
            let entity = self.get_entity(&event.collection, event.entity_id).await?;

            entity.apply_event(event)?;

            let state = entity.to_state()?;
            // Push the state buffers to storage.
            let collection = self.collections.get(&event.collection).await?;
            collection.add_event(&event).await?;
            let changed = collection.set_state(event.entity_id, &state).await?;

            if changed {
                changes.push(EntityChange { entity: entity.clone(), events: vec![event.clone()] });
            }
        }
        self.reactor.notify_change(changes);

        Ok(())
    }

    /// Apply events to local state buffer and broadcast to peers.
    pub async fn commit_events(self: &Arc<Self>, events: &Vec<proto::Event>) -> anyhow::Result<()> {
        self.commit_events_local(events).await?;

        // Then propagate to all peers
        let peer_ids: Vec<_> = self.peer_connections.iter().map(|i| i.key().clone()).collect();

        futures::future::join_all(peer_ids.iter().map(|peer_id| {
            let events = events.clone();
            async move {
                match self.request(peer_id.clone(), proto::NodeRequestBody::CommitEvents(events)).await {
                    Ok(proto::NodeResponseBody::CommitComplete) => {
                        debug!("Node({}) Peer {} confirmed commit", self.id, peer_id)
                    }
                    Ok(proto::NodeResponseBody::Error(e)) => warn!("Peer {} error: {}", peer_id, e),
                    Ok(_) => warn!("Peer {} unexpected response type", peer_id),
                    Err(_) => warn!("Peer {} internal channel closed", peer_id),
                }
            }
        }))
        .await;

        Ok(())
    }

    /// This should be called only by the transaction commit for newly created Entities
    /// This is necessary because Entities created in a transaction scope have no upstream
    /// so when they hand out read Entities, they have to work immediately.
    /// TODO: Discuss. The upside is that you can call .read() on a Mutable. The downside is that the behavior is inconsistent
    /// between newly created Entities and Entities that are created in a transaction scope.
    pub(crate) async fn insert_entity(self: &Arc<Self>, entity: Entity) -> anyhow::Result<()> {
        match self.entities.write().await.entry((entity.id, entity.collection.clone())) {
            Entry::Vacant(entry) => {
                entry.insert(entity.weak());
                Ok(())
            }
            Entry::Occupied(_) => Err(anyhow!("Entity already exists")),
        }
    }

    /// Register a Entity with the node, with the intention of preventing duplicate resident entities.
    /// Returns true if the Entity was already present.
    /// Note that this does not actually store the entity in the storage engine.
    #[must_use]
    pub(crate) async fn assert_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::ID,
        state: &proto::State,
    ) -> Result<Entity, RetrievalError> {
        let mut entities = self.entities.write().await;

        match entities.entry((id, collection_id.clone())) {
            Entry::Occupied(mut entry) => {
                if let Some(entity) = entry.get().upgrade() {
                    entity.apply_state(state)?;
                    Ok(entity)
                } else {
                    let entity = Entity::from_state(id, collection_id.clone(), state)?;
                    entry.insert(entity.weak());
                    Ok(entity)
                }
            }
            Entry::Vacant(entry) => {
                let entity = Entity::from_state(id, collection_id.clone(), state)?;
                entry.insert(entity.weak());
                Ok(entity)
            }
        }
    }

    pub(crate) async fn fetch_entity_from_node(&self, id: proto::ID, collection_id: &CollectionId) -> Option<Entity> {
        let entities = self.entities.read().await;
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(&(id, collection_id.clone())) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// Retrieve a single entity by id
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::ID,
        // cdata: &PA::ContextData,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.id, id, collection_id);

        if let Some(local) = self.fetch_entity_from_node(id, collection_id).await {
            debug!("Node({}).get_entity found local entity - returning", self.id);
            return Ok(local);
        }
        debug!("Node({}).get_entity fetching from storage", self.id);

        let collection = self.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                return self.assert_entity(collection_id, id, &entity_state).await;
            }
            Err(RetrievalError::NotFound(id)) => {
                // let scoped_entity = Entity::new(id, collection.to_string());
                // let ref_entity = Arc::new(scoped_entity);
                // Revisit this
                let entity = self.assert_entity(collection_id, id, &proto::State::default()).await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch a list of entities based on a predicate
    pub async fn fetch_entities(
        self: &Arc<Self>,
        collection_id: &CollectionId,
        args: MatchArgs,
        _cdata: &PA::ContextData,
    ) -> Result<Vec<Entity>, RetrievalError> {
        if !self.durable {
            // Fetch from peers and commit first response
            match self.fetch_from_peer(&collection_id, &args.predicate).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Fetch raw states from storage
        let storage_collection = self.collections.get(&collection_id).await?;
        let states = storage_collection.fetch_states(&args.predicate).await?;

        // Convert states to entities
        let mut entities = Vec::new();
        for (id, state) in states {
            let entity = self.assert_entity(&collection_id, id, &state).await?;
            entities.push(entity);
        }
        Ok(entities)
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        sub_id: proto::SubscriptionId,
        collection_id: &CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<SubscriptionHandle, RetrievalError> {
        let mut handle = SubscriptionHandle::new(Box::new(Node(self.clone())) as Box<dyn TNodeErased>, sub_id);

        // TODO spawn a task for these and make this fn syncrhonous - Pending error handling refinement / retry logic
        // spawn(async move {
        self.request_remote_subscribe(&mut handle, &collection_id, &args.predicate).await?;
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
            if let Err(e) = node.request_remote_unsubscribe(sub_id, peers).await {
                warn!("Error unsubscribing from peers: {}", e);
            }
        });
        Ok(())
    }
    /// Fetch entities from the first available durable peer.
    async fn fetch_from_peer(
        self: &Arc<Self>,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<(), RetrievalError> {
        let peer_id = self.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match self
            .request(peer_id.clone(), proto::NodeRequestBody::Fetch { collection: collection_id.clone(), predicate: predicate.clone() })
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
    pub fn get_durable_peer_random(&self) -> Option<proto::ID> {
        let mut rng = rand::thread_rng();
        // Convert to Vec since DashSet iterator doesn't support random selection
        let peers: Vec<_> = self.durable_peers.iter().collect();
        peers.choose(&mut rng).map(|i| i.key().clone())
    }

    /// Get all durable peer node IDs
    pub fn get_durable_peers(&self) -> Vec<proto::ID> { self.durable_peers.iter().map(|id| id.clone()).collect() }
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
