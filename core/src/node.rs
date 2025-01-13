use ankurah_proto::{self as proto, CollectionId};
use anyhow::anyhow;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, Weak},
};
use tokio::sync::{oneshot, RwLock};

use crate::{
    changes::{EntityChange, ItemChange},
    connector::PeerSender,
    error::RetrievalError,
    model::{Entity, View},
    reactor::Reactor,
    resultset::ResultSet,
    storage::{StorageCollectionWrapper, StorageEngine},
    subscription::SubscriptionHandle,
    transaction::Transaction,
};
use tracing::{debug, info, warn};

pub struct PeerState {
    sender: Box<dyn PeerSender>,
    durable: bool,
    subscriptions: BTreeMap<proto::SubscriptionId, Vec<SubscriptionHandle>>,
}

pub struct FetchArgs {
    pub predicate: ankql::ast::Predicate,
    pub cached: bool,
}

impl TryInto<FetchArgs> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<FetchArgs, Self::Error> {
        Ok(FetchArgs { predicate: ankql::parser::parse_selection(self)?, cached: false })
    }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

/// A participant in the Ankurah network, and primary place where queries are initiated
pub struct Node {
    pub id: proto::NodeId,
    pub durable: bool,
    storage_engine: Arc<dyn StorageEngine>,
    collections: RwLock<BTreeMap<CollectionId, StorageCollectionWrapper>>,

    entities: Arc<RwLock<EntityMap>>,
    // peer_connections: Vec<PeerConnection>,
    peer_connections: tokio::sync::RwLock<BTreeMap<proto::NodeId, PeerState>>,
    pending_requests: tokio::sync::RwLock<BTreeMap<proto::RequestId, oneshot::Sender<proto::NodeResponseBody>>>,

    /// The reactor for handling subscriptions
    reactor: Arc<Reactor>,
}

type EntityMap = BTreeMap<(proto::ID, proto::CollectionId), Weak<Entity>>;

impl Node {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        let reactor = Reactor::new(engine.clone());
        Self {
            id: proto::NodeId::new(),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            entities: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: tokio::sync::RwLock::new(BTreeMap::new()),
            pending_requests: tokio::sync::RwLock::new(BTreeMap::new()),
            reactor,
            durable: false,
        }
    }
    pub fn new_durable(engine: Arc<dyn StorageEngine>) -> Self {
        let reactor = Reactor::new(engine.clone());
        Self {
            id: proto::NodeId::new(),
            storage_engine: engine,
            collections: RwLock::new(BTreeMap::new()),
            entities: Arc::new(RwLock::new(BTreeMap::new())),
            peer_connections: tokio::sync::RwLock::new(BTreeMap::new()),
            pending_requests: tokio::sync::RwLock::new(BTreeMap::new()),
            reactor,
            durable: true,
        }
    }

    pub async fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        let mut peer_connections = self.peer_connections.write().await;
        peer_connections.insert(presence.node_id, PeerState { sender, durable: presence.durable, subscriptions: BTreeMap::new() });
        // TODO send hello message to the peer, including present head state for all relevant collections
    }
    pub async fn deregister_peer(&self, node_id: proto::NodeId) {
        let mut peer_connections = self.peer_connections.write().await;
        peer_connections.remove(&node_id);
    }

    pub async fn request(&self, node_id: proto::NodeId, request_body: proto::NodeRequestBody) -> anyhow::Result<proto::NodeResponseBody> {
        let (response_tx, response_rx) = oneshot::channel::<proto::NodeResponseBody>();
        let request_id = proto::RequestId::new();

        // Store the response channel
        self.pending_requests.write().await.insert(request_id.clone(), response_tx);

        let request = proto::NodeRequest { id: request_id, to: node_id.clone(), from: self.id.clone(), body: request_body };

        {
            // Get the peer connection

            let peer_connections = self.peer_connections.read().await;
            let connection = peer_connections.get(&node_id).ok_or_else(|| anyhow!("No connection to peer"))?.sender.cloned();

            drop(peer_connections);

            // Send the request
            connection.send_message(proto::PeerMessage::Request(request)).await.map_err(|_| anyhow!("Failed to send request"))?;
        }

        // Wait for response
        let response = response_rx.await.map_err(|_| anyhow!("Failed to receive response"))?;

        Ok(response)
    }

    pub async fn handle_message(self: &Arc<Self>, message: proto::PeerMessage) -> anyhow::Result<()> {
        match message {
            proto::PeerMessage::Request(request) => {
                info!("{} {request}", self.id);
                // TODO: Should we spawn a task here and make handle_message synchronous?
                // I think this depends on how we want to handle timeouts.
                // I think we want timeouts to be handled by the node, not the connector,
                // which would lend itself to spawning a task here and making this function synchronous.

                // double check to make sure we have a connection to the peer based on the node id
                if let Some(sender) = {
                    let peer_connections = self.peer_connections.read().await;
                    peer_connections.get(&request.from).map(|c| c.sender.cloned())
                } {
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
                        .send_message(proto::PeerMessage::Response(proto::NodeResponse {
                            request_id,
                            from: self.id.clone(),
                            to: from,
                            body,
                        }))
                        .await;
                    // TODO - do something with this error. Should probably send it to a log file
                }
            }
            proto::PeerMessage::Response(response) => {
                info!("{} {response}", self.id);
                if let Some(tx) = self.pending_requests.write().await.remove(&response.request_id) {
                    tx.send(response.body).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
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
                let states: Vec<_> = self.storage_engine.fetch_states(collection, &predicate).await?.into_iter().collect();
                Ok(proto::NodeResponseBody::Fetch(states))
            }
            proto::NodeRequestBody::Subscribe { collection, predicate } => {
                self.handle_subscribe_request(request.from, collection, predicate).await
            }
            proto::NodeRequestBody::Unsubscribe { subscription_id } => {
                // Remove and drop the subscription handle
                let mut peer_connections = self.peer_connections.write().await;
                if let Some(peer_state) = peer_connections.get_mut(&request.from) {
                    peer_state.subscriptions.remove(&subscription_id);
                }
                Ok(proto::NodeResponseBody::Success)
            }
        }
    }

    async fn handle_subscribe_request(
        self: &Arc<Self>,
        peer_id: proto::NodeId,
        collection_id: CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> anyhow::Result<proto::NodeResponseBody> {
        // First fetch initial state
        let states = self.storage_engine.fetch_states(collection_id.clone(), &predicate).await?;

        // Set up subscription that forwards changes to the peer
        let node = self.clone();
        let handle = {
            let peer_id = peer_id.clone();
            self.reactor
                .subscribe(&collection_id, predicate, move |changeset| {
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
                .await?
        };

        let subscription_id = handle.id;
        // Store the subscription handle
        let mut peer_connections = self.peer_connections.write().await;
        if let Some(peer_state) = peer_connections.get_mut(&peer_id) {
            peer_state.subscriptions.entry(handle.id).or_insert_with(Vec::new).push(handle);
        }

        Ok(proto::NodeResponseBody::Subscribe { initial: states, subscription_id })
    }

    pub async fn collection(&self, id: &CollectionId) -> StorageCollectionWrapper {
        let collections = self.collections.read().await;
        if let Some(store) = collections.get(id) {
            return store.clone();
        }
        drop(collections);

        let collection = StorageCollectionWrapper::new(self.storage_engine.collection(id).await.unwrap());

        let mut collections = self.collections.write().await;
        assert!(collections.insert(id.clone(), collection.clone()).is_none());
        drop(collections);

        collection
    }

    pub fn next_entity_id(&self) -> proto::ID { proto::ID::new() }

    /// Begin a transaction.
    ///
    /// This is the main way to edit Entities.
    pub fn begin(self: &Arc<Self>) -> Transaction { Transaction::new(self.clone()) }
    // TODO: Fix this - arghhh async lifetimes
    // pub async fn trx<T, F, Fut>(self: &Arc<Self>, f: F) -> anyhow::Result<T>
    // where
    //     F: for<'a> FnOnce(&'a Transaction) -> Fut,
    //     Fut: std::future::Future<Output = anyhow::Result<T>>,
    // {
    //     let trx = self.begin();
    //     let result = f(&trx).await?;
    //     trx.commit().await?;
    //     Ok(result)
    // }

    pub async fn commit_events_local(self: &Arc<Self>, events: &Vec<proto::Event>) -> anyhow::Result<()> {
        let mut changes = Vec::new();

        // First apply events locally
        for event in events {
            // Apply Events to the Node's registered Entities first.
            let entity = self.fetch_entity(event.entity_id, &event.collection).await?;

            entity.apply_event(event)?;

            let state = entity.to_state()?;
            // Push the state buffers to storage.
            let changed = self.collection(&event.collection).await.set_state(event.entity_id, &state).await?;

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
        let mut futures = Vec::new();
        let peer_ids = { self.peer_connections.read().await.iter().map(|(peer_id, _)| peer_id.clone()).collect::<Vec<_>>() };

        // Send commit request to all peers
        for peer_id in peer_ids {
            futures.push(self.request(peer_id.clone(), proto::NodeRequestBody::CommitEvents(events.to_vec())));
        }

        // // Wait for all peers to confirm
        for future in futures {
            match future.await {
                Ok(proto::NodeResponseBody::CommitComplete) => Ok(()),
                Ok(proto::NodeResponseBody::Error(e)) => Err(anyhow!(e)),
                Ok(_) => Err(anyhow!("Unexpected response type")),
                Err(e) => Err(e),
            }?;
        }

        Ok(())
    }

    /// This should be called only by the transaction commit for newly created Entities
    /// This is necessary because Entities created in a transaction scope have no upstream
    /// so when they hand out read Entities, they have to work immediately.
    /// TODO: Discuss. The upside is that you can call .read() on a Mutable. The downside is that the behavior is inconsistent
    /// between newly created Entities and Entities that are created in a transaction scope.
    pub(crate) async fn insert_entity(self: &Arc<Self>, entity: Arc<Entity>) -> anyhow::Result<()> {
        match self.entities.write().await.entry((entity.id, entity.collection.clone())) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::downgrade(&entity));
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
    ) -> Result<Arc<Entity>, RetrievalError> {
        let mut entities = self.entities.write().await;

        match entities.entry((id, collection_id.clone())) {
            Entry::Occupied(mut entry) => {
                if let Some(entity) = entry.get().upgrade() {
                    entity.apply_state(state)?;
                    Ok(entity)
                } else {
                    let entity = Arc::new(Entity::from_state(id, collection_id.clone(), state)?);
                    entry.insert(Arc::downgrade(&entity));
                    Ok(entity)
                }
            }
            Entry::Vacant(entry) => {
                let entity = Arc::new(Entity::from_state(id, collection_id.clone(), state)?);
                entry.insert(Arc::downgrade(&entity));
                Ok(entity)
            }
        }
    }

    pub(crate) async fn fetch_entity_from_node(&self, id: proto::ID, collection_id: &CollectionId) -> Option<Arc<Entity>> {
        let entities = self.entities.read().await;
        if let Some(entity) = entities.get(&(id, collection_id.clone())) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// Grab the entity state if it exists and create a new [`Entity`] based on that.
    // pub(crate) async fn fetch_entity_from_storage(
    //     &self,
    //     id: proto::ID,
    //     collection: &str,
    // ) -> Result<Entity, RetrievalError> {
    //     match self.get_entity_state(id, collection).await {
    //         Ok(entity_state) => {
    //             // info!("fetched entity state: {:?}", entity_state);
    //             Entity::from_entity_state(id, collection, &entity_state)
    //         }
    //         Err(RetrievalError::NotFound(id)) => {
    //             // info!("ID not found, creating new entity");
    //             Ok(Entity::new(id, collection.to_string()))
    //         }
    //         Err(err) => Err(err),
    //     }
    // }

    /// Fetch an entity.
    pub async fn fetch_entity(&self, id: proto::ID, collection: &CollectionId) -> Result<Arc<Entity>, RetrievalError> {
        debug!("fetch_entity {:?}-{:?}", id, collection);

        if let Some(local) = self.fetch_entity_from_node(id, collection).await {
            return Ok(local);
        }
        debug!("fetch_entity 2");

        let raw_bucket = self.collection(collection).await;
        match raw_bucket.0.get_state(id).await {
            Ok(entity_state) => {
                return self.assert_entity(collection, id, &entity_state).await;
            }
            Err(RetrievalError::NotFound(id)) => {
                // let scoped_entity = Entity::new(id, collection.to_string());
                // let ref_entity = Arc::new(scoped_entity);
                // Revisit this
                let entity = self.assert_entity(collection, id, &proto::State::default()).await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_entity<R: View>(&self, id: proto::ID) -> Result<R, RetrievalError> {
        use crate::model::Model;
        let collection_id = R::Model::collection();
        let entity = self.fetch_entity(id, &collection_id).await?;
        Ok(R::from_entity(entity))
    }

    /// Fetch entities from the first available durable peer.
    async fn fetch_from_peer(
        self: &Arc<Self>,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<(), RetrievalError> {
        // Get first durable peer
        let peer_id = {
            self.peer_connections
                .read()
                .await
                .iter()
                .find(|(_, state)| state.durable)
                .map(|(id, _)| id.clone())
                .ok_or(RetrievalError::NoDurablePeers)?
        };

        match self
            .request(peer_id.clone(), proto::NodeRequestBody::Fetch { collection: collection_id.clone(), predicate: predicate.clone() })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            proto::NodeResponseBody::Fetch(states) => {
                let raw_bucket = self.collection(collection_id).await;
                // do we have the ability to merge states?
                // because that's what we have to do I think
                for (id, state) in states {
                    raw_bucket.0.set_state(id, &state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
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

    pub async fn get<R: View>(self: &Arc<Self>, id: proto::ID) -> Result<R, RetrievalError> {
        let entity = self.fetch_entity(id, &R::collection()).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(
        self: &Arc<Self>,
        args: impl TryInto<FetchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<ResultSet<R>, RetrievalError> {
        let args: FetchArgs = args.try_into().map_err(|e| e.into())?;

        let predicate = args.predicate;

        use crate::model::Model;
        let collection_id = R::Model::collection();

        if !self.durable {
            // Fetch from peers and commit first response
            match self.fetch_from_peer(&collection_id, &predicate).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Fetch raw states from storage
        let states = self.storage_engine.fetch_states(collection_id.clone(), &predicate).await?;

        // Convert states to entities
        let mut entities = Vec::new();
        for (id, state) in states {
            let entity = self.assert_entity(&collection_id, id, &state).await?;
            entities.push(R::from_entity(entity));
        }

        Ok(ResultSet { items: entities })
    }

    /// Subscribe to changes in entities matching a predicate
    pub async fn subscribe<F, P, R>(self: &Arc<Self>, predicate: P, callback: F) -> anyhow::Result<crate::subscription::SubscriptionHandle>
    where
        F: Fn(crate::changes::ChangeSet<R>) + Send + Sync + 'static,
        P: TryInto<ankql::ast::Predicate>,
        P::Error: std::error::Error + Send + Sync + 'static,
        R: View,
    {
        use crate::model::Model;
        let collection_id = R::Model::collection();
        let predicate = predicate.try_into().map_err(|_e| anyhow!("Failed to parse predicate:"))?;

        // First, find any durable nodes to subscribe to
        let durable_peer_id = {
            let peer_connections = self.peer_connections.read().await;
            peer_connections.iter().find(|(_, state)| state.durable).map(|(id, _)| id.clone())
        };

        // If we have a durable node, send a subscription request to it
        if let Some(peer_id) = durable_peer_id {
            match self
                .request(peer_id, proto::NodeRequestBody::Subscribe { collection: collection_id.clone(), predicate: predicate.clone() })
                .await?
            {
                proto::NodeResponseBody::Subscribe { initial, subscription_id: _ } => {
                    // Apply initial states to our storage
                    let raw_bucket = self.collection(&collection_id).await;
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

        // Now set up our local subscription
        self.reactor
            .subscribe(&collection_id, predicate, move |changeset| {
                info!("Node notified");
                callback(changeset.into());
            })
            .await
    }
}
