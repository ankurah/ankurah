use crate::{
    changes::{ChangeSet, EntityChange},
    entity::Entity,
    error::{MutationError, RetrievalError},
    model::View,
    node::{MatchArgs, Node, TNodeErased},
    policy::{AccessDenied, PolicyAgent},
    reactor::{ReactorSubscription, ReactorUpdate},
    resultset::{EntityResultSet, ResultSet},
    retrieval::LocalRetriever,
    storage::{StorageCollectionWrapper, StorageEngine},
    task,
    transaction::Transaction,
};
use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityState};
use ankurah_signals::{
    broadcast::{BroadcastId, Listener, ListenerGuard},
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    Get, Peek, Signal, Subscribe,
};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Context is used to provide a local interface to fetch and subscribe to entities
/// with a specific ContextData. Generally this means your auth token for a specific user,
/// but ContextData is abstracted so you can use what you want.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
pub struct Context(Arc<dyn TContext + Send + Sync + 'static>);
impl Clone for Context {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct NodeAndContext<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: Node<SE, PA>,
    pub cdata: PA::ContextData,
}

#[async_trait]
pub trait TContext {
    fn node_id(&self) -> proto::EntityId;
    /// Create a brand new entity, and add it to the WeakEntitySet
    /// Note that this does not actually persist the entity to the storage engine
    /// It merely ensures that there are no duplicate entities with the same ID (except forked entities)
    fn create_entity(&self, collection: proto::CollectionId) -> Entity;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: Transaction) -> Result<(), MutationError>;
    fn query(
        &self,
        sub_id: proto::PredicateId,
        collection: &proto::CollectionId,
        args: MatchArgs,
    ) -> Result<(LocalSubscription, EntityResultSet), RetrievalError>;
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    fn node_id(&self) -> proto::EntityId { self.node.id }
    fn create_entity(&self, collection: proto::CollectionId) -> Entity { self.node.entities.create(collection) }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> { self.node.policy_agent.check_write(&self.cdata, entity, None) }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get(&id) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: Transaction) -> Result<(), MutationError> { self.commit_local_trx(trx).await }
    fn query(
        &self,
        sub_id: proto::PredicateId,
        collection: &proto::CollectionId,
        args: MatchArgs,
    ) -> Result<(LocalSubscription, EntityResultSet), RetrievalError> {
        // Create the ReactorSubscription
        let reactor_subscription = self.node.reactor.subscribe();

        // Add the predicate and get the EntityResultSet
        let entity_resultset = reactor_subscription.add_predicate(sub_id, collection, args.predicate.clone())?;

        // Create type-erased Subscription
        let subscription =
            LocalSubscription::new(Box::new(Node(self.node.0.clone())) as Box<dyn TNodeErased>, sub_id, reactor_subscription);

        // TODO: Spawn async initialization task
        // For now, return with empty EntityResultSet - async initialization will populate it
        // let node_clone = self.node.clone();
        // let cdata_clone = self.cdata.clone();
        // let collection_clone = collection.clone();
        // let args_clone = args.clone();
        // task::spawn(async move {
        //     if let Err(e) = initialize_query_async(node_clone, cdata_clone, sub_id, collection_clone, args_clone).await {
        //         warn!("Failed to initialize query: {:?}", e);
        //     }
        // });

        Ok((subscription, entity_resultset))
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.system.collection(id).await
    }
}

/// Async initialization function for query subscriptions
async fn initialize_query_async<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
    node: Node<SE, PA>,
    cdata: PA::ContextData,
    predicate_id: proto::PredicateId,
    collection_id: CollectionId,
    args: MatchArgs,
) -> Result<(), RetrievalError> {
    let context = NodeAndContext { node, cdata };

    // Fetch initial states from storage/peers
    let initial_entities = context.fetch_entities(&collection_id, args).await?;

    // Initialize the reactor with the fetched entities
    context
        .node
        .reactor
        .initialize(
            context.node.reactor.subscribe().id(), // This is wrong, need to get the actual subscription ID
            predicate_id,
            initial_entities,
        )
        .map_err(|e| RetrievalError::from(e))?;

    Ok(())
}

// This whole impl is conditionalized by the wasm feature flag
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Context {
    #[wasm_bindgen(js_name = "node_id")]
    pub fn js_node_id(&self) -> proto::EntityId { self.0.node_id() }
}

// This impl may or may not have the wasm_bindgen attribute but the functions will always be defined
#[cfg_attr(feature = "wasm", wasm_bindgen)]
impl Context {
    /// Begin a transaction.
    pub fn begin(&self) -> Transaction { Transaction::new(self.0.clone()) }
}

impl Context {
    pub fn new<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
        data: PA::ContextData,
    ) -> Self {
        Self(Arc::new(NodeAndContext { node, cdata: data }))
    }

    pub fn node_id(&self) -> proto::EntityId { self.0.node_id() }

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

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let entity = self.0.get_entity(id, &R::collection(), false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let entity = self.0.get_entity(id, &R::collection(), true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<ResultSet<R>, RetrievalError> {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        let collection_id = R::Model::collection();

        let entities = self.0.fetch_entities(&collection_id, args).await?;

        Ok(EntityResultSet::from_vec(entities, true).map::<R>())
    }

    pub async fn fetch_one<R: View + Clone + 'static>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let views = self.fetch::<R>(args).await?;
        Ok(views.into_iter().next())
    }
    /// Subscribe to changes in entities matching a predicate
    pub fn query<R>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<LiveQuery<R>, RetrievalError>
    where R: View {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;

        use crate::model::Model;
        let collection_id = R::Model::collection();

        // Using one subscription id for local and remote subscriptions
        let sub_id = proto::PredicateId::new();
        // Now set up our local subscription and get the EntityResultSet
        let (local_subscription, entity_resultset) = self.0.query(sub_id, &collection_id, args)?;

        // Create LiveQuery with the typed ResultSet
        Ok(LiveQuery::new(local_subscription, entity_resultset))
    }
    pub async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.0.collection(id).await
    }
}

impl<SE, PA> NodeAndContext<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.node.id, id, collection_id);

        if !self.node.durable {
            // Fetch from peers and commit first response
            match self.node.get_from_peer(collection_id, vec![id], &self.cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = self.node.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", self.node.id);
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", self.node);

        let collection = self.node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                let (_changed, entity) = self
                    .node
                    .entities
                    .with_state(&(collection_id.clone(), self), id, collection_id.clone(), entity_state.payload.state)
                    .await?;
                Ok(entity)
            }
            Err(RetrievalError::EntityNotFound(id)) => {
                let (_, entity) = self
                    .node
                    .entities
                    .with_state(&(collection_id.clone(), self), id, collection_id.clone(), proto::State::default())
                    .await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }
    /// Fetch a list of entities based on a predicate
    pub async fn fetch_entities(&self, collection_id: &CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        // Fetch raw states from storage

        let filtered_predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.predicate)?;

        // TODO implement cached: true
        let states = if !self.node.durable {
            // Fetch from peers and commit first response
            self.node.fetch_from_peer(collection_id, filtered_predicate, &self.cdata).await?
        } else {
            let storage_collection = self.node.collections.get(collection_id).await?;
            storage_collection.fetch_states(&filtered_predicate).await?
        };

        // Convert states to entities
        let mut entities = Vec::new();
        for state in states {
            let (_, entity) = self
                .node
                .entities
                .with_state(&(collection_id.clone(), self), state.payload.entity_id, collection_id.clone(), state.payload.state)
                .await?;
            entities.push(entity);
        }
        Ok(entities)
    }

    pub async fn query<R: View>(
        &self,
        predicate_id: proto::PredicateId,
        collection_id: &CollectionId,
        mut args: MatchArgs,
    ) -> Result<LiveQuery<R>, RetrievalError> {
        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        args.predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.predicate)?;

        // Store subscription context for event requests
        self.node.predicate_context.insert(predicate_id, self.cdata.clone());

        let predicate = args.predicate.clone();
        let cached = args.cached;

        // Create subscription container
        let subscription = self.node.reactor.subscribe();

        // Add the predicate to the subscription
        subscription.add_predicate(predicate_id, collection_id, args.predicate);

        // Create LiveQuery with the subscription
        let handle = unimplemented!(); // LiveQuery::<R>::new(Box::new(Node(self.node.0.clone())) as Box<dyn TNodeErased>, predicate_id, subscription.clone());

        let storage_collection = self.node.collections.get(collection_id).await?;

        let initial_states: Vec<Attested<EntityState>>;
        // Handle remote subscription setup
        if let Some(ref relay) = self.node.subscription_relay {
            relay.notify_subscribe(predicate_id, collection_id.clone(), predicate.clone(), self.cdata.clone());

            if !cached {
                // Create oneshot channel to wait for first remote update
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.node.pending_predicate_subs.insert(predicate_id, tx);

                // Wait for first remote update before initializing
                match rx.await {
                    Err(_) => {
                        // Channel was dropped, proceed with local initialization anyway
                        warn!("Failed to receive first remote update for subscription {}", predicate_id);
                        initial_states = storage_collection.fetch_states(&predicate).await?;
                    }
                    Ok(states) => initial_states = states,
                }
            } else {
                initial_states = storage_collection.fetch_states(&predicate).await?;
            }
        } else {
            initial_states = storage_collection.fetch_states(&predicate).await?;
        }

        // Convert states to entities
        let retriever = LocalRetriever::new(storage_collection.clone());
        let mut initial_entities = Vec::new();
        for state in initial_states {
            let (_, entity) =
                self.node.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            initial_entities.push(entity);
        }

        self.node.reactor.initialize(subscription.id(), predicate_id, initial_entities)?;

        Ok(handle)
    }

    /// Does all the things necessary to commit a local transaction
    /// notably, the application of events to Entities works differently versus remote transactions
    pub async fn commit_local_trx(&self, trx: Transaction) -> Result<(), MutationError> {
        let (trx_id, entity_events) = trx.into_parts()?;
        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        // Check policy and collect attestations
        for (entity, event) in entity_events {
            let attestation = self.node.policy_agent.check_event(&self.node, &self.cdata, &entity, &event)?;
            let attested = Attested::opt(event.clone(), attestation);
            attested_events.push(attested.clone());
            entity_attested_events.push((entity, attested));
        }

        // Relay to peers and wait for confirmation
        self.node.relay_to_required_peers(&self.cdata, trx_id, &attested_events).await?;

        // All peers confirmed, now we can update local state
        let mut changes: Vec<EntityChange> = Vec::new();
        for (entity, attested_event) in entity_attested_events {
            let collection = self.node.collections.get(&attested_event.payload.collection).await?;
            collection.add_event(&attested_event).await?;
            entity.commit_head(Clock::new([attested_event.payload.id()]));

            let collection_id = &attested_event.payload.collection;
            // If this entity has an upstream, propagate the changes
            if let Some(ref upstream) = entity.upstream {
                upstream.apply_event(&(collection_id.clone(), self), &attested_event.payload).await?;
            }

            // Persist

            let state = entity.to_state()?;

            let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
            let attestation = self.node.policy_agent.attest_state(&self.node, &entity_state);
            let attested = Attested::opt(entity_state, attestation);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(entity.clone(), vec![attested_event])?);
        }

        // Notify reactor of ALL changes
        self.node.reactor.notify_change(changes);
        Ok(())
    }
}

/// A local subscription that handles both reactor subscription and remote cleanup
/// This is a type-erased version that can be used in the TContext trait
pub struct LocalSubscription {
    pub(crate) predicate_id: proto::PredicateId,
    pub(crate) node: Box<dyn TNodeErased>,
    pub(crate) peers: Vec<proto::EntityId>,
    // Store the actual subscription - now non-generic!
    pub(crate) subscription: ReactorSubscription,
}

impl LocalSubscription {
    pub fn new(node: Box<dyn TNodeErased>, id: proto::PredicateId, subscription: ReactorSubscription) -> Self {
        Self { predicate_id: id, node, peers: Vec::new(), subscription }
    }
}

impl Drop for LocalSubscription {
    fn drop(&mut self) {
        // Handle remote subscription cleanup
        // FIXME: Add relay.notify_unsubscribe, node.predicate_context.remove, node.pending_subs.remove
        warn!("LocalSubscription dropped - remote cleanup not yet implemented");
    }
}

impl Subscribe<ReactorUpdate> for LocalSubscription {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<ReactorUpdate> {
        // FIXME
        unimplemented!()
        // self.subscription.subscribe(listener)
    }
}
// Implement Signal trait for LocalSubscription
// impl Signal for LocalSubscription {
//     fn listen(&self, listener: Listener) -> ListenerGuard { self.subscription.listen(listener) }

//     fn broadcast_id(&self) -> BroadcastId { self.subscription.broadcast_id() }
// }

// FIXME: LiveQuery needs to impl Clone so Subscribe can close over a clone to keep it alive until the SubscriptionGuard is dropped
/// LiveQuery is a typed handle to a LocalSubscription that automatically wraps Entities in View: R
pub struct LiveQuery<R: View> {
    local_subscription: LocalSubscription,
    resultset: ResultSet<R>,
}

impl<R: View> LiveQuery<R> {
    pub fn new(local_subscription: LocalSubscription, entity_resultset: EntityResultSet) -> Self {
        let resultset = entity_resultset.map::<R>();
        Self { local_subscription, resultset }
    }
}

impl<R: View> std::fmt::Debug for LiveQuery<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LiveQuery({:?})", self.local_subscription.predicate_id)
    }
}

// Implement Signal trait - delegate to the resultset
impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.resultset.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.resultset.broadcast_id() }
}

// Implement Get trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> { self.resultset.get() }
}

// Implement Peek trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Peek<Vec<R>> for LiveQuery<R> {
    fn peek(&self) -> Vec<R> { self.resultset.peek() }
}

// Implement Subscribe trait - convert ReactorUpdate to ChangeSet<R>
impl<R: View> Subscribe<ChangeSet<R>> for LiveQuery<R>
where R: Clone + Send + Sync + 'static
{
    fn subscribe<L>(&self, listener: L) -> SubscriptionGuard
    where L: IntoSubscribeListener<ChangeSet<R>> {
        let listener = listener.into_subscribe_listener();

        let me = *self.clone();
        // Subscribe to the underlying ReactorUpdate stream and convert to ChangeSet<R>
        let guard = self.local_subscription.subscribe(move |reactor_update: ReactorUpdate| {
            // Convert ReactorUpdate to ChangeSet<R>
            let changeset: ChangeSet<R> = (me.resultset.clone(), reactor_update).into();
            listener(changeset);
        });

        guard
    }
}

impl<R: View + Clone> From<(ResultSet<R>, ReactorUpdate)> for ChangeSet<R> {
    fn from((resultset, reactor_update): (ResultSet<R>, ReactorUpdate)) -> Self {
        use crate::changes::{ChangeSet, ItemChange};

        let mut changes = Vec::new();

        for item in reactor_update.items {
            let view = R::from_entity(item.entity.clone());

            // Determine the change type based on predicate relevance
            if let Some((_, membership_change)) = item.predicate_relevance.first() {
                match membership_change {
                    crate::reactor::MembershipChange::Initial => {
                        changes.push(ItemChange::Initial { item: view.clone() });
                    }
                    crate::reactor::MembershipChange::Add => {
                        changes.push(ItemChange::Add { item: view.clone(), events: item.events });
                    }
                    crate::reactor::MembershipChange::Remove => {
                        changes.push(ItemChange::Remove { item: view.clone(), events: item.events });
                    }
                }
            } else {
                // No membership change, just an update
                changes.push(ItemChange::Update { item: view.clone(), events: item.events });
            }
        }

        ChangeSet { changes, resultset }
    }
}
