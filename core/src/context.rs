use std::sync::Arc;

use crate::{
    changes::{ChangeSet, EntityChange},
    consistency::diff_resolver::DiffResolver,
    entity::Entity,
    error::{MutationError, RetrievalError},
    model::View,
    node::{MatchArgs, Node, TNodeErased},
    policy::{AccessDenied, PolicyAgent},
    resultset::ResultSet,
    retrieve::localrefetch::LocalRefetcher,
    storage::{StorageCollectionWrapper, StorageEngine},
    subscription::SubscriptionHandle,
    transaction::Transaction,
};
use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityState};
use async_trait::async_trait;
use tracing::debug;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Type-erased context wrapper
#[cfg_attr(feature = "wasm", wasm_bindgen)]
pub struct Context(Arc<dyn TContext + Send + Sync + 'static>);
impl Clone for Context {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct NodeAndContext<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: Node<SE, PA>,
    pub cdata: PA::ContextData,
}

impl<SE, PA> Clone for NodeAndContext<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn clone(&self) -> Self { Self { node: self.node.clone(), cdata: self.cdata.clone() } }
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
    async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection: &proto::CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(crate::changes::ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError>;
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
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get_resident(&id) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: Transaction) -> Result<(), MutationError> { self.commit_local_trx(trx).await }
    async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection: &proto::CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(crate::changes::ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError> {
        self.subscribe(sub_id, collection, args, callback).await
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.system.collection(id).await
    }
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

        let views = entities.into_iter().map(|entity| R::from_entity(entity)).collect();

        Ok(ResultSet { items: views, loaded: true })
    }

    pub async fn fetch_one<R: View>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let result_set = self.fetch::<R>(args).await?;
        Ok(result_set.items.into_iter().next())
    }
    /// Subscribe to changes in entities matching a predicate
    pub async fn subscribe<F, R>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
        callback: F,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError>
    where
        F: Fn(crate::changes::ChangeSet<R>) + Send + Sync + 'static,
        R: View,
    {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;

        use crate::model::Model;
        let collection_id = R::Model::collection();

        // Using one subscription id for local and remote subscriptions
        let sub_id = proto::SubscriptionId::new();
        // Now set up our local subscription
        let handle = self
            .0
            .subscribe(
                sub_id,
                &collection_id,
                args,
                Box::new(move |changeset| {
                    callback(changeset.into());
                }),
            )
            .await?;
        Ok(handle)
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
    // pub(crate) async fn get_entity(
    //     &self,
    //     collection_id: &CollectionId,
    //     id: proto::EntityId,
    //     cached: bool,
    // ) -> Result<Entity, RetrievalError> {
    //     debug!("Node({}).get_entity {:?}-{:?}", self.node.id, id, collection_id);

    //     if !self.node.durable {
    //         // Fetch from peers and commit first response
    //         match self.node.get_from_peer(collection_id, vec![id], &self.cdata).await {
    //             Ok(_) => (),
    //             Err(RetrievalError::NoDurablePeers) if cached => (),
    //             Err(e) => {
    //                 return Err(e);
    //             }
    //         }
    //     }

    //     if let Some(local) = self.node.entities.get(&id) {
    //         debug!("Node({}).get_entity found local entity - returning", self.node.id);
    //         return Ok(local);
    //     }
    //     debug!("{}.get_entity fetching from storage", self.node);

    //     let collection = self.node.collections.get(collection_id).await?;
    //     match collection.get_state(id).await {
    //         Ok(entity_state) => {
    //             let (_changed, entity) = self
    //                 .node
    //                 .entities
    //                 .with_state(&(collection_id.clone(), self), id, collection_id.clone(), entity_state.payload.state)
    //                 .await?;
    //             Ok(entity)
    //         }
    //         Err(RetrievalError::EntityNotFound(id)) => {
    //             let (_, entity) = self
    //                 .node
    //                 .entities
    //                 .with_state(&(collection_id.clone(), self), id, collection_id.clone(), proto::State::default())
    //                 .await?;
    //             Ok(entity)
    //         }
    //         Err(e) => Err(e),
    //     }
    // }
    /// Fetch a list of entities based on a predicate
    pub async fn fetch_entities(&self, collection_id: &CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        // TODO implement cached: true
        if !self.node.durable {
            // Fetch from peers and commit first response
            match self.node.entities.fetch(collection_id, &args.predicate, &self.cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if args.cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        // Use EntityManager's fetch_entities method directly
        let filtered_predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.predicate)?;
        self.node.entities.fetch(collection_id, &filtered_predicate, &self.cdata).await
    }

    pub async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection_id: &CollectionId,
        mut args: MatchArgs,
        callback: Box<dyn Fn(ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<SubscriptionHandle, RetrievalError> {
        let handle = SubscriptionHandle::new(Box::new(Node(self.node.0.clone())) as Box<dyn TNodeErased>, sub_id);

        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        args.predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.predicate)?;

        // Store subscription context for event requests
        self.node.subscription_context.insert(sub_id, self.cdata.clone());

        // Create the subscription (synchronous)
        let subscription =
            crate::subscription::Subscription::new(handle.id, collection_id.clone(), args.predicate.clone(), Arc::new(callback));

        match &self.node.subscription_relay {
            None => {
                // For durable nodes, create a simple fetcher that uses EntityManager directly
                let entities = self.node.entities.clone();
                let fetch_fn = move |collection_id: &CollectionId, predicate: &ankql::ast::Predicate| {
                    let entities = entities.clone();
                    let collection_id = collection_id.clone();
                    let predicate = predicate.clone();
                    async move { entities.fetch(&collection_id, &predicate, &self.cdata).await }
                };
                // TODO: Update reactor.register to accept the fetch function instead of a Fetch trait object
                todo!("Update reactor to use EntityManager directly")
            }
            Some(relay) => {
                // DiffResolver is used to detect and refresh entities which are present in our initial local fetch but not in the remote fetch
                let resolver = DiffResolver::new(self, collection_id.clone());

                relay.register(subscription.clone(), self.cdata.clone(), resolver.clone())?;

                // LocalRefetcher fetches from local storage, then calls resolver.local_entity_ids()
                // then, if use_cache is true, it yields that initial set of entities to fetcher.fetch inside reactor.register
                // then if use_cache is false, it will call resolver.wait_resolution() which resolves the differences and returns true if differences were detected
                // or returns false if no differences were detected
                // the LocalRefetcher will redo the fetch in the case that the resolver returns true
                // or it will return the initial set of entities if the resolver returns false

                let refetcher = LocalRefetcher::new(self.node.collections.clone(), self.node.entities.clone(), resolver, args.cached);
                self.node.reactor.register(subscription.clone(), refetcher)?;
            }
        }

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
                unimplemented!()
                // upstream.apply_event(&(collection_id.clone(), self), &attested_event.payload).await?;
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
