use std::sync::Arc;

use crate::{
    changes::{ChangeSet, EntityChange},
    consistency::diff_resolver::DiffResolver,
    datagetter::{DataGetter, LocalGetter},
    entity::Entity,
    error::{MutationError, RetrievalError},
    model::View,
    node::{MatchArgs, Node, TNodeErased},
    policy::{AccessDenied, PolicyAgent},
    resultset::ResultSet,
    storage::{StorageCollectionWrapper, StorageEngine},
    subscription::{Subscription, SubscriptionHandle},
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

pub struct NodeAndContext<SE, PA, DG>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    DG: DataGetter<SE, PA> + Send + Sync + 'static,
{
    pub node: Node<SE, PA, DG>,
    pub cdata: PA::ContextData,
}

impl<SE, PA, DG> Clone for NodeAndContext<SE, PA, DG>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    DG: DataGetter<SE, PA> + Send + Sync + 'static,
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
impl<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        DG: DataGetter<SE, PA> + Send + Sync + 'static,
    > TContext for NodeAndContext<SE, PA, DG>
{
    fn node_id(&self) -> proto::EntityId { self.node.id }
    fn create_entity(&self, collection: proto::CollectionId) -> Entity { self.node.entities.create(collection) }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> { self.node.policy_agent.check_write(&self.cdata, entity, None) }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.node.entities.get(collection, &id, &self.cdata).await
    }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.node.entities.fetch(collection, &args.predicate, &self.cdata).await
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
    pub fn new<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        DG: DataGetter<SE, PA> + Send + Sync + 'static,
    >(
        node: Node<SE, PA, DG>,
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

impl<SE, PA, DG> NodeAndContext<SE, PA, DG>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    DG: DataGetter<SE, PA> + Send + Sync + 'static,
{
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
        let subscription = Subscription::new(handle.id, collection_id.clone(), args.predicate.clone(), Arc::new(callback));
        self.node.reactor.register(subscription.clone());

        match &self.node.subscription_relay {
            None => {
                let fut = self.node.entities.fetch(&collection_id, &args.predicate, &self.cdata);
                subscription.initialize(fut);
            }
            Some(relay) => {
                let fut = relay.register(subscription.clone(), self.cdata.clone())?;
                subscription.initialize(fut);
            }
        };

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
            let attestation = self.node.policy_agent.check_event(&self.cdata, &entity, &event)?;
            let attested = Attested::opt(event.clone(), attestation);
            attested_events.push(attested.clone());
            entity_attested_events.push((entity, attested));
        }

        // Relay to peers and wait for confirmation
        self.node.relay_to_required_peers(&self.cdata, trx_id, &attested_events).await?;
        self.node.entities.commit_events(entity_attested_events);

        Ok(())
    }
}
