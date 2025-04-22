use std::sync::Arc;

use crate::{
    entity::Entity,
    error::{MutationError, RetrievalError},
    model::View,
    node::{MatchArgs, Node},
    policy::{AccessDenied, PolicyAgent},
    resultset::ResultSet,
    storage::{StorageCollectionWrapper, StorageEngine},
    transaction::Transaction,
};
use ankurah_proto::{self as proto};
use async_trait::async_trait;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Type-erased context wrapper
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
    node: Node<SE, PA>,
    cdata: PA::ContextData,
}

#[async_trait]
pub trait TContext {
    fn node_id(&self) -> proto::EntityId;
    /// Create a brand new entity, and add it to the WeakEntitySet
    /// Note that this does not actually persist the entity to the storage engine
    /// It merely ensures that there are no duplicate entities with the same ID (except forked entities)
    fn create_entity(&self, collection: proto::CollectionId) -> Entity;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId) -> Result<Entity, RetrievalError>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_transaction(&self, trx_id: proto::TransactionId, events: Vec<proto::Event>) -> Result<(), MutationError>;
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
    fn node_id(&self) -> proto::EntityId { self.node.id.clone() }
    fn create_entity(&self, collection: proto::CollectionId) -> Entity { self.node.entities.create(collection) }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        self.node.policy_agent.check_write(&self.cdata, entity, None).into()
    }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId) -> Result<Entity, RetrievalError> {
        self.node.get_entity(collection, id /*&self.cdata*/).await
    }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.node.fetch_entities(collection, args, &self.cdata).await
    }
    async fn commit_transaction(&self, trx_id: proto::TransactionId, events: Vec<proto::Event>) -> Result<(), MutationError> {
        self.node.commit_transaction(&self.cdata, trx_id, events.into_iter().map(|e| e.into()).collect()).await
    }
    // TODO: remove this and handle commit_mut_ref differently
    // async fn commit_transaction(&self, id: &proto::TransactionId, events: Vec<proto::Event>) -> Result<(), MutationError> {
    //     self.node
    //         .commit_transaction(
    //             &self.cdata,
    //             id.clone(),
    //             events.into_iter().map(|e| proto::Attested { payload: e, attestations: vec![] }).collect(),
    //         )
    //         .await
    // }
    async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection: &proto::CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(crate::changes::ChangeSet<Entity>) + Send + Sync + 'static>,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError> {
        self.node.subscribe(&self.cdata, sub_id, collection, args, callback).await
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
        let entity = self.0.get_entity(id, &R::collection()).await?;
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
