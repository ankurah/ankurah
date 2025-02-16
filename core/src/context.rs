use std::sync::Arc;

use crate::{
    error::RetrievalError,
    model::{Entity, View},
    node::{MatchArgs, Node},
    policy::PolicyAgent,
    reactor::Reactor,
    resultset::ResultSet,
    storage::StorageCollectionWrapper,
    transaction::Transaction,
};
use ankurah_proto as proto;
use async_trait::async_trait;
use tracing::info;

/// Type-erased context wrapper
pub struct Context(Box<dyn TContext>);

pub struct NodeAndContext<PA: PolicyAgent> {
    node: Node<PA>,
    cdata: PA::ContextData,
}

#[async_trait]
pub trait TContext {
    fn node_id(&self) -> proto::NodeId;
    fn next_entity_id(&self) -> proto::ID;
    async fn get_entity(&self, id: proto::ID, collection: &proto::CollectionId) -> Result<Arc<Entity>, RetrievalError>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Arc<Entity>>, RetrievalError>;
    async fn insert_entity(&self, entity: Arc<Entity>) -> anyhow::Result<()>;
    async fn commit_events(&self, events: &Vec<proto::Event>) -> anyhow::Result<()>;
    async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection: &proto::CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(crate::changes::ChangeSet<Arc<Entity>>) + Send + Sync + 'static>,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError>;
    fn cloned(&self) -> Box<dyn TContext>;
    async fn collection(&self, id: &proto::CollectionId) -> StorageCollectionWrapper;
}

#[async_trait]
impl<PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<PA> {
    fn node_id(&self) -> proto::NodeId { self.node.id.clone() }
    fn next_entity_id(&self) -> proto::ID { self.node.next_entity_id() }
    async fn get_entity(&self, id: proto::ID, collection: &proto::CollectionId) -> Result<Arc<Entity>, RetrievalError> {
        self.node.get_entity(collection, id /*&self.cdata*/).await
    }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Arc<Entity>>, RetrievalError> {
        self.node.fetch_entities(collection, args, &self.cdata).await
    }
    async fn insert_entity(&self, entity: Arc<Entity>) -> anyhow::Result<()> { self.node.insert_entity(entity).await }
    async fn commit_events(&self, events: &Vec<proto::Event>) -> anyhow::Result<()> { self.node.commit_events(events).await }
    async fn subscribe(
        &self,
        sub_id: proto::SubscriptionId,
        collection: &proto::CollectionId,
        args: MatchArgs,
        callback: Box<dyn Fn(crate::changes::ChangeSet<Arc<Entity>>) + Send + Sync + 'static>,
    ) -> Result<crate::subscription::SubscriptionHandle, RetrievalError> {
        self.node.subscribe(sub_id, collection, args, callback).await
    }
    fn cloned(&self) -> Box<dyn TContext> { Box::new(NodeAndContext { node: self.node.clone(), cdata: self.cdata.clone() }) }
    async fn collection(&self, id: &proto::CollectionId) -> StorageCollectionWrapper { self.node.collection(id).await }
}

impl Context {
    pub fn new<PA: PolicyAgent + Send + Sync + 'static>(node: Node<PA>, data: PA::ContextData) -> Self {
        Self(Box::new(NodeAndContext { node, cdata: data }))
    }

    pub fn node_id(&self) -> proto::NodeId { self.0.node_id() }

    /// Begin a transaction.
    pub fn begin(&self) -> Transaction { Transaction::new(self.0.cloned()) }
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

    pub async fn get<R: View>(&self, id: proto::ID) -> Result<R, RetrievalError> {
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

        Ok(ResultSet { items: views })
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
                    info!("Node notified");
                    callback(changeset.into());
                }),
            )
            .await?;
        Ok(handle)
    }
    pub async fn collection(&self, id: &proto::CollectionId) -> StorageCollectionWrapper { self.0.collection(id).await }
}
