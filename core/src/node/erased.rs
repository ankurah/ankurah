use ankql::ast::{Parsed, Resolved};
use ankurah_proto::{self as proto, CollectionId};

use super::{Node, NodeType};
use crate::entity::Entity;
use crate::error::RetrievalError;
use crate::policy::PolicyAgent;
use crate::reactor::{AbstractEntity, Reactor};
use crate::selection::filter::Filterable;
use crate::storage::StorageEngine;

#[async_trait::async_trait]
pub trait TNodeErased<E: AbstractEntity + Filterable + Send + 'static = Entity>: Send + Sync + 'static {
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId);
    fn update_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error>;
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<E>, RetrievalError>;
    fn reactor(&self) -> &Reactor<E>;
    fn has_subscription_relay(&self) -> bool;
    /// Bind a selection's property names through this node's catalog (the
    /// raw, collection-scoped resolution). A replacement live-query
    /// selection arrives here as a parsed string, below the typed entries,
    /// and must resolve before the reactor or the relay sees it.
    fn resolve_selection(
        &self,
        collection: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError>;
    /// Whether a durable authority has answered this node's catalog
    /// projection: the point past which a raw-name miss is authoritative.
    /// Defaults for test doubles with no catalog.
    fn is_catalog_synced(&self) -> bool { true }
    /// Wait for the point [`Self::is_catalog_synced`] probes.
    async fn wait_catalog_synced(&self) -> Result<(), RetrievalError> { Ok(()) }
}

#[async_trait::async_trait]
impl<SE, PA> TNodeErased<Entity> for Node<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId) {
        // Notify subscription relay for remote cleanup
        if let Some(ref relay) = self.subscription_relay {
            relay.unsubscribe_predicate(query_id);
        }
    }

    fn resolve_selection(
        &self,
        collection: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError> {
        self.catalog.resolve_selection(collection, selection)
    }

    fn is_catalog_synced(&self) -> bool { self.catalog.is_synced() }

    async fn wait_catalog_synced(&self) -> Result<(), RetrievalError> { self.catalog.wait_synced().await }

    fn update_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error> {
        if let Some(ref relay) = self.subscription_relay {
            // Admitted at query entry; forwarded as-is.
            relay.update_query(query_id, selection, version)?;
        }
        Ok(())
    }

    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        Node::fetch_entities_from_local(self, collection_id, selection).await
    }

    fn reactor(&self) -> &Reactor<Entity> { &self.0.reactor }

    fn has_subscription_relay(&self) -> bool { self.subscription_relay.is_some() }
}

/// Type erasure over [`NodeType`]: the query's node handle without the
/// node's generics. Whether it keeps the node alive is the NodeType
/// variant's choice.
pub(crate) trait ErasedNodeRef: Send + Sync {
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>>;
}

impl<SE, PA> ErasedNodeRef for NodeType<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>> {
        NodeType::upgrade(self).map(|node| Box::new(node.as_ref().clone()) as Box<dyn TNodeErased>)
    }
}
