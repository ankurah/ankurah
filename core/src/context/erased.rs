use crate::internal::prelude::*;
use crate::reactor::LocalEntitySource;
use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_proto::Event;
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};

use super::SchemaResolver;

/// Type-erased operations on a context's node and auth state.
#[async_trait]
pub(crate) trait DynContextInner: LocalEntitySource {
    /// Remove the remote subscription when the livequery is dropped.
    fn unsubscribe_remote_query(&self, query_id: proto::QueryId);

    /// Subscribe or update a resolved livequery using this context's sessions.
    /// Return `false` if the node has no relay.
    fn subscribe_remote_query(&self, query: &EntityLiveQuery, selection: Selection<Resolved>, version: u32)
        -> Result<bool, RetrievalError>;

    /// Return the reactor used to activate livequeries, or `None` if the node has been dropped.
    fn reactor(&self) -> Option<crate::reactor::Reactor>;

    /// Access this context's descriptor binding and selection resolution.
    fn schema_resolver(&self) -> &dyn SchemaResolver;

    /// Return the node's identity, even if its weak handle can no longer upgrade.
    fn node_id(&self) -> proto::EntityId;

    /// This node's system root entity id, which every non-root genesis binds
    /// into its own id. `None` before the node has created or adopted a system.
    fn system_id(&self) -> Option<proto::EntityId>;

    /// Create an entity from its genesis event, returning its transaction-local state.
    fn create_entity(&self, collection: proto::CollectionId, genesis: &Event, trx_alive: Arc<AtomicBool>) -> Result<Entity, MutationError>;

    /// Check whether this context may write the entity.
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;

    /// Retrieve an entity and enforce this context's read policy.
    async fn get_entity(&self, collection: &proto::CollectionId, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError>;

    /// Fetch a resolved selection with this context's read restrictions.
    /// Shared by `Context::fetch` and livequery gap filling.
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError>;

    /// Validate and commit the transaction, then publish its entity changes.
    /// Privileged contexts bypass policy, not epoch or event-validity checks.
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;

    /// Construct a livequery, resolving its selection now or scheduling asynchronous resolution.
    fn query(
        self: Arc<Self>,
        schema: Option<&'static ModelStructDescriptor>,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError>;

    /// Open a storage collection for tests, bypassing context policy checks.
    #[cfg(feature = "test-helpers")]
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}
