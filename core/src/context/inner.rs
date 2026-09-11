use crate::internal::prelude::*;
use crate::policy::ReadPolicy;
use crate::reactor::LocalEntitySource;
use crate::retrieval::CachedEventGetter;
use ankql::ast::{Parsed, Resolved};
use ankurah_proto::Event;
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};
use tracing::debug;

use super::{DynContextInner, SchemaResolver};

pub(crate) enum ContextAuth<PA>
where PA: PolicyAgent
{
    Sessions(crate::session::SessionSet<PA::ContextData>),
    /// Local authority for system/catalog writes; never user-constructible.
    Privileged,
}

pub(crate) struct ContextInner<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: NodeHandle<SE, PA>,
    pub auth: ContextAuth<PA>,
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> DynContextInner for ContextInner<SE, PA> {
    /// Remove the remote subscription when the livequery is dropped.
    fn unsubscribe_remote_query(&self, query_id: proto::QueryId) {
        if let Ok(node) = self.node.upgrade() {
            if let Some(relay) = &node.subscription_relay {
                relay.unsubscribe_predicate(query_id);
            }
        }
    }

    /// Subscribe or update a resolved livequery using this context's sessions.
    /// Return `false` if the node has no relay.
    fn subscribe_remote_query(
        &self,
        query: &EntityLiveQuery,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<bool, RetrievalError> {
        let node = self.node.upgrade()?;
        if node.subscription_relay.is_none() {
            return Ok(false);
        }
        let sessions = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.clone(),
            ContextAuth::Privileged => return Err(RetrievalError::Other("the privileged context does not query".into())),
        };
        node.subscribe_remote_query(query.query_id(), query.collection_id().clone(), selection, sessions, version, query.weak());
        Ok(true)
    }

    /// Return the reactor used to activate livequeries, or `None` if the node has been dropped.
    fn reactor(&self) -> Option<crate::reactor::Reactor> { self.node.upgrade().ok().map(|node| node.reactor.clone()) }

    /// Access this context's descriptor binding and selection resolution.
    fn schema_resolver(&self) -> &dyn SchemaResolver { self }

    /// Return the node's identity, even if its weak handle can no longer upgrade.
    fn node_id(&self) -> proto::EntityId { self.node.node_id() }

    /// This node's system root entity id, which every non-root genesis binds
    /// into its own id. `None` before the node has created or adopted a system.
    fn system_id(&self) -> Option<proto::EntityId> { self.node.upgrade().ok().and_then(|node| node.system.root_id()) }

    /// Create an entity from its genesis event, returning its transaction-local state.
    fn create_entity(&self, collection: proto::CollectionId, genesis: &Event, trx_alive: Arc<AtomicBool>) -> Result<Entity, MutationError> {
        self.node.upgrade()?.entities.create_entity(collection, genesis, trx_alive)
    }

    /// Check whether this context may write the entity.
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        let node = self.node.upgrade()?;
        match &self.auth {
            ContextAuth::Sessions(sessions) => {
                crate::node::event_admissibility::check_unprivileged_write(entity.collection())
                    .map_err(|_| AccessDenied::ByPolicy("reserved collections accept writes only from the node's privileged context"))?;
                node.policy_agent.check_write(&sessions.write_credential()?, entity, None)
            }
            ContextAuth::Privileged => Ok(()),
        }
    }

    /// Retrieve an entity and enforce this context's read policy.
    async fn get_entity(&self, collection_id: &CollectionId, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        debug!("Node({}).get_entity {:?}-{:?}", node.id, id, collection_id);
        let cdata = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.current(),
            ContextAuth::Privileged => Vec::new(),
        };

        if !node.durable {
            // Fetch from peers and commit first response
            match node.get_from_peer(collection_id, vec![id], &cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let policy = match &self.auth {
            ContextAuth::Privileged => ReadPolicy::privileged(collection_id),
            ContextAuth::Sessions(_) => ReadPolicy::new(&node.policy_agent, &cdata, collection_id),
        };

        if let Some(local) = node.entities.get(&id) {
            if local.collection() != collection_id {
                return Err(RetrievalError::EntityNotFound(id));
            }
            debug!("Node({}).get_entity found local entity - returning", node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            policy.check_read(&entity_id, &state)?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", node.as_ref());

        let collection = node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                if &entity_state.payload.collection != collection_id {
                    return Err(RetrievalError::EntityNotFound(id));
                }
                policy.check_read(&entity_state.payload.entity_id, &entity_state.payload.state)?;
                let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
                let event_getter = CachedEventGetter::new(collection_id.clone(), collection, node.as_ref(), &cdata);
                let (_changed, entity) =
                    node.entities.with_state(&state_getter, &event_getter, id, collection_id.clone(), entity_state.payload.state).await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch a resolved selection with this context's read restrictions.
    /// Shared by `Context::fetch` and livequery gap filling.
    async fn fetch_entities(&self, collection_id: &CollectionId, mut args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        let cdata = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.current(),
            ContextAuth::Privileged => Vec::new(),
        };
        let policy = match &self.auth {
            ContextAuth::Privileged => ReadPolicy::privileged(collection_id),
            ContextAuth::Sessions(_) => ReadPolicy::new(&node.policy_agent, &cdata, collection_id),
        };
        policy.check_collection()?;
        args.selection.predicate = policy.filter_predicate(args.selection.predicate)?;

        // TODO implement cached: true
        if !node.durable {
            node.fetch_from_peer(collection_id, args.selection, &cdata).await
        } else {
            let storage_collection = node.collections.get(collection_id).await?;
            let states = storage_collection.fetch_states(&args.selection).await?;

            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(storage_collection.clone());
            let event_getter = CachedEventGetter::new(collection_id.clone(), storage_collection, node.as_ref(), &cdata);
            for state in states {
                let (_, entity) = node
                    .entities
                    .with_state(&state_getter, &event_getter, state.payload.entity_id, collection_id.clone(), state.payload.state)
                    .await?;
                entities.push(entity);
            }
            Ok(entities)
        }
    }

    /// Validate and commit the transaction, then publish its entity changes.
    /// Privileged contexts bypass policy, not epoch or event-validity checks.
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        let node = self.node.upgrade()?;
        crate::transaction::commit::commit(node.as_ref(), &self.auth, trx).await
    }

    /// Construct a livequery, resolving its selection now or scheduling asynchronous resolution.
    fn query(
        self: Arc<Self>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError> {
        if matches!(&self.auth, ContextAuth::Privileged) {
            return Err(RetrievalError::Other("the privileged context does not query".into()));
        }
        let node = self.node.upgrade()?;
        EntityLiveQuery::new_with_context(node.as_ref(), self.clone(), schema, collection_id, args)
    }

    /// Open a storage collection for tests, bypassing context policy checks.
    #[cfg(feature = "test-helpers")]
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.upgrade()?.system.collection(id).await
    }
}

#[async_trait]
impl<SE, PA> LocalEntitySource for ContextInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.node.upgrade()?.fetch_entities_from_local(collection_id, selection).await
    }
}
