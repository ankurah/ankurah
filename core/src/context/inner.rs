use crate::internal::prelude::*;
use crate::{error::NodeDropped, node::NodeErased};
use crate::policy::ContextPolicy;
use crate::reactor::LocalEntitySource;
use crate::retrieval::CachedEventGetter;
use ankql::ast::{Parsed, Resolved};
use ankurah_proto::Event;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::debug;

use super::{DynContextInner, SchemaResolver};

#[derive(Clone)]
pub(crate) enum ContextAuth<C> {
    Sessions(C),
    /// Local authority for system/catalog writes; never user-constructible.
    Privileged,
}

pub(crate) struct ContextInner<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: NodeHandle<SE, PA>,
    pub auth: ContextAuth<SessionSet<PA::ContextData>>,
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> DynContextInner for ContextInner<SE, PA> {
    /// Access the node, retaining it for the caller; fail if a weak handle has expired.
    fn node(&self) -> Result<Arc<dyn NodeErased>, NodeDropped> { Ok(self.node.upgrade()?.0.clone()) }

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
        node.subscribe_remote_query(query.query_id(), selection, sessions, version, query.weak());
        Ok(true)
    }

    /// Access this context's descriptor binding and selection resolution.
    fn schema_resolver(&self) -> &dyn SchemaResolver { self }

    /// Return the node's identity, even if its weak handle can no longer upgrade.
    fn node_id(&self) -> proto::EntityId { self.node.node_id() }

    /// This node's system root entity id, which every non-root genesis binds
    /// into its own id. `None` before the node has created or adopted a system.
    fn system_id(&self) -> Option<proto::EntityId> { self.node.upgrade().ok().and_then(|node| node.system.root_id()) }

    /// Check whether this context may write the entity.
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        let node = self.node.upgrade()?;
        ContextPolicy::new(&node.policy_agent, self.auth.clone()).check_write(entity)
    }

    /// Retrieve an entity and enforce this context's read policy.
    async fn get_entity(&self, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        debug!("Node({}).get_entity {:?}", node.id, id);
        let policy = ContextPolicy::new(&node.policy_agent, self.auth.clone());
        let cdata = policy.credentials();

        if !node.durable {
            // Fetch from peers and commit first response
            match node.get_from_peer(vec![id], &cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = node.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            policy.check_read(&entity_id, &state)?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", node.as_ref());

        let entity_state: proto::Attested<proto::EntityState> = node.storage.get_states(vec![id], &policy.retrieval_predicate()).await?
            .into_iter().next().ok_or(RetrievalError::EntityNotFound(id))?.try_into()?;
        policy.check_read_state(&id, &entity_state.payload.state)?;
        let state_getter = crate::retrieval::LocalStateGetter::new(node.storage.clone());
        let event_getter = CachedEventGetter::new(node.as_ref(), &cdata);
        let (_changed, entity) = node.entities.with_state(&state_getter, &event_getter, id, entity_state.payload.state).await?;
        Ok(entity)
    }

    /// Fetch a resolved selection with this context's read restrictions.
    /// Shared by `Context::fetch` and livequery gap filling.
    async fn fetch_entities(&self, mut args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        let node = self.node.upgrade()?;
        node.system.check_not_halted()?;
        let policy = ContextPolicy::new(&node.policy_agent, self.auth.clone());
        let cdata = policy.credentials();
        args.selection.predicate = policy.filter_predicate(args.selection.predicate)?;

        // TODO honor CachePolicy::Local for fetch; Tracked currently behaves like Durable.
        let entities = if !node.durable {
            node.fetch_from_peer(args.selection, &cdata).await
        } else {
            let states = node.storage.fetch_states(&args.selection).await?;

            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(node.storage.clone());
            let event_getter = CachedEventGetter::new(node.as_ref(), &cdata);
            for state in states {
                let (_, entity) =
                    node.entities.with_state(&state_getter, &event_getter, state.payload.entity_id, state.payload.state).await?;
                entities.push(entity);
            }
            Ok(entities)
        }?;
        Ok(entities.into_iter().filter(|entity| policy.can_read(entity)).collect())
    }

    /// Validate and commit the transaction, then publish its entity changes.
    /// Privileged contexts bypass policy, not epoch or event-validity checks.
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        let node = self.node.upgrade()?;
        crate::transaction::commit::commit(node.as_ref(), &self.auth, trx).await
    }

    /// Construct a livequery, resolving its selection now or scheduling asynchronous resolution.
    fn query(self: Arc<Self>, schema: &'static ModelStructDescriptor, args: MatchArgs<Parsed>) -> Result<EntityLiveQuery, RetrievalError> {
        if matches!(&self.auth, ContextAuth::Privileged) {
            return Err(RetrievalError::Other("the privileged context does not query".into()));
        }
        let resolution = crate::livequery::QueryResolution::prepare(self.schema_resolver(), schema, args.selection)?;
        EntityLiveQuery::new(self, args.cache_policy, resolution)
    }
}

#[async_trait]
impl<SE, PA> LocalEntitySource for ContextInner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn can_read(&self, entity: &Entity) -> bool {
        let Ok(node) = self.node.upgrade() else { return false };
        ContextPolicy::new(&node.policy_agent, self.auth.clone()).can_read(entity)
    }

    async fn fetch_entities_from_local(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let node = self.node.upgrade()?;
        let policy = ContextPolicy::new(&node.policy_agent, self.auth.clone());
        let entities = node.fetch_entities_from_local(selection).await?;
        Ok(entities.into_iter().filter(|entity| policy.can_read(entity)).collect())
    }
}
