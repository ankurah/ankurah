use crate::internal::prelude::*;
use crate::node::event_admissibility::check_membership;
use crate::retrieval::{CachedEventGetter, SuspenseEvents};
use ankql::ast::{Parsed, Resolved};
use ankurah_proto::{Attested, Clock, EntityState, Event};
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};
use tracing::debug;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// A local scope for reads and writes backed by a live credential source.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
#[derive(Clone)]
pub struct Context(Arc<dyn TContext + Send + Sync + 'static>);

pub(crate) enum ContextAuth<PA>
where PA: PolicyAgent
{
    Sessions(crate::session::SessionSet<PA::ContextData>),
    /// Local authority for system/catalog writes; never user-constructible.
    Privileged,
}

pub(crate) struct NodeAndContext<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: NodeType<SE, PA>,
    pub auth: ContextAuth<PA>,
}

#[async_trait]
pub(crate) trait TContext {
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId);
    fn suspend_remote_query(&self, query_id: proto::QueryId);
    fn update_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error>;
    fn reactor(&self) -> Option<crate::reactor::Reactor>;
    fn has_subscription_relay(&self) -> Result<bool, RetrievalError>;
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError>;

    /// Ensure a compiled schema is registered and return its model id and
    /// the epoch that binding belongs to.
    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<(proto::ModelId, crate::schema::SchemaEpoch), crate::schema::registration::RegistrationError>;

    /// Bind every descriptor cell, registering the declaration if necessary.
    async fn bind_or_register(&self, schema: &'static crate::schema::ModelStructDescriptor) -> Result<(), RetrievalError>;

    /// Resolve typed names through descriptor cells and canonicalize literals.
    fn resolve_selection_with_descriptor(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError>;

    fn node_id(&self) -> proto::EntityId;
    /// This node's system root entity id, which every non-root genesis binds
    /// into its own id. `None` before the node has created or joined a system.
    fn system_id(&self) -> Option<proto::EntityId>;
    /// This node's current schema epoch. `None` before a system is ready.
    fn schema_epoch(&self) -> Option<crate::schema::SchemaEpoch>;
    fn bind_descriptor(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        epoch: crate::schema::SchemaEpoch,
    ) -> Result<(), RetrievalError>;
    /// Insert the resident (still empty) entity under the id `genesis`
    /// derived, and return the transaction entity whose baseline is that
    /// genesis. This is what makes the id available when `create()` returns.
    fn create_transaction_entity(
        &self,
        collection: proto::CollectionId,
        genesis: &Event,
        epoch: crate::schema::SchemaEpoch,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError>;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, collection: &proto::CollectionId, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    /// Construct a query now, or defer resolution when its schema/catalog is
    /// not ready yet.
    fn query(
        self: Arc<Self>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError>;

    fn resolve_query_selection(
        &self,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<Option<ankql::ast::Selection<Resolved>>, RetrievalError>;

    fn schedule_query_resolution(&self, query: &EntityLiveQuery, version: u32, cause: crate::livequery::ResolutionCause);
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId) {
        if let Some(node) = self.node.upgrade() {
            if let Some(relay) = &node.subscription_relay {
                relay.unsubscribe_predicate(query_id);
            }
        }
    }

    fn suspend_remote_query(&self, query_id: proto::QueryId) {
        if let Some(node) = self.node.upgrade() {
            if let Some(relay) = &node.subscription_relay {
                relay.suspend_query(query_id);
            }
        }
    }

    fn update_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error> {
        if let Some(relay) = &self.node()?.subscription_relay {
            relay.update_query(query_id, selection, version)?;
        }
        Ok(())
    }

    fn reactor(&self) -> Option<crate::reactor::Reactor> { self.node.upgrade().map(|node| node.reactor.clone()) }

    fn has_subscription_relay(&self) -> Result<bool, RetrievalError> { Ok(self.node()?.subscription_relay.is_some()) }

    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.node()?.fetch_entities_from_local(collection_id, selection).await
    }

    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<(proto::ModelId, crate::schema::SchemaEpoch), crate::schema::registration::RegistrationError> {
        let node = self.node().map_err(crate::schema::registration::RegistrationError::Retrieval)?;
        if let Some(system) = schema.system {
            return Ok((proto::ModelId::System(system), node.system.schema_epoch().unwrap_or(crate::schema::SchemaEpoch::BOOTSTRAP)));
        }
        let epoch = node.system.schema_epoch().ok_or(crate::schema::registration::RegistrationError::SystemNotReady)?;
        match node.catalog.bind_descriptor(Some(epoch), schema) {
            Ok(binding) => return Ok(binding),
            Err(RetrievalError::UnboundDeclaration { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        match &self.auth {
            ContextAuth::Sessions(sessions) => node.catalog.ensure_schema_for_use(&node, &sessions.write_credential()?, schema).await,
            ContextAuth::Privileged => Err(AccessDenied::ByPolicy("the privileged context has no principal to register models as").into()),
        }
    }

    async fn bind_or_register(&self, schema: &'static crate::schema::ModelStructDescriptor) -> Result<(), RetrievalError> {
        self.ensure_registered(schema).await.map(|_| ()).map_err(|error| match error {
            crate::schema::registration::RegistrationError::PolicyDenied(denied) => RetrievalError::Other(format!(
                "model '{}' is not registered in this system, and this credential may not register it: {denied}",
                schema.label
            )),
            other => other.into(),
        })
    }

    fn resolve_selection_with_descriptor(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError> {
        let node = self.node()?;
        node.catalog.resolve_selection_with_descriptor(node.system.schema_epoch(), schema, selection)
    }

    fn node_id(&self) -> proto::EntityId { self.node.node_id() }
    fn system_id(&self) -> Option<proto::EntityId> { self.node.upgrade().and_then(|node| node.system.root_id()) }
    fn schema_epoch(&self) -> Option<crate::schema::SchemaEpoch> { self.node.upgrade().and_then(|node| node.system.schema_epoch()) }
    fn bind_descriptor(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        epoch: crate::schema::SchemaEpoch,
    ) -> Result<(), RetrievalError> {
        let node = self.node()?;
        if schema.system.is_none() && node.system.schema_epoch() != Some(epoch) {
            return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
        }
        node.catalog.bind_descriptor(Some(epoch), schema).map(|_| ())
    }
    fn create_transaction_entity(
        &self,
        collection: proto::CollectionId,
        genesis: &Event,
        epoch: crate::schema::SchemaEpoch,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        self.node()?.entities.create_transaction_entity(collection, genesis, epoch, trx_alive)
    }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        let node = self.node.upgrade().ok_or(AccessDenied::NodeDropped)?;
        match &self.auth {
            ContextAuth::Sessions(_) if crate::schema::is_reserved_collection(entity.collection()) => {
                Err(AccessDenied::ByPolicy("reserved collections accept writes only from the node's privileged context"))
            }
            ContextAuth::Sessions(sessions) => node.policy_agent.check_write(&sessions.write_credential()?, entity, None),
            ContextAuth::Privileged => Ok(()),
        }
    }
    async fn get_entity(&self, collection: &proto::CollectionId, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        use std::sync::atomic::Ordering;

        let node = self.node()?;
        let epoch = node.system.schema_epoch().ok_or(MutationError::SystemNotReady)?;
        let _root_state = node.system.lock_root_state().await;
        if node.system.schema_epoch() != Some(epoch) {
            return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
        }

        if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(MutationError::General("Transaction already committed or rolled back".into()));
        }

        let cdata = match &self.auth {
            ContextAuth::Sessions(sessions) => Some(sessions.write_credential()?),
            ContextAuth::Privileged => None,
        };

        let trx_id = trx.id.clone();
        let genesis_events = trx.genesis_events.read().unwrap().clone();

        let mut entity_events = Vec::new();
        let mut seen_created = std::collections::HashSet::new();
        for entity in trx.entities.iter() {
            if entity.schema_epoch() != epoch && !crate::schema::is_reserved_collection(entity.collection()) {
                return Err(crate::schema::registration::RegistrationError::SystemChanged.into());
            }
            let mut events = Vec::with_capacity(2);
            if let Some((genesis, schema)) = genesis_events.get(&entity.id) {
                if !seen_created.insert(entity.id) {
                    return Err(MutationError::CommitInvariant("two transaction entities claim the same frozen genesis"));
                }
                if genesis.entity_id != entity.id {
                    return Err(MutationError::CommitInvariant("the frozen genesis names an entity other than the one holding it"));
                }
                if !genesis.is_entity_create() {
                    return Err(MutationError::CommitInvariant("the event frozen by create() is not a genesis"));
                }
                genesis.validate_structure()?;
                if entity.head() != Clock::new([genesis.id()]) {
                    return Err(MutationError::CommitInvariant("the created entity's head is not exactly its frozen genesis"));
                }
                check_membership(&node, Some(schema), genesis)?;
                events.push(genesis.clone());
            }

            if let Some(event) = entity.generate_commit_event(proto::AuthorId::Unknown)? {
                check_membership(&node, None, &event)?;
                events.push(event);
            }

            if !events.is_empty() {
                entity_events.push((entity.clone(), events));
            }
        }
        if seen_created.len() != genesis_events.len() {
            return Err(MutationError::CommitInvariant("an entity create() recorded is absent from the transaction's entities"));
        }

        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        for (entity, events) in entity_events {
            if matches!(&self.auth, ContextAuth::Sessions(_)) && crate::schema::is_reserved_collection(entity.collection()) {
                return Err(AccessDenied::ByPolicy("reserved collections accept writes only from the node's privileged context").into());
            }
            let validation_alive = Arc::new(AtomicBool::new(true));

            let mut entity_before = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity.clone(),
            };
            let collection = node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
            let mut entity_attested = Vec::with_capacity(events.len());

            for event in events {
                event_getter.stage_event(event.clone());
                let entity_after = entity_before.snapshot(validation_alive.clone());
                entity_after.apply_event(&event_getter, &event).await?;

                let attestation = match &cdata {
                    Some(cdata) => node.policy_agent.check_event(node.as_ref(), cdata, &entity_before, &entity_after, &event)?,
                    None => None,
                };
                let attested = Attested::opt(event, attestation);

                attested_events.push(attested.clone());
                entity_attested.push(attested);
                entity_before = entity_after;
            }
            entity_attested_events.push((entity, entity_attested));
        }

        for (entity, events) in &entity_attested_events {
            let collection = node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
            for attested in events {
                event_getter.commit_event(attested).await?;
            }
        }

        for (entity, events) in &entity_attested_events {
            if let Some(last) = events.last() {
                entity.commit_head(Clock::new([last.payload.id()]));
            }
        }
        if let Some(cdata) = &cdata {
            node.relay_to_required_peers(cdata, trx_id, &attested_events).await?;
        }

        let mut changes: Vec<EntityChange> = Vec::new();
        for (entity, events) in entity_attested_events {
            let collection = node.collections.get(entity.collection()).await?;

            let canonical_entity = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => {
                    let event_getter = crate::retrieval::LocalEventGetter::new(collection.clone(), node.durable);
                    for attested in &events {
                        upstream.apply_event(&event_getter, &attested.payload).await?;
                    }
                    upstream.clone()
                }
                crate::entity::EntityKind::Primary => entity,
            };

            let state = canonical_entity.to_state()?;

            let entity_state = EntityState { entity_id: canonical_entity.id(), collection: canonical_entity.collection().clone(), state };
            let attestation = match &self.auth {
                ContextAuth::Sessions(_) => node.policy_agent.attest_state(node.as_ref(), &entity_state),
                ContextAuth::Privileged => None,
            };
            let attested = Attested::opt(entity_state, attestation);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(canonical_entity, events)?);
        }

        node.reactor.notify_change(changes).await;

        Ok(attested_events.into_iter().map(|a| a.payload).collect())
    }
    fn query(
        self: Arc<Self>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError> {
        let sessions = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.clone(),
            ContextAuth::Privileged => return Err(RetrievalError::Other("the privileged context does not query".into())),
        };
        let node = self.node()?;
        let epoch = node.system.schema_epoch();
        let resolved = self.resolve_query_selection(schema, &collection_id, args.selection.clone())?;
        let context: Arc<dyn TContext + Send + Sync + 'static> = self.clone();
        Ok(EntityLiveQuery::new_with_context(
            node.as_ref(),
            Context(context),
            schema,
            collection_id,
            args,
            sessions,
            resolved.map(|selection| (selection, epoch)),
        ))
    }

    fn resolve_query_selection(
        &self,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<Option<ankql::ast::Selection<Resolved>>, RetrievalError> {
        let node = self.node()?;
        if node.system.schema_epoch().is_none() || (schema.is_none() && !node.catalog.is_synced()) {
            return Ok(None);
        }
        match self.resolve_and_scope(schema, collection_id, selection) {
            Err(RetrievalError::UnboundDeclaration { .. }) if schema.is_some() => Ok(None),
            result => result.map(Some),
        }
    }

    fn schedule_query_resolution(&self, query: &EntityLiveQuery, version: u32, cause: crate::livequery::ResolutionCause) {
        self.schedule_resolution(query, version, cause);
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node()?.system.collection(id).await
    }
}

// This whole impl is conditionalized by the wasm feature flag
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Context {
    #[wasm_bindgen(js_name = "node_id")]
    pub fn js_node_id(&self) -> proto::EntityId { self.0.node_id() }
}

// Generic methods cannot cross the wasm_bindgen boundary; they live in this
// plain impl and remain host-and-wasm callable from Rust.
impl Context {
    /// Register `M` and return its durable model id. Repeated calls are no-ops.
    pub async fn register_model<M: crate::model::Model>(&self) -> Result<proto::ModelId, crate::schema::registration::RegistrationError> {
        self.0.ensure_registered(M::descriptor()).await.map(|(model, _epoch)| model)
    }
}

// This impl may or may not have the wasm_bindgen attribute but the functions will always be defined
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", uniffi::export)]
impl Context {
    /// Begin a transaction.
    pub fn begin(&self) -> Transaction { Transaction::new(self.0.clone()) }
}

impl Context {
    pub(crate) fn unsubscribe_remote_predicate(&self, query_id: proto::QueryId) { self.0.unsubscribe_remote_predicate(query_id); }

    pub(crate) fn suspend_remote_query(&self, query_id: proto::QueryId) { self.0.suspend_remote_query(query_id); }

    pub(crate) fn update_remote_query(
        &self,
        query_id: proto::QueryId,
        selection: ankql::ast::Selection<Resolved>,
        version: u32,
    ) -> Result<(), anyhow::Error> {
        self.0.update_remote_query(query_id, selection, version)
    }

    pub(crate) fn reactor(&self) -> Option<crate::reactor::Reactor> { self.0.reactor() }

    pub(crate) fn has_subscription_relay(&self) -> Result<bool, RetrievalError> { self.0.has_subscription_relay() }

    pub(crate) fn schema_epoch(&self) -> Option<crate::schema::SchemaEpoch> { self.0.schema_epoch() }

    pub(crate) fn query_entity(
        &self,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError> {
        self.0.clone().query(schema, collection_id, args)
    }

    pub(crate) fn schedule_query_resolution(&self, query: &EntityLiveQuery, version: u32, cause: crate::livequery::ResolutionCause) {
        self.0.schedule_query_resolution(query, version, cause);
    }

    pub(crate) fn resolve_query_selection(
        &self,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<Option<ankql::ast::Selection<Resolved>>, RetrievalError> {
        self.0.resolve_query_selection(schema, collection_id, selection)
    }

    pub(crate) async fn fetch_entities_resolved(
        &self,
        collection_id: &CollectionId,
        selection: ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.0.fetch_entities(collection_id, MatchArgs { selection, cached: false }).await
    }
}

#[async_trait]
impl crate::node::TNodeErased for Context {
    async fn fetch_entities_from_local(
        &self,
        collection_id: &CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Entity>, RetrievalError> {
        self.0.fetch_entities_from_local(collection_id, selection).await
    }
}

impl Context {
    /// Type-erased query and transaction initiation context
    pub fn new<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
        sessions: impl Into<crate::session::SessionSet<PA::ContextData>>,
    ) -> Self {
        let sessions = sessions.into();
        // Attach the source to the node's registry: a live edge keeping
        // it the continuous superset of every session backing a context
        // (a no-op when the source IS the registry).
        node.sessions.attach(&sessions);
        Self(Arc::new(NodeAndContext { node: NodeType::Strong(node), auth: ContextAuth::Sessions(sessions) }))
    }

    /// A context that does NOT keep the node alive, for node-owned machinery
    /// (the catalog projection) whose strong context would cycle.
    pub(crate) fn new_weak<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: &Node<SE, PA>,
        sessions: impl Into<crate::session::SessionSet<PA::ContextData>>,
    ) -> Self {
        let sessions = sessions.into();
        node.sessions.attach(&sessions);
        Self(Arc::new(NodeAndContext { node: NodeType::Weak(node.weak()), auth: ContextAuth::Sessions(sessions) }))
    }

    /// An uncredentialed local context for system and catalog writes.
    pub(crate) fn new_privileged<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
    ) -> Self {
        Self(Arc::new(NodeAndContext { node: NodeType::Strong(node), auth: ContextAuth::Privileged }))
    }

    pub fn node_id(&self) -> proto::EntityId { self.0.node_id() }

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(&R::collection(), id, false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(&R::collection(), id, true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let collection_id = R::Model::collection();
        let args =
            MatchArgs { selection: self.0.resolve_selection_with_descriptor(R::Model::descriptor(), args.selection)?, cached: args.cached };

        let entities = self.0.fetch_entities(&collection_id, args).await?;

        Ok(entities.into_iter().map(|e| R::from_entity(e)).collect())
    }

    pub async fn fetch_one<R: View + Clone + 'static>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let views = self.fetch::<R>(args).await?;
        Ok(views.into_iter().next())
    }
    /// Subscribe to a typed selection, deferring schema registration when needed.
    pub fn query<R>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        Ok(self.0.clone().query(Some(R::Model::descriptor()), R::Model::collection(), args)?.map::<R>())
    }

    /// Subscribe to changes in entities matching a selection and wait for initialization
    pub async fn query_wait<R>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        use crate::model::Model;
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let livequery = self.query::<R>(args)?;
        livequery.wait_initialized().await?;
        Ok(livequery)
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
    fn resolve_and_scope(
        &self,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: &CollectionId,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError> {
        let node = self.node()?;
        let sessions = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions,
            ContextAuth::Privileged => return Err(RetrievalError::Other("the privileged context does not query".into())),
        };
        let exempt = crate::schema::reads_bypass_policy(collection_id);
        let credentials = sessions.current();
        if !exempt {
            node.policy_agent.can_access_collection(&credentials, collection_id)?;
        }
        let mut selection = match schema {
            Some(schema) => node.catalog.resolve_selection_with_descriptor(node.system.schema_epoch(), schema, selection)?,
            None => node.catalog.resolve_selection(collection_id, selection)?,
        };
        if !exempt {
            selection.predicate = node.policy_agent.filter_predicate(&credentials, collection_id, selection.predicate)?;
        }
        Ok(selection)
    }

    fn schedule_resolution(&self, query: &EntityLiveQuery, version: u32, cause: crate::livequery::ResolutionCause) {
        let sessions = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.clone(),
            ContextAuth::Privileged => {
                query.fail_resolution(version, RetrievalError::Other("the privileged context does not query".into()));
                return;
            }
        };
        let node = match self.node() {
            Ok(node) => node,
            Err(error) => {
                query.fail_resolution(version, error);
                return;
            }
        };
        let (schema, collection_id, selection) = query.resolution_state();
        let weak_query = query.weak();
        let mut query_drop = query.drop_receiver();
        let weak_node = node.weak();
        let system = node.system.clone();

        crate::task::spawn(async move {
            if matches!(cause, crate::livequery::ResolutionCause::SystemReset) || system.schema_epoch().is_none() {
                tokio::select! {
                    _ = system.wait_system_ready() => {}
                    _ = query_drop.changed() => return,
                }
            }
            if weak_query.upgrade().is_none() {
                return;
            }
            let Some(node) = weak_node.upgrade() else {
                if let Some(query) = weak_query.upgrade() {
                    query.fail_resolution(version, RetrievalError::Other("Node has been dropped".into()));
                }
                return;
            };
            let context = NodeAndContext { node: NodeType::Weak(weak_node), auth: ContextAuth::Sessions(sessions.clone()) };
            let ready = match schema {
                Some(schema) => context.bind_or_register(schema).await,
                None => node.catalog.wait_synced().await,
            };
            if let Err(error) = ready {
                if let Some(query) = weak_query.upgrade() {
                    query.fail_resolution(version, error);
                }
                return;
            }
            match context.resolve_and_scope(schema, &collection_id, selection) {
                Ok(selection) => {
                    if let Some(query) = weak_query.upgrade() {
                        query.install_resolved(&node, selection, sessions, version, cause);
                    }
                }
                Err(error) => {
                    if let Some(query) = weak_query.upgrade() {
                        query.fail_resolution(version, error);
                    }
                }
            }
        });
    }

    fn node(&self) -> Result<NodeRef<'_, SE, PA>, RetrievalError> {
        self.node.upgrade().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))
    }

    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        let node = self.node()?;
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

        let exempt = matches!(&self.auth, ContextAuth::Privileged) || crate::schema::reads_bypass_policy(collection_id);

        if let Some(local) = node.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            if !exempt {
                node.policy_agent.check_read(&cdata, &entity_id, collection_id, &state)?;
            }
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", node.as_ref());

        let collection = node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                if !exempt {
                    node.policy_agent.check_read(&cdata, &entity_state.payload.entity_id, collection_id, &entity_state.payload.state)?;
                }
                let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
                let event_getter = CachedEventGetter::new(collection_id.clone(), collection, node.as_ref(), &cdata);
                let (_changed, entity) =
                    node.entities.with_state(&state_getter, &event_getter, id, collection_id.clone(), entity_state.payload.state).await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }
    /// Fetch a list of entities based on a selection
    pub async fn fetch_entities(&self, collection_id: &CollectionId, mut args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        let node = self.node()?;
        let cdata = match &self.auth {
            ContextAuth::Sessions(sessions) => sessions.current(),
            ContextAuth::Privileged => Vec::new(),
        };
        if !matches!(&self.auth, ContextAuth::Privileged) && !crate::schema::reads_bypass_policy(collection_id) {
            node.policy_agent.can_access_collection(&cdata, collection_id)?;
            args.selection.predicate = node.policy_agent.filter_predicate(&cdata, collection_id, args.selection.predicate)?;
        }

        // TODO implement cached: true
        if !node.durable {
            Ok(self.fetch_from_peer(collection_id, args.selection, &cdata).await?)
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

    /// Fetch entities from the first available durable peer with known_matches support
    async fn fetch_from_peer(
        &self,
        collection_id: &proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        cdata: &Vec<PA::ContextData>,
    ) -> Result<Vec<crate::entity::Entity>, RetrievalError> {
        let node = self.node()?;
        let epoch =
            node.system.schema_epoch().ok_or_else(|| RetrievalError::Other("cannot fetch before the node has joined a system".into()))?;
        let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        let known_matched_entities = node.fetch_entities_from_local(collection_id, &selection).await?;

        let known_matches = known_matched_entities
            .iter()
            .map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() })
            .collect();

        let selection_clone = selection.clone();
        match node
            .request(peer_id, cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                let _root_state = node.system.lock_root_state().await;
                if node.system.schema_epoch() != Some(epoch) {
                    return Err(RetrievalError::Other("system changed while a fetch was in flight".into()));
                }
                let collection = node.collections.get(collection_id).await?;
                let event_getter = CachedEventGetter::new(collection_id.clone(), collection.clone(), node.as_ref(), cdata);
                let state_getter = crate::retrieval::LocalStateGetter::new(collection);

                crate::node::applier::NodeApplier::apply_deltas(node.as_ref(), &peer_id, deltas, &event_getter, &state_getter).await?;
                // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

                node.fetch_entities_from_local(collection_id, &selection_clone).await
            }
            proto::NodeResponseBody::Error(e) => {
                tracing::debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                tracing::debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }
}
