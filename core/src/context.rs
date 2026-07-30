//! A caller-scoped interface to one node.
//!
//! Here, a **context** is the authority under which typed operations run on
//! one [`crate::node::Node`]. A normal context pairs the node with one
//! [`crate::policy::PolicyAgent::ContextData`] and applies caller policy. A
//! **system-root context** is a core-only capability, constructible only for
//! a durable node, that bypasses the application policy authority for the
//! node's own system models. Both use the same fetch, live-query, model, and
//! transaction machinery.
//!
//! [`Context`] is the public, generic-erased handle. [`NodeAndContext`] is
//! the concrete implementation that retains the node and its authority,
//! while [`TContext`] is their object-safe boundary. Local transaction admission
//! belongs here because this is the last caller-scoped boundary before events
//! enter the node's trusted commit pipeline; protected system collections
//! must never cross it as ordinary application writes.

use crate::retrieval::SuspenseEvents;
use crate::{
    changes::EntityChange,
    entity::Entity,
    error::{MutationError, RetrievalError},
    livequery::{EntityLiveQuery, LiveQuery},
    model::View,
    node::{MatchArgs, Node},
    policy::{AccessDenied, PolicyAgent},
    storage::{StorageCollectionWrapper, StorageEngine},
    transaction::Transaction,
};
use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityState, Event};
use async_trait::async_trait;
use std::sync::{atomic::AtomicBool, Arc};
use tracing::debug;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Context is used to provide a local interface to fetch and subscribe to entities
/// with a specific ContextData. Generally this means your auth token for a specific user,
/// but ContextData is abstracted so you can use what you want.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
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
    context_type: ContextType<PA::ContextData>,
}

/// The authority behind a Context. A Session carries application context
/// data; SystemRoot is deliberately not a session and carries no application
/// credentials.
enum ContextType<CD> {
    Session(CD),
    SystemRoot,
}

#[async_trait]
pub trait TContext {
    /// Ensure the compiled schema is registered and return the model's
    /// durable identity: registers the model, its properties, and its
    /// model-property memberships with the durable allocator (executed
    /// locally on a durable node, forwarded as a RegisterSchema request on
    /// an ephemeral one). Idempotent and latched; with no reachable peer it
    /// may proceed only from a binding proven against locally held,
    /// allocator-derived catalog rows. Every registration path funnels
    /// here -- typed first use (create/fetch/get/query_wait) and the
    /// explicit [`Context::register_model`] alike; callers convert the
    /// typed error at their boundary (From impls into RetrievalError and
    /// MutationError).
    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<proto::ModelId, crate::schema::registration::RegistrationError>;

    fn node_id(&self) -> proto::EntityId;
    /// Create a brand new entity for a transaction, and add it to the WeakEntitySet.
    /// Note that this does not actually persist the entity to the storage engine
    /// It merely ensures that there are no duplicate entities with the same ID (except forked entities)
    fn create_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError>;
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<proto::ModelId, crate::schema::registration::RegistrationError> {
        // System models are pre-registered by definition: their identity is
        // a compile-time System ID, so no path here ever consults the
        // catalog they describe.
        if let Some(system) = schema.system {
            return Ok(proto::ModelId::System(system));
        }
        match &self.context_type {
            ContextType::Session(cdata) => self.node.catalog.ensure_schema_for_use(cdata, schema).await,
            ContextType::SystemRoot => {
                Err(crate::schema::registration::RegistrationError::SystemRootApplicationModel(schema.label.to_owned()))
            }
        }
    }

    fn node_id(&self) -> proto::EntityId { self.node.id }
    fn create_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity {
        let primary_entity = self.node.entities.create(collection);
        primary_entity.snapshot(trx_alive)
    }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> {
        match &self.context_type {
            ContextType::Session(cdata) => self.node.policy_agent.check_write(cdata, entity, None),
            ContextType::SystemRoot => Ok(()),
        }
    }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get(&id) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        let authority = match &self.context_type {
            ContextType::Session(cdata) => CommitAuthority::Session(cdata),
            ContextType::SystemRoot => CommitAuthority::SystemRoot,
        };
        commit_local_transaction(&self.node, trx, authority).await
    }
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError> {
        match &self.context_type {
            ContextType::Session(cdata) => EntityLiveQuery::new(&self.node, collection_id, args, cdata.clone()),
            ContextType::SystemRoot => EntityLiveQuery::new_system_root_weak_node(&self.node, collection_id, args),
        }
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.system.collection(id).await
    }
}

#[derive(Clone, Copy)]
enum CommitAuthority<'a, C> {
    Session(&'a C),
    SystemRoot,
}

/// Commit one ordinary typed transaction under either caller authority or
/// the node's local system-root capability. The entity/event/storage/reactor
/// path is shared; only application policy, protected-collection admission,
/// state attestation, and session-authenticated relay differ.
async fn commit_local_transaction<SE, PA>(
    node: &Node<SE, PA>,
    trx: &Transaction,
    authority: CommitAuthority<'_, PA::ContextData>,
) -> Result<Vec<Event>, MutationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    use std::sync::atomic::Ordering;

    if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
        return Err(MutationError::General("Transaction already committed or rolled back".into()));
    }

    let trx_id = trx.id.clone();
    let mut entity_events = Vec::new();
    for entity in trx.entities.iter() {
        if let Some(event) = entity.generate_commit_event()? {
            if matches!(authority, CommitAuthority::Session(_))
                && event.collection.as_str().starts_with(crate::schema::RESERVED_COLLECTION_PREFIX)
            {
                return Err(MutationError::General(
                    format!("collection '{}' is protected and not writable by application transactions", event.collection).into(),
                ));
            }
            if event.is_entity_create() {
                let created_ids = trx.created_entity_ids.read().unwrap();
                if !created_ids.contains(&entity.id) {
                    return Err(MutationError::General(
                        format!(
                            "Cannot commit phantom entity {}: entity has empty parent (creation event) \
                             but was not created in this transaction via create()",
                            entity.id
                        )
                        .into(),
                    ));
                }
            }
            node.check_membership_admissibility(&event)?;
            entity_events.push((entity.clone(), event));
        }
    }

    let mut attested_events = Vec::new();
    let mut entity_attested_events = Vec::new();
    for (entity, event) in entity_events {
        let trx_alive = Arc::new(AtomicBool::new(true));
        let forked = entity.snapshot(trx_alive);
        let entity_before = match &entity.kind {
            crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
            crate::entity::EntityKind::Primary => entity.clone(),
        };
        let collection = node.collections.get(&event.collection).await?;
        let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
        event_getter.stage_event(event.clone());
        forked.apply_event(&event_getter, &event).await?;

        let attestation = match authority {
            CommitAuthority::Session(cdata) => node.policy_agent.check_event(node, cdata, &entity_before, &forked, &event)?,
            CommitAuthority::SystemRoot => None,
        };
        let attested = Attested::opt(event.clone(), attestation);
        attested_events.push(attested.clone());
        entity_attested_events.push((entity, attested));
    }

    for (_, attested) in &entity_attested_events {
        let collection = node.collections.get(&attested.payload.collection).await?;
        let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
        event_getter.commit_event(attested).await?;
    }

    for (entity, attested_event) in &entity_attested_events {
        entity.commit_head(Clock::new([attested_event.payload.id()]));
    }
    if let CommitAuthority::Session(cdata) = authority {
        node.relay_to_required_peers(cdata, trx_id, &attested_events).await?;
    }

    let mut changes: Vec<EntityChange> = Vec::new();
    for (entity, attested_event) in entity_attested_events {
        let collection = node.collections.get(&attested_event.payload.collection).await?;
        let canonical_entity = match &entity.kind {
            crate::entity::EntityKind::Transacted { upstream, .. } => {
                let event_getter = crate::retrieval::LocalEventGetter::new(collection.clone(), node.durable);
                upstream.apply_event(&event_getter, &attested_event.payload).await?;
                upstream.clone()
            }
            crate::entity::EntityKind::Primary => entity,
        };
        let state = canonical_entity.to_state()?;
        let entity_state = EntityState { entity_id: canonical_entity.id(), collection: canonical_entity.collection().clone(), state };
        let attestation = match authority {
            CommitAuthority::Session(_) => node.policy_agent.attest_state(node, &entity_state),
            CommitAuthority::SystemRoot => None,
        };
        collection.set_state(Attested::opt(entity_state, attestation)).await?;
        changes.push(EntityChange::new(canonical_entity, vec![attested_event])?);
    }

    node.reactor.notify_change(changes).await;
    Ok(attested_events.into_iter().map(|a| a.payload).collect())
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
    /// Explicitly register `M`'s model, properties, and model-property
    /// memberships with the durable allocator, propagating any error, and
    /// return the model's allocated identity
    ///. Useful at startup so the catalog holds `M`'s definitions
    /// before anything else runs. A second call for the same compiled shape
    /// is a no-op.
    pub async fn register_model<M: crate::model::Model>(&self) -> Result<proto::ModelId, crate::schema::registration::RegistrationError> {
        self.0.ensure_registered(M::descriptor()).await
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
    pub fn new<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: Node<SE, PA>,
        data: PA::ContextData,
    ) -> Self {
        Self(Arc::new(NodeAndContext::new_session(node, data)))
    }

    /// Construct the core-only authority for a durable node's own system
    /// models. It is crate-private and refuses ephemeral nodes, so neither an
    /// application nor a remote request can turn ordinary credentials into
    /// system-root authority.
    pub(crate) fn new_system_root<SE, PA>(node: Node<SE, PA>) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if !node.durable {
            return Err(RetrievalError::Other("a SystemRoot context requires a durable node".to_owned()));
        }
        Ok(Self(Arc::new(NodeAndContext { node, context_type: ContextType::SystemRoot })))
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
        use crate::model::Model;
        // A typed direct get is a schema-dependent use: admit the exact
        // compiled schema (first-use registration) before decoding.
        self.0.ensure_registered(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(id, &R::collection(), false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.ensure_registered(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(id, &R::collection(), true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        // Typed predicate reads register at first use, so the fetch runs
        // against authoritative catalog rows instead of failing loud as
        // unregistered (and offline with no peer, it fails loud instead of
        // answering empty).
        self.0.ensure_registered(R::Model::descriptor()).await?;
        let collection_id = R::Model::collection();

        let entities = self.0.fetch_entities(&collection_id, args).await?;

        Ok(entities.into_iter().map(|e| R::from_entity(e)).collect())
    }

    pub async fn fetch_one<R: View + Clone + 'static>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<Option<R>, RetrievalError> {
        let views = self.fetch::<R>(args).await?;
        Ok(views.into_iter().next())
    }
    /// Subscribe to changes in entities matching a selection
    pub fn query<R>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<LiveQuery<R>, RetrievalError>
    where R: View {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        Ok(self.0.query(R::Model::collection(), args)?.map::<R>())
    }

    /// Subscribe to changes in entities matching a selection and wait for initialization
    pub async fn query_wait<R>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        use crate::model::Model;
        // The synchronous `query` cannot await first-use registration (its
        // initialization pipeline takes that over with the
        // propertyid-resolution PR); the awaited form registers here.
        self.0.ensure_registered(R::Model::descriptor()).await?;
        let livequery = self.query::<R>(args)?;
        livequery.wait_initialized().await;
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
    pub(crate) fn new_session(node: Node<SE, PA>, cdata: PA::ContextData) -> Self {
        Self { node, context_type: ContextType::Session(cdata) }
    }

    fn session_cdata(&self) -> Option<&PA::ContextData> {
        match &self.context_type {
            ContextType::Session(cdata) => Some(cdata),
            ContextType::SystemRoot => None,
        }
    }

    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.node.id, id, collection_id);

        if matches!(&self.context_type, ContextType::SystemRoot) {
            if let Some(local) = self.node.entities.get(&id).filter(|entity| entity.collection() == collection_id) {
                return Ok(local);
            }
            let collection = self.node.collections.get(collection_id).await?;
            let state = collection.get_state(id).await?;
            let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, true);
            let (_, entity) =
                self.node.entities.with_state(&state_getter, &event_getter, id, collection_id.clone(), state.payload.state).await?;
            return Ok(entity);
        }
        let cdata = self.session_cdata().expect("session contexts carry ContextData");

        if !self.node.durable {
            // Fetch from peers and commit first response
            match self.node.get_from_peer(collection_id, vec![id], cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = self.node.entities.get(&id).filter(|entity| entity.collection() == collection_id) {
            debug!("Node({}).get_entity found local entity - returning", self.node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            self.node.policy_agent.check_read(cdata, &entity_id, collection_id, &state)?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", self.node);

        let collection = self.node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                self.node.policy_agent.check_read(cdata, &entity_state.payload.entity_id, collection_id, &entity_state.payload.state)?;
                let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
                let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), collection, &self.node, cdata);
                let (_changed, entity) = self
                    .node
                    .entities
                    .with_state(&state_getter, &event_getter, id, collection_id.clone(), entity_state.payload.state)
                    .await?;
                Ok(entity)
            }
            Err(e) => Err(e),
        }
    }
    /// Fetch a list of entities based on a selection
    pub async fn fetch_entities(&self, collection_id: &CollectionId, mut args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        if matches!(&self.context_type, ContextType::SystemRoot) {
            args.selection = self.node.type_resolver.resolve_selection_types(args.selection);
            return self.node.fetch_entities_from_local(collection_id, &args.selection).await;
        }
        let cdata = self.session_cdata().expect("session contexts carry ContextData");
        self.node.policy_agent.can_access_collection(cdata, collection_id)?;
        // Fetch raw states from storage

        args.selection.predicate = self.node.policy_agent.filter_predicate(cdata, collection_id, args.selection.predicate)?;

        // Resolve types in the AST (converts literals for JSON path comparisons)
        args.selection = self.node.type_resolver.resolve_selection_types(args.selection);

        // TODO implement cached: true
        if !self.node.durable {
            // Fetch from peers and commit first response
            Ok(self.fetch_from_peer(collection_id, args.selection, cdata).await?)
        } else {
            let storage_collection = self.node.collections.get(collection_id).await?;
            let states = storage_collection.fetch_states(&args.selection).await?;

            // Convert states to entities
            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(storage_collection.clone());
            let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), storage_collection, &self.node, cdata);
            for state in states {
                let (_, entity) = self
                    .node
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
        selection: ankql::ast::Selection,
        cdata: &PA::ContextData,
    ) -> Result<Vec<crate::entity::Entity>, RetrievalError> {
        let peer_id = self.node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        // 1. Pre-fetch known_matches from local storage
        let known_matched_entities = self.node.fetch_entities_from_local(collection_id, &selection).await?;

        let known_matches = known_matched_entities
            .iter()
            .map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() })
            .collect();

        // 2. Send fetch request with known_matches
        let selection_clone = selection.clone();
        match self
            .node
            .request(peer_id, cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                let collection = self.node.collections.get(collection_id).await?;
                let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), collection.clone(), &self.node, cdata);
                let state_getter = crate::retrieval::LocalStateGetter::new(collection);

                // 3. Apply deltas to local storage using NodeApplier
                crate::node_applier::NodeApplier::apply_deltas(&self.node, &peer_id, deltas, &event_getter, &state_getter).await?;
                // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

                // 4. Re-fetch entities from local storage after applying deltas
                self.node.fetch_entities_from_local(collection_id, &selection_clone).await
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
