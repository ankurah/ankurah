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

/// A local scope for fetching, subscribing, and transacting, backed by a
/// live credential source: operations read the source's current state
/// (one snapshot per operation), so a session refresh reaches every
/// later operation without rebuilding the Context.
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
#[derive(Clone)]
pub struct Context(Arc<dyn TContext + Send + Sync + 'static>);

pub(crate) enum ContextAuth<PA>
where PA: PolicyAgent
{
    Sessions(crate::session::SessionSet<PA::ContextData>),
    /// The local node's own authority, for internal use -- in practice the
    /// system and catalog tables. Not possible to construct by the user, or
    /// over the wire. The PolicyAgent is never consulted and commits never
    /// relay, so internal callers must not privileged-write state that
    /// durable peers are authoritative for.
    Privileged,
}

pub(crate) struct NodeAndContext<SE, PA: PolicyAgent>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub node: NodeType<SE, PA>,
    /// The credential source for this context. Typically this is only one session
    /// except for system queries, which need a node's aggregate permissions
    pub auth: ContextAuth<PA>,
}

#[async_trait]
pub trait TContext {
    /// Ensure the compiled schema is registered and return the model's
    /// durable identity: registers the model, its properties, and its
    /// model-property memberships with the durable allocator (executed
    /// locally on a durable node, forwarded as a RegisterSchema request on
    /// an ephemeral one). Idempotent and latched; with no reachable peer it
    /// may proceed only from a binding proven against locally held,
    /// allocator-derived catalog rows. This is the write path's entry --
    /// `Transaction::create` and the explicit [`Context::register_model`];
    /// a read reaches the same registration through
    /// [`Self::bind_or_register`], which only registers what the catalog
    /// cannot already prove. Callers convert the typed error at their
    /// boundary (From impls into RetrievalError and MutationError).
    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<(proto::ModelId, crate::schema::SchemaEpoch), crate::schema::registration::RegistrationError>;

    /// Bind a compiled declaration to this system's durable identities, so
    /// the views a typed read returns resolve every accessor, and register
    /// the declaration first if the catalog cannot prove it yet. This is the
    /// read-path admission: a declaration the system has never been told
    /// about is healed here, and a credential that may not register it is
    /// told so.
    async fn bind_or_register(&self, schema: &'static crate::schema::ModelStructDescriptor) -> Result<(), RetrievalError>;

    /// Resolve a typed selection's property names through the compiled
    /// declaration's descriptor cells (catalog fallback for names the
    /// struct does not carry), and canonicalize its comparison values.
    /// Binds the declaration on the way past, so a display-name change
    /// cannot re-aim a typed query.
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
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    /// The live-query entry. Admission (name resolution against `schema`'s
    /// descriptor cells, policy injection) happens right here whenever those
    /// cells are bound, so an unresolvable name is this call's error rather
    /// than a query that quietly never populates. A declaration this system
    /// has not been told about is registered inside the query's
    /// initialization instead, since this entry cannot await.
    fn query(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError>;
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    async fn ensure_registered(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
    ) -> Result<(proto::ModelId, crate::schema::SchemaEpoch), crate::schema::registration::RegistrationError> {
        // Registration acts as one principal, like every write path.
        let node = self.node().map_err(crate::schema::registration::RegistrationError::Retrieval)?;
        node.catalog.ensure_schema_for_use(&self.sessions.write_credential()?, schema).await
    }

    async fn bind_or_register(&self, schema: &'static crate::schema::ModelStructDescriptor) -> Result<(), RetrievalError> {
        self.node()?.catalog.bind_or_register(&self.sessions, schema).await.map(|_| ())
    }

    fn resolve_selection_with_descriptor(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        selection: ankql::ast::Selection<Parsed>,
    ) -> Result<ankql::ast::Selection<Resolved>, RetrievalError> {
        self.node()?.catalog.resolve_selection_with_descriptor(schema, selection)
    }

    fn node_id(&self) -> proto::EntityId { self.node.node_id() }
    fn system_id(&self) -> Option<proto::EntityId> { self.node.upgrade().and_then(|node| node.system.root_id()) }
    fn schema_epoch(&self) -> Option<crate::schema::SchemaEpoch> { self.node.upgrade().and_then(|node| node.system.schema_epoch()) }
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
        node.policy_agent.check_write(&self.sessions.write_credential()?, entity, None)
    }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.upgrade().and_then(|node| node.entities.get(&id)) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        use std::sync::atomic::Ordering;

        let node = self.node()?;

        // Atomically mark transaction as no longer alive, preventing double-commit.
        // compare_exchange returns Err if the value was already false (already committed/rolled back).
        if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(MutationError::General("Transaction already committed or rolled back".into()));
        }

        // One credential snapshot for the whole commit: a session update
        // mid-commit must not mix credentials across its phases.
        let cdata = self.sessions.write_credential()?;

        // Generate the causally ordered event sequence for each transaction
        // entity. A created entity contributes its already-frozen genesis
        // first, then at most one update for edits made after create()
        // returned.
        let trx_id = trx.id.clone();
        let genesis_events = trx.genesis_events.read().unwrap().clone();

        let mut entity_events = Vec::new();
        let mut seen_created = std::collections::HashSet::new();
        for entity in trx.entities.iter() {
            let mut events = Vec::with_capacity(2);
            if let Some(genesis) = genesis_events.get(&entity.id) {
                // The genesis is this node's own mint, but it passed through
                // the transaction between minting and here; re-check the
                // whole of what makes it admissible rather than trusting it,
                // one condition at a time so a failure says which.
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
                events.push(genesis.clone());
            }

            // An entity with an empty head and no frozen genesis is a
            // phantom: never created here, so it has nothing to extend.
            if let Some(event) = entity.generate_commit_event(proto::AuthorId::Unknown)? {
                events.push(event);
            }

            // Membership admissibility is a commit-path gate, mirrored on
            // the remote funnel (commit_remote_transaction).
            for event in &events {
                node.check_membership_admissibility(event)?;
            }

            if !events.is_empty() {
                entity_events.push((entity.clone(), events));
            }
        }
        // seen_created only ever holds genesis_events keys and refuses
        // duplicates, so length equality is set equality.
        if seen_created.len() != genesis_events.len() {
            return Err(MutationError::CommitInvariant("an entity create() recorded is absent from the transaction's entities"));
        }

        // Now commit the events
        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        // Phase 1: check policy and collect attestations for EVERY event
        // before persisting ANY of them, so a later denial leaves nothing
        // durable (failure atomicity, V7). A created entity walks its states
        // in order: empty, then genesis, then the optional update.
        for (entity, events) in entity_events {
            use std::sync::atomic::AtomicBool;
            let validation_alive = Arc::new(AtomicBool::new(true));

            // Get the canonical (upstream) entity for before state
            let mut entity_before = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity.clone(),
            };
            let collection = node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
            let mut entity_attested = Vec::with_capacity(events.len());

            for event in events {
                // Stage event and apply to a fork for the after state (no
                // commit_event call here)
                event_getter.stage_event(event.clone());
                let entity_after = entity_before.snapshot(validation_alive.clone());
                entity_after.apply_event(&event_getter, &event).await?;

                let attestation = node.policy_agent.check_event(node.as_ref(), &cdata, &entity_before, &entity_after, &event)?;
                let attested = Attested::opt(event, attestation);

                attested_events.push(attested.clone());
                entity_attested.push(attested);
                entity_before = entity_after;
            }
            entity_attested_events.push((entity, entity_attested));
        }

        // Phase 2: all events attested; persist them.
        for (entity, events) in &entity_attested_events {
            let collection = node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, node.durable);
            for attested in events {
                event_getter.commit_event(attested).await?;
            }
        }

        // Update heads BEFORE relaying (makes entities visible to server echo)
        for (entity, events) in &entity_attested_events {
            if let Some(last) = events.last() {
                entity.commit_head(Clock::new([last.payload.id()]));
            }
        }
        // Relay to peers and wait for confirmation
        node.relay_to_required_peers(&cdata, trx_id, &attested_events).await?;

        // All peers confirmed, persist state to storage
        let mut changes: Vec<EntityChange> = Vec::new();
        for (entity, events) in entity_attested_events {
            let collection = node.collections.get(entity.collection()).await?;

            // Persist canonical entity (upstream for transactional forks, entity itself for primary)
            let canonical_entity = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => {
                    // Events are now in storage; apply the whole ordered
                    // sequence to the canonical entity, which for a creation
                    // is still empty.
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
            let attestation = node.policy_agent.attest_state(node.as_ref(), &entity_state);
            let attested = Attested::opt(entity_state, attestation);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(canonical_entity, events)?);
        }

        // Notify reactor of ALL changes
        node.reactor.notify_change(changes).await;

        Ok(attested_events.into_iter().map(|a| a.payload).collect())
    }
    fn query(
        &self,
        schema: &'static crate::schema::ModelStructDescriptor,
        collection_id: proto::CollectionId,
        args: MatchArgs<Parsed>,
    ) -> Result<EntityLiveQuery, RetrievalError> {
        EntityLiveQuery::new(self.node()?.as_ref(), Some(schema), collection_id, args, self.sessions.clone())
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
    /// Explicitly register `M`'s model, properties, and model-property
    /// memberships with the durable allocator, propagating any error, and
    /// return the model's allocated identity
    ///. Useful at startup so the catalog holds `M`'s definitions
    /// before anything else runs. A second call for the same compiled shape
    /// is a no-op.
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
        Self(Arc::new(NodeAndContext { node: NodeType::Strong(node), sessions }))
    }

    /// A context that does NOT keep the node alive, for node-owned machinery
    /// (the catalog projection) whose strong context would cycle.
    pub(crate) fn new_weak<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static>(
        node: &Node<SE, PA>,
        sessions: impl Into<crate::session::SessionSet<PA::ContextData>>,
    ) -> Self {
        let sessions = sessions.into();
        node.sessions.attach(&sessions);
        Self(Arc::new(NodeAndContext { node: NodeType::Weak(node.weak()), sessions }))
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
        // The decoded View's accessors read identities off the compiled
        // declaration's cells, so admit the declaration before decoding: a
        // shape this system already knows binds outright, and one it does
        // not is registered here so the accessors have identities to read.
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(id, &R::collection(), false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        use crate::model::Model;
        self.0.bind_or_register(R::Model::descriptor()).await?;
        let entity = self.0.get_entity(id, &R::collection(), true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        // Admit the declaration first: it heals a shape this system has not
        // been told about, and once every cell is bound the selection either
        // resolves here and now or names something the query got wrong.
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
    /// Subscribe to changes in entities matching a selection.
    ///
    /// A declaration this system already knows is admitted right here (name
    /// resolution, policy injection), so a query the caller got wrong is
    /// this call's error. One it does not know is registered inside the
    /// query's initialization, because this entry cannot await -- the query
    /// starts empty and a failure there surfaces through its error slot.
    /// [`Self::query_wait`] awaits that healing first and gets the error.
    pub fn query<R>(
        &self,
        args: impl TryInto<MatchArgs<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let args: MatchArgs<Parsed> = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        Ok(self.0.query(R::Model::descriptor(), R::Model::collection(), args)?.map::<R>())
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
        // Heal here rather than inside the query: with the declaration bound
        // before construction, the query admits synchronously and every
        // admission failure is this call's error.
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
        let cdata = self.sessions.current();

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

        // Catalog reads never consult the agent (crate::schema::reads_bypass_policy).
        let exempt = crate::schema::reads_bypass_policy(collection_id);

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
        // One credential snapshot for the whole operation: the composed
        // checks (collection gate, predicate narrowing) must come from
        // one policy world, or a mid-operation refresh yields a
        // composite no single credential authorized.
        let node = self.node()?;
        let cdata = self.sessions.current();
        // Catalog reads never consult the agent (crate::schema::reads_bypass_policy).
        if !crate::schema::reads_bypass_policy(collection_id) {
            node.policy_agent.can_access_collection(&cdata, collection_id)?;
            // The selection arrives resolved (its names bound where the query
            // entered), and the policy narrows it in the same vocabulary: what
            // the agent ANDs in is resolved too, so nothing here is left to bind.
            args.selection.predicate = node.policy_agent.filter_predicate(&cdata, collection_id, args.selection.predicate)?;
        }

        // TODO implement cached: true
        if !node.durable {
            // Fetch from peers and commit first response, under this
            // operation's one credential snapshot.
            Ok(self.fetch_from_peer(collection_id, args.selection, &cdata).await?)
        } else {
            let storage_collection = node.collections.get(collection_id).await?;
            let states = storage_collection.fetch_states(&args.selection).await?;

            // Convert states to entities
            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(storage_collection.clone());
            let event_getter = CachedEventGetter::new(collection_id.clone(), storage_collection, &node, &cdata);
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
        let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        // 1. Pre-fetch known_matches from local storage
        let known_matched_entities = node.fetch_entities_from_local(collection_id, &selection).await?;

        let known_matches = known_matched_entities
            .iter()
            .map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() })
            .collect();

        // 2. Send fetch request with known_matches
        let selection_clone = selection.clone();
        match node
            .request(peer_id, cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                let collection = node.collections.get(collection_id).await?;
                let event_getter = CachedEventGetter::new(collection_id.clone(), collection.clone(), &node, cdata);
                let state_getter = crate::retrieval::LocalStateGetter::new(collection);

                // 3. Apply deltas to local storage using NodeApplier
                crate::node_applier::NodeApplier::apply_deltas(&node, &peer_id, deltas, &event_getter, &state_getter).await?;
                // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

                // 4. Re-fetch entities from local storage after applying deltas
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
