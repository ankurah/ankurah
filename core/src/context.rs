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
use ankurah_proto::{self as proto, Clock, CollectionId, EntityState, Event};
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
    pub cdata: PA::ContextData,
}

#[async_trait]
pub trait TContext {
    fn node_id(&self) -> proto::NodeId;
    /// The pinned system root id bound into every ordinary entity genesis.
    fn system_id(&self) -> Option<proto::EntityId>;
    /// Create the writable pre-identity entity used to collect initial model
    /// values. It is not inserted into the resident set.
    fn create_provisional_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity;
    /// Insert an empty resident primary under the genesis-derived id and return
    /// the transaction snapshot materialized at the genesis head.
    fn create_transaction_entity(
        &self,
        collection: proto::CollectionId,
        genesis: &Event,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError>;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError>;
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError>;
    /// RFC 5.2 (specs/model-property-metadata/rfc.md) model first-use registration, object-safe so the mutating
    /// transaction paths (`create`/`edit`) can trigger it without the
    /// concrete `<SE, PA>`. Discriminates per plan decision 16 (rev 4): a
    /// failed reassertion may warn and proceed only when every field in this
    /// exact compiled shape already has a compatible canonical binding. A
    /// never-registered collection, missing field, or incompatible field
    /// fails the write.
    async fn ensure_registered(&self, schema: &'static crate::schema::ModelSchema) -> Result<(), MutationError>;
    /// Record a binary-known compiled schema for predicate first-use
    /// registration and unknown-collection classification. Object-safe so
    /// read paths (`get`/`fetch`/`query`) and the sync `edit` path can call it
    /// without the concrete `<SE, PA>`. Commit provenance is recorded on the
    /// transaction itself, not inferred from this process-global cache.
    fn cache_compiled(&self, schema: &'static crate::schema::ModelSchema);
    /// RFC 5.2 STRICT registration (the eager explicit `ctx.register::<M>()`
    /// form): propagate the error instead of swallowing it. Object-safe.
    async fn register_strict(
        &self,
        schema: &'static crate::schema::ModelSchema,
    ) -> Result<(), crate::schema::registration::RegistrationError>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    fn node_id(&self) -> proto::NodeId { self.node.id }
    fn system_id(&self) -> Option<proto::EntityId> { self.node.system.root_id() }
    fn create_provisional_entity(&self, collection: proto::CollectionId, trx_alive: Arc<AtomicBool>) -> Entity {
        self.node.entities.create_provisional(collection, trx_alive)
    }
    fn create_transaction_entity(
        &self,
        collection: proto::CollectionId,
        genesis: &Event,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        self.node.entities.create_transaction_entity(collection, genesis, trx_alive)
    }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> { self.node.policy_agent.check_write(&self.cdata, entity, None) }
    async fn get_entity(&self, id: proto::EntityId, collection: &proto::CollectionId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get(&id) }
    async fn fetch_entities(&self, collection: &proto::CollectionId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.fetch_entities(collection, args).await
    }
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError> {
        use std::sync::atomic::Ordering;

        // Atomically mark transaction as no longer alive, preventing double-commit.
        // compare_exchange returns Err if the value was already false (already committed/rolled back).
        if trx.alive.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return Err(MutationError::General("Transaction already committed or rolled back".into()));
        }

        // Close the edit-only registration gap (RFC 5.2 "durable write on
        // first mutating use"): Transaction::edit is sync and cannot await a
        // durable registration, so ensure-register the exact schema shapes
        // this transaction used. Transaction scope matters: a historically
        // failed declaration for the same collection must not poison later
        // transactions using a compatible shape.
        // Same discrimination as the create-path trigger (plan decision 16):
        // a failed reassertion proceeds only when every compiled field already
        // has a compatible canonical binding; otherwise the commit fails.
        let schemas = trx.schemas.read().unwrap().clone();
        for schema in schemas {
            TContext::ensure_registered(self, schema).await?;
        }

        // Resolve ordinary fields' transient uncommitted Name keys to their
        // property id now the catalog is warm (the PropertyKey amendment,
        // #289). Explicit-id accessors already stage under their literal Id.
        // Both forms canonicalize values to the property's catalog value_type
        // here (rfc.md 5.6 as amended 2026-07-10), and the committed/wire form
        // is always id-keyed for a registered user collection. A value the
        // canonical type cannot represent fails this writer's commit.
        for entity in trx.entities.iter() {
            entity.resolve_pending_keys()?;
        }

        // Generate the causally ordered event sequence for each transaction
        // entity. A created entity always contributes its already-frozen
        // genesis first, then at most one Update for post-create edits.
        let trx_id = trx.id.clone();
        let genesis_events = trx.genesis_events.read().unwrap().clone();
        let created_entity_ids = trx.created_entity_ids.read().unwrap().clone();
        if genesis_events.keys().copied().collect::<std::collections::HashSet<_>>() != created_entity_ids {
            return Err(MutationError::InvalidEvent);
        }

        let mut entity_events = Vec::new();
        let mut seen_created = std::collections::HashSet::new();
        for entity in trx.entities.iter() {
            let mut events = Vec::with_capacity(2);
            if let Some(genesis) = genesis_events.get(&entity.id) {
                if !seen_created.insert(entity.id)
                    || genesis.entity_id != entity.id
                    || !genesis.is_entity_create()
                    || genesis.validate_structure().is_err()
                    || entity.head() != Clock::new([genesis.id()])
                {
                    return Err(MutationError::InvalidEvent);
                }
                events.push(genesis.clone());
            }

            if let Some(event) = entity.generate_commit_event()? {
                events.push(event);
            }

            for event in &events {
                // Protected collections (system + metadata catalog) are not
                // mutable through ordinary transactions; the catalog's only
                // mutation path is the registration operation (RFC 4).
                if let Some(protected) = crate::schema::well_known_collection(&event.model) {
                    return Err(MutationError::General(
                        format!("collection '{}' is protected and not writable by transactions", protected).into(),
                    ));
                }
                self.node.validate_event_scope(event)?;
            }

            if !events.is_empty() {
                entity_events.push((entity.clone(), events));
            }
        }
        if seen_created != created_entity_ids {
            return Err(MutationError::InvalidEvent);
        }

        // Now validate and commit the events.
        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        // Phase 1: check policy and collect attestations for EVERY event
        // before persisting ANY of them, so a later denial leaves nothing
        // durable (failure atomicity, V7). For a created entity, walk the
        // states sequentially: empty -> genesis -> optional update.
        for (entity, events) in entity_events {
            use std::sync::atomic::AtomicBool;
            let validation_alive = Arc::new(AtomicBool::new(true));
            let mut entity_before = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity.clone(),
            };
            let collection = self.node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, self.node.durable);
            let mut entity_attested = Vec::with_capacity(events.len());

            for event in events {
                event_getter.stage_event(event.clone());
                let entity_after = entity_before.snapshot(validation_alive.clone());
                entity_after.apply_event(&event_getter, &event).await?;

                let admission = self.node.policy_agent.check_event(&self.node, &self.cdata, &entity_before, &entity_after, &event)?;
                let attested = self.node.attest_event(event, admission);

                attested_events.push(attested.clone());
                entity_attested.push(attested);
                entity_before = entity_after;
            }
            entity_attested_events.push((entity, entity_attested));
        }

        // Phase 2: all events attested; persist them.
        for (entity, events) in &entity_attested_events {
            let collection = self.node.collections.get(entity.collection()).await?;
            let event_getter = crate::retrieval::LocalEventGetter::new(collection, self.node.durable);
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
        self.node.relay_to_required_peers(&self.cdata, trx_id, &attested_events).await?;

        // All peers confirmed, persist state to storage
        let mut changes: Vec<EntityChange> = Vec::new();
        for (entity, events) in entity_attested_events {
            let collection = self.node.collections.get(entity.collection()).await?;

            // Persist canonical entity (upstream for transactional forks, entity itself for primary)
            let canonical_entity = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => {
                    // Events are now in storage. Apply the entire ordered
                    // sequence to the previously-empty (for creation) or
                    // existing canonical entity.
                    let event_getter = crate::retrieval::LocalEventGetter::new(collection.clone(), self.node.durable);
                    for attested in &events {
                        upstream.apply_event(&event_getter, &attested.payload).await?;
                    }
                    upstream.clone()
                }
                crate::entity::EntityKind::Primary => entity,
            };

            let state = canonical_entity.to_state()?;

            let entity_state = EntityState { entity_id: canonical_entity.id(), model: canonical_entity.model_id()?, state };
            let admission = self.node.policy_agent.attest_state(&self.node, &entity_state);
            let attested = self.node.attest_state(entity_state, admission);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(canonical_entity, events)?);
        }

        // Notify reactor of ALL changes
        self.node.reactor.notify_change(changes).await;

        Ok(attested_events.into_iter().map(|a| a.payload).collect())
    }
    fn query(&self, collection_id: proto::CollectionId, args: MatchArgs) -> Result<EntityLiveQuery, RetrievalError> {
        EntityLiveQuery::new(&self.node, collection_id, args, self.cdata.clone())
    }
    async fn collection(&self, id: &proto::CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.node.system.collection(id).await
    }
    async fn ensure_registered(&self, schema: &'static crate::schema::ModelSchema) -> Result<(), MutationError> {
        if let Err(e) = self.node.catalog.ensure_registered(&self.node, &self.cdata, schema).await {
            // An unavailable reassertion may proceed only when this exact
            // compiled shape can already resolve every field to a compatible
            // canonical definition. Otherwise an unregistered or incompatible
            // field would escape as Name residue in a registered user model.
            if self.node.catalog.schema_is_fully_bound_compatible(schema) {
                tracing::warn!(
                    "ensure_registered for fully bound collection '{}' failed (data write proceeds with cached canonical bindings): {}",
                    schema.collection,
                    e
                );
            } else {
                let message = if self.node.catalog.model_by_collection(schema.collection).is_none() {
                    format!("cannot write into unregistered collection '{}': {e}", schema.collection)
                } else {
                    format!("cannot write using an unconfirmed schema for collection '{}': {e}", schema.collection)
                };
                return Err(MutationError::General(message.into()));
            }
        }
        Ok(())
    }
    fn cache_compiled(&self, schema: &'static crate::schema::ModelSchema) { self.node.catalog.cache_compiled(schema); }
    async fn register_strict(
        &self,
        schema: &'static crate::schema::ModelSchema,
    ) -> Result<(), crate::schema::registration::RegistrationError> {
        self.node.catalog.ensure_registered(&self.node, &self.cdata, schema).await
    }
}

// This whole impl is conditionalized by the wasm feature flag
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl Context {
    #[wasm_bindgen(js_name = "node_id")]
    pub fn js_node_id(&self) -> proto::NodeId { self.0.node_id() }
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
        Self(Arc::new(NodeAndContext { node, cdata: data }))
    }

    pub fn node_id(&self) -> proto::NodeId { self.0.node_id() }

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

    /// RFC 5.2 eager explicit registration (STRICT form): register `M`'s
    /// model, properties, and memberships now, propagating any error. Automatic
    /// mutating paths also fail a never-registered write; a failed reassertion
    /// may proceed only when every field is already bound compatibly. Useful
    /// before first render so predicate resolution has the ids ready. A second
    /// call for the same compiled shape is a no-op.
    pub async fn register<M: crate::model::Model>(&self) -> Result<(), crate::schema::registration::RegistrationError> {
        self.0.register_strict(M::schema()).await
    }

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        // Record the compiled schema. Gets (id addressed, no predicate
        // resolution) do not trigger first-use registration; a later
        // predicate use can still identify this as a binary-known schema.
        self.0.cache_compiled(<R::Model as crate::model::Model>::schema());
        let entity = self.0.get_entity(id, &R::collection(), false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        self.0.cache_compiled(<R::Model as crate::model::Model>::schema());
        let entity = self.0.get_entity(id, &R::collection(), true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        use crate::model::Model;
        let collection_id = R::Model::collection();

        // Record the compiled schema. Resolution registers it at first use
        // (REN 2 revised, plan decision 25b): an idempotent upsert that
        // no-ops when the catalog already carries the schema. Denied or
        // offline registration surfaces as the loud UnregisteredCollection
        // error (RFC 5.3 addendum).
        self.0.cache_compiled(R::Model::schema());

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
        // Record the compiled schema before building the query; the async
        // resolution path registers it at first use (REN 2 revised).
        self.0.cache_compiled(R::Model::schema());
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
    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(
        &self,
        collection_id: &CollectionId,
        id: proto::EntityId,
        cached: bool,
    ) -> Result<Entity, RetrievalError> {
        debug!("Node({}).get_entity {:?}-{:?}", self.node.id, id, collection_id);

        if !self.node.durable {
            // Fetch from peers and commit first response
            match self.node.get_from_peer(collection_id, vec![id], &self.cdata).await {
                Ok(_) => (),
                Err(RetrievalError::NoDurablePeers) if cached => (),
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if let Some(local) = self.node.entities.get(&id) {
            debug!("Node({}).get_entity found local entity - returning", self.node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            self.node.policy_agent.check_read(&self.cdata, &entity_id, collection_id, &state, Some(self.node.catalog.resolver_weak()))?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", self.node);

        let collection = self.node.collections.get(collection_id).await?;
        match collection.get_state(id).await {
            Ok(entity_state) => {
                self.node.policy_agent.check_read(
                    &self.cdata,
                    &entity_state.payload.entity_id,
                    collection_id,
                    &entity_state.payload.state,
                    Some(self.node.catalog.resolver_weak()),
                )?;
                let state_getter = crate::retrieval::LocalStateGetter::new(collection.clone());
                let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), collection, &self.node, &self.cdata);
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
        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        // Fetch raw states from storage

        args.selection.predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.selection.predicate)?;

        // Resolve property references against the catalog (RFC 5.5 Phase A):
        // PathExpr -> Identifier, failing closed on unknown properties,
        // registering compiled models at first use (REN 2 revised).
        // Idempotent, so a selection resolved by an outer caller passes
        // through unchanged here. A collection that neither the catalog
        // cache nor first-use registration can resolve fails LOUD
        // (UnregisteredCollection): a lagging replica cannot prove
        // emptiness, and a successful registration answers empty truthfully
        // by querying the real, entity-free collection.
        args.selection = self
            .node
            .catalog
            .resolve_selection_deferred(&self.node, Some(&self.cdata), collection_id, &args.selection)
            .await
            .map_err(RetrievalError::from)?;

        // Resolve types in the AST (converts literals for JSON path comparisons)
        args.selection = self.node.type_resolver.resolve_selection_types(args.selection);

        // TODO implement cached: true
        if !self.node.durable {
            // Fetch from peers and commit first response
            Ok(self.fetch_from_peer(collection_id, args.selection).await?)
        } else {
            let storage_collection = self.node.collections.get(collection_id).await?;
            let states = storage_collection.fetch_states(&args.selection).await?;

            // Convert states to entities
            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(storage_collection.clone());
            let event_getter = crate::retrieval::CachedEventGetter::new(collection_id.clone(), storage_collection, &self.node, &self.cdata);
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
            .request(peer_id, &self.cdata, proto::NodeRequestBody::Fetch { collection: collection_id.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                let collection = self.node.collections.get(collection_id).await?;
                let event_getter =
                    crate::retrieval::CachedEventGetter::new(collection_id.clone(), collection.clone(), &self.node, &self.cdata);
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
