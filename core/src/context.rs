use crate::retrieval::SuspenseEvents;
use crate::ModelId;
use crate::{
    changes::EntityChange,
    entity::Entity,
    error::{MutationError, RetrievalError},
    livequery::{EntityLiveQuery, LiveQuery},
    model::{Model, View},
    node::{MatchArgs, Node},
    policy::{AccessDenied, PolicyAgent},
    storage::StorageEngine,
    storage_commit::ResidentWriteIntent,
    transaction::Transaction,
};
use ankurah_proto::{self as proto, Attested, Clock, Event};
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
    fn node_id(&self) -> proto::EntityId;
    /// Create a brand new entity for a transaction, and add it to the WeakEntitySet
    /// Note that this does not actually persist the entity to the storage engine
    /// It merely ensures that there are no duplicate entities with the same ID (except forked entities)
    fn create_entity(&self, collection: crate::ModelId, trx_alive: Arc<AtomicBool>) -> Entity;
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied>;
    async fn get_entity(&self, id: proto::EntityId, collection: &crate::ModelId, cached: bool) -> Result<Entity, RetrievalError>;
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity>;
    async fn fetch_entities(&self, collection: &crate::ModelId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError>;
    async fn commit_local_trx(&self, trx: &Transaction) -> Result<Vec<Event>, MutationError>;
    fn query(
        &self,
        collection_id: crate::ModelId,
        schema: &'static crate::schema::ModelSchema,
        args: MatchArgs,
    ) -> Result<EntityLiveQuery, RetrievalError>;
    /// RFC 5.2 (specs/model-property-metadata/rfc.md) model first-use registration, object-safe so the mutating
    /// transaction paths (`create`/`edit`) can trigger it without the
    /// concrete `<SE, PA>`. If no durable peer is available, use may proceed
    /// only when the local catalog proves every model and field identity in
    /// this exact compiled shape. Policy, executor, missing-field, and
    /// incompatible-field failures remain strict.
    async fn ensure_registered(&self, schema: &'static crate::schema::ModelSchema) -> Result<ModelId, MutationError>;
    /// RFC 5.2 STRICT registration (the eager explicit `ctx.register::<M>()`
    /// form): propagate the error instead of swallowing it. Object-safe.
    async fn register_strict(
        &self,
        schema: &'static crate::schema::ModelSchema,
    ) -> Result<ModelId, crate::schema::registration::RegistrationError>;
    /// Admit the exact compiled schema for a predicate read. Kept separate
    /// from the mutation helper so read failures do not claim a write failed.
    async fn ensure_query_schema(&self, schema: &'static crate::schema::ModelSchema) -> Result<ModelId, RetrievalError>;

    /// Return the identity retained for an already-admitted exact compiled
    /// schema. This is intentionally not a model-name lookup.
    fn model_id_for_schema(&self, schema: &'static crate::schema::ModelSchema) -> Option<ModelId>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> TContext for NodeAndContext<SE, PA> {
    fn node_id(&self) -> proto::EntityId { self.node.id }
    fn create_entity(&self, collection: crate::ModelId, trx_alive: Arc<AtomicBool>) -> Entity {
        // WeakEntitySet::create binds the PRIMARY to the live catalog
        // resolver before this snapshot, so the transaction fork inherits it.
        let primary_entity = self.node.entities.create(collection);
        primary_entity.snapshot(trx_alive)
    }
    fn check_write(&self, entity: &Entity) -> Result<(), AccessDenied> { self.node.policy_agent.check_write(&self.cdata, entity, None) }
    async fn get_entity(&self, id: proto::EntityId, collection: &crate::ModelId, cached: bool) -> Result<Entity, RetrievalError> {
        self.get_entity(collection, id, cached).await
    }
    fn get_resident_entity(&self, id: proto::EntityId) -> Option<Entity> { self.node.entities.get(&id) }
    async fn fetch_entities(&self, collection: &crate::ModelId, args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
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
        // As on create, the no-peer fallback requires a complete compatible
        // binding for this exact declaration; every other failure is strict.
        let schemas = trx.schemas.read().unwrap().clone();
        for (schema, expected_model) in schemas {
            let admitted_model = TContext::ensure_registered(self, schema).await?;
            if admitted_model != expected_model {
                return Err(MutationError::General(
                    format!(
                        "compiled schema '{}' is bound to {}, but this transaction used it with {}",
                        schema.name, admitted_model, expected_model
                    )
                    .into(),
                ));
            }
        }

        // Cast each staged value to its property's catalog value_type now the
        // catalog is warm (rfc.md 5.6 as amended 2026-07-10): every accessor
        // already resolved its `PropertyId` at construction (system fields
        // included), so there is no key left to resolve here, only values to
        // canonicalize. The committed/wire form is always id-keyed for a
        // registered user collection. A value the canonical type cannot
        // represent fails this writer's commit.
        for entity in trx.entities.iter() {
            entity.canonicalize_pending_values()?;
        }

        // Generate events from the transaction entities
        let trx_id = trx.id.clone();
        let mut entity_events = Vec::new();
        for entity in trx.entities.iter() {
            if let Some(event) = entity.generate_commit_event()? {
                // Protected models (system + metadata catalog) are not
                // mutable through ordinary transactions; the catalog's only
                // mutation path is the registration operation (RFC 4).
                if matches!(entity.collection(), proto::ModelId::System(_)) {
                    return Err(MutationError::General(
                        format!("model '{}' is protected and not writable by transactions", entity.collection()).into(),
                    ));
                }
                // Validate creation events: if parent is empty, this is a creation event
                // and the entity must have been created in this transaction via create()
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
                entity_events.push((entity.clone(), event));
            }
        }

        // Now validate and attest the events.
        let mut attested_events = Vec::new();
        let mut entity_attested_events = Vec::new();

        // Phase 1: check policy and collect attestations for EVERY event
        // before persisting ANY of them, so a later denial leaves nothing
        // durable (failure atomicity, V7).
        for (entity, event) in entity_events {
            // Create a temporary fork to apply the event for validation
            use std::sync::atomic::AtomicBool;
            let trx_alive = Arc::new(AtomicBool::new(true));
            let forked = entity.snapshot(trx_alive);

            // Get the canonical (upstream) entity for before state
            let entity_before = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity.clone(),
            };

            // Stage the event so validation replay can traverse it.
            let event_getter = crate::retrieval::LocalEventGetter::new(self.node.storage.clone(), self.node.durable);
            event_getter.stage_event(event.clone());
            forked.apply_event(&event_getter, &event).await?;

            let attestation = self.node.policy_agent.check_event(&self.node, &self.cdata, &entity_before, &forked, &event)?;
            let attested = Attested::opt(event.clone(), attestation);
            let accessed_as = *entity.collection();

            attested_events.push(proto::ModelContext::new(accessed_as, attested.clone()));
            entity_attested_events.push((entity, accessed_as, attested));
        }

        // Phase 2: all events attested; append the immutable event set before
        // any canonical-state CAS attempt. A later conflict or relay failure
        // may leave an unreferenced event, which is safe.
        let durable_events: Vec<Attested<Event>> = entity_attested_events.iter().map(|(_, _, event)| event.clone()).collect();
        self.node.storage.append_events(&durable_events).await?;

        // Update heads BEFORE relaying (makes entities visible to server echo)
        for (entity, _, attested_event) in &entity_attested_events {
            entity.commit_head(Clock::new([attested_event.payload.id()]));
        }
        // Relay to peers and wait for confirmation
        self.node.relay_to_required_peers(&self.cdata, trx_id, &attested_events).await?;

        // All peers confirmed. Reconcile and replay every affected resident,
        // then CAS the whole canonical/materialization batch atomically.
        let mut by_entity = std::collections::BTreeMap::new();
        let mut intents = Vec::with_capacity(entity_attested_events.len());
        for (entity, accessed_as, attested_event) in entity_attested_events {
            let canonical_entity = match &entity.kind {
                crate::entity::EntityKind::Transacted { upstream, .. } => upstream.clone(),
                crate::entity::EntityKind::Primary => entity,
            };
            by_entity.insert(canonical_entity.id(), (canonical_entity.clone(), vec![attested_event.clone()]));
            intents.push(ResidentWriteIntent::from_events(canonical_entity, vec![proto::ModelContext::new(accessed_as, attested_event)])?);
        }
        let event_getter = crate::retrieval::LocalEventGetter::new(self.node.storage.clone(), self.node.durable);
        let commit = self.node.commit_resident_writes(intents, Some(&self.cdata), &event_getter).await?;

        let mut changes: Vec<EntityChange> = Vec::new();
        for result in commit.entities {
            let (entity, events) = by_entity
                .remove(&result.entity_id)
                .ok_or_else(|| MutationError::General(format!("storage returned unknown entity {}", result.entity_id).into()))?;
            if result.canonical_changed {
                for model in result.materialized_as {
                    changes.push(EntityChange::new(entity.with_model_context(model), events.clone())?);
                }
            } else {
                for model in result.associations_added {
                    changes.push(EntityChange::new(entity.with_model_context(model), Vec::new())?);
                }
            }
        }

        // Notify reactor of ALL changes
        self.node.reactor.notify_change(changes).await;

        Ok(attested_events.into_iter().map(|event| event.value.payload).collect())
    }
    fn query(
        &self,
        collection_id: crate::ModelId,
        schema: &'static crate::schema::ModelSchema,
        args: MatchArgs,
    ) -> Result<EntityLiveQuery, RetrievalError> {
        EntityLiveQuery::new_for_model(&self.node, collection_id, schema, args, self.cdata.clone())
    }
    async fn ensure_registered(&self, schema: &'static crate::schema::ModelSchema) -> Result<ModelId, MutationError> {
        self.node.catalog.ensure_schema_for_use(&self.node, &self.cdata, schema).await.map_err(|error| {
            let message = if self.node.catalog.model_by_label(schema.collection).is_none() {
                format!("cannot write into unregistered collection '{}': {error}", schema.collection)
            } else {
                format!("cannot write using an unconfirmed schema for collection '{}': {error}", schema.collection)
            };
            MutationError::General(message.into())
        })
    }
    async fn register_strict(
        &self,
        schema: &'static crate::schema::ModelSchema,
    ) -> Result<ModelId, crate::schema::registration::RegistrationError> {
        self.node.catalog.ensure_registered(&self.node, &self.cdata, schema).await?;
        self.node.catalog.model_id_for_schema(schema).ok_or_else(|| {
            crate::schema::registration::RegistrationError::Retrieval(RetrievalError::Other(format!(
                "registration of '{}' did not retain its exact model identity",
                schema.collection
            )))
        })
    }
    async fn ensure_query_schema(&self, schema: &'static crate::schema::ModelSchema) -> Result<ModelId, RetrievalError> {
        self.node.catalog.ensure_schema_for_use(&self.node, &self.cdata, schema).await.map_err(|error| {
            if self.node.catalog.model_by_label(schema.collection).is_none() {
                RetrievalError::Other(format!("collection '{}' is not registered: {error}", schema.collection))
            } else {
                RetrievalError::Other(error.to_string())
            }
        })
    }
    fn model_id_for_schema(&self, schema: &'static crate::schema::ModelSchema) -> Option<ModelId> {
        self.node
            .catalog
            .model_id_for_schema(schema)
            .or_else(|| self.node.catalog.bind_compatible_schema(schema).then(|| self.node.catalog.model_id_for_schema(schema)).flatten())
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

    /// RFC 5.2 eager explicit registration (STRICT form): register `M`'s
    /// model, properties, and memberships now, propagating any error. Automatic
    /// mutating paths also fail a never-registered write; a failed reassertion
    /// may proceed only when every field is already bound compatibly. Useful
    /// before first render so predicate resolution has the ids ready. A second
    /// call for the same compiled shape is a no-op.
    pub async fn register<M: crate::model::Model>(&self) -> Result<ModelId, crate::schema::registration::RegistrationError> {
        self.0.register_strict(M::schema()).await
    }

    /// Admit `M` for query use and return its exact runtime identity.
    pub async fn model_id<M: crate::model::Model>(&self) -> Result<ModelId, RetrievalError> {
        self.0.ensure_query_schema(M::schema()).await
    }

    pub async fn get<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let model_id = self.0.ensure_query_schema(R::Model::schema()).await?;
        let entity = self.0.get_entity(id, &model_id, false).await?;
        Ok(R::from_entity(entity))
    }

    /// Get an entity, but its ok to return early if the entity is already in the local node storage
    pub async fn get_cached<R: View>(&self, id: proto::EntityId) -> Result<R, RetrievalError> {
        let model_id = self.0.ensure_query_schema(R::Model::schema()).await?;
        let entity = self.0.get_entity(id, &model_id, true).await?;
        Ok(R::from_entity(entity))
    }

    pub async fn fetch<R: View>(&self, args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>) -> Result<Vec<R>, RetrievalError> {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        // Predicate reads are schema-dependent and registration is their
        // admission point. Ensure this exact declaration before any catalog
        // name can resolve the selection through a different cached shape.
        let model_id = self.0.ensure_query_schema(R::Model::schema()).await?;

        let entities = self.0.fetch_entities(&model_id, args).await?;

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
        let model_id = self.0.model_id_for_schema(R::Model::schema()).ok_or_else(|| {
            RetrievalError::Other(format!(
                "model '{}' has not been admitted; call Context::register::<{}>() or use query_wait first",
                R::Model::model_name_hint(),
                std::any::type_name::<R::Model>()
            ))
        })?;
        Ok(self.0.query(model_id, R::Model::schema(), args)?.map::<R>())
    }

    /// Subscribe to changes in entities matching a selection and wait for initialization
    pub async fn query_wait<R>(
        &self,
        args: impl TryInto<MatchArgs, Error = impl Into<RetrievalError>>,
    ) -> Result<LiveQuery<R>, RetrievalError>
    where
        R: View,
    {
        let args: MatchArgs = args.try_into().map_err(|e| e.into())?;
        let model_id = self.0.ensure_query_schema(R::Model::schema()).await?;
        let livequery = self.0.query(model_id, R::Model::schema(), args)?.map::<R>();
        livequery.wait_initialized().await;
        // Initialization completing is not success: a failed resolution or
        // activation latches an error on the query and releases the waiters.
        // Surface it instead of handing back a dead query as Ok.
        if let Some(error) = livequery.latched_error() {
            return Err(error);
        }
        Ok(livequery)
    }
}

impl<SE, PA> NodeAndContext<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Retrieve a single entity, either by cloning the resident Entity from the Node's WeakEntitySet or fetching from storage
    pub(crate) async fn get_entity(&self, collection_id: &ModelId, id: proto::EntityId, cached: bool) -> Result<Entity, RetrievalError> {
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

        if let Some(local) = self.node.entities.get_as(&id, *collection_id) {
            debug!("Node({}).get_entity found local entity - returning", self.node.id);
            let state = local.to_state()?;
            let entity_id = local.id();
            self.node.policy_agent.check_read(&self.cdata, &entity_id, collection_id, &state, Some(self.node.catalog.resolver_weak()))?;
            return Ok(local);
        }
        debug!("{}.get_entity fetching from storage", self.node);

        match self.node.storage.get_state(id).await {
            Ok(entity_state) => {
                self.node.policy_agent.check_read(
                    &self.cdata,
                    &entity_state.payload.entity_id,
                    collection_id,
                    &entity_state.payload.state,
                    Some(self.node.catalog.resolver_weak()),
                )?;
                let state_getter = crate::retrieval::LocalStateGetter::new(self.node.storage.clone());
                let event_getter = crate::retrieval::CachedEventGetter::new(*collection_id, &self.node, &self.cdata);
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
    pub async fn fetch_entities(&self, collection_id: &ModelId, mut args: MatchArgs) -> Result<Vec<Entity>, RetrievalError> {
        self.node.policy_agent.can_access_collection(&self.cdata, collection_id)?;
        // Fetch raw states from storage

        args.selection.predicate = self.node.policy_agent.filter_predicate(&self.cdata, collection_id, args.selection.predicate)?;

        // Resolve property references against the catalog: PathExpr becomes a
        // stable Identifier and unknown properties fail closed.
        // Idempotent, so a selection resolved by an outer caller passes
        // through unchanged here. A collection that neither the catalog
        // cache nor first-use registration can resolve fails LOUD
        // (UnregisteredCollection): a lagging replica cannot prove
        // emptiness, and a successful registration answers empty truthfully
        // by querying the real, entity-free collection.
        args.selection = self
            .node
            .resolve_selection_names(Some(&self.cdata), collection_id, None, &args.selection)
            .await
            .map_err(RetrievalError::from)?;

        // TODO implement cached: true
        if !self.node.durable {
            // Fetch from peers and commit first response
            Ok(self.fetch_from_peer(collection_id, args.selection).await?)
        } else {
            let states = self.node.storage.fetch_states(collection_id, &args.selection).await?;

            // Convert states to entities
            let mut entities = Vec::new();
            let state_getter = crate::retrieval::LocalStateGetter::new(self.node.storage.clone());
            let event_getter = crate::retrieval::CachedEventGetter::new(*collection_id, &self.node, &self.cdata);
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
        collection_id: &crate::ModelId,
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
        let model = *collection_id;
        match self
            .node
            .request(peer_id, &self.cdata, proto::NodeRequestBody::Fetch { model: model.clone(), selection, known_matches })
            .await?
        {
            proto::NodeResponseBody::Fetch(deltas) => {
                if deltas.iter().any(|delta| delta.model != model) {
                    return Err(RetrievalError::Other("Fetch response crossed the requested model boundary".to_owned()));
                }
                let event_getter = crate::retrieval::CachedEventGetter::new(*collection_id, &self.node, &self.cdata);
                let state_getter = crate::retrieval::LocalStateGetter::new(self.node.storage.clone());

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
