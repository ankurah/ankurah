use crate::event_dag::DEFAULT_BUDGET;
use crate::retrieval::{GetEvents, GetState};
use crate::selection::filter::Filterable;
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    event_dag::AbstractCausalRelation,
    model::View,
    property::backend::{backend_from_string, PropertyBackend},
    property::PropertyKey,
    reactor::AbstractEntity,
    schema::CatalogResolver,
    value::Value,
};
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventBody, EventId, OperationSet, State};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tracing::{debug, error, warn};

/// Result of applying a state snapshot to an entity.
pub enum StateApplyResult {
    /// StrictDescends — state applied directly
    Applied,
    /// DivergedSince — cannot merge without events
    DivergedRequiresEvents,
    /// Equal — no-op, state already matches
    AlreadyApplied,
    /// StrictAscends — incoming state is older, no-op
    Older,
}

/// An entity represents a unique thing within a collection. Entity can only be constructed via a WeakEntitySet
/// which provides duplication guarantees.
#[derive(Debug, Clone)]
pub struct Entity(Arc<EntityInner>);

// TODO optimize this to be faster for scanning over entries in a collection
/// Used only for reconstituting state to filter database results. No duplication guarantees are provided
pub struct TemporaryEntity(Arc<EntityInner>);

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
struct EntityInnerState {
    head: Clock,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
}

impl EntityInnerState {
    /// Apply operations from an event, tracking which event set each property.
    ///
    /// This enables per-property conflict resolution when concurrent events arrive later.
    /// For CRDT backends (like Yrs), the event_id tracking is a no-op since CRDTs
    /// handle concurrency internally. For LWW backends, this stores the event_id
    /// alongside each property value.
    fn apply_operations_from_event(
        &mut self,
        backend_name: String,
        operations: &[ankurah_proto::Operation],
        event_id: EventId,
    ) -> Result<(), MutationError> {
        if let Some(backend) = self.backends.get(&backend_name) {
            backend.apply_operations_with_event(operations, event_id)?;
        } else {
            let backend = backend_from_string(&backend_name, None)?;
            backend.apply_operations_with_event(operations, event_id)?;
            self.backends.insert(backend_name, backend);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct EntityInner {
    pub id: EntityId,
    pub collection: CollectionId,
    /// Combined state RwLock for atomic head/backends updates
    state: std::sync::RwLock<EntityInnerState>,
    pub(crate) kind: EntityKind,
    /// Broadcast for notifying Signal subscribers about entity changes
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast,
    /// Catalog resolver stamped at assembly (the PropertyKey amendment, #289):
    /// lets this catalog-blind entity resolve a display name to the
    /// property-definition id its data is keyed under, for the sync read path.
    /// Absent for bare and temporary entities (they read name-keyed data only).
    resolver: std::sync::RwLock<Option<Weak<dyn CatalogResolver>>>,
    /// Shared liveness token for the system generation that assembled this
    /// entity. A hard reset flips the token before deleting storage, so even
    /// strong Entity/View handles retained by callers immediately become
    /// unusable and cannot observe or re-persist the old system.
    system_alive: Arc<AtomicBool>,
}

#[derive(Debug)]
pub enum EntityKind {
    Primary,                                                     // New or resident entity - TODO delineate these
    Transacted { trx_alive: Arc<AtomicBool>, upstream: Entity }, // Transaction fork with liveness tracking
}

impl std::ops::Deref for Entity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::Deref for TemporaryEntity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

/// A weak reference to an entity
pub struct WeakEntity(Weak<EntityInner>);

impl WeakEntity {
    pub fn upgrade(&self) -> Option<Entity> { self.0.upgrade().map(Entity) }
}

/// Resolver-aware read helpers shared by every EntityInner view: the resident
/// [`Entity`] and the [`TemporaryEntity`] (which is bound only when
/// constructed via [`TemporaryEntity::new_bound`]). Both deref here.
impl EntityInner {
    fn is_system_alive(&self) -> bool { self.system_alive.load(Ordering::Acquire) }

    /// The catalog resolver stamped at assembly, if still live.
    fn resolver(&self) -> Option<Arc<dyn CatalogResolver>> { self.resolver.read().unwrap().as_ref().and_then(|w| w.upgrade()) }

    /// Stamp the catalog resolver (assembly-time). Replaces the old
    /// backend-binding push (`Node::bind_entity`): the backend stays dumb and
    /// the entity resolves names to ids for the sync read path.
    pub(crate) fn set_resolver(&self, resolver: Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    /// Resolve a display name to the key its data is stored under: the
    /// property-definition id when the catalog knows the field, else the bare
    /// name (system/catalog collections, legacy residue, or a transient staged
    /// field whose commit must still confirm registration).
    fn resolve_key(&self, name: &str) -> PropertyKey {
        match self.resolver().and_then(|r| r.resolve(self.collection.as_str(), name)) {
            Some(id) => PropertyKey::Id(id),
            None => PropertyKey::Name(name.to_string()),
        }
    }

    /// Resolve a display name to its property key for the value accessors
    /// (read / write / subscribe). Same resolution as the internal read path.
    pub(crate) fn property_key(&self, name: &str) -> PropertyKey { self.resolve_key(name) }

    /// Resolve an ordinary user-model field without permitting a fresh Name
    /// fallback. A missing resolver slot is the intentional name-keyed
    /// system/bare-entity case; once a resolver has been stamped, failure to
    /// upgrade or resolve it is a missing/ambiguous catalog binding and must
    /// surface as an error. Explicit-id accessors bypass this method.
    pub(crate) fn checked_property_key(&self, name: &str) -> Result<PropertyKey, crate::property::traits::PropertyError> {
        let resolver = self.resolver.read().expect("other thread panicked, panic here too");
        let Some(resolver) = resolver.as_ref() else {
            return Ok(PropertyKey::Name(name.to_string()));
        };
        let Some(resolver) = resolver.upgrade() else {
            return Err(crate::property::traits::PropertyError::UnknownProperty {
                collection: self.collection.to_string(),
                name: name.to_string(),
            });
        };
        resolver.resolve(self.collection.as_str(), name).map(PropertyKey::Id).ok_or_else(|| {
            crate::property::traits::PropertyError::UnknownProperty { collection: self.collection.to_string(), name: name.to_string() }
        })
    }

    /// Defensive canonicalization at the read/evaluation boundary (rfc.md 5.6
    /// as amended 2026-07-10): commits canonicalize writes, but ingest is
    /// deliberately schema-blind (catalog lag), so a legacy or ill-typed
    /// payload can sit under a property id. Comparisons and the reactor's
    /// watcher index collate canonical bytes, so an off-type value casts to
    /// the canonical type here; a value the canonical type cannot represent
    /// reads as NULL with a warning, never a poisoned
    /// evaluation. No resolver, unknown id, or unknown type string: the value
    /// passes through unchanged.
    fn canonicalize_lenient(&self, id: &EntityId, value: Value) -> Option<Value> {
        let Some(target) =
            self.resolver().and_then(|r| r.canonical_value_type(id)).and_then(|s| crate::value::ValueType::from_property_str(&s))
        else {
            return Some(value);
        };
        if crate::value::ValueType::of(&value) == target {
            return Some(value);
        }
        match value.cast_to(target) {
            Ok(cast) => Some(cast),
            Err(err) => {
                tracing::warn!(
                    "entity {}: value under property {} does not fit its canonical type ({}); reading as NULL",
                    self.id,
                    id,
                    err
                );
                None
            }
        }
    }

    /// Read `name` for evaluation and lenient consumers: resolve to the id,
    /// then run the generic Id-then-legacy-Name presence dispatch
    /// ([`crate::property::read_resolved`]) uniformly over every backend --
    /// an id key present (even cleared) is authoritative and never resurrects
    /// a stale legacy name value (the PropertyKey amendment, #289). A field
    /// belongs to exactly one backend per schema, so the first backend
    /// holding an entry wins; there is no per-backend special-casing here
    /// (backends are addressed only through the [`PropertyBackend`] trait).
    fn read_lenient(&self, name: &str) -> Option<Value> {
        if !self.is_system_alive() {
            return None;
        }
        let state = self.state.read().expect("other thread panicked, panic here too");
        match self.resolve_key(name) {
            PropertyKey::Id(id) => state
                .backends
                .values()
                .find_map(|backend| crate::property::read_resolved(backend.as_ref(), id, name))
                .and_then(|value| self.canonicalize_lenient(&id, value)),
            key @ PropertyKey::Name(_) => state.backends.values().find_map(|backend| backend.property_value(&key)),
        }
    }

    /// Read for RESOLVED-identifier predicate evaluation: the same generic
    /// dispatch with the caller-supplied property id (no name re-resolution),
    /// canonicalized at the evaluation boundary. Absent evaluates NULL.
    /// Evaluation has no error surface: type admission is
    /// registration's job (the canonical value_type ruling, 2026-07-10), and
    /// an ill-typed stored value reads as NULL with a warning.
    pub(crate) fn read_resolved_eval(&self, property_id: EntityId, name: &str) -> Option<Value> {
        if !self.is_system_alive() {
            return None;
        }
        if name == "id" {
            return Some(Value::EntityId(self.id));
        }
        let state = self.state.read().expect("other thread panicked, panic here too");
        state
            .backends
            .values()
            .find_map(|backend| crate::property::read_resolved(backend.as_ref(), property_id, name))
            .and_then(|value| self.canonicalize_lenient(&property_id, value))
    }
}

impl Entity {
    pub fn id(&self) -> EntityId { self.id }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn head(&self) -> Clock {
        if !self.is_system_alive() {
            return Clock::default();
        }
        self.state.read().unwrap().head.clone()
    }

    /// Whether this handle still belongs to the node's active system
    /// generation. Primarily exposed for property projections; callers should
    /// normally observe the typed `SystemReset` errors from reads/writes.
    pub(crate) fn is_system_alive(&self) -> bool { self.0.is_system_alive() }

    /// Exact generation identity, not merely liveness. Two independently
    /// assembled resident entities may both carry live tokens, so async
    /// assembly must compare the token pointer as well as its value before it
    /// hands an entity back to a generation-bound caller.
    fn belongs_to_system_generation(&self, generation: &Arc<AtomicBool>) -> bool {
        self.is_system_alive() && Arc::ptr_eq(&self.system_alive, generation)
    }

    fn ensure_system_alive_mutation(&self) -> Result<(), MutationError> {
        if self.is_system_alive() {
            Ok(())
        } else {
            Err(MutationError::SystemReset)
        }
    }

    pub(crate) fn ensure_system_alive(&self) -> Result<(), crate::property::PropertyError> {
        if self.is_system_alive() {
            Ok(())
        } else {
            Err(crate::property::PropertyError::SystemReset)
        }
    }

    /// Resolve transient uncommitted `Name` keys to their `Id` at commit, using
    /// the catalog-aware resolver (the PropertyKey amendment, #289), and cast
    /// each staged LWW value to the property's CANONICAL value_type (rfc.md
    /// 5.6 as amended 2026-07-10), so backends, engines, and indexes store one
    /// consistent type regardless of the writer's compiled type. A value the
    /// canonical type cannot represent fails the commit here, at the writer.
    /// Pure key/value operations on the backends; the catalog never enters them.
    pub(crate) fn resolve_pending_keys(&self) -> Result<(), crate::property::traits::PropertyError> {
        self.ensure_system_alive()?;
        let Some(resolver) = self.resolver() else {
            // No resolver (a bare entity, or a system/catalog collection):
            // nothing to resolve, keys stay name-keyed as staged.
            return Ok(());
        };
        let collection = self.collection.as_str();
        let state = self.state.read().expect("other thread panicked, panic here too");
        for backend in state.backends.values() {
            for key in backend.uncommitted_keys() {
                let id = match &key {
                    PropertyKey::Name(name) => {
                        let Some(id) = resolver.resolve(collection, name) else {
                            // Resolver-bound entities are registered user
                            // models. An unresolved staged name here means the
                            // compiled field is missing or ambiguous; emitting
                            // it would leak fresh Name residue into the
                            // id-keyed wire epoch. System/catalog entities have
                            // no resolver, and committed legacy Name entries do
                            // not appear in `uncommitted_keys`, so both allowed
                            // name-addressed cases remain untouched.
                            return Err(crate::property::traits::PropertyError::UnknownProperty {
                                collection: collection.to_string(),
                                name: name.clone(),
                            });
                        };
                        backend.rekey(&key, PropertyKey::Id(id));
                        id
                    }
                    // Explicit bindings stage under their literal id from the
                    // outset. They still require the same writer-side cast to
                    // the catalog's canonical value_type as a re-keyed field.
                    PropertyKey::Id(id) => *id,
                };

                // Canonicalize the staged value through the generic trait
                // primitives (`entry` / `restage`): a backend whose edits are
                // not value-shaped (a text CRDT) stages nothing and restages
                // nothing. A staged clear (tombstone) has no value to cast.
                // An unknown canonical string (a newer fleet's type) writes
                // as compiled: registration admitted this binary, so the
                // types were equal.
                if let Some(target) = resolver.canonical_value_type(&id).and_then(|c| crate::value::ValueType::from_property_str(&c)) {
                    let staged = PropertyKey::Id(id);
                    if let Some(Some(value)) = backend.entry(&staged) {
                        if crate::value::ValueType::of(&value) != target {
                            let cast = value.cast_to(target)?;
                            backend.restage(&staged, Some(cast));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if this entity is writable (i.e., it's a transaction fork that's still alive)
    pub fn is_writable(&self) -> bool {
        if !self.is_system_alive() {
            return false;
        }
        match &self.kind {
            EntityKind::Primary => false, // Primary entities are read-only
            EntityKind::Transacted { trx_alive, .. } => trx_alive.load(Ordering::Acquire),
        }
    }

    pub fn to_state(&self) -> Result<State, StateError> {
        if !self.is_system_alive() {
            return Err(StateError::SystemReset);
        }
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut state_buffers = BTreeMap::default();
        for (name, backend) in &state.backends {
            let state_buffer = backend.to_state_buffer()?;
            state_buffers.insert(name.clone(), state_buffer);
        }
        let state_buffers = ankurah_proto::StateBuffers(state_buffers);
        Ok(State { state_buffers, head: state.head.clone() })
    }

    pub fn to_entity_state(&self) -> Result<EntityState, StateError> {
        let state = self.to_state()?;
        Ok(EntityState { entity_id: self.id(), model: self.model_id()?, state })
    }

    /// The model-definition id stamped on this entity's wire envelopes
    /// (#330): the well-known id for system/catalog collections, else the
    /// stamped catalog resolver's mapping for the collection. Fails
    /// `UnknownModel` when neither answers.
    pub(crate) fn model_id(&self) -> Result<EntityId, StateError> {
        if !self.is_system_alive() {
            return Err(StateError::SystemReset);
        }
        crate::schema::well_known_model_id(self.collection.as_str())
            .or_else(|| self.resolver().and_then(|r| r.model_id_for(self.collection.as_str())))
            .ok_or_else(|| StateError::UnknownModel(self.collection.to_string()))
    }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId) -> Self {
        Self::create_in_system(id, collection, Arc::new(AtomicBool::new(true)))
    }

    fn create_in_system(id: EntityId, collection: CollectionId, system_alive: Arc<AtomicBool>) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: Clock::default(), backends: BTreeMap::default() }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            resolver: std::sync::RwLock::new(None),
            system_alive,
        }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state_in_system(
        id: EntityId,
        collection: CollectionId,
        state: &State,
        system_alive: Arc<AtomicBool>,
    ) -> Result<Self, RetrievalError> {
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }

        Ok(Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            resolver: std::sync::RwLock::new(None),
            system_alive,
        })))
    }

    /// Extract all operations staged since the previous extraction.
    ///
    /// Creation uses this once, eagerly, to freeze the genesis payload before
    /// the entity id exists. Commit uses it again for any edits made after
    /// `Transaction::create` returned. Backends therefore remain the single
    /// source of truth for the extraction boundary.
    pub(crate) fn extract_operations(&self) -> Result<OperationSet, MutationError> {
        if !self.is_system_alive() {
            return Err(MutationError::SystemReset);
        }
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut operations = BTreeMap::<String, Vec<ankurah_proto::Operation>>::new();
        for (name, backend) in &state.backends {
            if let Some(ops) = backend.to_operations()? {
                operations.insert(name.clone(), ops);
            }
        }
        Ok(OperationSet(operations))
    }

    /// Generate the single update event for edits accumulated on a transaction
    /// entity. Genesis is frozen and stored by `Transaction::create`; an
    /// empty-head entity reaching this path is therefore a phantom, never an
    /// implicit creation.
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        if !self.is_system_alive() {
            return Err(MutationError::SystemReset);
        }
        let parent = self.head();
        if parent.is_empty() {
            return Err(MutationError::InvalidEvent);
        }

        let operations = self.extract_operations()?;
        if operations.is_empty() {
            return Ok(None);
        }

        Ok(Some(Event { entity_id: self.id, model: self.model_id()?, parent, body: EventBody::Update { operations } }))
    }

    /// Updates the head of the entity to the given clock, which should come exclusively from generate_commit_event
    pub(crate) fn commit_head(&self, new_head: Clock) {
        // TODO figure out how to implement CAS with the backend state
        // probably need an increment for local edits
        self.state.write().unwrap().head = new_head;
    }

    /// Attempts to mutate the entity state if the head matches the expected value.
    ///
    /// This provides TOCTOU protection: grabs the write lock, checks that `state.head == expected_head`,
    /// and only then runs the closure. If the head changed, updates `expected_head` to the current value
    /// and returns `Ok(false)` so the caller can retry with fresh lineage info.
    ///
    /// Returns `Ok(true)` if the mutation succeeded, `Ok(false)` if the head moved (retry needed),
    /// or `Err` if the closure returned an error.
    fn try_mutate<F, E>(&self, expected_head: &mut Clock, body: F) -> Result<bool, E>
    where F: FnOnce(&mut EntityInnerState) -> Result<(), E> {
        let mut state = self.state.write().unwrap();
        if &state.head != expected_head {
            *expected_head = state.head.clone();
            return Ok(false);
        }
        body(&mut state)?;
        Ok(true)
    }

    pub fn view<V: View>(&self) -> Option<V> {
        if !self.is_system_alive() || self.collection() != &V::collection() {
            None
        } else {
            Some(V::from_entity(self.clone()))
        }
    }

    /// Attempt to apply an event to the entity
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    pub async fn apply_event<E>(&self, getter: &E, event: &Event) -> Result<bool, MutationError>
    where E: GetEvents + Send + Sync {
        self.ensure_system_alive_mutation()?;
        debug!("apply_event head: {event} to {self}");

        // Structural identity is sufficient on every node class: a genesis
        // event names exactly the entity id derived from its own content, and
        // updates must target this entity. No definitive-storage oracle is
        // needed to decide whether a creation is admissible.
        if event.entity_id != self.id || event.validate_structure().is_err() {
            return Err(MutationError::InvalidEvent);
        }

        // Idempotency is handled by the comparison algorithm:
        // - Event already in head -> Equal -> no-op (Ok(false))
        // - Event is ancestor of head -> StrictAscends -> no-op (Ok(false))
        // - Event re-delivered but already integrated -> BFS finds it -> StrictAscends
        // An explicit event_stored() check is not used here because callers
        // (node_applier, system.rs) store events to storage BEFORE calling
        // apply_event (so BFS can find them), which would cause false positives.

        // Check for entity creation under the mutex to avoid TOCTOU race
        if event.is_entity_create() {
            let mut state = self.state.write().unwrap();
            self.ensure_system_alive_mutation()?;
            // Re-check if head is still empty now that we hold the lock
            if state.head.is_empty() {
                // this is the creation event for a new entity, so we simply accept it
                for (backend_name, operations) in event.operations().iter() {
                    state.apply_operations_from_event(backend_name.clone(), operations, event.id())?;
                }
                state.head = event.id().into();
                drop(state); // Release lock before broadcast
                self.ensure_system_alive_mutation()?;
                // Notify Signal subscribers about the change
                self.broadcast.send(());
                return Ok(true);
            }
            // If head is no longer empty, fall through to normal lineage comparison
        }

        // Non-creation event on an entity with empty heads means the entity was never created.
        // Reject early — the DAG comparison would produce DivergedSince(meet=[]) which would
        // incorrectly apply the update to a non-existent entity.
        if !event.is_entity_create() && self.head().is_empty() {
            return Err(MutationError::InvalidEvent);
        }

        let mut head = self.head();
        // Retry loop to handle head changes between lineage comparison and mutation
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            // Stage the event so BFS can discover it, then compare event's clock vs head
            let subject_clock: Clock = event.id().into();
            let comparison_result = crate::event_dag::compare(getter, &subject_clock, &head, DEFAULT_BUDGET).await?;
            self.ensure_system_alive_mutation()?;
            match comparison_result.relation {
                AbstractCausalRelation::Equal => {
                    debug!("Equal - skip");
                    return Ok(false);
                }
                AbstractCausalRelation::StrictDescends { .. } => {
                    debug!("Descends - apply (attempt {})", attempt + 1);
                    let new_head: Clock = event.id().into();
                    let event_id = event.id();
                    if self.try_mutate(&mut head, |state| -> Result<(), MutationError> {
                        self.ensure_system_alive_mutation()?;
                        for (backend_name, operations) in event.operations().iter() {
                            state.apply_operations_from_event(backend_name.clone(), operations, event_id.clone())?;
                        }
                        state.head = new_head.clone();
                        Ok(())
                    })? {
                        self.ensure_system_alive_mutation()?;
                        self.broadcast.send(());
                        return Ok(true);
                    }
                    continue;
                }
                AbstractCausalRelation::StrictAscends => {
                    // Incoming event is older than current state - no-op
                    debug!("StrictAscends - incoming event is older, ignoring");
                    return Ok(false);
                }
                AbstractCausalRelation::DivergedSince { ref meet, .. } => {
                    debug!("DivergedSince - true concurrency, applying via layers (attempt {})", attempt + 1);

                    let meet = meet.clone();

                    // Decompose the result to get the accumulator.
                    // The event is already in the accumulated DAG (found via staging in BFS).
                    let (_relation, accumulator) = comparison_result.into_parts();
                    let mut layers = accumulator.into_layers(meet.clone(), head.as_slice().to_vec());

                    let mut applied_layers: Vec<crate::event_dag::EventLayer> = Vec::new();

                    // Collect all layers first, then apply under lock
                    let mut all_layers = Vec::new();
                    while let Some(layer) = layers.next().await? {
                        self.ensure_system_alive_mutation()?;
                        all_layers.push(layer);
                    }

                    // Atomic update: apply layers and augment head under single lock
                    {
                        let mut state = self.state.write().unwrap();
                        self.ensure_system_alive_mutation()?;
                        // Re-check that head hasn't changed since lineage comparison
                        if state.head != head {
                            warn!("Head changed during lineage comparison, retrying...");
                            head = state.head.clone();
                            continue;
                        }

                        // Apply layers in causal order
                        for layer in all_layers {
                            // Check for backends that first appear in this layer's to_apply events
                            for evt in &layer.to_apply {
                                for (backend_name, _) in evt.operations().iter() {
                                    if !state.backends.contains_key(backend_name) {
                                        let backend = backend_from_string(backend_name, None)?;
                                        // Replay earlier layers for this newly-created backend
                                        for earlier in &applied_layers {
                                            backend.apply_layer(earlier)?;
                                        }
                                        state.backends.insert(backend_name.to_string(), backend);
                                    }
                                }
                            }

                            // Apply to all backends
                            for (_backend_name, backend) in state.backends.iter() {
                                backend.apply_layer(&layer)?;
                            }
                            applied_layers.push(layer);
                        }

                        // Update head: remove superseded tips, add new event
                        // The incoming event extends tips in its parent clock (meet).
                        // Any of those that are in the current head are now superseded.
                        for parent_id in &meet {
                            state.head.remove(parent_id);
                        }
                        state.head.insert(event.id());
                    }
                    self.ensure_system_alive_mutation()?;
                    self.broadcast.send(());
                    return Ok(true);
                }
                AbstractCausalRelation::Disjoint { .. } => {
                    return Err(LineageError::Disjoint.into());
                }
                AbstractCausalRelation::BudgetExceeded { subject, other } => {
                    return Err(LineageError::BudgetExceeded {
                        original_budget: DEFAULT_BUDGET,
                        subject_frontier: subject,
                        other_frontier: other,
                    }
                    .into());
                }
            }
        }

        warn!("apply_event retries exhausted while chasing moving head");
        Err(MutationError::TOCTOUAttemptsExhausted)
    }

    /// Apply a state snapshot to this entity.
    ///
    /// Returns `StateApplyResult` indicating what happened:
    /// - `Applied` — state was newer and applied directly (StrictDescends)
    /// - `AlreadyApplied` — state matches current head (Equal)
    /// - `Older` — incoming state is older than current (StrictAscends), no-op
    /// - `DivergedRequiresEvents` — state diverged, events needed for proper merge
    pub async fn apply_state<E>(&self, getter: &E, state: &State) -> Result<StateApplyResult, MutationError>
    where E: GetEvents + Send + Sync {
        self.ensure_system_alive_mutation()?;
        let mut head = self.head();
        let new_head = state.head.clone();

        debug!("{self} apply_state - new head: {new_head}");
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            let comparison_result = crate::event_dag::compare(getter, &new_head, &head, DEFAULT_BUDGET).await?;
            self.ensure_system_alive_mutation()?;
            match comparison_result.relation {
                AbstractCausalRelation::Equal => {
                    debug!("{self} apply_state - heads are equal, skipping");
                    return Ok(StateApplyResult::AlreadyApplied);
                }
                AbstractCausalRelation::StrictDescends { .. } => {
                    debug!("{self} apply_state - new head descends from current, applying (attempt {})", attempt + 1);
                    let new_head = state.head.clone();
                    if self.try_mutate(&mut head, |es| -> Result<(), MutationError> {
                        self.ensure_system_alive_mutation()?;
                        for (name, state_buffer) in state.state_buffers.iter() {
                            let backend = backend_from_string(name, Some(state_buffer))?;
                            es.backends.insert(name.to_owned(), backend);
                        }
                        es.head = new_head;
                        Ok(())
                    })? {
                        self.ensure_system_alive_mutation()?;
                        self.broadcast.send(());
                        return Ok(StateApplyResult::Applied);
                    }
                    continue;
                }
                AbstractCausalRelation::StrictAscends => {
                    // State is older than current - no-op
                    debug!("{self} apply_state - new head {new_head} is older than current {head}, ignoring");
                    return Ok(StateApplyResult::Older);
                }
                AbstractCausalRelation::DivergedSince { meet, .. } => {
                    // State snapshots cannot be merged without the underlying events.
                    // The caller should either:
                    // 1. Request the full event history and use apply_event() for each
                    // 2. Accept this state via policy if the attestation is trusted
                    // 3. Reject and resync from a known-good state
                    warn!(
                        "{self} apply_state - new head {new_head} diverged from {head}, meet: {meet:?}. \
                        State not applied; events required for proper merge."
                    );
                    return Ok(StateApplyResult::DivergedRequiresEvents);
                }
                AbstractCausalRelation::Disjoint { .. } => {
                    error!("{self} apply_state - heads are disjoint (different genesis)");
                    return Err(LineageError::Disjoint.into());
                }
                AbstractCausalRelation::BudgetExceeded { subject, other } => {
                    tracing::warn!("{self} apply_state - budget exceeded. subject: {subject:?}, other: {other:?}");
                    return Err(LineageError::BudgetExceeded {
                        original_budget: DEFAULT_BUDGET,
                        subject_frontier: subject,
                        other_frontier: other,
                    }
                    .into());
                }
            }
        }

        warn!("apply_state retries exhausted while chasing moving head");
        Err(MutationError::TOCTOUAttemptsExhausted)
    }

    /// Create a snapshot of the Entity which is detached from this one, and will not receive the updates this one does
    /// The trx_alive parameter tracks whether the transaction that owns this snapshot is still alive
    pub fn snapshot(&self, trx_alive: Arc<AtomicBool>) -> Self {
        // Inline fork logic
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut forked = BTreeMap::new();
        for (name, backend) in &state.backends {
            forked.insert(name.clone(), backend.fork());
        }

        Self(Arc::new(EntityInner {
            id: self.id,
            collection: self.collection.clone(),
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends: forked }),
            kind: EntityKind::Transacted { trx_alive, upstream: self.clone() },
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            // Carry the resolver across the fork so a transaction's read path
            // still resolves names to ids.
            resolver: std::sync::RwLock::new(self.resolver.read().unwrap().clone()),
            system_alive: self.system_alive.clone(),
        }))
    }

    /// Build the transaction-visible state immediately after a frozen genesis
    /// while keeping `self` as the empty resident primary until commit.
    ///
    /// Re-encoding each applied backend through its state buffer establishes a
    /// clean extraction baseline for every backend. This is important for Yrs:
    /// applying the genesis update mutates the document, but only a freshly
    /// decoded state advances its local `previous_state`, so the initial
    /// operations cannot leak into the later Update event.
    fn snapshot_after_genesis(&self, genesis: &Event, trx_alive: Arc<AtomicBool>) -> Result<Self, MutationError> {
        if !genesis.is_entity_create() || genesis.entity_id != self.id || genesis.validate_structure().is_err() {
            return Err(MutationError::InvalidEvent);
        }
        if genesis.model != self.model_id()? {
            return Err(MutationError::InvalidEvent);
        }

        let event_id = genesis.id();
        let mut backends = BTreeMap::new();
        for (name, operations) in genesis.operations().iter() {
            let backend = backend_from_string(name, None)?;
            backend.apply_operations_with_event(operations, event_id.clone())?;
            let state = backend.to_state_buffer()?;
            backends.insert(name.clone(), backend_from_string(name, Some(&state))?);
        }

        Ok(Self(Arc::new(EntityInner {
            id: self.id,
            collection: self.collection.clone(),
            state: std::sync::RwLock::new(EntityInnerState { head: event_id.into(), backends }),
            kind: EntityKind::Transacted { trx_alive, upstream: self.clone() },
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            resolver: std::sync::RwLock::new(self.resolver.read().unwrap().clone()),
            system_alive: self.system_alive.clone(),
        })))
    }

    /// Get a reference to the entity's broadcast for Signal implementations
    pub fn broadcast(&self) -> &ankurah_signals::broadcast::Broadcast { &self.broadcast }

    /// Get a specific backend, creating it if it doesn't exist
    pub fn get_backend<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        let backend_name = P::property_backend_name();
        let mut state = self.state.write().expect("other thread panicked, panic here too");
        if let Some(backend) = state.backends.get(backend_name) {
            let upcasted = backend.clone().as_arc_dyn_any();
            Ok(upcasted.downcast::<P>().unwrap()) // TODO: handle downcast error
        } else {
            let backend = backend_from_string(backend_name, None)?;
            let upcasted = backend.clone().as_arc_dyn_any();
            let typed_backend = upcasted.downcast::<P>().unwrap(); // TODO handle downcast error
            state.backends.insert(backend_name.to_owned(), backend);
            Ok(typed_backend)
        }
    }

    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        if !self.is_system_alive() {
            return Vec::new();
        }
        let state = self.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values().into_iter().map(|(k, v)| (k.display_name(), v))).collect()
    }
}

// Implement AbstractEntity for Entity (used by reactor)
impl AbstractEntity for Entity {
    fn collection(&self) -> ankurah_proto::CollectionId { self.collection.clone() }

    fn id(&self) -> &ankurah_proto::EntityId { &self.id }

    fn value(&self, field: &str) -> Option<crate::value::Value> {
        if field == "id" {
            Some(crate::value::Value::EntityId(self.id))
        } else {
            self.read_lenient(field)
        }
    }

    fn value_resolved(&self, property_id: ankurah_proto::EntityId, name: &str) -> Option<crate::value::Value> {
        self.read_resolved_eval(property_id, name)
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{} {:#})", self.collection, self.id.to_base64_short(), self.head())
    }
}

impl Filterable for Entity {
    fn collection(&self) -> &str { self.collection.as_str() }

    fn value(&self, name: &str) -> Option<Value> {
        if name == "id" {
            Some(Value::EntityId(self.id))
        } else {
            self.read_lenient(name)
        }
    }

    /// Read for RESOLVED-identifier predicate evaluation, via the shared
    /// [`EntityInner::read_resolved_eval`] dispatch.
    fn value_resolved(&self, property_id: EntityId, name: &str) -> Option<Value> { self.0.read_resolved_eval(property_id, name) }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        // Temporary entities parse UNBOUND by default (engine post-filter
        // tier): no resolver, so lenient name reads see only name-keyed data,
        // while resolved-identifier predicate reads carry their own id.
        Self::new_bound(id, collection, state, None)
    }

    /// [`Self::new`] with a catalog resolver stamped, for consumers that
    /// evaluate NAME-addressed predicates against id-keyed state -- policy
    /// agents inspecting entity state for scope enforcement (the
    /// `PolicyAgent::check_read` resolver parameter). Without the binding, a
    /// display name resolves to nothing on a post-epoch buffer and every
    /// scope predicate evaluates false.
    pub fn new_bound(
        id: EntityId,
        collection: CollectionId,
        state: &State,
        resolver: Option<std::sync::Weak<dyn CatalogResolver>>,
    ) -> Result<Self, RetrievalError> {
        // Inline from_state_buffers logic
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }

        Ok(Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends }),
            kind: EntityKind::Primary,
            // slightly annoying that we need to populate this, given that it won't be used
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            resolver: std::sync::RwLock::new(resolver),
            // Temporary entities are storage-decoding values, not resident
            // node handles, so they are scoped to the operation that built
            // them rather than to a resettable resident generation.
            system_alive: Arc::new(AtomicBool::new(true)),
        })))
    }
    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.0.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values().into_iter().map(|(k, v)| (k.display_name(), v))).collect()
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, name: &str) -> Option<Value> {
        if name == "id" {
            Some(Value::EntityId(self.0.id))
        } else {
            // The shared lenient dispatch: with a stamped resolver
            // (new_bound, e.g. policy scope inspection) the name resolves to
            // its id and reads id-keyed data; unbound (the engine
            // post-filter tier) it degenerates to the bare name-keyed scan,
            // exactly the pre-binding behavior. Schema-blind name projection
            // of id-keyed entries remains tracked by #312.
            self.0.read_lenient(name)
        }
    }

    /// Read for RESOLVED-identifier predicate evaluation, via the shared
    /// [`EntityInner::read_resolved_eval`] dispatch. An unbound temporary (no
    /// resolver) skips canonicalization: the caller-supplied resolved id is
    /// the only schema knowledge in play.
    fn value_resolved(&self, property_id: EntityId, name: &str) -> Option<Value> { self.0.read_resolved_eval(property_id, name) }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}/{}) = {}", &self.collection, self.id, self.0.state.read().unwrap().head)
    }
}

#[cfg(test)]
mod eager_genesis_tests {
    use super::*;
    use crate::property::backend::{LWWBackend, YrsBackend};
    use crate::property::PropertyKey;
    use crate::retrieval::GetEvents;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    struct PausingEvents {
        events: BTreeMap<EventId, Event>,
        pause_once: AtomicBool,
        seen: Notify,
        release: Notify,
    }

    #[async_trait]
    impl GetEvents for PausingEvents {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
            if self.pause_once.swap(false, Ordering::AcqRel) {
                self.seen.notify_one();
                self.release.notified().await;
            }
            self.events.get(event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
        }

        async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { Ok(self.events.contains_key(event_id)) }
    }

    #[test]
    fn genesis_is_the_clean_baseline_for_post_create_updates() {
        let entities = WeakEntitySet::default();
        let alive = Arc::new(AtomicBool::new(true));
        let collection = CollectionId::fixed_name(crate::system::SYSTEM_COLLECTION_ID);
        let provisional = entities.create_provisional(collection.clone(), alive.clone());

        provisional.get_backend::<LWWBackend>().unwrap().set(PropertyKey::name("title"), Some(Value::String("initial".into())));
        provisional.get_backend::<YrsBackend>().unwrap().insert(&PropertyKey::name("body"), 0, "initial").unwrap();

        let initial_operations = provisional.extract_operations().unwrap();
        assert_eq!(initial_operations.keys().map(String::as_str).collect::<Vec<_>>(), vec!["lww", "yrs"]);

        let model = crate::schema::well_known_model_id(crate::system::SYSTEM_COLLECTION_ID).unwrap();
        let genesis = Event::genesis(model, Some(EntityId::from_bytes([9; 32])), initial_operations);
        let transaction_entity = entities.create_transaction_entity(collection, &genesis, alive).unwrap();

        assert_eq!(transaction_entity.id(), genesis.entity_id);
        assert_eq!(transaction_entity.head(), Clock::new([genesis.id()]));
        assert!(transaction_entity.generate_commit_event().unwrap().is_none());

        // The resident primary is still an empty, removable phantom until the
        // commit path applies the policy-approved genesis.
        let resident = entities.get(&genesis.entity_id).unwrap();
        assert!(resident.head().is_empty());
        assert!(resident.values().is_empty());

        // Only this post-create write appears in the Update. In particular,
        // the Yrs genesis update must not be extracted a second time.
        transaction_entity.get_backend::<LWWBackend>().unwrap().set(PropertyKey::name("title"), Some(Value::String("after".into())));
        let update = transaction_entity.generate_commit_event().unwrap().unwrap();
        assert!(!update.is_entity_create());
        assert_eq!(update.parent, Clock::new([genesis.id()]));
        assert_eq!(update.operations().keys().map(String::as_str).collect::<Vec<_>>(), vec!["lww"]);
    }

    #[test]
    fn empty_head_transaction_entity_is_not_implicitly_created() {
        let collection = CollectionId::fixed_name(crate::system::SYSTEM_COLLECTION_ID);
        let primary = Entity::create(EntityId::from_bytes([7; 32]), collection);
        let phantom = primary.snapshot(Arc::new(AtomicBool::new(true)));

        assert!(matches!(phantom.generate_commit_event(), Err(MutationError::InvalidEvent)));
    }

    #[tokio::test]
    async fn reset_while_lineage_is_awaiting_prevents_apply_and_broadcast() {
        let entities = WeakEntitySet::default();
        let generation = entities.system_generation();
        let collection = CollectionId::fixed_name(crate::system::SYSTEM_COLLECTION_ID);
        let model = crate::schema::well_known_model_id(crate::system::SYSTEM_COLLECTION_ID).unwrap();

        let provisional =
            entities.create_provisional_in_generation(collection.clone(), Arc::new(AtomicBool::new(true)), &generation).unwrap();
        provisional.get_backend::<LWWBackend>().unwrap().set(PropertyKey::name("title"), Some(Value::String("initial".into())));
        let genesis = Event::genesis(model, None, provisional.extract_operations().unwrap());
        let primary = Entity::create_in_system(genesis.entity_id, collection, generation.clone());

        // Genesis has no lineage await and establishes the baseline.
        let immediate =
            PausingEvents { events: BTreeMap::new(), pause_once: AtomicBool::new(false), seen: Notify::new(), release: Notify::new() };
        assert!(primary.apply_event(&immediate, &genesis).await.unwrap());

        let fork = primary.snapshot(Arc::new(AtomicBool::new(true)));
        fork.get_backend::<LWWBackend>().unwrap().set(PropertyKey::name("title"), Some(Value::String("after".into())));
        let update = fork.generate_commit_event().unwrap().unwrap();
        let getter = Arc::new(PausingEvents {
            events: BTreeMap::from([(genesis.id(), genesis), (update.id(), update.clone())]),
            pause_once: AtomicBool::new(true),
            seen: Notify::new(),
            release: Notify::new(),
        });

        let applying = {
            let primary = primary.clone();
            let getter = getter.clone();
            tokio::spawn(async move { primary.apply_event(getter.as_ref(), &update).await })
        };
        getter.seen.notified().await;
        entities.system_reset();
        getter.release.notify_one();

        assert!(matches!(applying.await.unwrap(), Err(MutationError::SystemReset)));
        assert!(!generation.load(Ordering::Acquire));
        assert!(!primary.is_system_alive());
    }
}

// TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now
/// A set of entities held weakly.
///
/// Assembly is the single choke point for catalog-aware property access:
/// every method that hands out an `Entity` runs the `bind_hook`, installed by
/// `Node` to stamp its live catalog resolver. The hook is idempotent and
/// re-fires on resident hand-outs, so an entity assembled before the catalog
/// warmed observes the catalog on its next access.
#[derive(Clone)]
pub struct WeakEntitySet(Arc<WeakEntitySetInner>);

struct WeakEntitySetInner {
    entities: std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>,
    /// Current system generation. Entity construction takes this token while
    /// holding `entities`; reset takes the same locks in the same order,
    /// flips the old token, swaps a fresh token, and clears every resident
    /// lookup atomically with respect to insertion.
    system_alive: std::sync::RwLock<Arc<AtomicBool>>,
    bind_hook: std::sync::RwLock<Option<Box<dyn Fn(&Entity) + Send + Sync>>>,
}

impl Default for WeakEntitySet {
    fn default() -> Self { Self::with_system_generation(Arc::new(AtomicBool::new(true))) }
}

impl WeakEntitySet {
    pub(crate) fn with_system_generation(system_generation: Arc<AtomicBool>) -> Self {
        Self(Arc::new(WeakEntitySetInner {
            entities: std::sync::RwLock::new(BTreeMap::new()),
            system_alive: std::sync::RwLock::new(system_generation),
            bind_hook: std::sync::RwLock::new(None),
        }))
    }

    pub(crate) fn system_generation(&self) -> Arc<AtomicBool> { self.0.system_alive.read().unwrap().clone() }

    pub(crate) fn is_current_generation(&self, generation: &Arc<AtomicBool>) -> bool {
        generation.load(Ordering::Acquire) && Arc::ptr_eq(generation, &self.0.system_alive.read().unwrap())
    }

    /// Invalidate all strong handles from the previous system and atomically
    /// evict the resident lookup set. The bind hook survives: it belongs to
    /// the Node, not to a particular joined system.
    pub(crate) fn system_reset(&self) { self.system_reset_to(Arc::new(AtomicBool::new(true))); }

    pub(crate) fn system_reset_to(&self, fresh_generation: Arc<AtomicBool>) {
        let mut entities = self.0.entities.write().unwrap();
        let mut generation = self.0.system_alive.write().unwrap();
        generation.store(false, Ordering::Release);
        *generation = fresh_generation;
        entities.clear();
    }

    fn reset_during_retrieval() -> RetrievalError { RetrievalError::Other("system reset while retrieving entity".to_owned()) }

    /// Install the assembly-time bind hook. Called once from `Node`
    /// construction; the hook captures a `WeakNode` (no strong cycle).
    pub(crate) fn set_bind_hook(&self, hook: Box<dyn Fn(&Entity) + Send + Sync>) { *self.0.bind_hook.write().unwrap() = Some(hook); }

    /// Run the bind hook on an entity leaving the assembly boundary. Always
    /// called OUTSIDE the entities lock (the hook takes catalog/backend
    /// locks of its own).
    fn bind(&self, entity: &Entity) {
        if let Some(hook) = self.0.bind_hook.read().unwrap().as_ref() {
            hook(entity);
        }
    }

    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        // TODO: call policy agent with cdata
        let entity = { self.0.entities.read().unwrap().get(id).and_then(|weak| weak.upgrade()) };
        if let Some(ref entity) = entity {
            self.bind(entity);
        }
        entity
    }

    pub async fn get_or_retrieve<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        collection_id: &CollectionId,
        id: &EntityId,
    ) -> Result<Option<Entity>, RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        let generation = self.system_generation();
        self.get_or_retrieve_in_generation(state_getter, event_getter, collection_id, id, generation).await
    }
    /// Returns a resident entity, or fetches it from storage, or finally creates if neither of the two are found
    pub async fn get_retrieve_or_create<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        collection_id: &CollectionId,
        id: &EntityId,
    ) -> Result<Entity, RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        let generation = self.system_generation();
        self.get_retrieve_or_create_in_generation(state_getter, event_getter, collection_id, id, generation).await
    }

    pub(crate) async fn get_retrieve_or_create_in_generation<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        collection_id: &CollectionId,
        id: &EntityId,
        generation: Arc<AtomicBool>,
    ) -> Result<Entity, RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        if !self.is_current_generation(&generation) {
            return Err(Self::reset_during_retrieval());
        }
        match self.get_or_retrieve_in_generation(state_getter, event_getter, collection_id, id, generation.clone()).await? {
            Some(entity) => Ok(entity),
            None => {
                let entity = {
                    let mut entities = self.0.entities.write().unwrap();
                    let current = self.0.system_alive.read().unwrap();
                    if !generation.load(Ordering::Acquire) || !Arc::ptr_eq(&generation, &current) {
                        return Err(Self::reset_during_retrieval());
                    }
                    // TODO: call policy agent with cdata
                    match entities.get(id).and_then(|weak| weak.upgrade()) {
                        Some(entity) if entity.belongs_to_system_generation(&generation) => entity,
                        Some(_) => return Err(Self::reset_during_retrieval()),
                        None => {
                            let entity = Entity::create_in_system(*id, collection_id.to_owned(), generation.clone());
                            entities.insert(*id, entity.weak());
                            entity
                        }
                    }
                };
                if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
                    return Err(Self::reset_during_retrieval());
                }
                self.bind(&entity);
                if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
                    return Err(Self::reset_during_retrieval());
                }
                Ok(entity)
            }
        }
    }

    async fn get_or_retrieve_in_generation<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        collection_id: &CollectionId,
        id: &EntityId,
        generation: Arc<AtomicBool>,
    ) -> Result<Option<Entity>, RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        match self.get(id) {
            Some(entity) if self.is_current_generation(&generation) && entity.belongs_to_system_generation(&generation) => Ok(Some(entity)),
            Some(_) => Err(Self::reset_during_retrieval()),
            None => match state_getter.get_state(*id).await? {
                None => {
                    if !self.is_current_generation(&generation) {
                        return Err(Self::reset_during_retrieval());
                    }
                    Ok(None)
                }
                Some(state) => {
                    if !self.is_current_generation(&generation) {
                        return Err(Self::reset_during_retrieval());
                    }
                    let (_, entity) = self
                        .with_state_in_generation(
                            state_getter,
                            event_getter,
                            *id,
                            collection_id.to_owned(),
                            state.payload.state,
                            generation,
                        )
                        .await?;
                    Ok(Some(entity))
                }
            },
        }
    }
    /// Create the writable, pre-identity entity used only while a model writes
    /// its initial values. It is deliberately absent from the resident set:
    /// the real id does not exist until those values have been frozen into the
    /// genesis event.
    pub(crate) fn create_provisional(&self, collection: CollectionId, trx_alive: Arc<AtomicBool>) -> Entity {
        loop {
            let generation = self.system_generation();
            match self.create_provisional_in_generation(collection.clone(), trx_alive.clone(), &generation) {
                Ok(entity) => return entity,
                Err(MutationError::SystemReset) => continue,
                Err(error) => panic!("unexpected provisional entity error: {error}"),
            }
        }
    }

    pub(crate) fn create_provisional_in_generation(
        &self,
        collection: CollectionId,
        trx_alive: Arc<AtomicBool>,
        expected_generation: &Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        if !self.is_current_generation(expected_generation) {
            return Err(MutationError::SystemReset);
        }
        // The placeholder is never observable outside this create call and is
        // never inserted into the id-keyed resident set.
        let primary = Entity::create_in_system(EntityId::from_bytes([0; 32]), collection, expected_generation.clone());
        self.bind(&primary);
        Ok(primary.snapshot(trx_alive))
    }

    /// Insert the empty resident primary under the id derived by `genesis`,
    /// then return the transaction snapshot whose baseline is that genesis.
    /// Subsequent edits on the returned entity therefore extend genesis with
    /// at most one Update event.
    pub(crate) fn create_transaction_entity(
        &self,
        collection: CollectionId,
        genesis: &Event,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        let generation = self.system_generation();
        self.create_transaction_entity_in_generation(collection, genesis, trx_alive, &generation)
    }

    pub(crate) fn create_transaction_entity_in_generation(
        &self,
        collection: CollectionId,
        genesis: &Event,
        trx_alive: Arc<AtomicBool>,
        expected_generation: &Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        // Validate the generation and uniqueness before doing the potentially
        // non-trivial genesis materialization. Binding runs outside the map
        // lock, per the assembly-boundary lock-order contract.
        {
            let entities = self.0.entities.write().unwrap();
            let generation = self.0.system_alive.read().unwrap();
            if !expected_generation.load(Ordering::Acquire) || !Arc::ptr_eq(expected_generation, &generation) {
                return Err(MutationError::SystemReset);
            }
            if entities.get(&genesis.entity_id).and_then(WeakEntity::upgrade).is_some() {
                return Err(MutationError::AlreadyExists);
            }
        }

        let primary = Entity::create_in_system(genesis.entity_id, collection, expected_generation.clone());
        self.bind(&primary);
        let transaction_entity = primary.snapshot_after_genesis(genesis, trx_alive)?;

        // Re-check under the same lock order before publication. A reset in
        // between has flipped `expected_generation`, so an old primary can
        // never be inserted into the new resident map.
        let mut entities = self.0.entities.write().unwrap();
        let generation = self.0.system_alive.read().unwrap();
        if !expected_generation.load(Ordering::Acquire) || !Arc::ptr_eq(expected_generation, &generation) {
            return Err(MutationError::SystemReset);
        }
        if entities.get(&primary.id).and_then(WeakEntity::upgrade).is_some() {
            return Err(MutationError::AlreadyExists);
        }
        entities.insert(primary.id, primary.weak());
        Ok(transaction_entity)
    }

    /// Evict an entity from the set only if it is absent from storage-backed
    /// life: resident with an empty head (or already dead). An empty-head
    /// resident is a phantom, materialized speculatively for an incoming
    /// update that then failed to apply; leaving it resident makes the entity
    /// appear to exist with no state. Returns true if an entry was removed.
    pub fn remove_if_phantom(&self, id: &EntityId) -> bool {
        let generation = self.system_generation();
        self.remove_if_phantom_in_generation(id, &generation)
    }

    /// Generation-conditional phantom eviction. A failed old apply may resume
    /// after reset while a new system has independently materialized the same
    /// entity id; it must not remove that new resident entry.
    pub(crate) fn remove_if_phantom_in_generation(&self, id: &EntityId, expected_generation: &Arc<AtomicBool>) -> bool {
        let mut entities = self.0.entities.write().unwrap();
        let generation = self.0.system_alive.read().unwrap();
        if !expected_generation.load(Ordering::Acquire) || !Arc::ptr_eq(expected_generation, &generation) {
            return false;
        }
        if let Some(weak) = entities.get(id) {
            if let Some(entity) = weak.upgrade() {
                if !entity.belongs_to_system_generation(expected_generation) {
                    return false;
                }
                if !entity.head().is_empty() {
                    return false;
                }
            }
            entities.remove(id);
            return true;
        }
        false
    }

    /// TEST ONLY: Create a phantom entity with a specific ID.
    ///
    /// This creates an entity that was never properly created via Transaction::create(),
    /// has no creation event, and has an empty state. Used for adversarial testing to
    /// verify that commit paths properly reject such entities.
    ///
    /// WARNING: This bypasses all normal entity creation validation. Only use in tests
    /// to verify security properties.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn conjure_evil_phantom(&self, id: EntityId, collection: CollectionId) -> Entity {
        let mut entities = self.0.entities.write().unwrap();
        let generation = self.0.system_alive.read().unwrap().clone();
        let entity = Entity::create_in_system(id, collection, generation);
        entities.insert(id, entity.weak());
        entity
    }

    /// Get or create entity after async operations, checking for race conditions
    /// Returns (existed, entity) where existed is true if the entity was already present
    fn private_get_or_create(
        &self,
        id: EntityId,
        collection_id: &CollectionId,
        state: &State,
        expected_generation: &Arc<AtomicBool>,
    ) -> Result<(bool, Entity), RetrievalError> {
        let mut entities = self.0.entities.write().unwrap();
        let generation = self.0.system_alive.read().unwrap();
        if !expected_generation.load(Ordering::Acquire) || !Arc::ptr_eq(expected_generation, &generation) {
            return Err(Self::reset_during_retrieval());
        }
        if let Some(existing_weak) = entities.get(&id) {
            if let Some(existing_entity) = existing_weak.upgrade() {
                if !existing_entity.belongs_to_system_generation(expected_generation) {
                    return Err(Self::reset_during_retrieval());
                }
                debug!("Entity {id} was created by another thread during async work, using that one");
                return Ok((true, existing_entity));
            }
        }
        let entity = Entity::from_state_in_system(id, collection_id.to_owned(), state, expected_generation.clone())?;
        entities.insert(id, entity.weak());
        Ok((false, entity))
    }

    /// Returns a tuple of (changed, entity)
    /// changed is Some(true) if the entity was changed, Some(false) if it already exists and the state was not applied
    /// None if the entity was not previously on the local node (either in the WeakEntitySet or in storage)
    pub async fn with_state<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        id: EntityId,
        collection_id: CollectionId,
        state: State,
    ) -> Result<(Option<bool>, Entity), RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        let generation = self.system_generation();
        self.with_state_in_generation(state_getter, event_getter, id, collection_id, state, generation).await
    }

    pub(crate) async fn with_state_in_generation<S, E>(
        &self,
        state_getter: &S,
        event_getter: &E,
        id: EntityId,
        collection_id: CollectionId,
        state: State,
        generation: Arc<AtomicBool>,
    ) -> Result<(Option<bool>, Entity), RetrievalError>
    where
        S: GetState + Send + Sync,
        E: GetEvents + Send + Sync,
    {
        if !self.is_current_generation(&generation) {
            return Err(Self::reset_during_retrieval());
        }
        let entity = match self.get(&id) {
            Some(entity) if self.is_current_generation(&generation) && entity.belongs_to_system_generation(&generation) => entity,
            Some(_) => return Err(Self::reset_during_retrieval()),
            None => {
                // not yet resident. We have to retrieve our baseline state before applying the new state
                if let Some(stored_state) = state_getter.get_state(id).await? {
                    // get a resident entity for this retrieved state. It's possible somebody frontran us to create it
                    // but we don't actually care, so we ignore the created flag
                    self.private_get_or_create(id, &collection_id, &stored_state.payload.state, &generation)?.1
                } else {
                    // no stored state, so we can use the given state directly
                    match self.private_get_or_create(id, &collection_id, &state, &generation)? {
                        (true, entity) => entity, // some body frontran us to create it, so we have to apply the new state
                        (false, entity) => {
                            // we just created it with the given state, so there's nothing to apply. early return
                            if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
                                return Err(Self::reset_during_retrieval());
                            }
                            self.bind(&entity);
                            if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
                                return Err(Self::reset_during_retrieval());
                            }
                            return Ok((None, entity));
                        }
                    }
                }
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let result = entity.apply_state(event_getter, &state).await?;
        if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
            return Err(Self::reset_during_retrieval());
        }
        let changed = matches!(result, StateApplyResult::Applied);
        // Bind AFTER the apply: apply_state may rebuild backends from the
        // incoming raw buffers, which would discard a binding attached
        // beforehand and leave an unbound (v1-emitting) backend holding
        // id-keyed entries.
        self.bind(&entity);
        if !self.is_current_generation(&generation) || !entity.belongs_to_system_generation(&generation) {
            return Err(Self::reset_during_retrieval());
        }
        Ok((Some(changed), entity))
    }
}
