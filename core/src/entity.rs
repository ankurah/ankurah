use crate::event_dag::DEFAULT_BUDGET;
use crate::retrieval::{GetEvents, GetState};
use crate::selection::filter::Filterable;
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    event_dag::AbstractCausalRelation,
    model::View,
    property::backend::{backend_from_string, PropertyBackend},
    reactor::AbstractEntity,
    schema::CatalogResolver,
    value::Value,
};
use ankql::ast::PropertyId;
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State};
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
    /// Catalog resolver bound at assembly: lets this catalog-blind entity
    /// resolve a display name to the property-definition id its data is
    /// keyed under, for the sync read path. Absent for bare and temporary
    /// entities (they read system-keyed data only).
    resolver: std::sync::RwLock<Option<Weak<dyn CatalogResolver>>>,
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
    /// The catalog resolver bound at assembly, if still live.
    fn resolver(&self) -> Option<Arc<dyn CatalogResolver>> { self.resolver.read().unwrap().as_ref().and_then(|w| w.upgrade()) }

    /// Bind the catalog resolver (assembly-time). Replaces the old
    /// backend-binding push (`Node::bind_entity`): the backend stays dumb and
    /// the entity resolves names to ids for the sync read path.
    pub(crate) fn set_resolver(&self, resolver: Weak<dyn CatalogResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    /// Resolve a display name to the key its data is stored under, for
    /// LENIENT (infallible) reads: the property-definition id when the
    /// catalog knows the field, else the bare name as a
    /// [`PropertyId::System`] (system/catalog collections, or a field not yet
    /// registered). Evaluation has no error surface (see [`Self::read_lenient`]),
    /// so an unresolvable name degenerates to a system-keyed read rather than
    /// erroring; the fallible counterpart the value accessors need at
    /// construction time is [`Self::resolve_property_id`].
    fn resolve_key(&self, name: &str) -> PropertyId {
        match self.resolver().and_then(|r| r.resolve(self.collection.as_str(), name)) {
            Some(id) => PropertyId::EntityId(id.to_ulid()),
            None => PropertyId::System { name: name.to_string() },
        }
    }

    /// Resolve an ordinary user-model field to its durable identity, for the
    /// value accessors (`LWW`/`YrsString`) to call ONCE at construction
    /// (`from_entity`): a missing resolver slot is the intentional
    /// system-keyed bare-entity case (`PropertyId::System`); once a resolver
    /// has been bound, failure to upgrade it or to resolve the name is a
    /// missing/ambiguous catalog binding and surfaces as `UnknownProperty`
    /// (the unbound-field error the accessor's `set`/`get` then returns).
    /// Explicit-id accessors bypass this method entirely.
    pub(crate) fn resolve_property_id(&self, name: &str) -> Result<PropertyId, crate::property::traits::PropertyError> {
        let resolver = self.resolver.read().expect("other thread panicked, panic here too");
        let Some(resolver) = resolver.as_ref() else {
            return Ok(PropertyId::System { name: name.to_string() });
        };
        let Some(resolver) = resolver.upgrade() else {
            return Err(crate::property::traits::PropertyError::UnknownProperty {
                collection: self.collection.to_string(),
                name: name.to_string(),
            });
        };
        resolver.resolve(self.collection.as_str(), name).map(|id| PropertyId::EntityId(id.to_ulid())).ok_or_else(|| {
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

    /// Read `name` for evaluation and lenient consumers: resolve to a
    /// [`PropertyId`] (registered -> its id, else the system name), then scan
    /// the backends' three-way [`PropertyBackend::entry`] under that key --
    /// NO id-then-name fallback (a backend holds one durable key per
    /// property, full stop). A field belongs to exactly one backend per
    /// schema, so the first backend HOLDING AN ENTRY wins and its entry's
    /// value is the answer: a cleared tombstone (`Some(None)`) is
    /// authoritative absence and must shadow any value a later backend holds
    /// under the same key, exactly like the single-backend dispatch
    /// ([`crate::property::read_by_id`]). There is no per-backend
    /// special-casing here (backends are addressed only through the
    /// [`PropertyBackend`] trait).
    fn read_lenient(&self, name: &str) -> Option<Value> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let key = self.resolve_key(name);
        match &key {
            PropertyId::EntityId(ulid) => {
                let id = EntityId::from_ulid(*ulid);
                state
                    .backends
                    .values()
                    .find_map(|backend| backend.entry(&key))
                    .flatten()
                    .and_then(|value| self.canonicalize_lenient(&id, value))
            }
            PropertyId::System { .. } => state.backends.values().find_map(|backend| backend.entry(&key)).flatten(),
            // `resolve_key` never produces `Id`: the `id` pseudo-property is
            // handled by the `field == "id"` shortcut ahead of this method in
            // the `AbstractEntity`/`Filterable` impls below.
            PropertyId::Id => None,
        }
    }

    /// Read a REGISTERED property's value by its stable id for resolved-identifier
    /// predicate evaluation: the id-keyed entry scan, canonicalized at the
    /// evaluation boundary. An unwritten id is absent (evaluates NULL); there is
    /// NO display-name fallback. The first backend HOLDING AN ENTRY wins, so a
    /// cleared tombstone shadows any value a later backend holds under the same
    /// key (see [`Self::read_lenient`]). The `id` pseudo-property and
    /// system/name-keyed fields do not come through here -- they read by name via
    /// [`Self::read_lenient`]. Evaluation has no error surface: type admission is
    /// registration's job (the canonical value_type ruling, 2026-07-10), and an
    /// ill-typed stored value reads as NULL with a warning.
    pub(crate) fn read_by_id_eval(&self, property_id: EntityId) -> Option<Value> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let key = PropertyId::EntityId(property_id.to_ulid());
        state
            .backends
            .values()
            .find_map(|backend| backend.entry(&key))
            .flatten()
            .and_then(|value| self.canonicalize_lenient(&property_id, value))
    }
}

/// A display string for a backend key, for debug/dump consumers
/// ([`Entity::values`], [`TemporaryEntity::values`]) ONLY -- never a storage
/// or wire name (an engine's physical naming seeds from the catalog via
/// `CatalogResolver::name_for`, which needs a live catalog these dump
/// methods do not have). An id key renders as its base64 id, a system
/// key as its name; the `id` pseudo-property never appears as a backend key.
fn debug_key_name(key: &PropertyId) -> String {
    match key {
        PropertyId::Id => "id".to_string(),
        PropertyId::EntityId(ulid) => EntityId::from_ulid(*ulid).to_base64(),
        PropertyId::System { name } => name.clone(),
    }
}

impl Entity {
    pub fn id(&self) -> EntityId { self.id }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn head(&self) -> Clock { self.state.read().unwrap().head.clone() }

    /// Cast each staged value to its property's CANONICAL value_type (rfc.md
    /// 5.6 as amended 2026-07-10), so backends, engines, and indexes store one
    /// consistent type regardless of the writer's compiled type. A value the
    /// canonical type cannot represent fails the commit here, at the writer.
    /// Resolution itself now happens ONCE, upstream, at accessor construction
    /// (`from_entity`): every accessor already stages under its resolved
    /// `PropertyId`, so there is no key left to move at commit -- only this
    /// canonicalization pass remains.
    pub(crate) fn canonicalize_pending_values(&self) -> Result<(), crate::property::traits::PropertyError> {
        let Some(resolver) = self.resolver() else {
            // No resolver (a bare entity, or a system/catalog collection):
            // nothing to canonicalize against.
            return Ok(());
        };
        let state = self.state.read().expect("other thread panicked, panic here too");
        for backend in state.backends.values() {
            for key in backend.uncommitted_keys() {
                // Every accessor resolves to its `PropertyId` at construction
                // (rfc.md 5.5), so a staged key here is either a registered
                // user field (`EntityId`, the only kind the catalog has a
                // canonical type for) or a system field (`System`, which has
                // no catalog entry to canonicalize against). The `id`
                // pseudo-property is never staged.
                let PropertyId::EntityId(ulid) = key else { continue };
                let id = EntityId::from_ulid(ulid);

                // Canonicalize the staged value through the generic trait
                // primitives (`entry` / `restage`): a backend whose edits are
                // not value-shaped (a text CRDT) stages nothing and restages
                // nothing. A staged clear (tombstone) has no value to cast.
                // An unknown canonical string (a newer fleet's type) writes
                // as compiled: registration admitted this binary, so the
                // types were equal.
                if let Some(target) = resolver.canonical_value_type(&id).and_then(|c| crate::value::ValueType::from_property_str(&c)) {
                    let staged = PropertyId::EntityId(ulid);
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
        match &self.kind {
            EntityKind::Primary => false, // Primary entities are read-only
            EntityKind::Transacted { trx_alive, .. } => trx_alive.load(Ordering::Acquire),
        }
    }

    pub fn to_state(&self) -> Result<State, StateError> {
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

    /// The model-definition id carried on this entity's wire envelopes
    /// (#330): the well-known id for system/catalog collections, else the
    /// bound catalog resolver's mapping for the collection. Fails
    /// `UnknownModel` when neither answers.
    pub(crate) fn model_id(&self) -> Result<EntityId, StateError> {
        crate::schema::well_known_model_id(self.collection.as_str())
            .or_else(|| self.resolver().and_then(|r| r.model_id_for(self.collection.as_str())))
            .ok_or_else(|| StateError::UnknownModel(self.collection.to_string()))
    }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: Clock::default(), backends: BTreeMap::default() }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            resolver: std::sync::RwLock::new(None),
        }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
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
        })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        // Resolve the model id BEFORE draining backend operations:
        // to_operations flips staged entries Uncommitted -> Pending as a side
        // effect, so a model-id failure (resolver dropped, catalog emptied by
        // a concurrent hard reset) after the drain would strand those
        // operations where no retry can see them.
        let model = self.model_id()?;
        let mut operations = BTreeMap::<String, Vec<ankurah_proto::Operation>>::new();
        for (name, backend) in &state.backends {
            if let Some(ops) = backend.to_operations()? {
                operations.insert(name.clone(), ops);
            }
        }

        // No operations on an EXISTING entity means nothing changed: no event.
        // On a brand-new entity (empty head) it means every field is its
        // default, which is still an entity: emit a zero-operation creation
        // event so the entity exists, replicates, and persists (the #175
        // degenerate case; RFC 5.4 in specs/model-property-metadata/rfc.md). EventId hashes fine over empty
        // operations.
        if operations.is_empty() && !state.head.is_empty() {
            Ok(None)
        } else {
            let operations = OperationSet(operations);
            let event = Event { entity_id: self.id, model, operations, parent: state.head.clone() };
            Ok(Some(event))
        }
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
        if self.collection() != &V::collection() {
            None
        } else {
            Some(V::from_entity(self.clone()))
        }
    }

    /// Attempt to apply an event to the entity
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    pub async fn apply_event<E>(&self, getter: &E, event: &Event) -> Result<bool, MutationError>
    where E: GetEvents + Send + Sync {
        debug!("apply_event head: {event} to {self}");

        // Idempotency is handled by the comparison algorithm:
        // - Event already in head -> Equal -> no-op (Ok(false))
        // - Event is ancestor of head -> StrictAscends -> no-op (Ok(false))
        // - Event re-delivered but already integrated -> BFS finds it -> StrictAscends
        // An explicit event_stored() check is not used here because callers
        // (node_applier, system.rs) store events to storage BEFORE calling
        // apply_event (so BFS can find them), which would cause false positives.

        // Creation event on entity with non-empty head: either re-delivery or attack.
        // On durable nodes (definitive storage), we can cheaply distinguish:
        //   event_stored() == true  → re-delivery → no-op
        //   event_stored() == false → different genesis event → reject
        // On ephemeral nodes, event_stored() may return false for legitimate
        // re-deliveries (entity arrived via StateSnapshot without event storage),
        // so we fall through to BFS which correctly identifies:
        //   StrictAscends → re-delivery → no-op
        //   Disjoint → different genesis → reject
        if event.is_entity_create() && !self.head().is_empty() {
            if getter.event_stored(&event.id()).await? {
                return Ok(false);
            }
            if getter.storage_is_definitive() {
                return Err(LineageError::Disjoint.into());
            }
            // Ephemeral: fall through to comparison
        }

        // Check for entity creation under the mutex to avoid TOCTOU race
        if event.is_entity_create() {
            let mut state = self.state.write().unwrap();
            // Re-check if head is still empty now that we hold the lock
            if state.head.is_empty() {
                // this is the creation event for a new entity, so we simply accept it
                for (backend_name, operations) in event.operations.iter() {
                    state.apply_operations_from_event(backend_name.clone(), operations, event.id())?;
                }
                state.head = event.id().into();
                drop(state); // Release lock before broadcast
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
                        for (backend_name, operations) in event.operations.iter() {
                            state.apply_operations_from_event(backend_name.clone(), operations, event_id.clone())?;
                        }
                        state.head = new_head.clone();
                        Ok(())
                    })? {
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
                        all_layers.push(layer);
                    }

                    // Atomic update: apply layers and augment head under single lock
                    {
                        let mut state = self.state.write().unwrap();
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
                                for (backend_name, _) in evt.operations.iter() {
                                    if !state.backends.contains_key(backend_name) {
                                        let backend = backend_from_string(backend_name, None)?;
                                        // Replay earlier layers for this newly-created backend
                                        for earlier in &applied_layers {
                                            backend.apply_layer(earlier)?;
                                        }
                                        state.backends.insert(backend_name.clone(), backend);
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
        let mut head = self.head();
        let new_head = state.head.clone();

        debug!("{self} apply_state - new head: {new_head}");
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            let comparison_result = crate::event_dag::compare(getter, &new_head, &head, DEFAULT_BUDGET).await?;
            match comparison_result.relation {
                AbstractCausalRelation::Equal => {
                    debug!("{self} apply_state - heads are equal, skipping");
                    return Ok(StateApplyResult::AlreadyApplied);
                }
                AbstractCausalRelation::StrictDescends { .. } => {
                    debug!("{self} apply_state - new head descends from current, applying (attempt {})", attempt + 1);
                    let new_head = state.head.clone();
                    if self.try_mutate(&mut head, |es| -> Result<(), MutationError> {
                        for (name, state_buffer) in state.state_buffers.iter() {
                            let backend = backend_from_string(name, Some(state_buffer))?;
                            es.backends.insert(name.to_owned(), backend);
                        }
                        es.head = new_head;
                        Ok(())
                    })? {
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
        }))
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
        let state = self.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values().into_iter().map(|(k, v)| (debug_key_name(&k), v))).collect()
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

    fn value_by_id(&self, property_id: ankurah_proto::EntityId) -> Option<crate::value::Value> { self.read_by_id_eval(property_id) }
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

    /// Read a registered property by id for resolved-identifier evaluation, via
    /// the shared [`EntityInner::read_by_id_eval`] dispatch.
    fn value_by_id(&self, property_id: EntityId) -> Option<Value> { self.0.read_by_id_eval(property_id) }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        // Temporary entities parse UNBOUND by default (engine post-filter
        // tier): no resolver, so lenient name reads see only name-keyed data,
        // while resolved-identifier predicate reads carry their own id.
        Self::new_bound(id, collection, state, None)
    }

    /// [`Self::new`] with a catalog resolver bound, for consumers that
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
        })))
    }
    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.0.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values().into_iter().map(|(k, v)| (debug_key_name(&k), v))).collect()
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, name: &str) -> Option<Value> {
        if name == "id" {
            Some(Value::EntityId(self.0.id))
        } else {
            // The shared lenient dispatch: with a bound resolver
            // (new_bound, e.g. policy scope inspection) the name resolves to
            // its id and reads id-keyed data; unbound (the engine
            // post-filter tier) it degenerates to the bare name-keyed scan,
            // exactly the pre-binding behavior. Schema-blind name projection
            // of id-keyed entries remains tracked by #312.
            self.0.read_lenient(name)
        }
    }

    /// Read a registered property by id for resolved-identifier evaluation, via
    /// the shared [`EntityInner::read_by_id_eval`] dispatch. An unbound temporary
    /// (no resolver) skips canonicalization: the caller-supplied resolved id is
    /// the only schema knowledge in play.
    fn value_by_id(&self, property_id: EntityId) -> Option<Value> { self.0.read_by_id_eval(property_id) }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}/{}) = {}", &self.collection, self.id, self.0.state.read().unwrap().head)
    }
}

// TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now
/// A set of entities held weakly.
///
/// Assembly is the single choke point for catalog-aware property access:
/// every method that hands out an `Entity` runs the `bind_hook`, installed by
/// `Node` to bind its live catalog resolver. The hook is idempotent and
/// re-fires on resident hand-outs, so an entity assembled before the catalog
/// warmed observes the catalog on its next access.
#[derive(Clone, Default)]
pub struct WeakEntitySet(Arc<WeakEntitySetInner>);

#[derive(Default)]
struct WeakEntitySetInner {
    entities: std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>,
    bind_hook: std::sync::RwLock<Option<Box<dyn Fn(&Entity) + Send + Sync>>>,
}

impl WeakEntitySet {
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
        // do it in two phases to avoid holding the lock while waiting for the collection
        match self.get(id) {
            Some(entity) => Ok(Some(entity)),
            None => match state_getter.get_state(*id).await? {
                None => Ok(None),
                Some(state) => {
                    // technically someone could have added the entity since we last checked, so it's better to use the
                    // with_state method to re-check
                    let (_, entity) =
                        self.with_state(state_getter, event_getter, *id, collection_id.to_owned(), state.payload.state).await?;
                    Ok(Some(entity))
                }
            },
        }
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
        match self.get_or_retrieve(state_getter, event_getter, collection_id, id).await? {
            Some(entity) => Ok(entity),
            None => {
                let entity = {
                    let mut entities = self.0.entities.write().unwrap();
                    // TODO: call policy agent with cdata
                    match entities.get(id).and_then(|weak| weak.upgrade()) {
                        Some(entity) => entity,
                        None => {
                            let entity = Entity::create(*id, collection_id.to_owned());
                            entities.insert(*id, entity.weak());
                            entity
                        }
                    }
                };
                self.bind(&entity);
                Ok(entity)
            }
        }
    }
    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let entity = {
            let mut entities = self.0.entities.write().unwrap();
            let id = EntityId::new();
            let entity = Entity::create(id, collection);
            entities.insert(id, entity.weak());
            entity
        };
        self.bind(&entity);
        entity
    }

    /// Evict an entity from the set only if it is absent from storage-backed
    /// life: resident with an empty head (or already dead). An empty-head
    /// resident is a phantom, materialized speculatively for an incoming
    /// update that then failed to apply; leaving it resident makes the entity
    /// appear to exist with no state. Returns true if an entry was removed.
    pub fn remove_if_phantom(&self, id: &EntityId) -> bool {
        let mut entities = self.0.entities.write().unwrap();
        if let Some(weak) = entities.get(id) {
            if let Some(entity) = weak.upgrade() {
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
        let entity = Entity::create(id, collection);
        entities.insert(id, entity.weak());
        entity
    }

    /// Get or create entity after async operations, checking for race conditions
    /// Returns (existed, entity) where existed is true if the entity was already present
    fn private_get_or_create(&self, id: EntityId, collection_id: &CollectionId, state: &State) -> Result<(bool, Entity), RetrievalError> {
        let mut entities = self.0.entities.write().unwrap();
        if let Some(existing_weak) = entities.get(&id) {
            if let Some(existing_entity) = existing_weak.upgrade() {
                debug!("Entity {id} was created by another thread during async work, using that one");
                return Ok((true, existing_entity));
            }
        }
        let entity = Entity::from_state(id, collection_id.to_owned(), state)?;
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
        let entity = match self.get(&id) {
            Some(entity) => entity, // already resident
            None => {
                // not yet resident. We have to retrieve our baseline state before applying the new state
                if let Some(stored_state) = state_getter.get_state(id).await? {
                    // get a resident entity for this retrieved state. It's possible somebody frontran us to create it
                    // but we don't actually care, so we ignore the created flag
                    self.private_get_or_create(id, &collection_id, &stored_state.payload.state)?.1
                } else {
                    // no stored state, so we can use the given state directly
                    match self.private_get_or_create(id, &collection_id, &state)? {
                        (true, entity) => entity, // some body frontran us to create it, so we have to apply the new state
                        (false, entity) => {
                            // we just created it with the given state, so there's nothing to apply. early return
                            self.bind(&entity);
                            return Ok((None, entity));
                        }
                    }
                }
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let result = entity.apply_state(event_getter, &state).await?;
        let changed = matches!(result, StateApplyResult::Applied);
        // Bind AFTER the apply: apply_state may rebuild backends from the
        // incoming raw buffers, which would discard a binding attached
        // beforehand and leave an unbound (v1-emitting) backend holding
        // id-keyed entries.
        self.bind(&entity);
        Ok((Some(changed), entity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::backend::LWWBackend;

    fn property_key(byte: u8) -> PropertyId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        PropertyId::EntityId(ulid::Ulid::from_bytes(bytes))
    }

    /// A commit whose model id cannot be resolved must fail BEFORE the
    /// backends drain their staged operations (`to_operations` flips entries
    /// Uncommitted -> Pending), so the staged edits remain visible to a later
    /// retry instead of being stranded.
    #[test]
    fn commit_event_model_id_failure_leaves_operations_staged() {
        // "albums" is not a well-known collection and no resolver is bound,
        // so model_id() fails.
        let entity = Entity::create(EntityId::new(), CollectionId::fixed_name("albums"));
        let key = property_key(0x11);
        let backend = LWWBackend::new();
        backend.set(key.clone(), Some(Value::String("staged".into())));
        entity.state.write().unwrap().backends.insert("lww".to_owned(), Arc::new(backend));

        entity.generate_commit_event().expect_err("an unresolvable model id must fail the commit event");

        let state = entity.state.read().unwrap();
        assert_eq!(state.backends["lww"].uncommitted_keys(), vec![key], "the staged operation must survive the failed commit event");
    }

    /// An entity with two backends whose BTreeMap names order `first` ahead
    /// of `second`.
    fn entity_with_backends(first: LWWBackend, second: LWWBackend) -> Entity {
        let entity = Entity::create(EntityId::new(), CollectionId::fixed_name("albums"));
        let mut state = entity.state.write().unwrap();
        state.backends.insert("a_first".to_owned(), Arc::new(first));
        state.backends.insert("b_second".to_owned(), Arc::new(second));
        drop(state);
        entity
    }

    /// The read scan selects the FIRST backend holding an entry: a cleared
    /// tombstone (an entry holding no value) is authoritative absence and
    /// must shadow a value a later backend holds under the same key.
    #[test]
    fn tombstone_in_first_backend_shadows_later_backend_value() {
        let key = property_key(0x22);
        let first = LWWBackend::new();
        first.set(key.clone(), None); // a present entry with no value
        let second = LWWBackend::new();
        second.set(key.clone(), Some(Value::String("shadowed".into())));
        let entity = entity_with_backends(first, second);

        let PropertyId::EntityId(ulid) = key else { unreachable!() };
        assert_eq!(entity.read_by_id_eval(EntityId::from_ulid(ulid)), None, "a tombstone entry must win over a later backend's value");
    }

    /// A backend with NO entry under the key does not stop the scan: a later
    /// backend's entry still answers.
    #[test]
    fn absent_entry_falls_through_to_later_backend() {
        let key = property_key(0x33);
        let second = LWWBackend::new();
        second.set(key.clone(), Some(Value::String("present".into())));
        let entity = entity_with_backends(LWWBackend::new(), second);

        let PropertyId::EntityId(ulid) = key else { unreachable!() };
        assert_eq!(
            entity.read_by_id_eval(EntityId::from_ulid(ulid)),
            Some(Value::String("present".into())),
            "an absent entry must not shadow a later backend's entry"
        );
    }

    /// The entry-presence rule also governs the system-name branch of
    /// read_lenient (no resolver bound, so the name reads system-keyed).
    #[test]
    fn system_key_tombstone_shadows_later_backend_value() {
        let key = PropertyId::System { name: "nickname".to_owned() };
        let first = LWWBackend::new();
        first.set(key.clone(), None);
        let second = LWWBackend::new();
        second.set(key, Some(Value::String("shadowed".into())));
        let entity = entity_with_backends(first, second);

        assert_eq!(entity.read_lenient("nickname"), None, "a system-keyed tombstone must win over a later backend's value");
    }
}
