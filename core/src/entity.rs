use crate::retrieval::{GetEvents, GetState};
use crate::selection::filter::Filterable;
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    event_dag::AbstractCausalRelation,
    model::View,
    property::backend::{backend_from_string, PropertyBackend},
    reactor::AbstractEntity,
    value::Value,
};
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

impl Entity {
    pub fn id(&self) -> EntityId { self.id }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn head(&self) -> Clock { self.state.read().unwrap().head.clone() }

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
        Ok(EntityState { entity_id: self.id(), collection: self.collection.clone(), state })
    }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: Clock::default(), backends: BTreeMap::default() }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
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
        })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut operations = BTreeMap::<String, Vec<ankurah_proto::Operation>>::new();
        for (name, backend) in &state.backends {
            if let Some(ops) = backend.to_operations()? {
                operations.insert(name.clone(), ops);
            }
        }

        if operations.is_empty() {
            Ok(None)
        } else {
            let operations = OperationSet(operations);
            let event = Event { entity_id: self.id, collection: self.collection.clone(), operations, parent: state.head.clone() };
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
        tracing::info!("[TRACE-AE] apply_event called for entity={}, event_id={}, event_parent={}", self.id(), event.id(), event.parent);
        debug!("apply_event head: {event} to {self}");

        // Budget for DAG traversal - should be large enough for typical histories
        // but bounded to prevent runaway traversal on malicious/corrupted data.
        // Budget escalation is handled internally by compare (up to 4x).
        const DEFAULT_BUDGET: usize = 1000;

        // Idempotency is handled by the comparison algorithm:
        // - Event already in head -> Equal -> no-op (Ok(false))
        // - Event is ancestor of head -> StrictAscends -> no-op (Ok(false))
        // - Event re-delivered but already integrated -> BFS finds it -> StrictAscends
        // An explicit event_stored() check is not used here because callers
        // (node_applier, system.rs) store events to storage BEFORE calling
        // apply_event (so BFS can find them), which would cause false positives.

        // Entity creation uniqueness guard: at most one creation event per entity.
        // If a creation event arrives for an entity that already has a non-empty head:
        // - If this specific event is already in storage, it's a re-delivery -> idempotent no-op
        // - If it's a different creation event, reject to prevent disjoint genesis
        if event.is_entity_create() && !self.head().is_empty() {
            if getter.event_stored(&event.id()).await? {
                // Re-delivery of the same creation event that already established this entity
                return Ok(false);
            }
            return Err(MutationError::DuplicateCreation);
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

                    let mut applied_layers: Vec<crate::event_dag::accumulator::EventLayer> = Vec::new();

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
                AbstractCausalRelation::Disjoint { gca: _, subject_root: _, other_root: _ } => {
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
        const DEFAULT_BUDGET: usize = 1000;

        for attempt in 0..MAX_RETRIES {
            let comparison_result = crate::event_dag::compare(getter, &new_head, &head, DEFAULT_BUDGET).await?;
            tracing::info!(
                "[TRACE-AS] apply_state comparing new_head={} vs current_head={}, result={:?}",
                new_head,
                head,
                comparison_result.relation
            );
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
                AbstractCausalRelation::Disjoint { gca: _, subject_root: _, other_root: _ } => {
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
        state
            .backends
            .values()
            .flat_map(|backend| {
                backend
                    .property_values()
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect::<Vec<(String, Option<Value>)>>()
            })
            .collect()
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
            // Iterate through backends to find one that has this property
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(&field.into()))
        }
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
            // Iterate through backends to find one that has this property
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(&name.to_owned()))
        }
    }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
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
        })))
    }
    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.0.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values()).collect()
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, name: &str) -> Option<Value> {
        if name == "id" {
            Some(Value::EntityId(self.0.id))
        } else {
            // Iterate through backends to find one that has this property
            let state = self.0.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(&name.to_owned()))
        }
    }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}/{}) = {}", &self.collection, self.id, self.0.state.read().unwrap().head)
    }
}

// TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now
/// A set of entities held weakly
#[derive(Clone, Default)]
pub struct WeakEntitySet(Arc<std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>>);
impl WeakEntitySet {
    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.0.read().unwrap();
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(id) {
            entity.upgrade()
        } else {
            None
        }
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
                    let (_, entity) = self.with_state(state_getter, event_getter, *id, collection_id.to_owned(), state.payload.state).await?;
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
                let mut entities = self.0.write().unwrap();
                // TODO: call policy agent with cdata
                if let Some(entity) = entities.get(id) {
                    if let Some(entity) = entity.upgrade() {
                        return Ok(entity);
                    }
                }
                let entity = Entity::create(*id, collection_id.to_owned());
                entities.insert(*id, entity.weak());
                Ok(entity)
            }
        }
    }
    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection);
        entities.insert(id, entity.weak());
        entity
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
        let mut entities = self.0.write().unwrap();
        let entity = Entity::create(id, collection);
        entities.insert(id, entity.weak());
        entity
    }

    /// Get or create entity after async operations, checking for race conditions
    /// Returns (existed, entity) where existed is true if the entity was already present
    fn private_get_or_create(&self, id: EntityId, collection_id: &CollectionId, state: &State) -> Result<(bool, Entity), RetrievalError> {
        let mut entities = self.0.write().unwrap();
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
                        (true, entity) => entity, // somebody frontran us to create it, so we have to apply the new state
                        (false, entity) => {
                            // we just created it with the given state, so there's nothing to apply. early return
                            return Ok((None, entity));
                        }
                    }
                }
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let result = entity.apply_state(event_getter, &state).await?;
        let changed = matches!(result, StateApplyResult::Applied);
        Ok((Some(changed), entity))
    }
}
