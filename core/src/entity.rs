use crate::event_dag::DEFAULT_BUDGET;
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
use ankql::ast::PropertyId;
use ankurah_proto::{AuthorId, Clock, CollectionId, EntityId, EntityState, Event, EventId, ModelId, OperationSet, State};
use std::collections::{BTreeMap, BTreeSet};
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

mod membership;
use membership::MembershipSet;

/// Where a model writes its initial values before the entity they describe has
/// an identity.
///
/// An entity id is the id of its genesis event, so `Transaction::create` cannot
/// name the entity until the values that go into that genesis exist. They are
/// staged here, [`ProvisionalEntity::extract_operations`] freezes them, and the
/// id is derived from what comes out. Nothing here carries an id, nothing here
/// is resident, and none of it outlives the `create` call that built it.
///
/// [`TemporaryEntity`] is the opposite arrangement: an id and existing state,
/// reconstituted for evaluation.
#[derive(Debug, Default)]
pub struct ProvisionalEntity {
    /// Memberships staged for the genesis; an entity's first event carries
    /// exactly one.
    memberships: MembershipSet,
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
}

impl ProvisionalEntity {
    /// A vessel with nothing staged.
    pub fn new() -> Self { Self::default() }

    /// Stage membership in `model`, which rides the genesis as one of its
    /// frozen initial operations and is therefore inside the derived id.
    pub fn add_membership(&mut self, model: ModelId) { self.memberships.add(model); }

    /// The named property backend, created empty on first use.
    pub fn get_backend<P: PropertyBackend>(&mut self) -> Result<Arc<P>, RetrievalError> {
        let backend_name = P::property_backend_name();
        if let Some(backend) = self.backends.get(backend_name) {
            let upcasted = backend.clone().as_arc_dyn_any();
            return Ok(upcasted.downcast::<P>().unwrap()); // TODO: handle downcast error
        }
        let backend = backend_from_string(backend_name, None)?;
        let typed_backend = backend.clone().as_arc_dyn_any().downcast::<P>().unwrap(); // TODO: handle downcast error
        self.backends.insert(backend_name.to_owned(), backend);
        Ok(typed_backend)
    }

    /// Freeze everything staged into the operation set that becomes the genesis
    /// preimage. Consumes the vessel: `create` derives the id from these
    /// operations and builds the real entity from the genesis they produced.
    pub(crate) fn extract_operations(self) -> Result<OperationSet, MutationError> {
        let Self { mut memberships, backends } = self;
        assemble_operations(memberships.to_operations(), &backends)
    }
}

/// Assemble one operation set from already-drained membership operations and
/// each backend's pending diffs.
///
/// Both extraction points call this -- the create path out of a
/// [`ProvisionalEntity`], the commit path out of an [`Entity`] -- so the two
/// cannot drift in what an extraction contains or in what order it lands, and
/// an event's operations mean the same thing whichever path minted it.
fn assemble_operations(
    membership_operations: Vec<ankurah_proto::Operation>,
    backends: &BTreeMap<String, Arc<dyn PropertyBackend>>,
) -> Result<OperationSet, MutationError> {
    let mut extracted = BTreeMap::<String, Vec<ankurah_proto::BackendOperation>>::new();
    for (name, backend) in backends {
        if let Some(ops) = backend.to_operations()? {
            extracted.insert(name.clone(), ops);
        }
    }
    let mut operations = OperationSet::from_backends(extracted);
    for operation in membership_operations {
        operations.push(operation);
    }
    Ok(operations)
}

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
struct EntityInnerState {
    head: Clock,
    /// This entity's model memberships: applied plus staged (see [`MembershipSet`]).
    memberships: MembershipSet,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
}

impl EntityInnerState {
    /// Apply an event's operation stream, tracking which event set each
    /// property.
    ///
    /// Application is TOTAL over [`ankurah_proto::Operation`]: backend diffs
    /// dispatch to their named backend (per-property conflict resolution:
    /// the event_id tracking is a no-op for CRDT backends like Yrs, and is
    /// stored alongside each value for LWW), and membership operations apply
    /// into the membership set. The attested event stream is the sole
    /// authority for entity-to-model membership, so nothing is filtered
    /// here; admissibility (which operations may be EMITTED today) is a
    /// commit-path concern, not an application one.
    fn apply_operations_from_event(&mut self, operations: &ankurah_proto::OperationSet, event_id: EventId) -> Result<(), MutationError> {
        for operation in operations.iter() {
            match operation {
                ankurah_proto::Operation::Backend { backend: backend_name, operations } => {
                    if let Some(backend) = self.backends.get(backend_name.as_str()) {
                        backend.apply_operations_with_event(operations, event_id.clone())?;
                    } else {
                        let backend = backend_from_string(backend_name, None)?;
                        backend.apply_operations_with_event(operations, event_id.clone())?;
                        self.backends.insert(backend_name.clone(), backend);
                    }
                }
                ankurah_proto::Operation::Membership(ankurah_proto::Membership::Add(model)) => {
                    self.memberships.apply(*model);
                }
            }
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
    /// Schema epoch used by this entity's typed accessors.
    schema_epoch: crate::schema::SchemaEpoch,
    schema_epoch_source: Arc<std::sync::RwLock<Option<crate::schema::SchemaEpoch>>>,
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

    pub fn schema_epoch(&self) -> crate::schema::SchemaEpoch { self.schema_epoch }

    pub(crate) fn with_current_schema_epoch<T>(&self, f: impl FnOnce(crate::schema::SchemaEpoch) -> T) -> Option<T> {
        let current = self.schema_epoch_source.read().unwrap();
        if *current != Some(self.schema_epoch) {
            return None;
        }
        let result = f(self.schema_epoch);
        drop(current);
        Some(result)
    }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn head(&self) -> Clock { self.state.read().unwrap().head.clone() }

    /// Durable model-backed memberships accumulated by this entity's event
    /// history (exactly one for any created entity under the current
    /// emission rules).
    pub fn memberships(&self) -> BTreeSet<ModelId> { self.state.read().unwrap().memberships.applied() }

    /// Whether this entity's causal history established membership in `model`.
    pub fn has_membership(&self, model: &ModelId) -> bool { self.state.read().unwrap().memberships.is_applied(model) }

    /// Stage this entity's membership in `model` to ride the next event it
    /// records. The membership becomes canonical only when that event
    /// applies; under the current protocol the commit funnels admit
    /// membership operations only on an entity's first event.
    pub fn add_membership(&self, model: ModelId) { self.state.write().unwrap().memberships.add(model); }

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
        Ok(State { state_buffers, memberships: state.memberships.applied(), head: state.head.clone() })
    }

    pub fn to_entity_state(&self) -> Result<EntityState, StateError> {
        let state = self.to_state()?;
        Ok(EntityState { entity_id: self.id(), collection: self.collection.clone(), state })
    }

    #[cfg(test)]
    pub(crate) fn create(id: EntityId, collection: CollectionId, schema_epoch: crate::schema::SchemaEpoch) -> Self {
        Self::create_with_epoch_source(id, collection, schema_epoch, Arc::new(std::sync::RwLock::new(Some(schema_epoch))))
    }

    fn create_with_epoch_source(
        id: EntityId,
        collection: CollectionId,
        schema_epoch: crate::schema::SchemaEpoch,
        schema_epoch_source: Arc<std::sync::RwLock<Option<crate::schema::SchemaEpoch>>>,
    ) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState {
                head: Clock::default(),
                memberships: MembershipSet::default(),
                backends: BTreeMap::default(),
            }),
            schema_epoch,
            schema_epoch_source,
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(
        id: EntityId,
        collection: CollectionId,
        state: &State,
        schema_epoch: crate::schema::SchemaEpoch,
        schema_epoch_source: Arc<std::sync::RwLock<Option<crate::schema::SchemaEpoch>>>,
    ) -> Result<Self, RetrievalError> {
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }

        Ok(Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                memberships: MembershipSet::from_applied(&state.memberships),
                backends,
            }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            schema_epoch,
            schema_epoch_source,
        })))
    }

    /// Drain everything staged since the last extraction: each backend's
    /// pending diffs plus any staged membership additions.
    ///
    /// Commit calls this for the edits made after `Transaction::create`
    /// returned. The genesis was frozen earlier and elsewhere, out of a
    /// [`ProvisionalEntity`], because it had to exist before this entity's id
    /// did. The backends remain the single source of truth for where one
    /// extraction ends and the next begins.
    pub(crate) fn extract_operations(&self) -> Result<OperationSet, MutationError> {
        // Drain staged memberships in a short exclusive scope, then read the
        // backends under a read lock (they carry their own interior
        // mutability). Holding the write lock across extraction showed up as
        // a read-your-writes delivery failure in the deterministic sim on CI
        // runners (isolated by A/B probe branches); the exclusive section
        // stays minimal on the commit path.
        let membership_operations = self.state.write().expect("other thread panicked, panic here too").memberships.to_operations();
        let state = self.state.read().expect("other thread panicked, panic here too");
        assemble_operations(membership_operations, &state.backends)
    }

    /// Generate the one update event carrying whatever was edited on this
    /// transaction entity, or `None` when nothing changed.
    ///
    /// A created entity's genesis is frozen by `Transaction::create` and held
    /// there, so this path never mints a creation. An entity that reaches it
    /// with an empty head was never created; refusing here is what stops a
    /// phantom from being promoted into an entity.
    pub(crate) fn generate_commit_event(&self, author: AuthorId) -> Result<Option<Event>, MutationError> {
        let parent = self.head();
        if parent.is_empty() {
            return Err(MutationError::PhantomEntity(self.id));
        }

        let operations = self.extract_operations()?;
        if operations.is_empty() {
            return Ok(None);
        }
        Ok(Some(Event::update(self.collection.clone(), self.id, parent, author, operations)))
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
        // (node/applier.rs, system.rs) store events to storage BEFORE calling
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
                state.apply_operations_from_event(event.operations(), event.id())?;
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
                        state.apply_operations_from_event(event.operations(), event_id.clone())?;
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
                            // Check for backends that first appear in this layer's to_apply events,
                            // and union any membership operations the layer carries (application
                            // is total over the operation stream; backends cannot apply these).
                            for evt in &layer.to_apply {
                                for (backend_name, _) in evt.operations().backends() {
                                    if !state.backends.contains_key(backend_name) {
                                        let backend = backend_from_string(backend_name, None)?;
                                        // Replay earlier layers for this newly-created backend
                                        for earlier in &applied_layers {
                                            backend.apply_layer(earlier)?;
                                        }
                                        state.backends.insert(backend_name.to_owned(), backend);
                                    }
                                }
                                for membership in evt.operations().memberships() {
                                    let ankurah_proto::Membership::Add(model) = membership;
                                    state.memberships.apply(*model);
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
                        es.memberships.set_applied(&state.memberships);
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

    /// Build the transaction-visible state that sits immediately after a
    /// frozen genesis, leaving `self` the empty resident primary until commit
    /// applies that genesis to it.
    ///
    /// Each applied backend is re-encoded through its state buffer so every
    /// backend starts the transaction with a clean extraction baseline. This
    /// matters for Yrs: applying the genesis update mutates the document, but
    /// only a freshly decoded state advances its local `previous_state`, so
    /// without the round trip the initial values would be extracted a second
    /// time into the post-create update event.
    fn snapshot_after_genesis(&self, genesis: &Event, trx_alive: Arc<AtomicBool>) -> Result<Self, MutationError> {
        if genesis.entity_id != self.id {
            return Err(MutationError::CommitInvariant("the genesis being applied names a different entity than the one receiving it"));
        }
        genesis.validate_structure()?;

        let event_id = genesis.id();
        let mut memberships = MembershipSet::default();
        let mut backends = BTreeMap::new();
        for operation in genesis.operations().iter() {
            match operation {
                ankurah_proto::Operation::Backend { backend: name, operations } => {
                    let backend = backend_from_string(name, None)?;
                    backend.apply_operations_with_event(operations, event_id.clone())?;
                    let buffer = backend.to_state_buffer()?;
                    backends.insert(name.clone(), backend_from_string(name, Some(&buffer))?);
                }
                ankurah_proto::Operation::Membership(ankurah_proto::Membership::Add(model)) => memberships.apply(*model),
            }
        }

        Ok(Self(Arc::new(EntityInner {
            id: self.id,
            collection: self.collection.clone(),
            state: std::sync::RwLock::new(EntityInnerState { head: event_id.into(), memberships, backends }),
            kind: EntityKind::Transacted { trx_alive, upstream: self.clone() },
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            schema_epoch: self.schema_epoch,
            schema_epoch_source: self.schema_epoch_source.clone(),
        })))
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
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                memberships: state.memberships.clone(),
                backends: forked,
            }),
            kind: EntityKind::Transacted { trx_alive, upstream: self.clone() },
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            schema_epoch: self.schema_epoch,
            schema_epoch_source: self.schema_epoch_source.clone(),
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

    fn value(&self, property: &PropertyId) -> Option<crate::value::Value> {
        if *property == PropertyId::Id {
            Some(crate::value::Value::EntityId(self.id))
        } else {
            // Iterate through backends to find one that has this property
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(property))
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

    fn value(&self, property: &PropertyId) -> Option<Value> {
        if *property == PropertyId::Id {
            Some(Value::EntityId(self.id))
        } else {
            // Iterate through backends to find one that has this property
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(property))
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
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                memberships: MembershipSet::from_applied(&state.memberships),
                backends,
            }),
            kind: EntityKind::Primary,
            // slightly annoying that we need to populate this, given that it won't be used
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
            // Evaluation-only vessel: never typed-accessed.
            schema_epoch: crate::schema::SchemaEpoch::BOOTSTRAP,
            schema_epoch_source: Arc::new(std::sync::RwLock::new(Some(crate::schema::SchemaEpoch::BOOTSTRAP))),
        })))
    }
    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.0.state.read().expect("other thread panicked, panic here too");
        state
            .backends
            .values()
            .flat_map(|backend| backend.property_values())
            .map(|(property, value)| (property.to_string(), value))
            .collect()
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, property: &PropertyId) -> Option<Value> {
        if *property == PropertyId::Id {
            Some(Value::EntityId(self.0.id))
        } else {
            // Iterate through backends to find one that has this property
            let state = self.0.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(property))
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
#[derive(Clone)]
pub struct WeakEntitySet {
    entities: Arc<std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>>,
    epoch_source: Arc<std::sync::RwLock<Option<crate::schema::SchemaEpoch>>>,
}

impl WeakEntitySet {
    pub(crate) fn new(epoch_source: Arc<std::sync::RwLock<Option<crate::schema::SchemaEpoch>>>) -> Self {
        Self { entities: Arc::new(std::sync::RwLock::new(BTreeMap::new())), epoch_source }
    }

    fn current_epoch(&self) -> crate::schema::SchemaEpoch {
        self.epoch_source.read().unwrap().unwrap_or(crate::schema::SchemaEpoch::BOOTSTRAP)
    }
}
impl WeakEntitySet {
    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.entities.read().unwrap();
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
                let mut entities = self.entities.write().unwrap();
                // TODO: call policy agent with cdata
                if let Some(entity) = entities.get(id) {
                    if let Some(entity) = entity.upgrade() {
                        return Ok(entity);
                    }
                }
                let entity =
                    Entity::create_with_epoch_source(*id, collection_id.to_owned(), self.current_epoch(), self.epoch_source.clone());
                entities.insert(*id, entity.weak());
                Ok(entity)
            }
        }
    }
    /// Insert the empty resident primary for the system root, whose genesis
    /// `SystemManager::create` applies to it directly instead of through a
    /// transaction.
    pub(crate) fn create_root(&self, collection: CollectionId, id: EntityId) -> Entity {
        let mut entities = self.entities.write().unwrap();
        let entity = Entity::create_with_epoch_source(id, collection, self.current_epoch(), self.epoch_source.clone());
        entities.insert(id, entity.weak());
        entity
    }

    /// Insert the empty resident primary under the id `genesis` derived, and
    /// return the transaction entity whose baseline is that genesis.
    ///
    /// The primary stays empty until commit applies the genesis to it; the
    /// returned transaction entity already has the genesis applied, so edits
    /// made after `create` returns extend it with at most one update event.
    pub(crate) fn create_transaction_entity(
        &self,
        collection: CollectionId,
        genesis: &Event,
        epoch: crate::schema::SchemaEpoch,
        trx_alive: Arc<AtomicBool>,
    ) -> Result<Entity, MutationError> {
        let primary = Entity::create_with_epoch_source(genesis.entity_id, collection, epoch, self.epoch_source.clone());
        let transaction_entity = primary.snapshot_after_genesis(genesis, trx_alive)?;

        let mut entities = self.entities.write().unwrap();
        if entities.get(&primary.id).and_then(|weak| weak.upgrade()).is_some() {
            // A 256-bit hash over creator-random bytes: reaching this means
            // the same genesis was minted twice, not that two creations
            // collided.
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
        let mut entities = self.entities.write().unwrap();
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
        let mut entities = self.entities.write().unwrap();
        let entity = Entity::create_with_epoch_source(id, collection, self.current_epoch(), self.epoch_source.clone());
        entities.insert(id, entity.weak());
        entity
    }

    /// Get or create entity after async operations, checking for race conditions
    /// Returns (existed, entity) where existed is true if the entity was already present
    fn private_get_or_create(&self, id: EntityId, collection_id: &CollectionId, state: &State) -> Result<(bool, Entity), RetrievalError> {
        let mut entities = self.entities.write().unwrap();
        if let Some(existing_weak) = entities.get(&id) {
            if let Some(existing_entity) = existing_weak.upgrade() {
                debug!("Entity {id} was created by another thread during async work, using that one");
                return Ok((true, existing_entity));
            }
        }
        let entity = Entity::from_state(id, collection_id.to_owned(), state, self.current_epoch(), self.epoch_source.clone())?;
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
