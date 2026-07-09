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
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State};
use std::collections::{BTreeMap, HashSet, VecDeque};
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

/// Default bound for the applied-set (plan D2-5): generously above the
/// largest expected bridge batch (the lane benches exercise 5000-event
/// bridges), about 2 MB per hot resident at 32 bytes an id when full.
pub(crate) const DEFAULT_APPLIED_CAP: usize = 65536;

/// The applied-set (plan D2-5 as amended by REV 5 sections C and F): a
/// bounded in-memory record of event ids whose incorporation a COMPLETED
/// persist of this resident's state buffer proves (derivations section 5:
/// rows are written only after a set_state returns Ok whose head covers
/// them, so membership testifies "a locally persisted buffer reflects this
/// event", which is crash-stable). Membership is the only O(1) POSITIVE
/// already-incorporated conclusion in the system; generations only ever
/// reject, and anything inconclusive walks (derivations section 2).
///
/// Never forks (a snapshot starts empty), never persisted, never shared
/// beyond this resident. Lives inside `EntityInnerState` so every read and
/// write happens under the resident's head lock (the re-read-under-lock
/// rule); no interior locking of its own.
///
/// Lazy: allocates nothing until the first insert (`HashSet::new` and
/// `VecDeque::new` do not allocate), so idle residents stay light and the
/// sim memory-audit tier is unaffected. FIFO eviction at the cap: losing a
/// row only costs the evicted event's redelivery a walk to the same no-op
/// (R-D2-3c pins that eviction changes cost, never outcome).
#[derive(Debug)]
pub(crate) struct AppliedSet {
    /// Insertion order, oldest first, for FIFO eviction at the cap.
    order: VecDeque<EventId>,
    members: HashSet<EventId>,
    cap: usize,
}

impl Default for AppliedSet {
    fn default() -> Self { Self::with_cap(DEFAULT_APPLIED_CAP) }
}

impl AppliedSet {
    /// Constructor-configurable cap (D2-5); no preallocation.
    pub(crate) fn with_cap(cap: usize) -> Self { Self { order: VecDeque::new(), members: HashSet::new(), cap: cap.max(1) } }

    /// Record an id a completed persist proves covered. Re-inserting a
    /// member keeps its original eviction position; the oldest member is
    /// evicted at the cap (safe: its redelivery walks instead of skipping).
    pub(crate) fn insert(&mut self, id: EventId) {
        if !self.members.insert(id.clone()) {
            return;
        }
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(evicted) = self.order.pop_front() {
                self.members.remove(&evicted);
            }
        }
    }

    /// O(1) membership: the already-incorporated positive conclusion.
    pub(crate) fn contains(&self, id: &EventId) -> bool { self.members.contains(id) }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize { self.order.len() }
}

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
struct EntityInnerState {
    head: Clock,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
    /// The applied-set (D2-5): under the same lock as the head, consumed
    /// only by apply_event's O(1) skip, inserted only post-persist.
    applied: AppliedSet,
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
            state: std::sync::RwLock::new(EntityInnerState {
                head: Clock::default(),
                backends: BTreeMap::default(),
                applied: AppliedSet::default(),
            }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        }))
    }

    /// TEST ONLY: create an entity whose applied-set carries a small cap, so
    /// eviction behavior (R-D2-3c) is exercisable without 65536 inserts.
    #[cfg(test)]
    pub(crate) fn create_with_applied_cap(id: EntityId, collection: CollectionId, cap: usize) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState {
                head: Clock::default(),
                backends: BTreeMap::default(),
                applied: AppliedSet::with_cap(cap),
            }),
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
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends, applied: AppliedSet::default() }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    ///
    /// The commit-lane stamp (D2-2): the event carries
    /// `1 + max(parent generations)` over the head's parent EVENTS (genesis,
    /// an empty head, stamps exactly 1; saturating at u32::MAX). Parent
    /// generations are read from the parent event payloads through `getter`,
    /// never from any side store (the registry ban: a generation lives only
    /// on the event it stamps). The resolution order is staging, then local
    /// storage, then, on ephemeral nodes whose head was adopted from a state
    /// snapshot, a durable peer (CachedEventGetter); that per-commit fetch
    /// cost is expressly pre-accepted (maintainer ruling 2026-07-09). An
    /// unresolvable parent fails the commit loudly: the stamp MUST be
    /// correct, because every receiving lane verifies the equation at
    /// admission (D2-3) and would reject a guessed value.
    pub(crate) async fn generate_commit_event<G: GetEvents>(&self, getter: &G) -> Result<Option<Event>, MutationError> {
        // Resolve parent generations BEFORE taking the state lock: the fetch
        // may await. The head is stable across that await for both callers:
        // a transaction fork's head is transaction-private (nothing advances
        // it before commit_head), and the system-create path owns its fresh
        // entity exclusively. The equality check under the lock makes that
        // assumption loud instead of silent.
        let parent = self.head();
        let generation = if parent.is_empty() {
            1 // genesis: no parents, generation exactly 1 (266-A)
        } else {
            let mut parent_generations = Vec::with_capacity(parent.len());
            for parent_id in parent.iter() {
                let parent_event = getter
                    .get_event(parent_id)
                    .await
                    .map_err(|e| MutationError::FailedStep("commit-lane generation stamp: parent event unresolvable", e.to_string()))?;
                parent_generations.push(parent_event.generation);
            }
            Event::generation_from_parents(parent_generations)
        };

        let state = self.state.read().expect("other thread panicked, panic here too");
        if state.head != parent {
            return Err(MutationError::FailedStep(
                "commit-lane generation stamp",
                "entity head moved during commit-event generation".to_owned(),
            ));
        }
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
            let event =
                Event { entity_id: self.id, collection: self.collection.clone(), operations, parent: state.head.clone(), generation };
            Ok(Some(event))
        }
    }

    /// Updates the head of the entity to the given clock, which should come exclusively from generate_commit_event
    pub(crate) fn commit_head(&self, new_head: Clock) {
        // TODO figure out how to implement CAS with the backend state
        // probably need an increment for local edits
        self.state.write().unwrap().head = new_head;
    }

    /// The insertion half of the shared post-persist hook (derivations
    /// section 5; plan REV 5 sections E and F): record event ids a
    /// JUST-COMPLETED set_state of this resident proves covered. Callers
    /// invoke this immediately after their persist call returns Ok, with
    /// exactly the lane's proven-covered ids (the executor's applied and
    /// already-integrated outcomes; the adopted head's own ids after a
    /// state-adoption persist; the local commit lane's committed event). A
    /// suppressed or failed persist inserts NOTHING. Sound under head
    /// movement because coverage is monotone: an id covered by the resident
    /// head when the persist began is covered by every later persisted head
    /// (heads never regress).
    ///
    /// M4 extends this same post-persist boundary with the persist-currency
    /// marker stamp; proven-no-op VERDICT inserts (derivations 5's
    /// consequence for D2-3.d: recording a walk's Equal/StrictAscends
    /// conclusion without a fresh persist) stay out until that marker
    /// exists, because only marker currency makes them sound.
    pub(crate) fn mark_applied<I: IntoIterator<Item = EventId>>(&self, ids: I) {
        let mut state = self.state.write().unwrap();
        for id in ids {
            state.applied.insert(id);
        }
    }

    /// TEST ONLY: whether the applied-set holds `id`. The reds (R-D2-3a/3b/3c)
    /// pin insertion discipline through this observable.
    #[cfg(test)]
    pub(crate) fn applied_contains(&self, id: &EventId) -> bool { self.state.read().unwrap().applied.contains(id) }

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

    /// Apply an accumulated comparison's layers from the meet forward and
    /// supersede the meet's head entries with `new_tip`, atomically under one
    /// state lock. Shared by apply_event's DivergedSince arm (true
    /// concurrency: both sides advanced since the meet) and its StrictDescends
    /// gap-replay arm (linear history the resident never incorporated), which
    /// is the degenerate case with an empty divergent branch and meet equal to
    /// the current head. Layers partition each generation into to_apply and
    /// already_applied against the current head's ancestry, so a chain that
    /// includes already-incorporated events costs only the walk.
    ///
    /// Returns Ok(false) when the head moved between the comparison and the
    /// lock; the caller re-compares and retries (the TOCTOU discipline).
    ///
    /// A consequence downstream layers must not forget: because the
    /// StrictDescends arm routes gap-crossing applies through here,
    /// `apply_event` is no longer a single-event primitive. One call may
    /// incorporate several committed-but-unincorporated events on the way
    /// to the incoming one.
    async fn apply_accumulated_layers<G: GetEvents + Send + Sync>(
        &self,
        mut layers: crate::event_dag::layers::EventLayers<G>,
        meet: &[EventId],
        expected_head: &mut Clock,
        new_tip: EventId,
    ) -> Result<bool, MutationError> {
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
            if state.head != *expected_head {
                warn!("Head changed during lineage comparison, retrying...");
                *expected_head = state.head.clone();
                return Ok(false);
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

            // Update head: remove superseded tips, add the new event. The
            // new tip extends tips in its parent clock (the meet); any of
            // those in the current head are now superseded.
            for parent_id in meet {
                state.head.remove(parent_id);
            }
            state.head.insert(new_tip);
        }
        self.broadcast.send(());
        Ok(true)
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
        // The applied-set skip inside the retry loop below serves the
        // applied-and-persisted redelivery case O(1); the comparison remains
        // the general idempotency path for everything the set does not hold.

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

        let event_id = event.id();
        let mut head = self.head();
        // Retry loop to handle head changes between lineage comparison and mutation
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            // The applied-set O(1) skip (D2-5 as amended by REV 5 section C:
            // this is the ONLY consumption site in the system). Membership
            // is re-read under the head lock at the moment of consumption,
            // immediately before the comparison walk would run, and
            // re-checked on every TOCTOU retry; a hit is the O(1)
            // already-incorporated no-op. Rows are inserted only after a
            // COMPLETED persist (derivations section 5), so membership
            // testifies that a locally persisted buffer reflects this
            // event. The storedness half of obligation (d) stays with the
            // callers (the planner's backfill escape and the executor's
            // Ok(false) arm re-check event_stored), so a member whose log
            // row is missing still gets its backfill. The walk's own
            // Equal/StrictAscends conclusions below are NOT recorded here:
            // proven-no-op verdict inserts need the M4 persist-currency
            // marker (derivations 5's consequence for D2-3.d).
            let already_incorporated = self.state.read().unwrap().applied.contains(&event_id);
            if already_incorporated {
                debug!("applied-set hit - already incorporated, skip");
                return Ok(false);
            }

            // Stage the event so BFS can discover it, then compare event's clock vs head
            let subject_clock: Clock = event_id.clone().into();
            let comparison_result = crate::event_dag::compare(getter, &subject_clock, &head, DEFAULT_BUDGET).await?;
            match comparison_result.relation {
                AbstractCausalRelation::Equal => {
                    debug!("Equal - skip");
                    return Ok(false);
                }
                AbstractCausalRelation::StrictDescends { ref chain } => {
                    debug!("Descends - apply (attempt {})", attempt + 1);
                    // Verdict-driven gap replay (R10). A chain longer than
                    // the event itself means the comparison walked history
                    // the current head does not contain: committed-but-
                    // unincorporated events (the commit_event/save_state
                    // crash window) that a plain head jump would orphan
                    // while the new head transitively claims them. Route
                    // those applies through the layer machinery, which
                    // incorporates the gap events' operations and the
                    // incoming event atomically; chain members the head
                    // already covers land in the already_applied partition
                    // and cost only the walk. The plan (REV 4, 2.3) placed
                    // this replay in the executor with apply_event
                    // unchanged; amended by the maintainer 2026-07-06
                    // (option "Fix the arm"): the defect is this arm's
                    // single-event assumption, and fixing it here heals
                    // every caller (the commit lanes' fork previews cross
                    // the same gap), applies under one lock inside the
                    // existing TOCTOU discipline, and reuses the layer path
                    // the DivergedSince arm already trusts, of which the
                    // linear gap is the degenerate case (empty divergent
                    // branch, meet = current head).
                    if chain.iter().any(|id| *id != event_id) {
                        let meet: Vec<EventId> = head.as_slice().to_vec();
                        let (_relation, accumulator) = comparison_result.into_parts();
                        let layers = accumulator.into_layers(meet.clone(), meet.clone());
                        if self.apply_accumulated_layers(layers, &meet, &mut head, event_id.clone()).await? {
                            return Ok(true);
                        }
                        continue;
                    }
                    let new_head: Clock = event.id().into();
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
                    let layers = accumulator.into_layers(meet.clone(), head.as_slice().to_vec());

                    if self.apply_accumulated_layers(layers, &meet, &mut head, event.id()).await? {
                        return Ok(true);
                    }
                    continue;
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
            // The applied-set NEVER FORKS (D2-5): a fork starts empty, so
            // fork previews always take the honest comparison path and a
            // fork's rows can never testify about the canonical resident.
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends: forked, applied: AppliedSet::default() }),
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
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends, applied: AppliedSet::default() }),
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

#[cfg(test)]
mod applied_set_tests {
    use super::*;

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    #[test]
    fn starts_empty_and_lazy() {
        let set = AppliedSet::default();
        assert_eq!(set.len(), 0);
        assert!(!set.contains(&id(1)));
        // Laziness (no preallocation) is by construction: HashSet::new and
        // VecDeque::new allocate nothing; the default cap only bounds growth.
        assert_eq!(set.cap, DEFAULT_APPLIED_CAP);
    }

    #[test]
    fn insert_contains_and_dedup() {
        let mut set = AppliedSet::with_cap(8);
        set.insert(id(1));
        set.insert(id(1));
        set.insert(id(2));
        assert!(set.contains(&id(1)) && set.contains(&id(2)) && !set.contains(&id(3)));
        assert_eq!(set.len(), 2, "re-inserting an id must not duplicate it");
    }

    #[test]
    fn cap_evicts_oldest_first() {
        let mut set = AppliedSet::with_cap(3);
        for b in 1..=5u8 {
            set.insert(id(b));
        }
        assert_eq!(set.len(), 3);
        assert!(!set.contains(&id(1)) && !set.contains(&id(2)), "oldest ids evicted at the cap (cost, not outcome: R-D2-3c)");
        assert!(set.contains(&id(3)) && set.contains(&id(4)) && set.contains(&id(5)));
    }

    #[test]
    fn fork_snapshot_starts_with_an_empty_applied_set() {
        let entity = Entity::create(EntityId::new(), "test".into());
        entity.mark_applied([id(7)]);
        assert!(entity.applied_contains(&id(7)));
        let fork = entity.snapshot(Arc::new(AtomicBool::new(true)));
        assert!(!fork.applied_contains(&id(7)), "the applied-set never forks (D2-5)");
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

    /// Evict an entity from the set only if it is absent from storage-backed
    /// life: resident with an empty head (or already dead). An empty-head
    /// resident is a phantom, materialized speculatively for an incoming
    /// update that then failed to apply; leaving it resident makes the entity
    /// appear to exist with no state. Returns true if an entry was removed.
    pub fn remove_if_phantom(&self, id: &EntityId) -> bool {
        let mut entities = self.0.write().unwrap();
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
