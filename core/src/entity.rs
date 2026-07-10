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
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventId, GClock, OperationSet, State};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// The persist-currency marker (D2-6): testimony that a set_state of THIS
/// resident COMPLETED for exactly `head`, with the node's reset epoch
/// captured BEFORE that persist began. A redundant persist may be elided
/// only when `epoch` equals the current reset epoch AND `head` equals the
/// resident's current head. The reset fence (WeakEntitySet::reset_fence,
/// M4 remediation item 5) keeps any funnel persist from interleaving a
/// hard_reset's bump, purge, or wipe, so every marker is stamped strictly
/// before or strictly after a reset; the epoch conjunct's live job is
/// distrusting markers stamped BEFORE a reset on residents that survive
/// the purge through held strong references. The marker may LAG storage
/// (the raw set_state bypasses in system.rs never stamp it), which only
/// costs a redundant monotone-safe write; it may never LEAD storage,
/// which is why only a completed set_state stamps it AND why the funnel
/// serializes each entity's whole snapshot-write-stamp span on the node's
/// per-id persist lock (WeakEntitySet::persist_span): unserialized lanes
/// could land engine writes in the opposite order of their snapshots,
/// leaving the store regressed behind the head the marker testifies to
/// (the M4 post-review remediation of the adversarial finding 2).
///
/// KNOWN RESIDUE (dev-only reset surface): a funnel persist STARTING
/// after a reset completes, on a dead-system resident kept alive only by
/// held strong references, writes that resident's bytes into the
/// successor system's storage and stamps a truthfully current marker over
/// its own write. The marker never lies there (storage really holds what
/// it names); the illegitimacy is the cross-system delivery itself, whose
/// enforcement is D3/D6 scope (plan REV 5 section D).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PersistMarker {
    pub(crate) epoch: u64,
    pub(crate) head: Clock,
}

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
struct EntityInnerState {
    head: Clock,
    /// Per-tip head generations (D2 REV 5 section K, home 2 of 3): the
    /// materialized annotation of `head`, maintained exactly and read-free
    /// alongside every head mutation (an entry is pinned from the applied
    /// event's stamp when its tip joins the head and dropped when the tip
    /// is superseded). INVARIANT: `head_generations.matches_head(&head)` on
    /// every resident; commit stamping and covered-parent admission
    /// verification read this instead of retrieving event payloads.
    head_generations: GClock,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
    /// The applied-set (D2-5): under the same lock as the head, consumed
    /// only by apply_event's O(1) skip, inserted only post-persist.
    applied: AppliedSet,
    /// The persist-currency marker (D2-6): stamped only by the shared
    /// persist funnel (NodePersist) after a completed set_state, consulted
    /// there to elide redundant persists. Never forks, never persisted.
    marker: Option<PersistMarker>,
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
        Ok(State { state_buffers, head: state.head.clone(), head_generations: state.head_generations.clone() })
    }

    /// The resident's materialized per-tip head generations (REV 5 K), read
    /// under the head lock. Cloned out (heads are tiny) so callers never
    /// hold the lock across an await.
    pub(crate) fn head_generations(&self) -> GClock { self.state.read().unwrap().head_generations.clone() }

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
                head_generations: GClock::default(),
                backends: BTreeMap::default(),
                applied: AppliedSet::default(),
                marker: None,
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
                head_generations: GClock::default(),
                backends: BTreeMap::default(),
                applied: AppliedSet::with_cap(cap),
                marker: None,
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
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                // Adopted with the state: engine rehydration reconstitutes the
                // node's own materialization; wire-carried values pass the
                // ingress validation (structural always, payload-contradiction
                // on durable nodes) before reaching here, and ephemeral nodes
                // adopt inside the state's own trust envelope (REV 5 K).
                head_generations: state.head_generations.clone(),
                backends,
                applied: AppliedSet::default(),
                marker: None,
            }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    ///
    /// The commit-lane stamp (D2-2, rewired at M4 per plan REV 5 section K):
    /// the event carries `1 + max(parent generations)` (genesis, an empty
    /// head, stamps exactly 1; saturating at u32::MAX). A commit's parents
    /// are exactly the head tips, so the operands come from the resident's
    /// MATERIALIZED head generations, read under the same lock as the head
    /// and operations: NO event retrieval and NO peer fetch, on any node
    /// (this is what makes the bodiless-adoption commit work offline-clean;
    /// the M2 fetch machinery remains in the codebase as admission
    /// verification's fallback for uncovered parents, not for stamping).
    /// Every materialized entry originates from an admission-verified stamp
    /// or the state's own trust envelope; a head tip without an entry is a
    /// broken invariant and fails the commit loudly, because every
    /// receiving lane verifies the equation at admission (D2-3) and would
    /// reject a guessed value. Running entirely under one read lock, there
    /// is no longer a head-motion window to re-check.
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let generation = if state.head.is_empty() {
            1 // genesis: no parents, generation exactly 1 (266-A)
        } else {
            if !state.head_generations.matches_head(&state.head) {
                return Err(MutationError::FailedStep(
                    "commit-lane generation stamp",
                    format!(
                        "materialized head generations {} do not annotate the head {} (broken maintenance invariant)",
                        state.head_generations, state.head
                    ),
                ));
            }
            Event::generation_from_parents(state.head_generations.iter().map(|(g, _)| *g))
        };

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

    /// Updates the head of the entity to the just-committed event, which
    /// should come exclusively from generate_commit_event. Takes the EVENT
    /// (not a bare clock) so the materialized head generation advances in
    /// the same write (REV 5 K: maintenance rides every head mutation).
    pub(crate) fn commit_head(&self, event: &Event) {
        // TODO figure out how to implement CAS with the backend state
        // probably need an increment for local edits
        let mut state = self.state.write().unwrap();
        state.head = event.id().into();
        state.head_generations = GClock::from((event.generation, event.id()));
    }

    /// Stamp the persist-currency marker (D2-6). Called ONLY by the shared
    /// persist funnel immediately after a set_state of this resident
    /// COMPLETED, with `epoch` captured BEFORE that persist began and `head`
    /// the exact head the completed persist wrote. Overwriting an existing
    /// marker is safe because the funnel serializes each entity's persists
    /// on the node's per-id span lock (WeakEntitySet::persist_span): stamps
    /// land in write order, so every stamp names exactly the newest
    /// completed write and the marker can only lag storage (costing a
    /// redundant monotone-safe write), never lead it.
    pub(crate) fn stamp_persist_marker(&self, epoch: u64, head: Clock) {
        self.state.write().unwrap().marker = Some(PersistMarker { epoch, head });
    }

    /// Whether the marker proves a persist of the CURRENT head in the
    /// CURRENT reset epoch already completed (D2-6 elision predicate):
    /// `marker.epoch == epoch AND marker.head == current head`. A marker
    /// stamped before a hard_reset carries a dead epoch and never
    /// satisfies this (the fence orders every stamp against the reset, so
    /// "stamped before" is well defined); a marker stamped for an older
    /// head is defeated by the head comparison (the ef68e081 two-lane
    /// interleaving, R-D2-4c).
    pub(crate) fn persist_marker_current(&self, epoch: u64) -> bool {
        let state = self.state.read().unwrap();
        matches!(&state.marker, Some(marker) if marker.epoch == epoch && marker.head == state.head)
    }

    /// TEST ONLY: the D2-6 elision predicate against an explicit epoch.
    /// The epoch-conjunct pin asserts a pre-reset marker is not current
    /// under the successor epoch even though its head still matches.
    ///
    /// Requires the `test-helpers` feature to be enabled.
    #[cfg(feature = "test-helpers")]
    pub fn persist_marker_current_for_test(&self, epoch: u64) -> bool { self.persist_marker_current(epoch) }

    /// The insertion half of the shared post-persist hook (derivations
    /// section 5; plan REV 5 sections E and F): record event ids a
    /// COMPLETED set_state of this resident proves covered. Callers invoke
    /// this immediately after the persist funnel returns Ok, with
    /// exactly the lane's proven-covered ids (the executor's applied and
    /// already-integrated outcomes; the adopted head's own ids after a
    /// state-adoption persist; the local commit lane's committed event). A
    /// suppressed or failed persist inserts NOTHING. Sound under head
    /// movement because coverage is monotone: an id covered by the resident
    /// head when the persist began is covered by every later persisted head
    /// (heads never regress).
    ///
    /// The funnel's Ok includes the M4 marker ELISION (a completed persist
    /// already covers the current head in the current epoch), which is what
    /// makes recording proven-no-op outcomes behind it sound: derivations
    /// 5's D2-3.d rule, enabled at this hook (the insert happens only when
    /// a persist completed or the marker is current).
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
        new_tip_generation: u32,
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
            // those in the current head are now superseded. The materialized
            // generations mirror the head exactly (REV 5 K): superseded tips
            // drop their entries, the new tip pins its stamp, surviving
            // concurrent tips keep theirs.
            for parent_id in meet {
                state.head.remove(parent_id);
                state.head_generations.remove(parent_id);
            }
            state.head.insert(new_tip.clone());
            state.head_generations.insert(new_tip_generation, new_tip);
            debug_assert!(state.head_generations.matches_head(&state.head), "head generations must annotate exactly the head");
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
                state.head_generations = GClock::from((event.generation, event.id()));
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
            // Equal/StrictAscends conclusions below are still NOT recorded
            // HERE: derivations 5's D2-3.d (proven-no-op verdict inserts
            // gated on marker currency) is enabled at the POST-PERSIST
            // hook, where the executor records no-op skip outcomes behind
            // the funnel's marker-checked persist-or-elide; recording at
            // this site instead would need the reset epoch, which the
            // entity deliberately does not hold (M4 report carries the
            // deferral rationale).
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
                        if self.apply_accumulated_layers(layers, &meet, &mut head, event_id.clone(), event.generation).await? {
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
                        // StrictDescends supersedes the whole head: the new
                        // tip's stamp is the only surviving entry (REV 5 K).
                        state.head_generations = GClock::from((event.generation, event_id.clone()));
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

                    if self.apply_accumulated_layers(layers, &meet, &mut head, event.id(), event.generation).await? {
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
                        // The carried annotation travels with the head it
                        // annotates: validated at the ingress boundary
                        // (structural everywhere, payload-contradiction on
                        // durable nodes), adopted inside the state's trust
                        // envelope on ephemeral nodes (REV 5 K).
                        es.head_generations = state.head_generations.clone();
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
            // The head generations DO fork (they annotate the forked head:
            // a transaction fork's commit stamps from them), while the
            // persist marker does NOT (it testifies about the canonical
            // resident's persistence; forks never persist).
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                head_generations: state.head_generations.clone(),
                backends: forked,
                applied: AppliedSet::default(),
                marker: None,
            }),
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
            state: std::sync::RwLock::new(EntityInnerState {
                head: state.head.clone(),
                head_generations: state.head_generations.clone(),
                backends,
                applied: AppliedSet::default(),
                marker: None,
            }),
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

#[cfg(test)]
mod persist_marker_tests {
    use super::*;

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    /// THE MARKER-RACE PIN, mechanism seam (maintainer ruling 2026-07-09,
    /// replacing half of R-D2-4b): a persist that captures epoch N,
    /// straddles a hard_reset (bump to N+1), and completes, stamps a marker
    /// that is NEVER trusted; the next apply persists for real. Exercised
    /// against the primitives the funnel composes: WeakEntitySet's epoch
    /// cell (capture-before, bump = the reset instant) and the entity's
    /// stamp/currency pair. The end-to-end variant (a real hard_reset
    /// across a gated set_state) lives in the integration suite.
    #[test]
    fn marker_race_a_straddling_persist_is_never_trusted() {
        let set = WeakEntitySet::default();
        let entity = set.create("test".into());
        let head: Clock = id(1).into();

        // The persist begins: the epoch is captured BEFORE set_state.
        let captured = set.reset_epoch();
        // hard_reset lands mid-flight.
        set.bump_reset_epoch();
        // The persist completes and stamps the CAPTURED epoch (D2-6: never
        // the current one; stamping the current epoch here is exactly the
        // bug this pin exists to catch).
        entity.stamp_persist_marker(captured, head);

        assert!(
            !entity.persist_marker_current(set.reset_epoch()),
            "a marker stamped by a persist that straddled a reset must never be trusted: the next apply persists for real"
        );

        // Control: a persist fully inside one epoch is trusted...
        let current = set.reset_epoch();
        entity.stamp_persist_marker(current, entity.head());
        assert!(entity.persist_marker_current(current), "a completed same-epoch persist of the current head elides");
        // ...until the NEXT reset distrusts it too.
        set.bump_reset_epoch();
        assert!(!entity.persist_marker_current(set.reset_epoch()), "every earlier epoch's markers die at a reset");
    }

    /// The head conjunct (the other half of the elision predicate; the
    /// R-D2-4c mechanism): a marker stamped for an older head is defeated
    /// the moment the head advances, epoch match notwithstanding.
    #[test]
    fn marker_for_an_older_head_is_defeated_by_the_head_conjunct() {
        let set = WeakEntitySet::default();
        let entity = set.create("test".into());
        let epoch = set.reset_epoch();

        entity.stamp_persist_marker(epoch, Clock::default());
        assert!(entity.persist_marker_current(epoch), "marker for the current (empty) head is trusted in its epoch");

        // The head advances (a sibling lane's apply); the old marker lies
        // about coverage and must not elide.
        entity.commit_head(&Event {
            entity_id: entity.id(),
            collection: "test".into(),
            operations: OperationSet(BTreeMap::new()),
            parent: Clock::default(),
            generation: 1,
        });
        assert!(!entity.persist_marker_current(epoch), "a marker naming an older head must never elide the newer head's persist");
    }

    /// The purge (REV 5 section D.1) at the mechanism seam: the map goes
    /// EMPTY (dead weak entries included), held strong references keep
    /// working, and the purged id resolves to None.
    #[test]
    fn purge_empties_the_map_without_touching_held_entities() {
        let set = WeakEntitySet::default();
        let held = set.create("test".into());
        let held_id = held.id();
        // A dead weak entry: the strong reference from create drops at the
        // end of the statement, leaving a tombstone in the map.
        let dropped_id = set.create("test".into()).id();
        assert!(set.get(&dropped_id).is_none(), "precondition: the entry is a dead weak (upgrade fails)");
        assert_eq!(set.resident_count(), 2, "precondition: live and dead entries populate the map");

        set.purge();

        assert_eq!(set.resident_count(), 0, "the purge clears the map, tombstones included");
        assert!(set.get(&held_id).is_none(), "a purged id is unreachable even while strongly held elsewhere");
        assert_eq!(held.id(), held_id, "the held reference itself stays alive and readable");
    }
}

#[cfg(test)]
mod saturation_stamp_tests {
    use super::*;
    use crate::ingest::testkit::{FailingCommitStore, NoState};
    use crate::ingest::{check_generation, GenerationCheck, StagingArea};
    use ankurah_proto::StateBuffers;

    /// Stamping FROM a saturated adopted tip (M4 remediation item 9,
    /// test-adequacy panel MAJOR 3's entity-level arm): the trust envelope
    /// makes a u32::MAX materialized entry reachable today (an ephemeral
    /// adopting a GClock entry of u32::MAX then committing evaluates
    /// 1 + u32::MAX in production code). The commit stamp must stay AT the
    /// sentinel (never wrap; a wrap-to-0 stamp makes a maximal-depth tip
    /// look below-genesis to any M5 precheck, inverting the conservative
    /// direction 266-C.iv guarantees), and admission verification of that
    /// commit against the same materialization must agree (stamp and
    /// verify share one helper; this pins that they stay shared, so no
    /// honest self-rejection wedge opens at the ceiling).
    #[tokio::test]
    async fn commit_from_a_saturated_adopted_tip_stamps_the_sentinel_and_verifies() {
        let set = WeakEntitySet::default();
        let entity_id = EntityId::new();
        let tip = EventId::from_bytes([9; 32]);

        // Bodiless adoption of a saturated tip (the trust envelope).
        let adopted = State {
            state_buffers: StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![tip.clone()]),
            head_generations: GClock::from((u32::MAX, tip.clone())),
        };
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::ephemeral(staging, EventId::from_bytes([0xEE; 32]));
        let (_, entity) = set.with_state(&NoState, &getter, entity_id, "test".into(), adopted).await.expect("bodiless adoption");

        // A local write so the commit has operations to carry.
        let lww = entity.get_backend::<crate::property::backend::lww::LWWBackend>().expect("backend");
        use crate::property::backend::PropertyBackend;
        lww.set("title".into(), Some(Value::String("at-the-ceiling".to_owned())));

        let event = entity.generate_commit_event().expect("the stamp reads the materialization").expect("a write produces an event");
        assert_eq!(event.generation, u32::MAX, "stamping from a saturated tip stays AT the sentinel (no wrap, no panic)");
        assert_eq!(event.parent, Clock::from(vec![tip]), "the commit parents on the adopted head");

        // Verification agrees via the SAME shared helper: no honest
        // self-rejection wedge at the ceiling.
        let materialized = entity.head_generations();
        let check = check_generation(&getter, Some(&materialized), &event).await.expect("the check runs");
        assert!(
            matches!(check, GenerationCheck::Verified),
            "admission verification of a sentinel-stamped commit against the same materialization must agree"
        );
    }
}

// TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now

/// Shared interior of [`WeakEntitySet`]: the resident map plus the node's
/// reset epoch. They live together because the SAME Arc is cloned into both
/// `NodeInner.entities` (the persist path reaches it through the node) and
/// `SystemManager`'s inner (hard_reset reaches it through the system
/// manager), which is exactly the shared-reachability the epoch needs; a
/// bare NodeInner field would be unreachable from hard_reset (REV 5 G).
#[derive(Default)]
struct WeakEntitySetInner {
    entities: std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>,
    /// The memory-only reset epoch (D2-6): bumped by hard_reset, captured by
    /// the persist funnel BEFORE each set_state begins, stamped into the
    /// persist marker at completion. Never persisted; process restart
    /// resetting it to zero is safe because markers restart empty too (a
    /// marker can only lag storage).
    reset_epoch: AtomicU64,
    /// Per-entity persist serialization (D2-6, the M4 post-review
    /// remediation of the cross-lane stamp reorder): the shared persist
    /// funnel (NodePersist::persist) holds this lock for the entity's id
    /// across its full elision-check, snapshot, set_state, stamp span.
    /// Every engine's set_state is a blind last-writer upsert, so without
    /// the span lock two funnel lanes persisting one entity can commit
    /// their writes in the opposite order of their snapshots, regressing
    /// the stored buffer to an older head while the marker elision keeps
    /// the regression sticky. Serialized, writes of one entity are totally
    /// ordered, the last write carries the newest snapshot (heads never
    /// regress), and the marker can never LEAD storage.
    ///
    /// Keyed by EntityId at the NODE level, not by Entity instance: the
    /// pre-existing remove_if_phantom/with_state interplay can transiently
    /// yield two live instances for one id (concurrency panel NOTE 4), and
    /// instance-keyed locks would silently stop serializing exactly then.
    /// Entries are never removed: they are tiny (an id, an Arc, an idle
    /// mutex), growth is bounded by distinct entities persisted over one
    /// process lifetime, and eager cleanup would reintroduce the
    /// two-locks-for-one-id hazard this map exists to close.
    persist_locks: crate::util::safemap::SafeMap<EntityId, Arc<tokio::sync::Mutex<()>>>,
    /// The reset fence (M4 remediation, item 5): hard_reset holds the WRITE
    /// half across its epoch bump, purge, and storage wipe; the persist
    /// funnel holds the READ half across each full persist span. In-flight
    /// persists therefore DRAIN before the wipe and no new one can start
    /// until it completes (tokio's fair RwLock queues later readers behind
    /// a waiting writer), so a persist can never land dead-system bytes in
    /// the successor system's storage (postgres would otherwise recreate
    /// the state table under an in-flight set_state and durably resurrect
    /// the dead row). The fence also closes the epoch's bump-instant
    /// blind spots (concurrency panel finding 2): a persist span can no
    /// longer interleave any reset step, so it cannot stamp a
    /// current-epoch marker over storage the wipe then erases (W2) nor
    /// elide on a pre-reset marker after the wipe (W3).
    ///
    /// ACQUISITION RULE: never nest fence read guards on one task (the
    /// fair queue deadlocks a re-acquisition behind a waiting writer);
    /// every holder acquires once at its entry point.
    reset_fence: tokio::sync::RwLock<()>,
}

/// A set of entities held weakly
#[derive(Clone, Default)]
pub struct WeakEntitySet(Arc<WeakEntitySetInner>);
impl WeakEntitySet {
    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.0.entities.read().unwrap();
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(id) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// The current reset epoch (D2-6). The persist funnel captures this
    /// BEFORE a set_state begins and stamps the captured value at
    /// completion, so a persist straddling a reset stamps the pre-reset
    /// epoch and its marker is never trusted.
    pub(crate) fn reset_epoch(&self) -> u64 { self.0.reset_epoch.load(Ordering::Acquire) }

    /// Bump the reset epoch (hard_reset only). Every marker stamped under
    /// an earlier epoch becomes permanently untrusted.
    pub(crate) fn bump_reset_epoch(&self) -> u64 { self.0.reset_epoch.fetch_add(1, Ordering::AcqRel) + 1 }

    /// The persist-serialization lock for one entity id (see the
    /// `persist_locks` field doc). Funnel use only: hold it across the full
    /// elision-check, snapshot, set_state, stamp span.
    pub(crate) fn persist_span(&self, id: EntityId) -> Arc<tokio::sync::Mutex<()>> { self.0.persist_locks.get_or_default(id) }

    /// The reset fence, read half (see the `reset_fence` field doc): held
    /// across a whole persist span. Acquire ONCE per task entry point,
    /// never nested.
    pub(crate) async fn reset_fence_read(&self) -> tokio::sync::RwLockReadGuard<'_, ()> { self.0.reset_fence.read().await }

    /// The reset fence, write half: hard_reset only, held across bump,
    /// purge, and wipe. Drains in-flight fence readers first and holds new
    /// ones out until it drops.
    pub(crate) async fn reset_fence_write(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> { self.0.reset_fence.write().await }

    /// The hard_reset purge (REV 5 D.1): clear the resident map, taking only
    /// the map's own lock (no entity locks, so no lock-order hazard). Every
    /// entry in the map at reset time belongs to the dead system by
    /// definition (one entity id lives in exactly one system, never two);
    /// clearing makes stale residents unreachable from ingest and also
    /// drops the accumulated dead weak entries. Holders of strong Entity
    /// references keep reading their stale snapshots unchanged; the
    /// successor system simply never hands them out again.
    pub(crate) fn purge(&self) { self.0.entities.write().unwrap().clear() }

    /// Number of entries in the resident map (live or dead weak). The purge
    /// pin asserts EMPTY, tombstones included.
    pub(crate) fn resident_count(&self) -> usize { self.0.entities.read().unwrap().len() }

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
        // do it in two phases to avoid holding the map lock while waiting for storage
        match self.get(id) {
            Some(entity) => Ok(Some(entity)),
            None => {
                // M4 remediation item 6 (the materialization-porosity pin):
                // the WHOLE read-then-insert span holds the reset fence in
                // read mode, so it cannot straddle a hard_reset. A
                // materialization either completes before the reset (its
                // insert is then purged with everything else) or starts
                // after the wipe (post-wipe storage yields nothing and the
                // id resolves clean). Without the fence, a read completing
                // before the wipe could insert a dead-system resident into
                // the PURGED map. apply_state runs after the guard drops
                // (it can await event retrieval, including peer fetches,
                // and must not hold the fence). Acquired once here, never
                // nested (see the reset_fence field doc).
                let (needs_apply, entity, state) = {
                    let _fence = self.reset_fence_read().await;
                    match state_getter.get_state(*id).await? {
                        None => return Ok(None),
                        Some(state) => {
                            let (needs_apply, entity) =
                                self.materialize_not_resident(state_getter, *id, collection_id, &state.payload.state).await?;
                            (needs_apply, entity, state.payload.state)
                        }
                    }
                };
                if needs_apply {
                    entity.apply_state(event_getter, &state).await?;
                }
                Ok(Some(entity))
            }
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
                let mut entities = self.0.entities.write().unwrap();
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
        let mut entities = self.0.entities.write().unwrap();
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

    /// The not-resident half of a two-phase materialization: read the
    /// stored baseline, then insert (M4 remediation item 6). CALLER HOLDS
    /// THE RESET FENCE in read mode across this whole call, so the read
    /// and the insert land on the same side of any hard_reset. Returns
    /// (needs_apply, entity): the caller applies its in-hand state after
    /// dropping the fence, except when the entity was created FROM that
    /// state (nothing to apply).
    async fn materialize_not_resident<S>(
        &self,
        state_getter: &S,
        id: EntityId,
        collection_id: &CollectionId,
        fallback: &State,
    ) -> Result<(bool, Entity), RetrievalError>
    where
        S: GetState + Send + Sync,
    {
        if let Some(stored_state) = state_getter.get_state(id).await? {
            // A stored baseline exists: seed the resident from it; the
            // caller's state applies on top (it may equal, descend from, or
            // diverge against the baseline; apply_state compares).
            Ok((true, self.private_get_or_create(id, collection_id, &stored_state.payload.state)?.1))
        } else {
            match self.private_get_or_create(id, collection_id, fallback)? {
                // Somebody frontran us to create it: apply the caller's state.
                (true, entity) => Ok((true, entity)),
                // Freshly created from the caller's state: nothing to apply.
                (false, entity) => Ok((false, entity)),
            }
        }
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
        let (needs_apply, entity) = match self.get(&id) {
            Some(entity) => (true, entity), // already resident
            None => {
                // Not yet resident: the baseline read and the map insert
                // run under the reset fence (read mode) so the two-phase
                // materialization cannot straddle a hard_reset (M4
                // remediation item 6; see get_or_retrieve). The guard drops
                // at the end of this arm, BEFORE apply_state (which may
                // await event retrieval). Acquired once here, never nested.
                let _fence = self.reset_fence_read().await;
                self.materialize_not_resident(state_getter, id, &collection_id, &state).await?
            }
        };

        if !needs_apply {
            // we just created it with the given state, so there's nothing to apply. early return
            return Ok((None, entity));
        }

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let result = entity.apply_state(event_getter, &state).await?;
        let changed = matches!(result, StateApplyResult::Applied);
        Ok((Some(changed), entity))
    }
}
