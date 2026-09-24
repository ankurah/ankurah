use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    event_dag::{AbstractCausalRelation, DEFAULT_BUDGET},
    retrieval::GetEvents,
    value::Value,
    property::backend::{backend_from_string, PropertyBackend},
};
use ankurah_proto::{Clock, Event, EventId, ModelId, OperationSet, PropertyId, State};
use ankurah_signals::broadcast::Broadcast;
use tracing::{debug, error, warn};
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, RwLock}};

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

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
pub(super) struct EntityInnerState {
    pub(super) head: Clock,
    /// This state's model memberships, including a local transaction's own additions.
    pub(super) memberships: BTreeSet<ModelId>,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    pub(super) backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
}

impl EntityInnerState {
    /// No history, memberships, or properties: an entity before its genesis applies.
    pub(super) fn empty() -> Self { Self { head: Clock::default(), memberships: BTreeSet::new(), backends: BTreeMap::new() } }

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
    pub(super) fn apply_operations_from_event(&mut self, operations: &ankurah_proto::OperationSet, event_id: EventId) -> Result<(), MutationError> {
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
                    self.memberships.insert(*model);
                }
            }
        }
        Ok(())
    }
    pub(super) fn from_state(state: &State) -> Result<Self, RetrievalError> {
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            backends.insert(name.clone(), backend_from_string(name, Some(state_buffer))?);
        }
        Ok(Self { head: state.head.clone(), memberships: state.memberships.clone(), backends })
    }

    pub(super) fn to_state(&self) -> Result<State, StateError> {
        let mut state_buffers = BTreeMap::new();
        for (name, backend) in &self.backends {
            state_buffers.insert(name.clone(), backend.to_state_buffer()?);
        }
        Ok(State {
            state_buffers: ankurah_proto::StateBuffers(state_buffers),
            memberships: self.memberships.clone(),
            head: self.head.clone(),
        })
    }

    pub(super) fn fork(&self) -> Self {
        Self {
            head: self.head.clone(),
            memberships: self.memberships.clone(),
            backends: self.backends.iter().map(|(name, backend)| (name.clone(), backend.fork())).collect(),
        }
    }
}

/// State and notifications shared by committed and transaction-local entities.
#[derive(Debug)]
pub(crate) struct EntityState {
    inner: RwLock<EntityInnerState>,
    pub(super) broadcast: Broadcast,
}

impl EntityState {
    pub(super) fn new(state: EntityInnerState) -> Self {
        Self { inner: RwLock::new(state), broadcast: Broadcast::new() }
    }

    /// A state with no history: an entity before its genesis applies.
    pub(crate) fn empty() -> Self { Self::new(EntityInnerState::empty()) }

    pub(crate) fn head(&self) -> Clock { self.inner.read().unwrap().head.clone() }

    pub(crate) fn to_state(&self) -> Result<State, StateError> { self.inner.read().unwrap().to_state() }

    pub(super) fn fork(&self) -> Self { Self::new(self.inner.read().unwrap().fork()) }

    pub(super) fn take(&self) -> EntityInnerState { std::mem::replace(&mut *self.inner.write().unwrap(), EntityInnerState::empty()) }

    pub(super) fn memberships(&self) -> BTreeSet<ModelId> { self.inner.read().unwrap().memberships.clone() }

    /// Add a membership to this working state, reporting whether it is new.
    pub(super) fn add_membership(&self, model: ModelId) -> bool {
        let added = self.inner.write().unwrap().memberships.insert(model);
        if added { self.broadcast.send(()); }
        added
    }

    pub(crate) fn value(&self, property: &PropertyId) -> Option<Value> {
        self.inner.read().unwrap().backends.values().find_map(|backend| backend.property_value(property))
    }

    pub(super) fn values(&self) -> Vec<(PropertyId, Option<Value>)> {
        self.inner.read().unwrap().backends.values().flat_map(|backend| backend.property_values()).collect()
    }

    pub(super) fn read_property(&self, backend: &str, property: &PropertyId) -> Option<Value> {
        self.inner.read().unwrap().backends.get(backend).and_then(|backend| backend.property_value(property))
    }

    pub(super) fn get_backend<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        let mut state = self.inner.write().unwrap();
        let name = P::property_backend_name();
        if !state.backends.contains_key(name) {
            state.backends.insert(name.to_owned(), backend_from_string(name, None)?);
        }
        Ok(state.backends[name].clone().as_arc_dyn_any().downcast::<P>().unwrap())
    }

    /// Drain the backends' pending operations.
    pub(super) fn extract_backend_operations(&self) -> Result<OperationSet, MutationError> {
        let state = self.inner.read().unwrap();
        let mut backends = BTreeMap::new();
        for (name, backend) in &state.backends {
            if let Some(operations) = backend.to_operations()? {
                backends.insert(name.clone(), operations);
            }
        }
        Ok(OperationSet::from_backends(backends))
    }

    pub(super) fn set_head(&self, head: Clock) { self.inner.write().unwrap().head = head; }

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
        let mut state = self.inner.write().unwrap();
        if &state.head != expected_head {
            *expected_head = state.head.clone();
            return Ok(false);
        }
        body(&mut state)?;
        Ok(true)
    }

    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %event.entity_id, event = %event)))]
    pub(crate) async fn apply_event<E>(&self, getter: &E, event: &Event) -> Result<bool, MutationError>
    where E: GetEvents + Send + Sync {
        debug!("apply_event head: {event}");

        // Idempotency is handled by the comparison algorithm:
        // - Event already in head -> Equal -> no-op (Ok(false))
        // - Event is ancestor of head -> StrictAscends -> no-op (Ok(false))
        // - Event re-delivered but already integrated -> BFS finds it -> StrictAscends
        // Storage presence alone does not prove application: callers can persist
        // events before applying them to a resident.

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
            let mut state = self.inner.write().unwrap();
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
                        let mut state = self.inner.write().unwrap();
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
                                    state.memberships.insert(*model);
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
    pub(super) async fn apply_state<E>(&self, getter: &E, state: &State) -> Result<StateApplyResult, MutationError>
    where E: GetEvents + Send + Sync {
        let mut head = self.head();
        let new_head = state.head.clone();

        debug!("apply_state - new head: {new_head}");
        const MAX_RETRIES: usize = 5;

        for attempt in 0..MAX_RETRIES {
            let comparison_result = crate::event_dag::compare(getter, &new_head, &head, DEFAULT_BUDGET).await?;
            match comparison_result.relation {
                AbstractCausalRelation::Equal => {
                    debug!("apply_state - heads are equal, skipping");
                    return Ok(StateApplyResult::AlreadyApplied);
                }
                AbstractCausalRelation::StrictDescends { .. } => {
                    debug!("apply_state - new head descends from current, applying (attempt {})", attempt + 1);
                    let new_head = state.head.clone();
                    if self.try_mutate(&mut head, |es| -> Result<(), MutationError> {
                        for (name, state_buffer) in state.state_buffers.iter() {
                            let backend = backend_from_string(name, Some(state_buffer))?;
                            es.backends.insert(name.to_owned(), backend);
                        }
                        es.memberships = state.memberships.clone();
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
                    debug!("apply_state - new head {new_head} is older than current {head}, ignoring");
                    return Ok(StateApplyResult::Older);
                }
                AbstractCausalRelation::DivergedSince { meet, .. } => {
                    // State snapshots cannot be merged without the underlying events.
                    // The caller should either:
                    // 1. Request the full event history and use apply_event() for each
                    // 2. Accept this state via policy if the attestation is trusted
                    // 3. Reject and resync from a known-good state
                    warn!(
                        "apply_state - new head {new_head} diverged from {head}, meet: {meet:?}. \
                        State not applied; events required for proper merge."
                    );
                    return Ok(StateApplyResult::DivergedRequiresEvents);
                }
                AbstractCausalRelation::Disjoint { .. } => {
                    error!("apply_state - heads are disjoint (different genesis)");
                    return Err(LineageError::Disjoint.into());
                }
                AbstractCausalRelation::BudgetExceeded { subject, other } => {
                    tracing::warn!("apply_state - budget exceeded. subject: {subject:?}, other: {other:?}");
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

}
