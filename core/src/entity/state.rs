use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    event_dag::{AbstractCausalRelation, EventLayer, EventLayers, DEFAULT_BUDGET},
    property::backend::{backend_from_string, PropertyBackend},
    retrieval::GetEvents,
    value::Value,
};
use ankurah_proto::{Clock, Event, ModelId, OperationSet, PropertyId, State};
use ankurah_signals::broadcast::Broadcast;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};
use tracing::{debug, error, warn};

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

    /// Apply backend and membership operations in causal order, then advance the head.
    /// New backends receive preceding layers as well, so they have the same merge context.
    fn apply_layers(&mut self, layers: &[crate::event_dag::EventLayer]) -> Result<(), MutationError> {
        for (i, layer) in layers.iter().enumerate() {
            for event in &layer.to_apply {
                for (name, _) in event.operations().backends() {
                    if !self.backends.contains_key(name) {
                        let backend = backend_from_string(name, None)?;
                        for earlier in &layers[..i] {
                            backend.apply_layer(earlier)?;
                        }
                        self.backends.insert(name.to_owned(), backend);
                    }
                }
                for membership in event.operations().memberships() {
                    let ankurah_proto::Membership::Add(model) = membership;
                    self.memberships.insert(*model);
                }
            }
            for backend in self.backends.values() {
                backend.apply_layer(layer)?;
            }
        }
        // Advance the head only after every layer has applied successfully.
        for layer in layers {
            for event in &layer.to_apply {
                for parent in event.parent.ids() {
                    self.head.remove(parent);
                }
                self.head.join(event);
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
    pub(super) fn new(state: EntityInnerState) -> Self { Self { inner: RwLock::new(state), broadcast: Broadcast::new() } }

    /// A state with no history: an entity before its genesis applies.
    pub(crate) fn empty() -> Self { Self::new(EntityInnerState::empty()) }

    pub(crate) fn head(&self) -> Clock { self.inner.read().unwrap().head.clone() }

    pub(crate) fn to_state(&self) -> Result<State, StateError> { self.inner.read().unwrap().to_state() }

    pub(super) fn fork(&self) -> Self { Self::new(self.inner.read().unwrap().fork()) }

    pub(super) fn take(&self) -> EntityInnerState { std::mem::replace(&mut *self.inner.write().unwrap(), EntityInnerState::empty()) }

    pub(super) fn memberships(&self) -> BTreeSet<ModelId> { self.inner.read().unwrap().memberships.clone() }

    /// Replace this state only if its head matches `expected_head`.
    /// Fork `prepared` so its backends remain independent, then notify this state's listeners.
    pub(super) fn replace_if_head_matches(&self, expected_head: &Clock, prepared: &EntityState) -> bool {
        let mut state = self.inner.write().unwrap();
        if &state.head != expected_head {
            return false;
        }
        *state = prepared.inner.read().unwrap().fork();
        drop(state);
        self.broadcast.send(());
        true
    }

    /// Add a membership to this working state, reporting whether it is new.
    pub(super) fn add_membership(&self, model: ModelId) -> bool {
        let added = self.inner.write().unwrap().memberships.insert(model);
        if added {
            self.broadcast.send(());
        }
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

    /// Apply an already-admitted batch to a resident, retrying if its head moves during preparation.
    pub(crate) async fn apply_events<E>(&self, getter: &E, events: &[impl AsRef<Event>]) -> Result<Vec<bool>, MutationError>
    where E: GetEvents + Send + Sync {
        let mut head = self.head();
        const MAX_RETRIES: usize = 5;
        for _ in 0..MAX_RETRIES {
            let prepared = EventLayers::prepare(getter, events, &head).await?;
            let applied: Vec<_> = prepared.per_event().map(|layers| !layers.is_empty()).collect();
            if !applied.iter().any(|applied| *applied) {
                return Ok(applied);
            }
            if self.try_mutate(&mut head, |state| state.apply_layers(prepared.layers()))? {
                self.broadcast.send(());
                return Ok(applied);
            }
        }

        warn!("apply_events retries exhausted while chasing moving head");
        Err(MutationError::TOCTOUAttemptsExhausted)
    }

    /// Apply prepared layers to the same head they were prepared for, updating the expected head.
    pub(super) fn apply_prepared_layers(&self, head: &mut Clock, layers: &[EventLayer]) -> Result<bool, MutationError> {
        let mut state = self.inner.write().unwrap();
        if state.head != *head {
            return Err(MutationError::WriteConflict);
        }
        let applied = !layers.is_empty();
        state.apply_layers(layers)?;
        *head = state.head.clone();
        drop(state);
        if applied {
            self.broadcast.send(());
        }
        Ok(applied)
    }

    /// Adopt a snapshot that strictly extends this entity's history.
    /// Returns whether it was adopted; equal, older, or concurrent snapshots leave state unchanged.
    pub(super) async fn apply_state<E>(&self, getter: &E, state: &State) -> Result<bool, MutationError>
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
                    return Ok(false);
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
                        return Ok(true);
                    }
                    continue;
                }
                AbstractCausalRelation::StrictAscends => {
                    // State is older than current - no-op
                    debug!("apply_state - new head {new_head} is older than current {head}, ignoring");
                    return Ok(false);
                }
                AbstractCausalRelation::DivergedSince { meet, .. } => {
                    // State snapshots cannot be merged without the underlying events.
                    // The caller should either:
                    // 1. Request the full event history and use apply_events()
                    // 2. Accept this state via policy if the attestation is trusted
                    // 3. Reject and resync from a known-good state
                    warn!(
                        "apply_state - new head {new_head} diverged from {head}, meet: {meet:?}. \
                        State not applied; events required for proper merge."
                    );
                    return Ok(false);
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

#[cfg(test)]
mod tests;
