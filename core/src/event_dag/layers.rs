//! Event layers: the forward, apply-phase view of an accumulated DAG.
//!
//! `EventLayers` owns the fully loaded layers and their per-input boundaries.
//! Each `EventLayer` supplies concurrent events and their causal context to
//! property backends.
//! The backward, comparison-phase machinery lives in `accumulator`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::{LineageError, MutationError, RetrievalError};
use crate::event_dag::accumulator::{compute_ancestry_from_dag, is_descendant_dag, ComparisonResult, EventAccumulator};
use crate::event_dag::{compare, AbstractCausalRelation, DEFAULT_BUDGET};
use crate::retrieval::GetEvents;
use ankurah_proto::{Clock, Event, EventId};
use smallvec::SmallVec;

/// Causal relation types for event layer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}

/// Fully loaded layers grouped by input event, ready to apply without history reads.
///
/// With local history `R -> H`, receiving A from `R -> X -> A` first loads X.
/// Apply X's layer, then A's, then check A's policy before the next input.
pub(crate) struct EventLayers {
    layers: SmallVec<[EventLayer; 1]>,
    /// Separates each input event's layers so its policy check sees the state
    /// after that event applies, before later inputs change it.
    boundaries: SmallVec<[usize; 2]>,
}

impl EventLayers {
    /// Compare the batch's causal history with the entity head and load the event
    /// layers needed to apply it, without changing entity state.
    /// Rejects batches where a supplied parent follows its child.
    pub(crate) async fn prepare<E: GetEvents + Send + Sync>(
        getter: &E,
        events: &[impl AsRef<Event>],
        head: &Clock,
    ) -> Result<Self, MutationError> {
        let event_count = events.len();
        let mut advancing_head = head.clone();
        let mut subject = Clock::default();
        let mut positions = BTreeMap::new();
        for (i, event) in events.iter().enumerate() {
            positions.entry(event.as_ref().id()).or_insert(i);
        }
        for (i, event) in events.iter().enumerate() {
            let event = event.as_ref();
            if event.parent.ids().any(|id| positions.get(id).is_some_and(|parent| *parent >= i)) {
                return Err(MutationError::InvalidUpdate("event batch must be in causal order"));
            }
            // Snapshots can hold a genesis without storing its event on ephemeral nodes.
            if event.is_entity_create() && !head.is_empty() {
                if getter.event_stored(&event.id()).await? {
                    continue;
                }
                if getter.storage_is_definitive() {
                    return Err(LineageError::Disjoint.into());
                }
            }
            if head.is_empty() && event.parent.ids().any(|id| !positions.get(id).is_some_and(|parent| *parent < i)) {
                return Err(MutationError::InvalidEvent);
            }
            if advancing_head.is_empty() && !event.is_entity_create() {
                return Err(MutationError::InvalidEvent);
            }
            // Refuse generation jumps before fetching history, including within the batch.
            if event.generation() > advancing_head.child_generation() {
                return Err(LineageError::BeyondHead {
                    event: event.id(),
                    claimed: event.generation(),
                    head: advancing_head.max_generation().unwrap_or(0),
                }
                .into());
            }
            for parent in event.parent.ids() {
                advancing_head.remove(parent);
                subject.remove(parent);
            }
            advancing_head.join(event);
            subject.join(event);
        }
        if subject.is_empty() {
            return Ok(Self::empty(event_count));
        }
        // Supplied events should not consume the allowance for additional history.
        let budget = DEFAULT_BUDGET.saturating_add(event_count);
        let ComparisonResult { relation, mut accumulator } = compare(getter, &subject, head, budget).await?;
        let meet = match relation {
            AbstractCausalRelation::Equal | AbstractCausalRelation::StrictAscends => return Ok(Self::empty(event_count)),
            AbstractCausalRelation::StrictDescends { .. } => head.ids().cloned().collect(),
            AbstractCausalRelation::DivergedSince { meet, .. } => meet,
            AbstractCausalRelation::Disjoint { .. } => return Err(LineageError::Disjoint.into()),
            AbstractCausalRelation::BudgetExceeded { subject, other } => {
                return Err(
                    LineageError::BudgetExceeded { original_budget: budget, subject_frontier: subject, other_frontier: other }.into()
                );
            }
        };
        // Empty-head comparison reads nothing. Load the creation chain before
        // building layers, so a batch can create and then update an entity.
        if head.is_empty() {
            let mut remaining: Vec<_> = subject.ids().cloned().collect();
            let mut roots = BTreeSet::new();
            while let Some(id) = remaining.pop() {
                if accumulator.dag().contains_key(&id) {
                    continue;
                }
                let event = accumulator.get_event(&id).await?;
                if event.parent.is_empty() {
                    if !event.is_entity_create() {
                        return Err(MutationError::InvalidEvent);
                    }
                    roots.insert(id);
                }
                remaining.extend(event.parent.ids().cloned());
                accumulator.accumulate(&event);
            }
            if roots.len() != 1 {
                return Err(MutationError::InvalidEvent);
            }
            accumulator.validate_generation_claims(&subject, head)?;
        }
        Self::load(accumulator, meet, head.ids().cloned().collect(), events).await
    }

    fn empty(event_count: usize) -> Self { Self { layers: SmallVec::new(), boundaries: smallvec::smallvec![0; event_count + 1] } }

    pub(super) async fn load<E: GetEvents>(
        accumulator: EventAccumulator<E>,
        meet: Vec<EventId>,
        current_head: Vec<EventId>,
        events: &[impl AsRef<Event>],
    ) -> Result<Self, MutationError> {
        let event_count = events.len();
        let mut source = LayerTraversal::new(accumulator, meet, current_head);
        // A common ancestor for the combined clock does not connect every input.
        // Prove each input shares history with the current head using the loaded graph.
        if event_count > 1 && !source.current_head_ancestry.is_empty() {
            let mut connected = source.current_head_ancestry.clone();
            let mut remaining: Vec<_> = connected.iter().cloned().collect();
            while let Some(id) = remaining.pop() {
                for child in source.children_index.get(&id).into_iter().flatten() {
                    if connected.insert(child.clone()) {
                        remaining.push(child.clone());
                    }
                }
            }
            // Inputs below the comparison's loaded history contribute no layers.
            if events
                .iter()
                .map(|event| event.as_ref().id())
                .any(|id| source.accumulator.dag().contains_key(&id) && !connected.contains(&id))
            {
                return Err(LineageError::Disjoint.into());
            }
        }
        let mut prepared = Self::empty(event_count);
        if event_count == 1 {
            while let Some(layer) = source.next().await? {
                prepared.layers.push(layer);
            }
            prepared.boundaries[1] = prepared.layers.len();
            return Ok(prepared);
        }

        // Assign each new event, including fetched ancestors, to the first input that needs it.
        let dag = source.accumulator.dag();
        let mut pending: BTreeSet<_> =
            dag.keys().filter(|id| !source.current_head_ancestry.contains(*id) && !source.processed.contains(*id)).cloned().collect();
        let mut owner = BTreeMap::new();
        for (i, event) in events.iter().enumerate() {
            let mut remaining = vec![event.as_ref().id()];
            while let Some(id) = remaining.pop() {
                if pending.remove(&id) {
                    remaining.extend(dag[&id].ids().cloned());
                    owner.insert(id, i);
                }
            }
        }
        if !pending.is_empty() {
            return Err(MutationError::InvalidEvent);
        }

        let mut ordered = Vec::new();
        while let Some(layer) = source.next().await? {
            let mut groups: BTreeMap<usize, Vec<Event>> = BTreeMap::new();
            for event in layer.to_apply {
                groups.entry(owner[&event.id()]).or_default().push(event);
            }
            let mut already_applied = layer.already_applied;
            for (i, to_apply) in groups {
                ordered.push((
                    i,
                    EventLayer {
                        already_applied: already_applied.clone(),
                        to_apply: to_apply.clone(),
                        dag_from_meet: Arc::clone(&layer.dag_from_meet),
                    },
                ));
                already_applied.extend(to_apply);
            }
        }
        // Keep causal order within each input while placing its layers before its policy check.
        ordered.sort_by_key(|(i, _)| *i);
        for (i, layer) in ordered {
            prepared.layers.push(layer);
            prepared.boundaries[i + 1] += 1;
        }
        for i in 1..prepared.boundaries.len() {
            prepared.boundaries[i] += prepared.boundaries[i - 1];
        }
        Ok(prepared)
    }

    pub(crate) fn layers(&self) -> &[EventLayer] { &self.layers }

    /// One slice per supplied event, including empty slices for already-applied events.
    pub(crate) fn per_event(&self) -> impl ExactSizeIterator<Item = &[EventLayer]> {
        self.boundaries.windows(2).map(|range| &self.layers[range[0]..range[1]])
    }
}

/// Walk the accumulated graph in causal order while loading event payloads.
struct LayerTraversal<E: GetEvents> {
    accumulator: EventAccumulator<E>,
    current_head_ancestry: BTreeSet<EventId>,

    /// Parent->children index, built once at construction: O(N)
    children_index: BTreeMap<EventId, Vec<EventId>>,

    /// Parent links shared by every yielded layer; see `EventLayer::dag_from_meet`.
    dag_from_meet: Arc<BTreeMap<EventId, Clock>>,

    // Iteration state
    processed: BTreeSet<EventId>,
    frontier: BTreeSet<EventId>,
}

impl<E: GetEvents> LayerTraversal<E> {
    fn new(accumulator: EventAccumulator<E>, meet: Vec<EventId>, current_head: Vec<EventId>) -> Self {
        // Build parent->children index from accumulated DAG: O(N)
        let mut children_index: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        for (id, parents) in accumulator.dag() {
            for parent in parents.ids() {
                children_index.entry(parent.clone()).or_default().push(id.clone());
            }
        }

        // Compute current head ancestry for partitioning.
        let current_head_ancestry = compute_ancestry_from_dag(accumulator.dag(), &current_head);

        // Share the parent links with each EventLayer.
        let dag_from_meet = Arc::new(accumulator.dag().clone());

        // Initialize frontier: all events in the DAG whose in-DAG parents are
        // all processed. This is a generalized topological-sort seed.
        let processed: BTreeSet<EventId> = meet.iter().cloned().collect();
        let frontier: BTreeSet<EventId> = accumulator
            .dag()
            .keys()
            .filter(|id| !processed.contains(*id))
            .filter(|id| {
                accumulator
                    .dag()
                    .get(*id)
                    .map(|ps| ps.ids().all(|p| processed.contains(p) || !accumulator.dag().contains_key(p)))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self { accumulator, current_head_ancestry, children_index, dag_from_meet, processed, frontier }
    }

    /// Load the next layer containing new events, in causal order.
    async fn next(&mut self) -> Result<Option<EventLayer>, RetrievalError> {
        while !self.frontier.is_empty() {
            let mut to_apply = Vec::new();
            let mut already_applied = Vec::new();
            let layer_frontier: Vec<EventId> = std::mem::take(&mut self.frontier).into_iter().collect();

            for id in &layer_frontier {
                if self.processed.contains(id) {
                    continue;
                }
                self.processed.insert(id.clone());

                let event = self.accumulator.get_event(id).await?;

                // Partition based on whether already in local head ancestry
                if self.current_head_ancestry.contains(id) {
                    already_applied.push(event);
                } else {
                    to_apply.push(event);
                }
            }

            // Advance to next frontier
            let mut next_frontier = BTreeSet::new();
            for id in &layer_frontier {
                if let Some(children) = self.children_index.get(id) {
                    for child in children {
                        if !self.processed.contains(child) && !next_frontier.contains(child) {
                            let all_parents_done = self
                                .accumulator
                                .dag()
                                .get(child)
                                .map(|ps| ps.ids().all(|p| self.processed.contains(p) || !self.accumulator.dag().contains_key(p)))
                                .unwrap_or(false);
                            if all_parents_done {
                                next_frontier.insert(child.clone());
                            }
                        }
                    }
                }
            }
            self.frontier = next_frontier;

            if !to_apply.is_empty() {
                return Ok(Some(EventLayer { to_apply, already_applied, dag_from_meet: Arc::clone(&self.dag_from_meet) }));
            }
        }
        Ok(None)
    }
}

// ---- EventLayer with DAG structure ----

/// Events ready to apply together, with history supplied by their comparison.
/// Production layers are prepared against the entity head by `EventLayers::prepare`.
///
/// All layers from one comparison share the same parent-link map, including
/// events in earlier and later layers. Causal lookup uses that map; the event
/// vectors carry the operations for this layer.
///
/// `pub` only so `PropertyBackend::apply_layer` (a public trait method) has a
/// reachable parameter type; its constructor and accessors remain
/// crate-internal while the external backend API surface is designed
/// (ankurah#267).
#[derive(Debug, Clone)]
pub struct EventLayer {
    /// Previously applied events, supplied only as comparison context.
    pub(crate) already_applied: Vec<Event>,
    /// Concurrent events whose operations are ready to apply.
    pub(crate) to_apply: Vec<Event>,
    /// Parent links covering both compared histories from the meet to their heads.
    /// Includes this layer's events and may also contain older history read during
    /// comparison. Boundary parents may be referenced without their own map entries.
    /// An immediate descendant needs only its own entry: comparison established
    /// that its parents are exactly the entity head, without fetching those events.
    dag_from_meet: Arc<BTreeMap<EventId, Clock>>,
}

impl EventLayer {
    /// Create a layer with parent links covering the compared histories from the meet.
    #[cfg(test)]
    pub(crate) fn new(already_applied: Vec<Event>, to_apply: Vec<Event>, dag_from_meet: Arc<BTreeMap<EventId, Clock>>) -> Self {
        Self { already_applied, to_apply, dag_from_meet }
    }

    /// Check whether an event_id is present in the accumulated DAG.
    /// Used by LWW's causal fallback when generations have saturated.
    pub(crate) fn dag_contains(&self, id: &EventId) -> bool { self.dag_from_meet.contains_key(id) }

    /// Compare two event IDs using accumulated DAG context.
    ///
    /// Missing entries end traversal. `Concurrent` means neither ID reaches the
    /// other through the supplied graph; it does not establish concurrency for
    /// arbitrary historical IDs outside that graph's coverage.
    pub(crate) fn compare(&self, a: &EventId, b: &EventId) -> CausalRelation {
        if a == b {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag_from_meet, a, b) {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag_from_meet, b, a) {
            return CausalRelation::Ascends;
        }
        CausalRelation::Concurrent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_layer_compare() {
        // DAG: A <- B, A <- C (B and C are concurrent)
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);

        dag.insert(a.clone(), Clock::default());
        dag.insert(b.clone(), Clock::new(vec![(1, a.clone())]).unwrap());
        dag.insert(c.clone(), Clock::new(vec![(1, a.clone())]).unwrap());

        let layer = EventLayer { to_apply: vec![], already_applied: vec![], dag_from_meet: Arc::new(dag) };

        assert_eq!(layer.compare(&b, &a), CausalRelation::Descends);
        assert_eq!(layer.compare(&a, &b), CausalRelation::Ascends);
        assert_eq!(layer.compare(&b, &c), CausalRelation::Concurrent);
        assert_eq!(layer.compare(&a, &a), CausalRelation::Descends); // same = Descends
    }

    #[test]
    fn test_event_layer_dag_contains() {
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);

        dag.insert(a.clone(), Clock::default());
        dag.insert(b.clone(), Clock::new(vec![(1, a.clone())]).unwrap());

        let layer = EventLayer { to_apply: vec![], already_applied: vec![], dag_from_meet: Arc::new(dag) };

        assert!(layer.dag_contains(&a));
        assert!(layer.dag_contains(&b));
        assert!(!layer.dag_contains(&c)); // not in DAG
    }
}
