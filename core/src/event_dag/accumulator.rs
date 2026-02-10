//! Event accumulator for DAG traversal and layer computation.
//!
//! Provides `EventAccumulator` which collects DAG structure during comparison BFS,
//! and `EventLayers` which computes concurrency layers from the accumulated DAG.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;

use crate::error::RetrievalError;
use crate::event_dag::relation::AbstractCausalRelation;
use crate::retrieval::GetEvents;
use ankurah_proto::{Event, EventId};

// Re-export CausalRelation from layers (same enum, avoid duplication)
pub use super::layers::CausalRelation;

/// Accumulates event DAG structure during comparison and provides
/// read-through caching for event retrieval during layer iteration.
///
/// Events are assumed to be already stored in local storage before
/// comparison begins (eager storage model). The accumulator tracks
/// DAG structure (parent pointers) discovered during BFS and caches
/// hot events to reduce storage round-trips.
pub struct EventAccumulator<E: GetEvents> {
    /// DAG structure: event id -> parent ids (always in memory, cheap)
    dag: BTreeMap<EventId, Vec<EventId>>,

    /// LRU cache of Event objects fetched from storage (bounded, eviction-safe)
    cache: LruCache<EventId, Event>,

    /// Event getter with storage access
    event_getter: E,
}

impl<E: GetEvents> EventAccumulator<E> {
    /// Create a new accumulator with the given retriever.
    /// LRU cache defaults to 1000 entries.
    pub fn new(event_getter: E) -> Self {
        Self {
            dag: BTreeMap::new(),
            cache: LruCache::new(NonZeroUsize::new(1000).unwrap()),
            event_getter,
        }
    }

    /// Called during BFS traversal -- records DAG structure and caches the event.
    pub fn accumulate(&mut self, event: &Event) {
        let id = event.id();
        let parents: Vec<EventId> = event.parent.as_slice().to_vec();
        self.dag.insert(id, parents);
        self.cache.put(event.id(), event.clone());
    }

    /// Get event by id: cache -> retriever (storage).
    /// All events are already in storage (eager storage model).
    pub async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.event_getter.get_event(id).await?;
        self.cache.put(id.clone(), event.clone());
        Ok(event)
    }

    /// Check whether an event_id is present in the accumulated DAG structure.
    /// Used by LWW resolution to distinguish "known older" from "unknown".
    pub fn contains(&self, id: &EventId) -> bool {
        self.dag.contains_key(id)
    }

    /// Get a reference to the DAG structure.
    pub fn dag(&self) -> &BTreeMap<EventId, Vec<EventId>> {
        &self.dag
    }

    /// Produce layer iterator for merge (consumes self).
    /// Only valid for DivergedSince results -- the DAG must be complete.
    pub fn into_layers(
        self,
        meet: Vec<EventId>,
        current_head: Vec<EventId>,
    ) -> EventLayers<E> {
        EventLayers::new(self, meet, current_head)
    }
}

/// Result of a causal comparison, carrying the accumulated DAG.
pub struct ComparisonResult<E: GetEvents> {
    /// The causal relation between the compared clocks.
    pub relation: AbstractCausalRelation<EventId>,
    /// The event accumulator with DAG structure (private -- access via into_layers).
    accumulator: EventAccumulator<E>,
}

impl<E: GetEvents> ComparisonResult<E> {
    /// Create a new ComparisonResult.
    pub fn new(relation: AbstractCausalRelation<EventId>, accumulator: EventAccumulator<E>) -> Self {
        Self { relation, accumulator }
    }

    /// For DivergedSince results, consume self to get a layer iterator.
    /// Returns None for non-divergent relations.
    pub fn into_layers(self, current_head: Vec<EventId>) -> Option<EventLayers<E>> {
        match &self.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                Some(self.accumulator.into_layers(meet.clone(), current_head))
            }
            _ => None,
        }
    }

    /// Get a reference to the accumulator (for inspection/testing).
    pub fn accumulator(&self) -> &EventAccumulator<E> {
        &self.accumulator
    }

    /// Decompose into relation and accumulator.
    pub fn into_parts(self) -> (AbstractCausalRelation<EventId>, EventAccumulator<E>) {
        (self.relation, self.accumulator)
    }
}

// ---- EventLayers: async layer iterator ----

/// Async iterator over EventLayer for merge application.
/// Computes layers lazily using forward expansion from the meet point.
/// Pre-builds a parent->children index at construction for O(1) lookups.
pub struct EventLayers<E: GetEvents> {
    accumulator: EventAccumulator<E>,
    #[allow(dead_code)]
    meet: Vec<EventId>,
    current_head_ancestry: BTreeSet<EventId>,

    /// Parent->children index, built once at construction: O(N)
    children_index: BTreeMap<EventId, Vec<EventId>>,

    /// Shared DAG structure reference, passed to each yielded EventLayer
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,

    // Iteration state
    processed: BTreeSet<EventId>,
    frontier: BTreeSet<EventId>,
}

impl<E: GetEvents> EventLayers<E> {
    fn new(
        accumulator: EventAccumulator<E>,
        meet: Vec<EventId>,
        current_head: Vec<EventId>,
    ) -> Self {
        // Build parent->children index from accumulated DAG: O(N)
        let mut children_index: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        for (id, parents) in &accumulator.dag {
            for parent in parents {
                children_index.entry(parent.clone()).or_default().push(id.clone());
            }
        }

        // Compute current head ancestry for partitioning.
        let current_head_ancestry = compute_ancestry_from_dag(&accumulator.dag, &current_head);

        // Snapshot the DAG structure into an Arc for sharing with EventLayers
        let dag = Arc::new(accumulator.dag.clone());

        // Initialize frontier: all events in the DAG whose in-DAG parents are
        // all processed. This is a generalized topological-sort seed.
        let processed: BTreeSet<EventId> = meet.iter().cloned().collect();
        let frontier: BTreeSet<EventId> = accumulator
            .dag
            .keys()
            .filter(|id| !processed.contains(*id))
            .filter(|id| {
                accumulator
                    .dag
                    .get(*id)
                    .map(|ps| {
                        ps.iter()
                            .all(|p| processed.contains(p) || !accumulator.dag.contains_key(p))
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self {
            accumulator,
            meet,
            current_head_ancestry,
            children_index,
            dag,
            processed,
            frontier,
        }
    }

    /// Get next layer (async -- may fetch events from storage via accumulator).
    /// Returns layers in topological order (earliest first).
    pub async fn next(&mut self) -> Result<Option<EventLayer>, RetrievalError> {
        if self.frontier.is_empty() {
            return Ok(None);
        }

        let mut to_apply = Vec::new();
        let mut already_applied = Vec::new();
        let layer_frontier: Vec<EventId> = std::mem::take(&mut self.frontier)
            .into_iter()
            .collect();

        for id in &layer_frontier {
            if self.processed.contains(id) {
                continue;
            }
            self.processed.insert(id.clone());

            // Events are already in storage (eager storage model)
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
                            .dag
                            .get(child)
                            .map(|ps| {
                                ps.iter().all(|p| {
                                    self.processed.contains(p)
                                        || !self.accumulator.dag.contains_key(p)
                                })
                            })
                            .unwrap_or(false);
                        if all_parents_done {
                            next_frontier.insert(child.clone());
                        }
                    }
                }
            }
        }
        self.frontier = next_frontier;

        if to_apply.is_empty() && already_applied.is_empty() {
            return Ok(None);
        }

        Ok(Some(EventLayer {
            to_apply,
            already_applied,
            dag: Arc::clone(&self.dag),
        }))
    }
}

// ---- New EventLayer with DAG structure ----

/// A layer of concurrent events for unified backend application.
///
/// This is the new EventLayer that carries DAG structure (parent pointers only)
/// rather than full event clones. The `compare()` method is infallible since
/// it only traverses parent pointers, treating missing entries as dead ends.
#[derive(Debug, Clone)]
pub struct EventLayer {
    pub already_applied: Vec<Event>,
    pub to_apply: Vec<Event>,
    /// Shared DAG structure for causal comparison: event_id -> parent_ids.
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
}

impl EventLayer {
    /// Create a new EventLayer with the given events and DAG structure.
    pub fn new(already_applied: Vec<Event>, to_apply: Vec<Event>, dag: Arc<BTreeMap<EventId, Vec<EventId>>>) -> Self {
        Self { already_applied, to_apply, dag }
    }

    /// Check whether an event_id is present in the accumulated DAG.
    /// Used by LWW to implement the "older than meet" rule.
    pub fn dag_contains(&self, id: &EventId) -> bool {
        self.dag.contains_key(id)
    }

    /// Compare two event IDs using accumulated DAG context.
    ///
    /// Returns the causal relation between `a` and `b`. Missing entries
    /// are treated as dead ends (below the meet), not errors.
    pub fn compare(&self, a: &EventId, b: &EventId) -> CausalRelation {
        if a == b {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag, a, b) {
            return CausalRelation::Descends;
        }
        if is_descendant_dag(&self.dag, b, a) {
            return CausalRelation::Ascends;
        }
        CausalRelation::Concurrent
    }

}

// ---- Helper functions ----

/// Compute ancestry set by walking backward through DAG parent pointers.
/// Returns all event IDs reachable from `head` (inclusive).
pub(crate) fn compute_ancestry_from_dag(
    dag: &BTreeMap<EventId, Vec<EventId>>,
    head: &[EventId],
) -> BTreeSet<EventId> {
    let mut ancestry = BTreeSet::new();
    let mut frontier: Vec<EventId> = head.to_vec();
    while let Some(id) = frontier.pop() {
        if !ancestry.insert(id.clone()) {
            continue;
        }
        if let Some(parents) = dag.get(&id) {
            for parent in parents {
                if !ancestry.contains(parent) {
                    frontier.push(parent.clone());
                }
            }
        }
    }
    ancestry
}

/// Walk backward from `descendant` through parent pointers looking for `ancestor`.
/// Missing entries are treated as dead ends (below the meet), not errors.
pub(crate) fn is_descendant_dag(
    dag: &BTreeMap<EventId, Vec<EventId>>,
    descendant: &EventId,
    ancestor: &EventId,
) -> bool {
    let mut visited = BTreeSet::new();
    let mut frontier = vec![descendant.clone()];
    while let Some(id) = frontier.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        if &id == ancestor {
            return true;
        }
        let Some(parents) = dag.get(&id) else {
            continue;
        };
        for parent in parents {
            if !visited.contains(parent) {
                frontier.push(parent.clone());
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // We can't easily test EventAccumulator with a real GetEvents impl in unit tests,
    // but we can test the helper functions and EventLayer::compare.

    #[test]
    fn test_compute_ancestry_from_dag() {
        // DAG: A <- B <- D, A <- C
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);
        let d = EventId::from_bytes([4; 32]);

        dag.insert(a.clone(), vec![]); // genesis
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![a.clone()]);
        dag.insert(d.clone(), vec![b.clone()]);

        // Ancestry of D should be {A, B, D}
        let ancestry = compute_ancestry_from_dag(&dag, &[d.clone()]);
        assert!(ancestry.contains(&a));
        assert!(ancestry.contains(&b));
        assert!(ancestry.contains(&d));
        assert!(!ancestry.contains(&c));

        // Ancestry of [D, C] should be {A, B, C, D}
        let ancestry = compute_ancestry_from_dag(&dag, &[d.clone(), c.clone()]);
        assert!(ancestry.contains(&a));
        assert!(ancestry.contains(&b));
        assert!(ancestry.contains(&c));
        assert!(ancestry.contains(&d));
    }

    #[test]
    fn test_is_descendant_dag() {
        // DAG: A <- B <- C
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);

        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![b.clone()]);

        assert!(is_descendant_dag(&dag, &c, &a)); // C descends from A
        assert!(is_descendant_dag(&dag, &c, &b)); // C descends from B
        assert!(!is_descendant_dag(&dag, &a, &c)); // A does not descend from C
        assert!(!is_descendant_dag(&dag, &b, &c)); // B does not descend from C
    }

    #[test]
    fn test_event_layer_compare() {
        // DAG: A <- B, A <- C (B and C are concurrent)
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);

        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![a.clone()]);

        let layer = EventLayer {
            to_apply: vec![],
            already_applied: vec![],
            dag: Arc::new(dag),
        };

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

        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);

        let layer = EventLayer {
            to_apply: vec![],
            already_applied: vec![],
            dag: Arc::new(dag),
        };

        assert!(layer.dag_contains(&a));
        assert!(layer.dag_contains(&b));
        assert!(!layer.dag_contains(&c)); // not in DAG
    }
}
