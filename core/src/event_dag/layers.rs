//! Event layers: the forward, apply-phase view of an accumulated DAG.
//!
//! `EventLayers` computes topological generations ("layers") from the meet
//! forward, and `EventLayer` is the per-generation unit handed to property
//! backends, carrying the DAG context they need for causal comparisons.
//! The backward, comparison-phase machinery lives in `accumulator`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::RetrievalError;
use crate::event_dag::accumulator::{compute_ancestry_from_dag, is_descendant_dag, EventAccumulator};
use crate::retrieval::GetEvents;
use ankurah_proto::{Event, EventId};

/// Causal relation types for event layer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}

// ---- EventLayers: async layer iterator ----

/// Async iterator over EventLayer for merge application.
/// Computes layers lazily using forward expansion from the meet point.
/// Pre-builds a parent->children index at construction for O(1) lookups.
pub(crate) struct EventLayers<E: GetEvents> {
    accumulator: EventAccumulator<E>,
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
    pub(crate) fn new(accumulator: EventAccumulator<E>, meet: Vec<EventId>, current_head: Vec<EventId>) -> Self {
        // Build parent->children index from accumulated DAG: O(N)
        let mut children_index: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        for (id, parents) in accumulator.dag() {
            for parent in parents {
                children_index.entry(parent.clone()).or_default().push(id.clone());
            }
        }

        // Compute current head ancestry for partitioning.
        let current_head_ancestry = compute_ancestry_from_dag(accumulator.dag(), &current_head);

        // Snapshot the DAG structure into an Arc for sharing with EventLayers
        let dag = Arc::new(accumulator.dag().clone());

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
                    .map(|ps| ps.iter().all(|p| processed.contains(p) || !accumulator.dag().contains_key(p)))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self { accumulator, current_head_ancestry, children_index, dag, processed, frontier }
    }

    /// Get next layer (async -- may fetch events from storage via accumulator).
    /// Returns layers in topological order (earliest first).
    pub(crate) async fn next(&mut self) -> Result<Option<EventLayer>, RetrievalError> {
        if self.frontier.is_empty() {
            return Ok(None);
        }

        let mut to_apply = Vec::new();
        let mut to_apply_ids = Vec::new();
        let mut already_applied = Vec::new();
        let mut already_applied_ids = Vec::new();
        let layer_frontier: Vec<EventId> = std::mem::take(&mut self.frontier).into_iter().collect();

        for id in &layer_frontier {
            if self.processed.contains(id) {
                continue;
            }
            self.processed.insert(id.clone());

            // Events are already in storage (eager storage model)
            let event = self.accumulator.get_event(id).await?;

            // Partition based on whether already in local head ancestry
            if self.current_head_ancestry.contains(id) {
                already_applied_ids.push(id.clone());
                already_applied.push(event);
            } else {
                to_apply_ids.push(id.clone());
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
                            .map(|ps| ps.iter().all(|p| self.processed.contains(p) || !self.accumulator.dag().contains_key(p)))
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

        Ok(Some(EventLayer { to_apply, to_apply_ids, already_applied, already_applied_ids, dag: Arc::clone(&self.dag) }))
    }
}

// ---- EventLayer with DAG structure ----

/// A layer of concurrent events for unified backend application.
///
/// Carries DAG structure (parent pointers only) rather than full event
/// clones. The `compare()` method is infallible since it only traverses
/// parent pointers, treating missing entries as dead ends.
///
/// `pub` only so `PropertyBackend::apply_layer` (a public trait method) has a
/// reachable parameter type; its constructor and accessors remain
/// crate-internal while the external backend API surface is designed
/// (ankurah#267).
#[derive(Debug, Clone)]
pub struct EventLayer {
    pub(crate) already_applied: Vec<Event>,
    pub(crate) to_apply: Vec<Event>,
    /// Requested ids that key `already_applied` in the accumulated DAG. These
    /// normally equal `Event::id()`, but remain authoritative if a diagnostic
    /// comparison is deliberately driven over a mismatched served payload.
    already_applied_ids: Vec<EventId>,
    /// Requested ids that key `to_apply` in the accumulated DAG. Backends must
    /// use these ids for provenance and causal comparisons rather than
    /// recomputing identity from the served payload.
    to_apply_ids: Vec<EventId>,
    /// Shared DAG structure for causal comparison: event_id -> parent_ids.
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
}

impl EventLayer {
    /// Create a new EventLayer with the given events and DAG structure.
    #[cfg(test)]
    pub(crate) fn new(already_applied: Vec<Event>, to_apply: Vec<Event>, dag: Arc<BTreeMap<EventId, Vec<EventId>>>) -> Self {
        let already_applied = already_applied.into_iter().map(|event| (event.id(), event)).collect();
        let to_apply = to_apply.into_iter().map(|event| (event.id(), event)).collect();
        Self::new_with_ids(already_applied, to_apply, dag)
    }

    /// Create a layer from the authoritative ids used by the comparison walk.
    /// The pair form is intentionally crate-private: production layers obtain
    /// these ids from the frontier, while focused tests can exercise a served
    /// payload whose recomputed id differs from its requested id.
    #[cfg(test)]
    pub(crate) fn new_with_ids(
        already_applied: Vec<(EventId, Event)>,
        to_apply: Vec<(EventId, Event)>,
        dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
    ) -> Self {
        let (already_applied_ids, already_applied) = already_applied.into_iter().unzip();
        let (to_apply_ids, to_apply) = to_apply.into_iter().unzip();
        Self { already_applied, to_apply, already_applied_ids, to_apply_ids, dag }
    }

    pub(crate) fn already_applied_with_ids(&self) -> impl Iterator<Item = (&EventId, &Event)> {
        debug_assert_eq!(self.already_applied_ids.len(), self.already_applied.len());
        self.already_applied_ids.iter().zip(&self.already_applied)
    }

    pub(crate) fn to_apply_with_ids(&self) -> impl Iterator<Item = (&EventId, &Event)> {
        debug_assert_eq!(self.to_apply_ids.len(), self.to_apply.len());
        self.to_apply_ids.iter().zip(&self.to_apply)
    }

    /// Check whether an event_id is present in the accumulated DAG.
    /// Used by LWW to implement the "older than meet" rule.
    pub(crate) fn dag_contains(&self, id: &EventId) -> bool { self.dag.contains_key(id) }

    /// Compare two event IDs using accumulated DAG context.
    ///
    /// Returns the causal relation between `a` and `b`. Missing entries
    /// are treated as dead ends (below the meet), not errors.
    pub(crate) fn compare(&self, a: &EventId, b: &EventId) -> CausalRelation {
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

        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![a.clone()]);

        let layer = EventLayer::new(vec![], vec![], Arc::new(dag));

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

        let layer = EventLayer::new(vec![], vec![], Arc::new(dag));

        assert!(layer.dag_contains(&a));
        assert!(layer.dag_contains(&b));
        assert!(!layer.dag_contains(&c)); // not in DAG
    }
}
