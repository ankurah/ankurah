//! Event accumulator for the comparison-phase BFS.
//!
//! Provides `EventAccumulator`, which collects DAG structure during the
//! backward comparison traversal and offers read-through event caching, plus
//! the pure DAG-walk helpers shared with the layer machinery. The forward,
//! apply-phase view (`EventLayers` / `EventLayer`) lives in `layers`.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use lru::LruCache;

use crate::error::RetrievalError;
use crate::event_dag::layers::EventLayers;
use crate::event_dag::relation::AbstractCausalRelation;
use crate::event_dag::stats::CompareStats;
use crate::retrieval::GetEvents;
use ankurah_proto::{Event, EventId};

/// Accumulates event DAG structure during comparison and provides
/// read-through caching for event retrieval during layer iteration.
///
/// Events are assumed to be already stored in local storage before
/// comparison begins (eager storage model). The accumulator tracks
/// DAG structure (parent pointers) discovered during BFS and caches
/// hot events to reduce storage round-trips.
pub(crate) struct EventAccumulator<E: GetEvents> {
    /// DAG structure: event id -> parent ids (always in memory, cheap)
    dag: BTreeMap<EventId, Vec<EventId>>,

    /// LRU cache of Event objects fetched from storage (bounded, eviction-safe)
    cache: LruCache<EventId, Event>,

    /// Event getter with storage access
    event_getter: E,

    /// Per-comparison counters (D2 M5, dispositions Q4). They live on the
    /// accumulator because it sees every fetch and survives budget-escalation
    /// retries; ComparisonResult snapshots them at construction. WRITE-ONLY
    /// during the walk (see the `stats` module doc).
    pub(crate) stats: CompareStats,
}

impl<E: GetEvents> EventAccumulator<E> {
    /// Create a new accumulator with the given retriever.
    /// LRU cache defaults to 1000 entries.
    pub(crate) fn new(event_getter: E) -> Self {
        Self { dag: BTreeMap::new(), cache: LruCache::new(NonZeroUsize::new(1000).unwrap()), event_getter, stats: CompareStats::default() }
    }

    /// Called during BFS traversal -- records DAG structure and caches the event.
    pub(crate) fn accumulate(&mut self, event: &Event) {
        let id = event.id();
        let parents: Vec<EventId> = event.parent.as_slice().to_vec();
        self.dag.insert(id, parents);
        self.cache.put(event.id(), event.clone());
    }

    /// Get event by id: cache -> retriever (storage).
    /// All events are already in storage (eager storage model).
    pub(crate) async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.event_getter.get_event(id).await?;
        self.stats.events_fetched += 1;
        self.cache.put(id.clone(), event.clone());
        Ok(event)
    }

    /// Get a reference to the DAG structure.
    pub(crate) fn dag(&self) -> &BTreeMap<EventId, Vec<EventId>> { &self.dag }

    /// Produce layer iterator for merge (consumes self).
    /// Only valid for DivergedSince results -- the DAG must be complete.
    pub(crate) fn into_layers(self, meet: Vec<EventId>, current_head: Vec<EventId>) -> EventLayers<E> {
        EventLayers::new(self, meet, current_head)
    }
}

/// Result of a causal comparison, carrying the accumulated DAG.
pub struct ComparisonResult<E: GetEvents> {
    /// The causal relation between the compared clocks.
    pub(crate) relation: AbstractCausalRelation<EventId>,
    /// The event accumulator with DAG structure (private -- access via into_layers).
    accumulator: EventAccumulator<E>,
    /// The walk's counters, snapshotted at verdict time (dispositions Q4):
    /// post-verdict layer iteration through the accumulator does not bleed
    /// into them. Crate-visible; readers are tests, the immunity oracle,
    /// and bench evidence, never the walk itself (write-only rule).
    pub(crate) stats: CompareStats,
}

impl<E: GetEvents> ComparisonResult<E> {
    /// Create a new ComparisonResult, snapshotting the accumulator's
    /// counters as of the verdict.
    pub(crate) fn new(relation: AbstractCausalRelation<EventId>, accumulator: EventAccumulator<E>) -> Self {
        let stats = accumulator.stats.clone();
        Self { relation, accumulator, stats }
    }

    /// For DivergedSince results, consume self to get a layer iterator.
    /// Returns None for non-divergent relations.
    pub(crate) fn into_layers(self, current_head: Vec<EventId>) -> Option<EventLayers<E>> {
        match &self.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => Some(self.accumulator.into_layers(meet.clone(), current_head)),
            _ => None,
        }
    }

    /// Get a reference to the accumulator (for inspection/testing).
    pub(crate) fn accumulator(&self) -> &EventAccumulator<E> { &self.accumulator }

    /// Decompose into relation and accumulator.
    pub(crate) fn into_parts(self) -> (AbstractCausalRelation<EventId>, EventAccumulator<E>) { (self.relation, self.accumulator) }
}

// ---- Helper functions ----

/// Compute ancestry set by walking backward through DAG parent pointers.
/// Returns all event IDs reachable from `head` (inclusive).
pub(crate) fn compute_ancestry_from_dag(dag: &BTreeMap<EventId, Vec<EventId>>, head: &[EventId]) -> BTreeSet<EventId> {
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
pub(crate) fn is_descendant_dag(dag: &BTreeMap<EventId, Vec<EventId>>, descendant: &EventId, ancestor: &EventId) -> bool {
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
    // but we can test the helper functions. EventLayer::compare is tested in `layers`.

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
}
