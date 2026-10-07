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
use crate::retrieval::GetEvents;
use ankurah_proto::{Clock, Event, EventId, EventStructureError};

/// Accumulates event DAG structure during comparison and provides
/// read-through caching for event retrieval during layer iteration.
///
/// Events are assumed to be already stored in local storage before
/// comparison begins (eager storage model). The accumulator tracks
/// DAG structure (parent pointers) discovered during BFS and caches
/// hot events to reduce storage round-trips.
pub(crate) struct EventAccumulator<E: GetEvents> {
    /// Parent links and generations discovered during comparison; never evicted.
    dag: BTreeMap<EventId, Clock>,

    /// LRU cache of Event objects fetched from storage (bounded, eviction-safe)
    cache: LruCache<EventId, Event>,

    /// Event getter with storage access
    event_getter: E,
}

impl<E: GetEvents> EventAccumulator<E> {
    /// Create a new accumulator with the given retriever.
    /// LRU cache defaults to 1000 entries.
    pub(crate) fn new(event_getter: E) -> Self {
        Self { dag: BTreeMap::new(), cache: LruCache::new(NonZeroUsize::new(1000).unwrap()), event_getter }
    }

    /// Called during BFS traversal -- records DAG structure and caches the event.
    pub(crate) fn accumulate(&mut self, event: &Event) {
        let id = event.id();
        self.dag.insert(id.clone(), event.parent.clone());
        self.cache.put(id, event.clone());
    }

    /// Get event by id: cache -> retriever (storage).
    /// All events are already in storage (eager storage model).
    pub(crate) async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.event_getter.get_event(id).await?;
        self.cache.put(id.clone(), event.clone());
        Ok(event)
    }

    /// Get a reference to the DAG structure.
    pub(crate) fn dag(&self) -> &BTreeMap<EventId, Clock> { &self.dag }

    /// The event's generation, calculated from its retained parent clock even
    /// after the event itself has been evicted from the cache.
    pub(crate) fn generation_of_event(&self, id: &EventId) -> Option<u32> { self.dag.get(id).map(Clock::child_generation) }

    /// Check subject tips and parent clocks read during comparison against event generations or the comparison head.
    /// Unknown generations are left unverified; this makes no additional reads.
    pub(crate) fn validate_generation_claims(&self, subject: &Clock, comparison: &Clock) -> Result<(), EventStructureError> {
        let clocks = std::iter::once(subject).chain(self.dag.values());
        for clock in clocks {
            for &(claimed, ref event) in clock {
                let Some(known) = self.generation_of_event(event).or_else(|| comparison.generation_of(event)) else { continue };
                if claimed != known {
                    return Err(EventStructureError::GenerationMismatch { event: event.clone(), claimed, known });
                }
            }
        }
        Ok(())
    }

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
    /// Event history collected during comparison.
    accumulator: EventAccumulator<E>,
}

impl<E: GetEvents> ComparisonResult<E> {
    /// Create a new ComparisonResult.
    pub(crate) fn new(relation: AbstractCausalRelation<EventId>, accumulator: EventAccumulator<E>) -> Self {
        Self { relation, accumulator }
    }

    /// For DivergedSince results, consume self to get a layer iterator.
    /// Returns None for non-divergent relations.
    pub(crate) fn into_layers(self, current_head: Vec<EventId>) -> Option<EventLayers<E>> {
        match &self.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => Some(self.accumulator.into_layers(meet.clone(), current_head)),
            _ => None,
        }
    }

    /// Borrow the event history collected during comparison.
    #[cfg(test)]
    pub(crate) fn accumulator(&self) -> &EventAccumulator<E> { &self.accumulator }

    /// Decompose into relation and accumulator.
    pub(crate) fn into_parts(self) -> (AbstractCausalRelation<EventId>, EventAccumulator<E>) { (self.relation, self.accumulator) }
}

// ---- Helper functions ----

/// Compute ancestry set by walking backward through DAG parent pointers.
/// Returns all event IDs reachable from `head` (inclusive).
pub(crate) fn compute_ancestry_from_dag(dag: &BTreeMap<EventId, Clock>, head: &[EventId]) -> BTreeSet<EventId> {
    let mut ancestry = BTreeSet::new();
    let mut frontier: Vec<EventId> = head.to_vec();
    while let Some(id) = frontier.pop() {
        if !ancestry.insert(id.clone()) {
            continue;
        }
        if let Some(parents) = dag.get(&id) {
            for parent in parents.ids() {
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
pub(crate) fn is_descendant_dag(dag: &BTreeMap<EventId, Clock>, descendant: &EventId, ancestor: &EventId) -> bool {
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
        for parent in parents.ids() {
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

    struct NoReads;

    #[async_trait::async_trait]
    impl GetEvents for NoReads {
        async fn get_event(&self, _: &EventId) -> Result<Event, RetrievalError> { panic!("unexpected event read") }
        async fn event_stored(&self, _: &EventId) -> Result<bool, RetrievalError> { panic!("unexpected event read") }
    }

    #[test]
    fn dag_metadata_survives_payload_eviction() {
        use ankurah_proto::AuthorId;
        let genesis = Event::genesis(None, AuthorId::Unknown, Default::default());
        let child = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, Default::default());
        let mut accumulator = EventAccumulator::new(NoReads);
        accumulator.cache.resize(NonZeroUsize::new(1).unwrap());
        accumulator.accumulate(&child);
        accumulator.accumulate(&genesis);

        assert!(!accumulator.cache.contains(&child.id()), "the payload must actually be evicted");
        assert_eq!(accumulator.generation_of_event(&child.id()), Some(2));
        assert_eq!(accumulator.dag()[&child.id()], Clock::singleton(&genesis));
    }

    #[test]
    fn test_compute_ancestry_from_dag() {
        // DAG: A <- B <- D, A <- C
        let mut dag = BTreeMap::new();
        let a = EventId::from_bytes([1; 32]);
        let b = EventId::from_bytes([2; 32]);
        let c = EventId::from_bytes([3; 32]);
        let d = EventId::from_bytes([4; 32]);

        dag.insert(a.clone(), Clock::default()); // genesis
        dag.insert(b.clone(), Clock::new(vec![(1, a.clone())]).unwrap());
        dag.insert(c.clone(), Clock::new(vec![(1, a.clone())]).unwrap());
        dag.insert(d.clone(), Clock::new(vec![(2, b.clone())]).unwrap());

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

        dag.insert(a.clone(), Clock::default());
        dag.insert(b.clone(), Clock::new(vec![(1, a.clone())]).unwrap());
        dag.insert(c.clone(), Clock::new(vec![(2, b.clone())]).unwrap());

        assert!(is_descendant_dag(&dag, &c, &a)); // C descends from A
        assert!(is_descendant_dag(&dag, &c, &b)); // C descends from B
        assert!(!is_descendant_dag(&dag, &a, &c)); // A does not descend from C
        assert!(!is_descendant_dag(&dag, &b, &c)); // B does not descend from C
    }
}
