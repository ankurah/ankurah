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

    // ---- Walk-time edge-check state (D2 M5, plan D2-4) ----
    // Transient per-comparison locals (the registry-ban exemption): ids
    // only, dropped with the accumulator, surviving budget retries so no
    // edge is double-counted across them.
    //
    /// Children whose equation was evaluated (pass or fail); one check per
    /// child, ever.
    edge_checked: BTreeSet<EventId>,
    /// Children whose first incomplete check already registered them in
    /// `edge_waiting` under every then-missing parent. A re-check that is
    /// still incomplete must NOT re-register: the original registrations
    /// stand and drain as those parents arrive. Without this guard a wide
    /// antichain multiplies registrations combinatorially (every drain
    /// re-checks once per accumulated copy, every incomplete re-check
    /// re-registers under every still-missing parent: 2^width entries),
    /// which is a memory explosion, not a slowdown. With it, total
    /// `edge_waiting` entries are bounded by the DAG's edge count. Cleared
    /// when the check completes (`edge_checked` then short-circuits).
    edge_pending: BTreeSet<EventId>,
    /// Parent id -> children whose check is blocked on that parent's
    /// payload arriving; drained when it does (backward BFS fetches parents
    /// after children, so this is the productive direction).
    edge_waiting: BTreeMap<EventId, Vec<EventId>>,
    /// Children demoted to per-comparison ineligibility by a failed check
    /// (D2-4: demotion, never retroactive rejection). Consumed by the
    /// eligibility-keyed frontier ordering; never by a verdict path.
    demoted: BTreeSet<EventId>,
}

impl<E: GetEvents> EventAccumulator<E> {
    /// Create a new accumulator with the given retriever.
    /// LRU cache defaults to 1000 entries.
    pub(crate) fn new(event_getter: E) -> Self {
        Self {
            dag: BTreeMap::new(),
            cache: LruCache::new(NonZeroUsize::new(1000).unwrap()),
            event_getter,
            stats: CompareStats::default(),
            edge_checked: BTreeSet::new(),
            edge_pending: BTreeSet::new(),
            edge_waiting: BTreeMap::new(),
            demoted: BTreeSet::new(),
        }
    }

    /// Called during BFS traversal -- records DAG structure and caches the
    /// event UNDER THE REQUESTED ID (R1, dispositions Q1): mid-walk identity
    /// is the requested-id space, and content addressing remains the
    /// integrity mechanism at admission and at rest. In every honest run the
    /// requested id equals the payload's recomputed id (engines key rows by
    /// payload id at write); under lying or corrupt storage this keying
    /// yields a coherent walk over the served structure, where recomputed-id
    /// keying stalled the walk into BudgetExceeded with poisoned grounding.
    ///
    /// Each accumulation also drives the walk-time edge checks: the arrival
    /// may complete the in-hand parent set of the event itself (multi-parent
    /// re-encounters) or of children registered as waiting on it.
    pub(crate) fn accumulate(&mut self, id: &EventId, event: &Event) {
        let parents: Vec<EventId> = event.parent.as_slice().to_vec();
        self.dag.insert(id.clone(), parents);
        self.cache.put(id.clone(), event.clone());

        // The edge check is free where the walk already holds the payloads
        // (plan D2-4): check the arrival as a child, then any children whose
        // last missing parent this arrival was.
        self.edge_check(id);
        if let Some(waiters) = self.edge_waiting.remove(id) {
            for child in waiters {
                self.edge_check(&child);
            }
        }
    }

    /// Evaluate gen(child) == 1 + max(gen(parents)) (saturating, the same
    /// arithmetic as the stamp) when child and all parents are in hand; on
    /// violation WARN with a counter and demote the child to per-comparison
    /// ineligibility, then continue (D2-4: a walk never retroactively
    /// rejects committed history; admission is the rejection boundary).
    /// Genesis events carry no edge, so nothing in-walk checks their
    /// absolute claim (that is admission's genesis law; the comparison must
    /// not treat gen 1 as a root signal, GC-GENESIS).
    ///
    /// Opportunistic by design: a payload evicted from the bounded cache
    /// before its family completes simply misses its check. Checks are
    /// keyed to run at most once per child, across budget retries too, and
    /// a blocked child REGISTERS at most once (`edge_pending`): only its
    /// first incomplete check writes `edge_waiting`, under every parent
    /// missing at that moment. A parent in hand then but evicted before the
    /// last registered one arrives is another opportunistic miss.
    fn edge_check(&mut self, child_id: &EventId) {
        if self.edge_checked.contains(child_id) {
            return;
        }
        let Some(parents) = self.dag.get(child_id) else {
            return;
        };
        if parents.is_empty() {
            self.edge_checked.insert(child_id.clone());
            self.edge_pending.remove(child_id);
            return;
        }
        let mut parent_gens = Vec::with_capacity(parents.len());
        let mut missing: Vec<EventId> = Vec::new();
        for parent in parents {
            match self.cache.peek(parent) {
                Some(event) => parent_gens.push(event.generation),
                None => missing.push(parent.clone()),
            }
        }
        if !missing.is_empty() {
            if self.edge_pending.insert(child_id.clone()) {
                for parent in missing {
                    self.edge_waiting.entry(parent).or_default().push(child_id.clone());
                }
            }
            return;
        }
        let Some(child_gen) = self.cache.peek(child_id).map(|e| e.generation) else {
            return; // evicted between accumulation and drain; opportunistic
        };
        let expected = Event::generation_from_parents(parent_gens.into_iter());
        self.stats.edge_checks_evaluated += 1;
        if child_gen != expected {
            tracing::warn!(
                child = %child_id,
                claimed = child_gen,
                expected,
                "walk-time edge check: child generation contradicts its in-hand parents; demoting to ineligible for this comparison"
            );
            self.stats.edge_check_violations += 1;
            if self.demoted.insert(child_id.clone()) {
                self.stats.demotions += 1;
            }
        }
        self.edge_checked.insert(child_id.clone());
        self.edge_pending.remove(child_id);
    }

    /// Get event by id: cache -> retriever (storage).
    /// All events are already in storage (eager storage model).
    ///
    /// A served payload whose recomputed id differs from the requested id is
    /// COUNTED (write-only observability, R1: a counter is permitted, a
    /// verify-and-error is not; a hard error here would both add a per-fetch
    /// hash cost on honest lanes and make the immunity oracle's seam
    /// unimplementable). The recompute rides underlying fetches only; LRU
    /// hits skip it.
    pub(crate) async fn get_event(&mut self, id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(event) = self.cache.get(id) {
            return Ok(event.clone());
        }
        let event = self.event_getter.get_event(id).await?;
        self.stats.events_fetched += 1;
        if event.id() != *id {
            self.stats.id_mismatches += 1;
        }
        self.cache.put(id.clone(), event.clone());
        Ok(event)
    }

    /// Get a reference to the DAG structure.
    pub(crate) fn dag(&self) -> &BTreeMap<EventId, Vec<EventId>> { &self.dag }

    /// The in-hand payload's generation for `id`, if cached. `peek` does not
    /// disturb LRU recency: scheduling reads must not change eviction order.
    pub(crate) fn peek_generation(&self, id: &EventId) -> Option<u32> { self.cache.peek(id).map(|e| e.generation) }

    /// Whether a walk-time edge check demoted `id` for this comparison
    /// (D2-4): a demoted id's value never feeds an acceleration.
    pub(crate) fn is_demoted(&self, id: &EventId) -> bool { self.demoted.contains(id) }

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
    /// and bench evidence, never the walk itself (write-only rule), hence
    /// the not(test) dead-code allowance.
    #[cfg_attr(not(test), allow(dead_code))]
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
    use ankurah_proto::{Clock, EntityId, OperationSet};

    // The helper functions are tested directly below; the accumulator itself
    // is driven through `accumulate` (which never fetches), so the edge-check
    // bookkeeping tests need no real `GetEvents`. EventLayer::compare is
    // tested in `layers`; the fetch paths in `tests`.

    /// `accumulate`-driven tests never fetch; a getter that proves it.
    struct NoFetch;

    #[async_trait::async_trait]
    impl GetEvents for NoFetch {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
            unreachable!("accumulate-driven edge-check tests never fetch (asked for {event_id})")
        }
        async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
    }

    /// Honestly stamped test event (the tests.rs idiom, duplicated locally
    /// because sibling test modules cannot share private helpers).
    fn stamped(seed: u16, parents: &[&Event]) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0..2].copy_from_slice(&seed.to_be_bytes());
        Event {
            entity_id: EntityId::from_bytes(entity_id_bytes),
            collection: "test".into(),
            operations: OperationSet(BTreeMap::new()),
            parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
        }
    }

    /// The walk-time edge checks' blocked-registration bookkeeping must be
    /// bounded by the DAG's EDGE COUNT: a child registers under a missing
    /// parent AT MOST ONCE per comparison, so the total entries across
    /// `edge_waiting` can never exceed the number of parent edges (here 128:
    /// 64 child-of-root edges plus the join's 64 parent edges).
    ///
    /// The arrival order is the backward-BFS order of the production
    /// wide-antichain shape (compare join vs root, e.g. a stale head syncing
    /// across a burst of concurrent writes): the join arrives FIRST, its 64
    /// parents trickle in afterwards, the root last. That order is what makes
    /// duplicate registrations multiply: every arrival that drains a
    /// duplicated waiter list re-checks the join once PER COPY, and every
    /// still-incomplete re-check that re-registers doubles the copies under
    /// every still-missing parent -- 2^width growth, the compare() memory
    /// explosion observed at the event_dag bench's wide_antichain/64 (one
    /// call on this 66-event DAG exceeded 11 GB RSS).
    #[test]
    fn wide_antichain_edge_registrations_are_bounded_by_edge_count() {
        const WIDTH: usize = 64;
        let root = stamped(0, &[]);
        let children: Vec<Event> = (0..WIDTH).map(|i| stamped(i as u16 + 1, &[&root])).collect();
        let join = stamped(WIDTH as u16 + 1, &children.iter().collect::<Vec<_>>());

        // Total parent edges in the DAG: the registration bound.
        let edge_count: usize =
            std::iter::once(&root).chain(children.iter()).chain(std::iter::once(&join)).map(|e| e.parent.as_slice().len()).sum();
        assert_eq!(edge_count, 2 * WIDTH, "shape check: WIDTH child-of-root edges plus WIDTH join-parent edges");

        let mut acc = EventAccumulator::new(NoFetch);
        let arrivals: Vec<&Event> = std::iter::once(&join).chain(children.iter()).chain(std::iter::once(&root)).collect();
        for (arrived, event) in arrivals.iter().enumerate() {
            acc.accumulate(&event.id(), event);
            let total: usize = acc.edge_waiting.values().map(Vec::len).sum();
            assert!(
                total <= edge_count,
                "edge_waiting holds {total} registrations after arrival {} of {} -- the DAG has only {edge_count} edges, \
                 so blocked-check registrations are multiplying instead of registering at most once per child",
                arrived + 1,
                arrivals.len(),
            );
        }

        // Everything arrived and the cache (capacity 1000) evicted nothing:
        // every registration drained and every event completed its check.
        assert!(acc.edge_waiting.is_empty(), "all registered parents arrived; nothing may remain blocked");
        assert_eq!(acc.edge_checked.len(), WIDTH + 2, "root, children, and join all completed their one check");
        assert_eq!(acc.stats.edge_check_violations, 0, "the corpus is honestly stamped");
    }

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
