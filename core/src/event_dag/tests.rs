#![cfg(test)]

use crate::error::RetrievalError;
use crate::event_dag::EventLayer;
use crate::property::backend::lww::LWWBackend;
use crate::property::backend::PropertyBackend;
use crate::retrieval::GetEvents;
use crate::value::Value;

use super::comparison::compare;
use super::relation::AbstractCausalRelation;
use ankurah_proto::{Clock, EntityId, Event, EventId, OperationSet};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ============================================================================
// TEST INFRASTRUCTURE
// ============================================================================

/// Mock event getter implementing `GetEvents` for comparison tests.
/// Stores events by EventId and returns them on demand.
#[derive(Clone)]
struct MockRetriever {
    events: HashMap<EventId, Event>,
}

impl MockRetriever {
    fn new() -> Self { Self { events: HashMap::new() } }

    fn add_event(&mut self, event: &Event) { self.events.insert(event.id(), event.clone()); }
}

#[async_trait]
impl GetEvents for MockRetriever {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.events.get(event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    /// MockRetriever simulates ephemeral storage: events are retrievable via
    /// get_event but not considered durably stored.
    async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
}

/// A retriever that LIES about generations (D2 M5, oracle brief section
/// 1.2): serves a doctored payload under the ORIGINAL requested id. The
/// honest corpus stays untouched in `inner`; `overrides` maps original id
/// to a doctored event that is byte-identical except for the corrupted
/// field(s). This models a wrong value reaching the comparison WITHOUT
/// admission having vetted it: storage corruption in place, a hostile
/// backend, a plumbing bug, or an adopted-history value the admission
/// equation never saw; exactly the domain of the 5b-ii immunity theorem.
///
/// Doctored events are NEVER inserted through MockRetriever::add_event
/// (which keys by the payload's recomputed id, so the lie would mint a
/// fresh id instead of wearing the original one; red-team fold F4 trap).
#[derive(Clone)]
struct GenCorruptedRetriever {
    inner: MockRetriever,
    overrides: HashMap<EventId, Event>,
}

impl GenCorruptedRetriever {
    fn new(inner: MockRetriever) -> Self { Self { inner, overrides: HashMap::new() } }

    /// Serve `doctored` whenever `original_id` is requested.
    fn doctor(&mut self, original_id: EventId, doctored: Event) { self.overrides.insert(original_id, doctored); }
}

#[async_trait]
impl GetEvents for GenCorruptedRetriever {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        if let Some(doctored) = self.overrides.get(event_id) {
            return Ok(doctored.clone());
        }
        self.inner.get_event(event_id).await
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { self.inner.event_stored(event_id).await }
}

/// Create a test event with deterministic content-hashed IDs.
/// The seed differentiates events; the parent EVENTS determine the parent
/// clock, and the generation is stamped `1 + max` of their payload
/// generations (genesis 1). Payload-authoritative by construction: the
/// registry ban (M1 review follow-up ruling, 2026-07-09) forbids any side
/// store of generations, so helpers take the parent events themselves.
fn make_test_event(seed: u8, parents: &[&Event]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0] = seed;
    let entity_id = EntityId::from_bytes(entity_id_bytes);

    Event {
        entity_id,
        collection: "test".into(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
        generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
    }
}

/// Like make_test_event but with a two-byte seed, for tests that need a wide
/// or exhaustive seed space (id-ordering searches, randomized DAG generation).
fn make_test_event_u16(seed: u16, parents: &[&Event]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0..2].copy_from_slice(&seed.to_be_bytes());
    let entity_id = EntityId::from_bytes(entity_id_bytes);
    Event {
        entity_id,
        collection: "test".into(),
        operations: OperationSet(BTreeMap::new()),
        parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
        generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
    }
}

/// Create a Clock from EventIds without consuming them.
macro_rules! clock {
    ($($id:expr),* $(,)?) => {
        Clock::from(vec![$($id.clone()),*])
    };
}

/// Create a test event with LWW operations.
/// Uses a seed to create deterministic but different entity IDs for each event.
fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event { make_lww_event_with_parent(seed, properties, &[]) }

/// Like make_lww_event but with explicit parent events, for multi-layer DAG scenarios.
fn make_lww_event_with_parent(seed: u8, properties: Vec<(&str, &str)>, parents: &[&Event]) -> Event {
    // Use seed for entity_id to get different event hashes
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0] = seed;
    let entity_id = EntityId::from_bytes(entity_id_bytes);

    let backend = LWWBackend::new();
    for (name, value) in properties {
        backend.set(name.into(), Some(Value::String(value.into())));
    }
    let ops = backend.to_operations().unwrap().unwrap();
    Event {
        entity_id,
        collection: "test".into(),
        operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
        generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
    }
}

fn layer_from_refs_with_context(already_applied: &[&Event], to_apply: &[&Event], context_events: &[&Event]) -> EventLayer {
    let mut dag = BTreeMap::new();
    for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
        dag.insert(event.id(), event.parent.as_slice().to_vec());
    }
    EventLayer::new(already_applied.iter().map(|e| (*e).clone()).collect(), to_apply.iter().map(|e| (*e).clone()).collect(), Arc::new(dag))
}

fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
    layer_from_refs_with_context(already_applied, to_apply, &[])
}

// ============================================================================
// BASIC COMPARISON TESTS
// ============================================================================

#[tokio::test]
async fn test_linear_history() {
    let mut retriever = MockRetriever::new();

    // Create a linear chain: ev1 <- ev2 <- ev3
    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let ev2 = make_test_event(2, &[&ev1]);
    retriever.add_event(&ev2);

    let ev3 = make_test_event(3, &[&ev2]);
    let id3 = ev3.id();
    retriever.add_event(&ev3);

    let ancestor = clock!(id1);
    let descendant = clock!(id3);

    // descendant descends from ancestor
    let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // ancestor is strictly before descendant (subject is older)
    let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_concurrent_history() {
    let mut retriever = MockRetriever::new();

    // Example history (arrows represent descendency not reference, which is the reverse direction):
    //      1
    //   ↙  ↓  ↘
    //  2   3   4  - at this point, we have a non-ancestral concurrency (1 is not concurrent)
    //   ↘ ↙ ↘ ↙
    //    5   6    - ancestral concurrency introduced
    //     ↘ ↙
    //      7
    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let ev2 = make_test_event(2, &[&ev1]);
    let id2 = ev2.id();
    retriever.add_event(&ev2);

    let ev3 = make_test_event(3, &[&ev1]);
    let id3 = ev3.id();
    retriever.add_event(&ev3);

    let ev4 = make_test_event(4, &[&ev1]);
    retriever.add_event(&ev4);

    let ev5 = make_test_event(5, &[&ev2, &ev3]);
    let id5 = ev5.id();
    retriever.add_event(&ev5);

    let ev6 = make_test_event(6, &[&ev3, &ev4]);
    let id6 = ev6.id();
    retriever.add_event(&ev6);

    let ev7 = make_test_event(7, &[&ev5, &ev6]);
    let _id7 = ev7.id();
    retriever.add_event(&ev7);

    {
        // concurrency in lineage *between* clocks, but the descendant clock fully descends from the ancestor clock
        let ancestor = clock!(id1);
        let descendant = clock!(id5);
        let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        // ancestor is strictly before descendant
        let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
    }
    {
        // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
        let ancestor = clock!(id2, id3);
        let descendant = clock!(id5);

        let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        // ancestor is strictly before descendant
        let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
    }

    {
        // a and b are fully concurrent, but still comparable
        let a = clock!(id2);
        let b = clock!(id3);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id1.clone()]
        ));
        let result = compare(retriever.clone(), &b, &a, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id1.clone()]
        ));
    }

    {
        // a partially descends from b, but b has a component that is not in a
        let a = clock!(id6);
        let b = clock!(id2, id3);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id3.clone()]
        ));
    }
}

#[tokio::test]
async fn test_incomparable() {
    let mut retriever = MockRetriever::new();

    //   1        6
    //   ↓  ↘     ↓
    //   2   4    7
    //   ↓   ↓    ↓
    //   3   5    8
    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let ev2 = make_test_event(2, &[&ev1]);
    let id2 = ev2.id();
    retriever.add_event(&ev2);

    let ev3 = make_test_event(3, &[&ev2]);
    let id3 = ev3.id();
    retriever.add_event(&ev3);

    let ev4 = make_test_event(4, &[&ev1]);
    let _id4 = ev4.id();
    retriever.add_event(&ev4);

    let ev5 = make_test_event(5, &[&ev4]);
    let id5 = ev5.id();
    retriever.add_event(&ev5);

    // 6 is an unrelated root event
    let ev6 = make_test_event(6, &[]);
    let id6 = ev6.id();
    retriever.add_event(&ev6);

    let ev7 = make_test_event(7, &[&ev6]);
    retriever.add_event(&ev7);

    let ev8 = make_test_event(8, &[&ev7]);
    let id8 = ev8.id();
    retriever.add_event(&ev8);

    {
        // fully incomparable (different roots) - properly returns Disjoint
        let a = clock!(id3);
        let b = clock!(id8);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
                if *subject_root == id1 && *other_root == id6
        ));
    }
    {
        // fully incomparable (just a different tier) - properly returns Disjoint
        let a = clock!(id2);
        let b = clock!(id8);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
                if *subject_root == id1 && *other_root == id6
        ));
    }
    {
        // partially incomparable: [3] shares root 1 with [5], but [8] has different root 6
        let a = clock!(id3);
        let b = clock!(id5, id8);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
        ));
    }
}

#[tokio::test]
async fn test_empty_clocks() {
    let mut retriever = MockRetriever::new();

    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let empty = Clock::default();
    let non_empty = clock!(id1);

    // Two empty clocks are the same clock: Equal, not diverged.
    let result = compare(retriever.clone(), &empty, &empty, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);

    // One-sided empty clocks share no history: diverged with an empty meet.
    let expected =
        AbstractCausalRelation::DivergedSince { meet: vec![], subject: vec![], other: vec![], subject_chain: vec![], other_chain: vec![] };
    let result = compare(retriever.clone(), &non_empty, &empty, 100).await.unwrap();
    assert_eq!(result.relation, expected);
    let result = compare(retriever.clone(), &empty, &non_empty, 100).await.unwrap();
    assert_eq!(result.relation, expected);
}

#[tokio::test]
async fn test_budget_exceeded() {
    let mut retriever = MockRetriever::new();

    // Create a long chain: ev1 <- ev2 <- ... <- ev20
    // With budget=1, max_budget=4, a chain of 20 events will still exceed
    let mut events: Vec<Event> = Vec::new();
    for i in 0..20u8 {
        let parents: Vec<&Event> = if i == 0 { vec![] } else { vec![&events[i as usize - 1]] };
        let ev = make_test_event(i + 1, &parents);
        retriever.add_event(&ev);
        events.push(ev);
    }

    let ancestor = clock!(events[0].id());
    let descendant = clock!(events[19].id());

    // With budget=1 (escalates to max 4), a 20-long chain will exceed
    let result = compare(retriever.clone(), &descendant, &ancestor, 1).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::BudgetExceeded { .. }));

    // With high budget, should resolve
    let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // ancestor is strictly before descendant - determined quickly because
    // expanding descendant immediately walks backward toward ancestor
    let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_self_comparison() {
    let mut retriever = MockRetriever::new();

    // Create a simple event to compare with itself
    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let clock = clock!(id1);

    // A clock does NOT descend itself
    let result = compare(retriever.clone(), &clock, &clock, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);
}

#[tokio::test]
async fn multiple_roots() {
    let mut retriever = MockRetriever::new();

    // Six independent roots
    let mut roots: Vec<Event> = Vec::new();
    let mut root_ids = Vec::new();
    for i in 1..=6u8 {
        let ev = make_test_event(i, &[]);
        root_ids.push(ev.id());
        retriever.add_event(&ev);
        roots.push(ev);
    }

    // merge-point 7 references all six heads
    let ev7 = make_test_event(7, &roots.iter().collect::<Vec<_>>());
    retriever.add_event(&ev7);

    // subject head 8 descends only from 7
    let ev8 = make_test_event(8, &[&ev7]);
    let id8 = ev8.id();
    retriever.add_event(&ev8);

    let subject = clock!(id8);
    let big_other = Clock::from(root_ids.clone());

    // 8 descends from all heads in big_other via 7
    let result = compare(retriever.clone(), &subject, &big_other, 1_000).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // In the opposite direction, big_other is strictly before subject
    let result = compare(retriever.clone(), &big_other, &subject, 1_000).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_compare_event_unstored() {
    let mut retriever = MockRetriever::new();

    // Create a chain: ev1 <- ev2 <- ev3 (stored)
    let ev1 = make_test_event(1, &[]);
    let id1 = ev1.id();
    retriever.add_event(&ev1);

    let ev2 = make_test_event(2, &[&ev1]);
    let id2 = ev2.id();
    retriever.add_event(&ev2);

    let ev3 = make_test_event(3, &[&ev2]);
    let id3 = ev3.id();
    retriever.add_event(&ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(4, &[&ev3]);

    let clock_1 = clock!(id1);
    let clock_2 = clock!(id2);
    let clock_3 = clock!(id3);

    // Stage the unstored event so compare can find it
    retriever.add_event(&unstored_event);

    // The unstored event should descend from all ancestors
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_2, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an unstored event that has multiple parents
    let unstored_merge_event = make_test_event(5, &[&ev2, &ev3]);
    retriever.add_event(&unstored_merge_event);

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_merge_event.id()]), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an incomparable case - different roots should return Disjoint
    let ev10 = make_test_event(10, &[]); // Independent root
    let id10 = ev10.id();
    retriever.add_event(&ev10);
    let incomparable_clock = clock!(id10);

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &incomparable_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
            if *subject_root == id1 && *other_root == id10
    ));

    // Test root event case
    let root_event = make_test_event(11, &[]);
    retriever.add_event(&root_event);

    let empty_clock = Clock::default();
    let result = compare(retriever.clone(), &Clock::from(vec![root_event.id()]), &empty_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
    ));

    let result = compare(retriever.clone(), &Clock::from(vec![root_event.id()]), &clock_1, 100).await.unwrap();
    // Two independent root events with different lineages should be Disjoint
    assert!(matches!(result.relation, AbstractCausalRelation::Disjoint { .. }));

    // Test that a non-empty unstored event does not descend from an empty clock
    let empty_clock = Clock::default();
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &empty_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
    ));
}

#[tokio::test]
async fn test_compare_event_redundant_delivery() {
    let mut retriever = MockRetriever::new();

    // Create a chain: ev1 <- ev2 <- ev3 (stored)
    let ev1 = make_test_event(1, &[]);
    retriever.add_event(&ev1);

    let ev2 = make_test_event(2, &[&ev1]);
    retriever.add_event(&ev2);

    let ev3 = make_test_event(3, &[&ev2]);
    let id3 = ev3.id();
    retriever.add_event(&ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(4, &[&ev3]);
    let id4 = unstored_event.id();

    // Stage the unstored event so compare can find it
    retriever.add_event(&unstored_event);

    // Test the normal case first
    let clock_3 = clock!(id3);
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Now store event 4 to simulate it being applied
    let ev4_stored = make_test_event(4, &[&ev3]);
    retriever.add_event(&ev4_stored);

    // Test redundant delivery: the event is already in the clock (exact match)
    let clock_with_event = clock!(id4);
    // The equality check should catch this case and return Equal
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_with_event, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);

    // Test case where the event is in the clock but with other events too
    let clock_with_multiple = clock!(id3, id4);
    // Event 4 is already in the head [3, 4], so this is redundant delivery.
    // Since id4 descends from id3, comparing [id4] vs [id3, id4] may return
    // StrictDescends (id4 covers all of [id3, id4]) rather than Equal.
    // Either way, it's NOT DivergedSince, so the caller knows it's a no-op.
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_with_multiple, 100).await.unwrap();
    assert!(
        matches!(result.relation, AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. }),
        "Redundant delivery with multiple heads should return Equal or StrictDescends, got {:?}",
        result.relation
    );
}

// ============================================================================
// MISSING EVENT BUSYLOOP TESTS
// ============================================================================

/// Verifies that compare() returns an error when a frontier event can't be fetched.
///
/// Setup: A (creation, in retriever) -> B (child of A, NOT in retriever) -> C (child of B, in retriever).
/// compare(subject=[C], comparison=[A]) starts BFS. It fetches C (ok, parents=[B]),
/// adds B to subject frontier. Tries to fetch B -> EventNotFound -> error.
///
/// Previously this was a busyloop because EventNotFound did `continue`, skipping
/// process_event (the only place that removes IDs from frontiers). Now it errors.
#[tokio::test]
async fn test_missing_event_busyloop() {
    let mut retriever = MockRetriever::new();

    // A: creation event, in retriever
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(&ev_a);

    // B: child of A, deliberately NOT added to retriever (missing event)
    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    // Do NOT add ev_b to retriever

    // C: child of B, in retriever
    let ev_c = make_test_event(3, &[&ev_b]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    // compare(subject=[C], comparison=[A]):
    // BFS fetches C (ok), adds parent B to subject frontier.
    // BFS fetches A (ok, it's comparison frontier).
    // BFS tries to fetch B -> EventNotFound -> returns error
    let result = compare(retriever, &clock!(id_c), &clock!(id_a), 100).await;

    let err = result.err().expect("Expected EventNotFound error for missing event B, but got Ok");
    assert!(
        matches!(err, RetrievalError::EventNotFound(ref id) if *id == id_b),
        "Error should be EventNotFound for event B, got {:?}",
        err
    );
}

/// Test that an unfetchable event on BOTH frontiers is handled as a common
/// ancestor without fetching, returning StrictDescends instead of an error.
///
/// DAG:  A (creation, NOT in retriever) → B (in retriever)
/// compare(subject=[B], comparison=[A])
///
/// Step 1 (quick-check): fetches B, sees parent=[A]. A == comparison head,
///   so quick-check returns StrictDescends immediately.
///
/// But we also test the BFS path by using a two-step chain so the quick-check
/// doesn't short-circuit:
///   A (NOT in retriever) → B → C (both in retriever)
///   compare(subject=[C], comparison=[A])
///
/// Quick-check: fetches C, parent=[B]. B != A, so quick-check doesn't fire.
/// BFS step 1: fetches C (subject frontier), processes it, adds B to subject frontier.
///   Fetches/processes A (comparison frontier) — but A is unfetchable!
///   However, A is only on comparison frontier here, not subject. So we need
///   a scenario where A ends up on BOTH frontiers.
///
/// Better scenario: A (NOT in retriever) → B (in retriever)
///   compare(subject=[B], comparison=[A])
///   Quick-check fetches B, parent=[A]. comparison_set={A}, all_parents={A}.
///   comparison_set ⊆ all_parents → returns StrictDescends. Never reaches BFS.
///
/// To actually exercise BFS both-frontiers: use a diamond.
///   A (NOT in retriever) → B, A → C (B and C in retriever)
///   compare(subject=[B], comparison=[C])
///   Quick-check: fetches B, parent=[A]. comparison_set={C}. A != C. No shortcut.
///   BFS: subject_frontier={B}, comparison_frontier={C}
///   Step 1: fetch B (ok), parents=[A], add A to subject_frontier.
///           fetch C (ok), parents=[A], add A to comparison_frontier.
///   Step 2: A is on BOTH frontiers. Process with empty parents. Common ancestor found.
///   Result: DivergedSince { meet=[A] }
#[tokio::test]
async fn test_both_frontiers_unfetchable_meet_point() {
    let mut retriever = MockRetriever::new();

    // A: creation event, deliberately NOT added to retriever
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    // Do NOT add ev_a to retriever

    // B: child of A, in retriever
    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    // C: child of A, in retriever
    let ev_c = make_test_event(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    // compare(subject=[B], comparison=[C])
    // BFS will walk B and C back to A, which is on both frontiers but unfetchable.
    // The both-frontiers optimization should process A as common ancestor with empty parents.
    let result = compare(retriever.clone(), &clock!(id_b), &clock!(id_c), 100).await;

    let result = result.expect("Should succeed because A is on both frontiers and processed as common ancestor");
    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id_a.clone()]),
        "Should find meet at A (the unfetchable common ancestor), got {:?}",
        result.relation
    );
}

// ============================================================================
// MULTI-HEAD BUG FIX TESTS (Phase 2A)
// ============================================================================

#[tokio::test]
async fn test_multihead_event_extends_one_tip() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C   <- entity head is [B, C]
    //       |
    //       D   <- incoming event with parent [C]
    let ev_a = make_test_event(1, &[]);
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    // Event D extends C
    let event_d = make_test_event(4, &[&ev_c]);

    // Entity head is [B, C]
    let entity_head = clock!(id_b, id_c);

    // Stage event D so compare can find it
    retriever.add_event(&event_d);

    // D should NOT return StrictAscends - it should be DivergedSince
    // because D extends C which is a tip, and is concurrent with B
    let result = compare(retriever.clone(), &Clock::from(vec![event_d.id()]), &entity_head, 100).await.unwrap();

    // The meet should be [C]; B is concurrent (not a child of meet C) so
    // it appears in other_chain rather than the immediate-children `other` field.
    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    // Verify the meet field
    if let AbstractCausalRelation::DivergedSince { meet, .. } = &result.relation {
        assert_eq!(meet, &vec![id_c], "Meet should be [C]");
    }
}

#[tokio::test]
async fn test_multihead_event_extends_multiple_tips() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C   <- entity head is [B, C]
    //    \ /
    //     D   <- incoming event with parent [B, C]
    let ev_a = make_test_event(1, &[]);
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    // Event D merges both tips
    let event_d = make_test_event(4, &[&ev_b, &ev_c]);

    // Entity head is [B, C]
    let entity_head = clock!(id_b, id_c);

    // Stage event D so compare can find it
    retriever.add_event(&event_d);

    // D's parent equals entity head, so this should be StrictDescends
    let result = compare(retriever.clone(), &Clock::from(vec![event_d.id()]), &entity_head, 100).await.unwrap();

    assert!(
        matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
        "Expected StrictDescends when event merges all tips, got {:?}",
        result.relation
    );
}

#[tokio::test]
async fn test_multihead_three_way_concurrency() {
    let mut retriever = MockRetriever::new();

    // Create:
    //       A
    //      /|\
    //     B C D   <- entity head is [B, C, D]
    //     |
    //     E       <- incoming event with parent [B]
    let ev_a = make_test_event(1, &[]);
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_a]);
    let id_d = ev_d.id();
    retriever.add_event(&ev_d);

    // Event E extends only B
    let event_e = make_test_event(5, &[&ev_b]);

    // Entity head is [B, C, D]
    let entity_head = clock!(id_b, id_c, id_d);

    // Stage event E so compare can find it
    retriever.add_event(&event_e);

    let result = compare(retriever.clone(), &Clock::from(vec![event_e.id()]), &entity_head, 100).await.unwrap();

    // Should be DivergedSince because E extends B but is concurrent with C and D
    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, .. } = &result.relation {
        assert_eq!(meet, &vec![id_b], "Meet should be [B]");
        // C and D are siblings of B (children of A), not children of meet B,
        // so they appear in other_chain rather than the immediate `other` field.
    }
}

// ============================================================================
// DEEP CONCURRENCY TESTS (Phase 2G)
// ============================================================================

#[tokio::test]
async fn test_deep_diamond_asymmetric_branches() {
    let mut retriever = MockRetriever::new();

    // Create asymmetric diamond:
    //       A
    //      / \
    //     B   C
    //     |   |
    //     D   E
    //     |   |
    //     F   G
    //     |   |
    //     H   I
    //
    // Compare H vs I - meet should be A
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_a]);
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_b]);
    retriever.add_event(&ev_d);

    let ev_e = make_test_event(5, &[&ev_c]);
    retriever.add_event(&ev_e);

    let ev_f = make_test_event(6, &[&ev_d]);
    retriever.add_event(&ev_f);

    let ev_g = make_test_event(7, &[&ev_e]);
    retriever.add_event(&ev_g);

    let ev_h = make_test_event(8, &[&ev_f]);
    let id_h = ev_h.id();
    retriever.add_event(&ev_h);

    let ev_i = make_test_event(9, &[&ev_g]);
    let id_i = ev_i.id();
    retriever.add_event(&ev_i);

    let clock_h = clock!(id_h);
    let clock_i = clock!(id_i);

    let result = compare(retriever.clone(), &clock_h, &clock_i, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A");
        // subject_chain should go from A to H: B, D, F, H
        assert_eq!(subject_chain.len(), 4, "Subject chain should have 4 events");
        // other_chain should go from A to I: C, E, G, I
        assert_eq!(other_chain.len(), 4, "Other chain should have 4 events");
    }
}

#[tokio::test]
async fn test_short_branch_from_deep_point() {
    let mut retriever = MockRetriever::new();

    // Create:
    // A -> B -> C -> D -> E -> F -> G -> H (long chain, head)
    //             |
    //             X -> Y (short branch from D, arrives late)
    //
    // Compare Y vs H - meet should be D, NOT genesis A!
    let ev_a = make_test_event(1, &[]);
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_b]);
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_c]);
    retriever.add_event(&ev_d);

    let ev_e = make_test_event(5, &[&ev_d]);
    retriever.add_event(&ev_e);

    let ev_f = make_test_event(6, &[&ev_e]);
    retriever.add_event(&ev_f);

    let ev_g = make_test_event(7, &[&ev_f]);
    retriever.add_event(&ev_g);

    let ev_h = make_test_event(8, &[&ev_g]);
    let id_h = ev_h.id();
    retriever.add_event(&ev_h);

    // Short branch from D
    let ev_x = make_test_event(9, &[&ev_d]); // X (parent is D)
    retriever.add_event(&ev_x);

    let ev_y = make_test_event(10, &[&ev_x]); // Y
    let _id_y = ev_y.id();

    let clock_h = clock!(id_h);

    // Stage event Y so compare can find it
    retriever.add_event(&ev_y);

    // Event Y arrives late, with parent X
    let result = compare(retriever.clone(), &Clock::from(vec![ev_y.id()]), &clock_h, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        // Meet should not be empty
        assert!(!meet.is_empty(), "Meet should not be empty");
        // Subject chain: just Y (since we're comparing unstored event)
        assert!(!subject_chain.is_empty(), "Subject chain should not be empty");
        // Other chain: events from meet to head H
        assert!(!other_chain.is_empty(), "Other chain should not be empty");
    }
}

#[tokio::test]
async fn test_late_arrival_long_branch_from_genesis() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A (genesis)
    //    / \
    //   B   X (long branch arrives late)
    //   |   |
    //   C   Y
    //   |   |
    //   D   Z
    //
    // Entity at [D], then X->Y->Z branch arrives
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_b]);
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_c]);
    let id_d = ev_d.id();
    retriever.add_event(&ev_d);

    // Long branch from genesis
    let ev_x = make_test_event(5, &[&ev_a]);
    retriever.add_event(&ev_x);

    let ev_y = make_test_event(6, &[&ev_x]);
    retriever.add_event(&ev_y);

    let ev_z = make_test_event(7, &[&ev_y]);
    let id_z = ev_z.id();
    retriever.add_event(&ev_z);

    let clock_d = clock!(id_d);
    let clock_z = clock!(id_z);

    let result = compare(retriever.clone(), &clock_d, &clock_z, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A (genesis)");
        // Both chains should have 3 events
        assert_eq!(subject_chain.len(), 3, "Subject chain B, C, D should have 3 events");
        assert_eq!(other_chain.len(), 3, "Other chain X, Y, Z should have 3 events");
    }
}

// ============================================================================
// CHAIN ORDERING TESTS (Phase 2G)
// ============================================================================

#[tokio::test]
async fn test_forward_chain_ordering() {
    let mut retriever = MockRetriever::new();

    // Create: A -> B -> C -> D -> E
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_b]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_c]);
    let id_d = ev_d.id();
    retriever.add_event(&ev_d);

    let ev_e = make_test_event(5, &[&ev_d]);
    let id_e = ev_e.id();
    retriever.add_event(&ev_e);

    let clock_a = clock!(id_a);
    let clock_e = clock!(id_e);

    let result = compare(retriever.clone(), &clock_e, &clock_a, 100).await.unwrap();

    // E strictly descends from A
    if let AbstractCausalRelation::StrictDescends { chain } = &result.relation {
        // Chain should be in forward (causal) order: B, C, D, E (from A toward E)
        assert_eq!(chain.len(), 4, "Chain should have 4 events");
        assert_eq!(chain, &vec![id_b, id_c, id_d, id_e], "Chain should be [B, C, D, E] in causal order");
    } else {
        panic!("Expected StrictDescends, got {:?}", result.relation);
    }
}

#[tokio::test]
async fn test_diverged_chains_ordering() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C
    //   |   |
    //   D   E
    //
    // Compare D vs E - both chains should be in forward order from meet A
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(&ev_a);

    let ev_b = make_test_event(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(&ev_b);

    let ev_c = make_test_event(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(&ev_c);

    let ev_d = make_test_event(4, &[&ev_b]);
    let id_d = ev_d.id();
    retriever.add_event(&ev_d);

    let ev_e = make_test_event(5, &[&ev_c]);
    let id_e = ev_e.id();
    retriever.add_event(&ev_e);

    let clock_d = clock!(id_d);
    let clock_e = clock!(id_e);

    let result = compare(retriever.clone(), &clock_d, &clock_e, 100).await.unwrap();

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A");
        // Subject chain: B, D (from A toward D)
        assert_eq!(subject_chain, &vec![id_b, id_d], "Subject chain should be [B, D]");
        // Other chain: C, E (from A toward E)
        assert_eq!(other_chain, &vec![id_c, id_e], "Other chain should be [C, E]");
    } else {
        panic!("Expected DivergedSince, got {:?}", result.relation);
    }
}

// ============================================================================
// IDEMPOTENCY TESTS (Phase 2G)
// ============================================================================

// ============================================================================
// LWW LAYER APPLICATION TESTS (Phase 3)
// ============================================================================

#[cfg(test)]
mod lww_layer_tests {
    use super::*;

    #[test]
    fn test_apply_layer_higher_event_id_wins() {
        let backend = LWWBackend::new();

        // Create two concurrent events with same property but different seeds
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID (computed from content hash)
        let (winner_value, _loser_value) = if event_a.id() > event_b.id() { ("value_a", "value_b") } else { ("value_b", "value_a") };

        // Apply layer where both events are in to_apply
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Higher event_id should win
        assert_eq!(backend.get(&"x".into()), Some(Value::String(winner_value.into())));
    }

    #[test]
    fn test_apply_layer_already_applied_wins() {
        let backend = LWWBackend::new();

        // Create two concurrent events
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID
        let (winner_event, loser_event, _winner_value) =
            if event_a.id() > event_b.id() { (&event_a, &event_b, "value_a") } else { (&event_b, &event_a, "value_b") };

        // Put winner in already_applied, loser in to_apply
        let already_applied: Vec<&Event> = vec![winner_event];
        let to_apply: Vec<&Event> = vec![loser_event];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Nothing should be set because winner is in already_applied
        // (already_applied values are already in state, we don't mutate for them)
        assert_eq!(backend.get(&"x".into()), None);
    }

    #[test]
    fn test_apply_layer_to_apply_overwrites_already_applied() {
        let backend = LWWBackend::new();

        // Create two concurrent events
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID
        let (winner_event, loser_event, winner_value) =
            if event_a.id() > event_b.id() { (&event_a, &event_b, "value_a") } else { (&event_b, &event_a, "value_b") };

        // Put loser in already_applied, winner in to_apply
        let already_applied: Vec<&Event> = vec![loser_event];
        let to_apply: Vec<&Event> = vec![winner_event];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // to_apply event should win and be applied
        assert_eq!(backend.get(&"x".into()), Some(Value::String(winner_value.into())));
    }

    #[test]
    fn test_apply_layer_multiple_properties() {
        let backend = LWWBackend::new();

        // Event A sets x only
        let event_a = make_lww_event(1, vec![("x", "x_from_a")]);
        // Event B sets x and y
        let event_b = make_lww_event(2, vec![("x", "x_from_b"), ("y", "y_from_b")]);

        // Apply layer with both in to_apply
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // x: winner is determined by higher event ID
        let expected_x = if event_a.id() > event_b.id() { "x_from_a" } else { "x_from_b" };
        assert_eq!(backend.get(&"x".into()), Some(Value::String(expected_x.into())));

        // y: only event_b sets it, so it wins
        assert_eq!(backend.get(&"y".into()), Some(Value::String("y_from_b".into())));
    }

    #[test]
    fn test_apply_layer_three_way_concurrency() {
        let backend = LWWBackend::new();

        // Three concurrent events all setting same property
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);
        let event_c = make_lww_event(3, vec![("x", "value_c")]);

        // Find which event has highest ID
        let events = vec![(&event_a, "value_a"), (&event_b, "value_b"), (&event_c, "value_c")];
        let (winner_event, winner_value) = events.iter().max_by_key(|(e, _)| e.id()).unwrap();

        // Put a different event in already_applied
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_b, &event_c];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Winner should be set (if winner is in to_apply) or nothing (if winner is in already_applied)
        if winner_event.id() == event_a.id() {
            // Winner was in already_applied - nothing should be set
            // Actually no, the other events should compete and the highest among to_apply wins
            // Let's verify: winner among all is in already_applied, so among to_apply, the higher one wins
            let _to_apply_winner_value = if event_b.id() > event_c.id() { "value_b" } else { "value_c" };
            assert_eq!(backend.get(&"x".into()), None);
        } else {
            // Winner is in to_apply - that value should win
            assert_eq!(backend.get(&"x".into()), Some(Value::String((*winner_value).into())));
        }
    }

    /// H1 verification: cross-layer handling of winners drawn from already_applied events.
    ///
    /// DAG (M = meet, layers as EventLayers yields them for head=[A], incoming=B):
    ///   M -- A            (local branch, sets x, in head ancestry => already_applied, layer 1)
    ///   M -- X -- B       (remote branch, B sets x => to_apply; X layer 1, B layer 2)
    /// A and B are concurrent; the test arranges A.id > B.id, so correct LWW
    /// resolution says A's value must win on every replica.
    ///
    /// apply_layer only persists winners with from_to_apply=true (lww.rs), so a
    /// winner drawn from already_applied is never written back to self.values.
    /// This test pins down both halves of the question:
    ///
    /// 1. REACHABLE state (invariant): by the time A is in head ancestry, entity.rs
    ///    has already applied A's operations (apply_operations_from_event on create/
    ///    StrictDescends/commit, or as a to_apply winner in an earlier merge), so the
    ///    stored entry for x is A (or something that beat A). Re-seeding each layer
    ///    from stored state therefore resolves correctly without persisting
    ///    already_applied winners. Verified for both the local replica and a remote
    ///    replica that receives A fresh -- they converge on A's value.
    ///
    /// 2. UNREACHABLE-state mechanics (the H1 hazard): if the stored entry for x were
    ///    stamped with a below-meet event Z while A sat only in already_applied, then
    ///    layer 1 resolves A as the winner for x but does NOT persist it, layer 2
    ///    re-seeds from stale Z (older_than_meet), and B auto-wins: wrong final value
    ///    AND wrong stored event_id. The assertions document that mechanical hazard;
    ///    entity.rs cannot produce this precondition because head only advances after
    ///    the event's operations are applied to backend state.
    #[test]
    fn test_cross_layer_already_applied_winner_persistence() {
        // Below-meet event Z: last writer of x before the meet. Deliberately NOT
        // included in the layers' DAG context, modeling the ephemeral cut where
        // traversal terminates at an unfetchable common ancestor. (The exhaustive
        // durable path DOES accumulate below the meet; there Z would be in the DAG
        // and layer 2's stale re-seed would lose to B via Descends rather than
        // older_than_meet -- same outcome, same unreachable precondition.)
        let event_z = make_lww_event(9, vec![("x", "value_from_Z")]);

        let meet = make_test_event(50, &[]);
        let remote_mid = make_test_event(51, &[&meet]); // X: remote event, no lww ops
        let event_b = make_lww_event_with_parent(2, vec![("x", "value_from_B")], &[&remote_mid]);
        // Local-branch event A, concurrent with B, with A.id > B.id (search over seeds).
        let event_a = (0u8..=255)
            .map(|seed| make_lww_event_with_parent(seed, vec![("x", "value_from_A")], &[&meet]))
            .find(|a| a.id() > event_b.id())
            .expect("some seed yields A.id > B.id");

        let lww_key = "lww".to_string();
        let a_ops = event_a.operations.get(&lww_key).unwrap();
        let z_ops = event_z.operations.get(&lww_key).unwrap();

        // All layers share the same accumulated DAG {M, A, X, B}; Z is absent from it.
        let layer1 = || layer_from_refs_with_context(&[&event_a], &[&remote_mid], &[&meet, &event_b]);
        let layer1_a_fresh = || layer_from_refs_with_context(&[], &[&event_a, &remote_mid], &[&meet, &event_b]);
        let layer2 = || layer_from_refs_with_context(&[], &[&event_b], &[&meet, &event_a, &remote_mid]);

        // (1a) REACHABLE local replica: stored x already reflects A, because A's
        // operations were applied when A entered head ancestry.
        let backend_local = LWWBackend::new();
        backend_local.apply_operations_with_event(a_ops, event_a.id()).unwrap();
        backend_local.apply_layer(&layer1()).unwrap();
        backend_local.apply_layer(&layer2()).unwrap();
        assert_eq!(
            backend_local.get(&"x".into()),
            Some(Value::String("value_from_A".into())),
            "reachable state: stored entry reflects already_applied A, so A must survive B"
        );

        // (1b) Convergence control: a replica that receives A fresh (to_apply) agrees.
        let backend_remote = LWWBackend::new();
        backend_remote.apply_operations_with_event(z_ops, event_z.id()).unwrap();
        backend_remote.apply_layer(&layer1_a_fresh()).unwrap();
        backend_remote.apply_layer(&layer2()).unwrap();
        assert_eq!(
            backend_remote.get(&"x".into()),
            Some(Value::String("value_from_A".into())),
            "replica receiving A via to_apply must converge to A's value"
        );

        // (2) UNREACHABLE-state mechanics: stored x stamped with below-meet Z while A
        // is only in already_applied. Layer 1's winner (A) is not persisted...
        let backend_stale = LWWBackend::new();
        backend_stale.apply_operations_with_event(z_ops, event_z.id()).unwrap();
        backend_stale.apply_layer(&layer1()).unwrap();
        assert_eq!(
            backend_stale.get_event_id(&"x".into()),
            Some(event_z.id()),
            "already_applied winner A is not written back; stored entry still stamped with Z"
        );
        // ...so layer 2 re-seeds from stale Z (older_than_meet) and B auto-wins,
        // even though A.id > B.id says A should win. This is the H1 hazard, shown
        // here to be purely mechanical: the precondition is unreachable via entity.rs.
        backend_stale.apply_layer(&layer2()).unwrap();
        assert_eq!(
            backend_stale.get(&"x".into()),
            Some(Value::String("value_from_B".into())),
            "stale-seed mechanics: with an (entity-unreachable) below-meet stored entry, B beats A"
        );
        assert_eq!(backend_stale.get_event_id(&"x".into()), Some(event_b.id()));
    }
}

// ============================================================================
// YRS LAYER APPLICATION TESTS (Phase 3e - Category 2)
// ============================================================================

#[cfg(test)]
mod yrs_layer_tests {
    use super::*;
    use crate::property::backend::yrs::YrsBackend;

    /// Create a test event with Yrs text operations.
    /// Each text operation inserts the given string at position 0.
    fn make_yrs_event(seed: u8, text_field: &str, insert_text: &str) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = seed;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        let backend = YrsBackend::new();
        backend.insert(text_field, 0, insert_text).unwrap();
        let ops = backend.to_operations().unwrap().unwrap();

        Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::from([("yrs".to_string(), ops)])),
            parent: Clock::default(),
            generation: 1, // genesis: no parents (266-A)
        }
    }

    #[test]
    fn test_yrs_apply_layer_concurrent_inserts() {
        let backend = YrsBackend::new();

        // Two concurrent events inserting at position 0
        let event_a = make_yrs_event(1, "text", "hello");
        let event_b = make_yrs_event(2, "text", "world");

        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Both inserts should be applied (Yrs CRDT merges them)
        let result = backend.get_string("text").unwrap();
        // Order depends on Yrs internal logic, but both should be present
        assert!(result.contains("hello") || result.contains("world"), "Expected at least one insert to be present, got: {}", result);
    }

    #[test]
    fn test_yrs_apply_layer_ignores_already_applied() {
        let backend = YrsBackend::new();

        // Apply event_a first
        let event_a = make_yrs_event(1, "text", "hello");
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a];
        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        let initial_text = backend.get_string("text").unwrap();
        assert_eq!(initial_text, "hello");

        // Now apply with event_a in already_applied and event_b in to_apply
        let event_b = make_yrs_event(2, "text", "world");
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_b];
        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Only event_b should be applied again (if it wasn't already)
        // The text should contain "world" from event_b
        let final_text = backend.get_string("text").unwrap();
        assert!(final_text.contains("world"), "Expected 'world' to be in text, got: {}", final_text);
    }

    #[test]
    fn test_yrs_apply_layer_order_independent() {
        // Apply events in different orders, verify same result
        let event_a = make_yrs_event(1, "text", "A");
        let event_b = make_yrs_event(2, "text", "B");
        let event_c = make_yrs_event(3, "text", "C");

        // Order 1: A, B, C
        let backend1 = YrsBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        let result1 = backend1.get_string("text").unwrap();

        // Order 2: C, A, B
        let backend2 = YrsBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        let result2 = backend2.get_string("text").unwrap();

        // Order 3: B, C, A
        let backend3 = YrsBackend::new();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        let result3 = backend3.get_string("text").unwrap();

        // All results should be identical (CRDT convergence)
        assert_eq!(result1, result2, "Order 1 vs Order 2 should produce same result");
        assert_eq!(result2, result3, "Order 2 vs Order 3 should produce same result");
    }

    #[test]
    fn test_yrs_apply_layer_empty_to_apply() {
        let backend = YrsBackend::new();
        backend.insert("text", 0, "initial").unwrap();

        let event_a = make_yrs_event(1, "text", "hello");

        // Apply with event_a in already_applied but nothing in to_apply
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Text should be unchanged (only "initial")
        let result = backend.get_string("text").unwrap();
        assert_eq!(result, "initial");
    }
}

// ============================================================================
// DETERMINISM VERIFICATION TESTS (Phase 3e - Category 5)
// ============================================================================

#[cfg(test)]
mod determinism_tests {
    use super::*;

    #[test]
    fn test_two_event_determinism() {
        // Create two concurrent events setting same property
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Order 1: Apply A then B
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b])).unwrap();
        let result1 = backend1.get(&"x".into());

        // Order 2: Apply B then A (as a single layer - order within layer shouldn't matter)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b, &event_a])).unwrap();
        let result2 = backend2.get(&"x".into());

        // Both should produce same result (lexicographic winner)
        assert_eq!(result1, result2, "Different orderings should produce same result");
    }

    #[test]
    fn test_three_event_determinism() {
        let event_a = make_lww_event(1, vec![("x", "A")]);
        let event_b = make_lww_event(2, vec![("x", "B")]);
        let event_c = make_lww_event(3, vec![("x", "C")]);

        // All permutations of applying the three events
        let permutations: Vec<Vec<&Event>> = vec![
            vec![&event_a, &event_b, &event_c],
            vec![&event_a, &event_c, &event_b],
            vec![&event_b, &event_a, &event_c],
            vec![&event_b, &event_c, &event_a],
            vec![&event_c, &event_a, &event_b],
            vec![&event_c, &event_b, &event_a],
        ];

        let mut results = Vec::new();
        for perm in &permutations {
            let backend = LWWBackend::new();
            backend.apply_layer(&layer_from_refs(&[], perm)).unwrap();
            results.push(backend.get(&"x".into()));
        }

        // All results should be identical
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result, &results[0], "Permutation {} produced different result than permutation 0", i);
        }
    }

    #[test]
    fn test_multi_property_determinism() {
        // Events setting different properties
        let event_a = make_lww_event(1, vec![("x", "x_from_a"), ("y", "y_from_a")]);
        let event_b = make_lww_event(2, vec![("x", "x_from_b"), ("z", "z_from_b")]);
        let event_c = make_lww_event(3, vec![("y", "y_from_c"), ("z", "z_from_c")]);

        // Order 1
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b, &event_c])).unwrap();

        // Order 2 (reversed)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_c, &event_b, &event_a])).unwrap();

        // Same final state for all properties
        assert_eq!(backend1.get(&"x".into()), backend2.get(&"x".into()));
        assert_eq!(backend1.get(&"y".into()), backend2.get(&"y".into()));
        assert_eq!(backend1.get(&"z".into()), backend2.get(&"z".into()));
    }

    #[test]
    fn test_sequential_layer_determinism() {
        // Simulate two layers being applied
        let event_a = make_lww_event(1, vec![("x", "layer1_a")]);
        let event_b = make_lww_event(2, vec![("x", "layer1_b")]);
        let event_c = make_lww_event(3, vec![("x", "layer2_c")]);
        let event_d = make_lww_event(4, vec![("x", "layer2_d")]);

        // Apply layer 1 then layer 2
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[&event_a, &event_b], &[&event_c, &event_d])).unwrap();

        // Apply layer 1 (reversed) then layer 2 (reversed)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b, &event_a])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[&event_b, &event_a], &[&event_d, &event_c])).unwrap();

        // Final state should be the same
        assert_eq!(backend1.get(&"x".into()), backend2.get(&"x".into()));
    }
}

// ============================================================================
// EDGE CASE TESTS (Phase 3e - Category 7)
// ============================================================================

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    fn empty_layer() -> EventLayer { EventLayer::new(Vec::new(), Vec::new(), Arc::new(BTreeMap::new())) }

    #[test]
    fn test_empty_layer_application() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Apply empty layer
        backend.apply_layer(&empty_layer()).unwrap();

        // State should be unchanged
        assert_eq!(backend.get(&"x".into()), Some(Value::String("initial".into())));
    }

    #[test]
    fn test_event_with_no_lww_operations() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Create an event with empty operations
        let entity_id = EntityId::from_bytes([99u8; 16]);
        let empty_event = Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::new()),
            parent: Clock::default(),
            generation: 1, // genesis: no parents (266-A)
        };

        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&empty_event];

        backend.apply_layer(&layer_from_refs_with_context(&already_applied, &to_apply, &[&init_event])).unwrap();

        // State should be unchanged
        assert_eq!(backend.get(&"x".into()), Some(Value::String("initial".into())));
    }

    #[test]
    fn test_same_event_in_both_lists() {
        // Edge case: what if same event appears in both already_applied and to_apply?
        // This shouldn't happen in practice, but the backend should handle it gracefully
        let backend = LWWBackend::new();

        let event_a = make_lww_event(1, vec![("x", "value_a")]);

        // Put same event in both lists
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_a];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Event should be applied once (its value should be set)
        assert_eq!(backend.get(&"x".into()), Some(Value::String("value_a".into())));
    }

    #[test]
    fn test_many_concurrent_events() {
        let backend = LWWBackend::new();

        // Create 10 concurrent events
        let events: Vec<Event> = (0..10).map(|i| make_lww_event(i as u8, vec![("x", &format!("value_{}", i))])).collect();

        let event_refs: Vec<&Event> = events.iter().collect();

        backend.apply_layer(&layer_from_refs(&[], &event_refs)).unwrap();

        // Winner should be the one with highest EventId
        let winner_idx = events.iter().enumerate().max_by_key(|(_, e)| e.id()).map(|(i, _)| i).unwrap();

        let expected = format!("value_{}", winner_idx);
        assert_eq!(backend.get(&"x".into()), Some(Value::String(expected)));
    }

    #[test]
    fn test_property_deletion() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Create an event that deletes the property (sets to None)
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 1;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        let delete_backend = LWWBackend::new();
        delete_backend.set("x".into(), None); // Delete
        let ops = delete_backend.to_operations().unwrap().unwrap();
        let delete_event = Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
            parent: Clock::default(),
            generation: 1, // genesis: no parents (266-A)
        };

        backend.apply_layer(&layer_from_refs_with_context(&[], &[&delete_event], &[&init_event])).unwrap();

        let expected = if delete_event.id() > init_event.id() { None } else { Some(Value::String("initial".into())) };
        assert_eq!(backend.get(&"x".into()), expected);
    }

    #[test]
    fn test_stored_last_write_competes() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        let challenger_event = make_lww_event(2, vec![("x", "challenger")]);
        backend.apply_layer(&layer_from_refs_with_context(&[], &[&challenger_event], &[&init_event])).unwrap();

        let expected = if challenger_event.id() > init_event.id() {
            Some(Value::String("challenger".into()))
        } else {
            Some(Value::String("initial".into()))
        };
        assert_eq!(backend.get(&"x".into()), expected);
    }

    #[test]
    fn test_event_id_tracking_after_layer() {
        let backend = LWWBackend::new();

        let event_a = make_lww_event(1, vec![("x", "value_a")]);

        backend.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();

        // The event_id should be tracked
        let tracked_id = backend.get_event_id(&"x".into());
        assert!(tracked_id.is_some());
        assert_eq!(tracked_id.unwrap(), event_a.id());
    }
}

// ============================================================================
// PHASE 4 TESTS
// ============================================================================

/// Phase 4 Test 1: Stored event_id below meet loses to layer candidate.
///
/// When the LWW backend has a stored value whose event_id is NOT in the accumulated
/// DAG (i.e., it is below the meet), any layer candidate should win regardless of
/// event_id ordering. This tests the `older_than_meet` rule.
#[cfg(test)]
mod phase4_stored_below_meet {
    use super::*;

    #[test]
    fn test_stored_event_id_below_meet_loses_to_layer_candidate() {
        let backend = LWWBackend::new();

        // Step 1: Apply an initial event so the backend has a stored value with an event_id.
        let old_event = make_lww_event(1, vec![("x", "old_value")]);
        let old_event_id = old_event.id();

        // Apply the old event as a normal layer first (so the backend tracks the event_id)
        {
            let dag = BTreeMap::from([(old_event_id.clone(), vec![])]);
            let layer = EventLayer::new(vec![], vec![old_event.clone()], Arc::new(dag));
            backend.apply_layer(&layer).unwrap();
        }

        // Verify old value is stored
        assert_eq!(backend.get(&"x".into()), Some(Value::String("old_value".into())));
        assert_eq!(backend.get_event_id(&"x".into()), Some(old_event_id.clone()));

        // Step 2: Create a new layer candidate event. Build the DAG WITHOUT the
        // old_event_id. This simulates the case where the stored value's event_id
        // is below the meet (not in the accumulated DAG).
        let new_event = make_lww_event(2, vec![("x", "new_value")]);
        let new_event_id = new_event.id();

        // DAG only contains the new event, NOT the old event.
        // This means dag_contains(old_event_id) returns false => older_than_meet.
        let dag = BTreeMap::from([(new_event_id.clone(), vec![])]);
        let layer = EventLayer::new(vec![], vec![new_event.clone()], Arc::new(dag));

        backend.apply_layer(&layer).unwrap();

        // The new event should always win because the stored value is older_than_meet,
        // regardless of event_id ordering.
        assert_eq!(
            backend.get(&"x".into()),
            Some(Value::String("new_value".into())),
            "Layer candidate must beat stored value whose event_id is below the meet"
        );
        assert_eq!(backend.get_event_id(&"x".into()), Some(new_event_id), "Winning event_id must be the new event");
    }
}

/// Phase 4 Test 2: Re-delivery of historical event is rejected (idempotency).
///
/// When an event is already stored and its id appears in the comparison head (or
/// BFS discovers it as an ancestor), compare should return Equal or StrictAscends,
/// signaling a no-op to the caller.
#[cfg(test)]
mod phase4_idempotency {
    use super::*;

    #[tokio::test]
    async fn test_redelivery_of_historical_event_is_noop() {
        let mut retriever = MockRetriever::new();

        // Build a chain: A -> B -> C (current head is [C])
        let ev_a = make_test_event(1, &[]);
        retriever.add_event(&ev_a);

        let ev_b = make_test_event(2, &[&ev_a]);
        retriever.add_event(&ev_b);

        let ev_c = make_test_event(3, &[&ev_b]);
        let id_c = ev_c.id();
        retriever.add_event(&ev_c);

        let entity_head = clock!(id_c);

        // Re-deliver event B (which is already in the history, but not at head).
        // With the staging+compare pattern, B is already in the retriever (same
        // content = same EventId as ev_b). compare([B], [C]) can walk backward
        // from C and discover B as an ancestor, returning StrictAscends.
        let event_b_again = make_test_event(2, &[&ev_a]);
        let result = compare(retriever.clone(), &Clock::from(vec![event_b_again.id()]), &entity_head, 100).await.unwrap();

        // Re-delivery of event C (which IS at the head) should return Equal.
        let event_c_again = make_test_event(3, &[&ev_b]);
        let result_c = compare(retriever.clone(), &Clock::from(vec![event_c_again.id()]), &entity_head, 100).await.unwrap();
        assert_eq!(result_c.relation, AbstractCausalRelation::Equal, "Re-delivery of head event must return Equal");

        // For event B (ancestor, not at head): with staging, the comparison can
        // now correctly identify B as an ancestor of C, returning StrictAscends.
        assert!(
            matches!(result.relation, AbstractCausalRelation::StrictAscends),
            "Historical non-head event re-delivery should return StrictAscends, got {:?}",
            result.relation
        );
    }
}

/// Phase 4 Test 3: Second creation event for same entity is rejected.
///
/// An entity may have at most one creation event (empty parent clock). If a second
/// creation event arrives after the entity already has a non-empty head, it must
/// be rejected with DuplicateCreation.
#[cfg(test)]
mod phase4_duplicate_creation {
    use super::*;
    use crate::entity::Entity;
    use crate::error::MutationError;

    fn make_creation_event(seed: u8) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42; // Same entity for both events
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        // Use LWW operations with distinct values to produce different event IDs.
        // Without differing content, identical events would produce the same EventId.
        let backend = LWWBackend::new();
        backend.set("x".into(), Some(Value::String(format!("value_{}", seed))));
        let ops = backend.to_operations().unwrap().unwrap();

        // empty parent = creation event
        Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
            parent: Clock::default(),
            generation: 1, // genesis: no parents (266-A)
        }
    }

    #[tokio::test]
    async fn test_second_creation_event_rejected() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = MockRetriever::new();

        // First creation event should succeed
        let creation_event_1 = make_creation_event(1);
        retriever.add_event(&creation_event_1);

        let result = entity.apply_event(&retriever, &creation_event_1).await;
        assert!(result.is_ok(), "First creation event should succeed");
        assert!(result.unwrap(), "First creation event should return true (applied)");

        // Entity head should now be non-empty
        assert!(!entity.head().is_empty(), "Entity head should be non-empty after creation");

        // Second creation event (different seed = different event) should be rejected.
        // BFS detects two different roots -> Disjoint -> LineageError::Disjoint
        let creation_event_2 = make_creation_event(2);
        retriever.add_event(&creation_event_2);
        let result = entity.apply_event(&retriever, &creation_event_2).await;

        assert!(result.is_err(), "Second creation event should fail");
        let err = result.unwrap_err();
        assert!(matches!(err, MutationError::LineageError(crate::error::LineageError::Disjoint)), "Error should be Disjoint, got: {err:?}");
    }

    #[tokio::test]
    async fn test_redelivery_of_same_creation_event_is_noop() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = MockRetriever::new();

        // First creation event
        let creation_event = make_creation_event(1);
        retriever.add_event(&creation_event);

        let result = entity.apply_event(&retriever, &creation_event).await;
        assert!(result.is_ok() && result.unwrap(), "First apply should succeed");

        // Re-deliver the SAME creation event (same content, same id)
        // Since event_stored returns false but event is at head, comparison returns Equal -> no-op
        let result = entity.apply_event(&retriever, &creation_event).await;
        assert!(result.is_ok(), "Re-delivery of same creation event should not error");
        assert!(!result.unwrap(), "Re-delivery should return false (no-op)");
    }
}

/// Phase 4 Test 5: Budget escalation succeeds where initial budget fails.
///
/// Create a DAG deep enough that budget=2 fails, but the internal 4x escalation
/// (to budget=8) succeeds.
#[cfg(test)]
mod phase4_budget_escalation {
    use super::*;

    #[tokio::test]
    async fn test_budget_escalation_succeeds() {
        let mut retriever = MockRetriever::new();

        // Create a chain of 6 events: ev0 -> ev1 -> ev2 -> ev3 -> ev4 -> ev5
        // With budget=2, the initial traversal exhausts at 2 steps.
        // With 4x escalation (budget=8), it succeeds for a 6-long chain.
        let mut events: Vec<Event> = Vec::new();
        for i in 0..6u8 {
            let parents: Vec<&Event> = if i == 0 { vec![] } else { vec![&events[i as usize - 1]] };
            let ev = make_test_event(i + 1, &parents);
            retriever.add_event(&ev);
            events.push(ev);
        }

        let ancestor = clock!(events[0].id());
        let descendant = clock!(events[5].id());

        // With budget=2, initial attempt fails. But internal escalation retries
        // with budget=8 (2 * 4), which should succeed for a 6-deep chain.
        let result = compare(retriever.clone(), &descendant, &ancestor, 2).await.unwrap();
        assert!(
            matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "Budget escalation (2 -> 8) should succeed for a 6-deep chain, got {:?}",
            result.relation
        );

        // Verify that without escalation room, it would actually fail.
        // Budget=1 escalates to max 4, which is not enough for a chain of 6.
        let result = compare(retriever.clone(), &descendant, &ancestor, 1).await.unwrap();
        assert!(
            matches!(result.relation, AbstractCausalRelation::BudgetExceeded { .. }),
            "Budget=1 (max 4) should fail for a 6-deep chain, got {:?}",
            result.relation
        );
    }
}

/// Phase 4 Test 6: TOCTOU retry exhaustion produces clean error.
///
/// This tests the retry loop in `apply_event`. A proper test would require a mock
/// that changes the entity head between comparison and CAS, which requires interior
/// mutability and a custom Retrieve implementation that modifies entity state during
/// comparison. This is too complex for a unit test.
#[cfg(test)]
mod phase4_toctou_retry {
    #[test]
    #[ignore = "Testing TOCTOU retry exhaustion requires a mock that modifies the entity \
        head between comparison and the CAS attempt inside try_mutate. This requires: \
        (1) a custom Retrieve that triggers a side-effect during get_event to modify \
        entity state, (2) precise timing control over the entity's RwLock. The expected \
        behavior is: after MAX_RETRIES (5) attempts where the head moves each time, \
        apply_event returns Err(MutationError::TOCTOUAttemptsExhausted). This error \
        variant exists and is returned at the end of the retry loop."]
    fn test_toctou_retry_exhaustion_produces_clean_error() {
        // See ignore message above.
    }
}

/// Phase 4 Test 7: Merge event with parent from non-meet branch is correctly layered.
///
/// A merge event may have parents that span the meet boundary. This tests the
/// generalized topological-sort seed in EventLayers (not just children of meet).
#[cfg(test)]
mod phase4_mixed_parent_merge {
    use super::*;
    use crate::event_dag::accumulator::EventAccumulator;

    #[tokio::test]
    async fn test_merge_event_with_parent_from_non_meet_branch() {
        let mut retriever = MockRetriever::new();

        // DAG:
        //       A (meet)
        //      / \
        //     B   G (independent lineage brought in by merge)
        //     |   |
        //     C   |
        //      \ /
        //       E  (merge: parents = [C, G])
        //
        // current head = [B] (only the B side is local)
        // incoming = [E] (brings in G from a non-meet branch)
        //
        // After layer computation:
        // - Layer 1: B (already_applied), C, G (to_apply -- all are children of meet
        //   or have all parents processed)
        //   Actually G's parent is A (the meet), and C's parent is B.
        //   So initial frontier = {B, G} (both have all in-DAG parents processed).
        //   Wait -- B's parent is A, which is in the processed set (meet).
        //   G's parent is A, also in processed set. So frontier = {B, G}.
        //
        // - Layer 1: B (already_applied), G (to_apply)
        // - After processing B: C becomes ready (parent B is processed)
        // - Layer 2: C (to_apply)
        // - After processing C: E becomes ready (parents C processed, G processed)
        // - Layer 3: E (to_apply)

        let ev_a = make_test_event(1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);

        let ev_b = make_test_event(2, &[&ev_a]);
        let id_b = ev_b.id();
        retriever.add_event(&ev_b);

        let ev_g = make_test_event(3, &[&ev_a]);
        let id_g = ev_g.id();
        retriever.add_event(&ev_g);

        let ev_c = make_test_event(4, &[&ev_b]);
        let id_c = ev_c.id();
        retriever.add_event(&ev_c);

        let ev_e = make_test_event(5, &[&ev_c, &ev_g]);
        let id_e = ev_e.id();
        retriever.add_event(&ev_e);

        // Build accumulator manually to simulate what comparison would produce.
        // The DAG contains all events above the meet (A is the meet).
        let mut accumulator = EventAccumulator::new(retriever.clone());
        for id in [&id_b, &id_g, &id_c, &id_e] {
            let event = retriever.get_event(id).await.unwrap();
            accumulator.accumulate(&event);
        }

        // Meet = [A], current head = [B]
        let mut layers = accumulator.into_layers(vec![id_a.clone()], vec![id_b.clone()]);

        // Collect all layers
        let mut all_layers = Vec::new();
        while let Some(layer) = layers.next().await.unwrap() {
            all_layers.push(layer);
        }

        // Verify we got multiple layers and all events are covered
        assert!(all_layers.len() >= 2, "Expected at least 2 layers for mixed-parent merge, got {}", all_layers.len());

        // Collect all event ids from all layers
        let mut all_event_ids: Vec<EventId> = Vec::new();
        for layer in &all_layers {
            for e in &layer.already_applied {
                all_event_ids.push(e.id());
            }
            for e in &layer.to_apply {
                all_event_ids.push(e.id());
            }
        }

        // All events above meet should be present
        assert!(all_event_ids.contains(&id_b), "B should be in layers");
        assert!(all_event_ids.contains(&id_g), "G should be in layers");
        assert!(all_event_ids.contains(&id_c), "C should be in layers");
        assert!(all_event_ids.contains(&id_e), "E (merge event) should be in layers");

        // E must appear in a later layer than both C and G
        let e_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_e) || l.already_applied.iter().any(|ev| ev.id() == id_e))
            .expect("E must appear in some layer");

        let g_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_g) || l.already_applied.iter().any(|ev| ev.id() == id_g))
            .expect("G must appear in some layer");

        let c_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_c) || l.already_applied.iter().any(|ev| ev.id() == id_c))
            .expect("C must appear in some layer");

        assert!(e_layer_idx > g_layer_idx, "Merge event E (layer {}) must be after G (layer {})", e_layer_idx, g_layer_idx);
        assert!(e_layer_idx > c_layer_idx, "Merge event E (layer {}) must be after C (layer {})", e_layer_idx, c_layer_idx);

        // B should be in already_applied (it's in current head ancestry)
        let b_in_already = all_layers.iter().any(|l| l.already_applied.iter().any(|ev| ev.id() == id_b));
        assert!(b_in_already, "B should be in already_applied (current head ancestry)");

        // G should be in to_apply (it's NOT in current head ancestry)
        let g_in_to_apply = all_layers.iter().any(|l| l.to_apply.iter().any(|ev| ev.id() == id_g));
        assert!(g_in_to_apply, "G should be in to_apply (not in current head ancestry)");
    }
}

/// Phase 4 Test 8: Eagerly stored peer events are discoverable by BFS during comparison.
///
/// This verifies that when events are stored in the retriever (simulating eager
/// storage on receipt), the BFS in comparison can discover them and produce
/// correct causal relationships.
#[cfg(test)]
mod phase4_eager_storage_bfs {
    use super::*;

    #[tokio::test]
    async fn test_eagerly_stored_events_discoverable_by_bfs() {
        let mut retriever = MockRetriever::new();

        // Simulate a scenario where events arrive from a peer and are eagerly stored
        // before comparison. Build a DAG:
        //
        //     A (genesis)
        //    / \
        //   B   C  (concurrent branches)
        //   |   |
        //   D   E  (tips)
        //
        // All events are stored in the retriever (simulating eager storage).
        // Then compare D vs E. The BFS should discover all events via the retriever.

        let ev_a = make_test_event(1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);

        let ev_b = make_test_event(2, &[&ev_a]);
        let id_b = ev_b.id();
        retriever.add_event(&ev_b);

        let ev_c = make_test_event(3, &[&ev_a]);
        let id_c = ev_c.id();
        retriever.add_event(&ev_c);

        let ev_d = make_test_event(4, &[&ev_b]);
        let id_d = ev_d.id();
        retriever.add_event(&ev_d);

        let ev_e = make_test_event(5, &[&ev_c]);
        let id_e = ev_e.id();
        retriever.add_event(&ev_e);

        let clock_d = clock!(id_d);
        let clock_e = clock!(id_e);

        // BFS should walk backward from D and E, discovering B, C, and A.
        let result = compare(retriever.clone(), &clock_d, &clock_e, 100).await.unwrap();

        // Verify BFS produced the correct result
        assert!(
            matches!(result.relation, AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id_a.clone()]),
            "BFS should discover meet at A via eagerly stored events, got {:?}",
            result.relation
        );

        // Verify the accumulator contains all events discovered by BFS.
        // The accumulator's DAG should have entries for B, C, D, E (events above meet),
        // and possibly A (the meet itself, depending on whether it's accumulated).
        let accumulator = result.accumulator();
        let dag = accumulator.dag();

        assert!(dag.contains_key(&id_b), "BFS should have accumulated event B");
        assert!(dag.contains_key(&id_c), "BFS should have accumulated event C");
        assert!(dag.contains_key(&id_d), "BFS should have accumulated event D");
        assert!(dag.contains_key(&id_e), "BFS should have accumulated event E");

        // Now verify the result can produce valid layers
        let mut layers = result.into_layers(vec![id_d.clone()]).unwrap();
        let mut layer_count = 0;
        while let Some(layer) = layers.next().await.unwrap() {
            layer_count += 1;
            // Each layer should have events
            assert!(!layer.to_apply.is_empty() || !layer.already_applied.is_empty(), "Layer should not be empty");
        }
        assert!(layer_count > 0, "Should produce at least one layer from eagerly stored events");
    }
}

// ============================================================================
// BFS REVISIT BUG VERIFICATION (no per-side visited set in comparison.rs)
// ============================================================================

#[cfg(test)]
mod bfs_revisit_bugs {
    use super::*;

    /// CLAIM A: A second, longer path re-adds an already-processed node to the
    /// subject frontier. The re-visit re-runs the `original_comparison.contains`
    /// check and decrements `unseen_comparison_heads` a second time for the SAME
    /// head, so it can hit 0 while another comparison head (D) was never reached.
    ///
    /// DAG (parents point right):
    ///   S -> {A, B};  A -> N;  B -> C;  C -> N;  N -> R;  D -> R
    /// Head (comparison) = {N, D}. Subject = {S}.
    /// S descends from N via two paths of different length but does NOT descend
    /// from D, so the correct relation is DivergedSince { meet: [N], .. }.
    /// The bug double-decrements for N and falsely reports StrictDescends,
    /// which entity.rs turns into head = {S}, silently discarding tip D.
    #[tokio::test]
    async fn double_decrement_falsely_reports_strict_descends() {
        let mut retriever = MockRetriever::new();

        let ev_r = make_test_event(1, &[]);
        retriever.add_event(&ev_r);

        let ev_n = make_test_event(2, &[&ev_r]);
        let id_n = ev_n.id();
        retriever.add_event(&ev_n);

        let ev_d = make_test_event(3, &[&ev_r]);
        let id_d = ev_d.id();
        retriever.add_event(&ev_d);

        let ev_a = make_test_event(4, &[&ev_n]);
        retriever.add_event(&ev_a);

        // Pick a seed for C such that id(N) < id(C). The frontier is a BTreeSet
        // iterated in id order, so this makes the subject BFS process N (short
        // path, via A) BEFORE C re-adds N to the frontier (long path, via B),
        // forcing the re-visit. Content hashing makes this deterministic once a
        // seed is found.
        let ev_c =
            (10u8..=255).map(|seed| make_test_event(seed, &[&ev_n])).find(|ev| ev.id() > id_n).expect("some seed must yield id(C) > id(N)");
        retriever.add_event(&ev_c);

        let ev_b = make_test_event(5, &[&ev_c]);
        retriever.add_event(&ev_b);

        let ev_s = make_test_event(6, &[&ev_a, &ev_b]);
        let id_s = ev_s.id();
        retriever.add_event(&ev_s);

        let subject = clock!(id_s);
        let comparison = clock!(id_n, id_d); // two concurrent tips

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_n.clone()], "meet should be exactly [N]");
            }
            other => panic!("S does NOT descend from concurrent tip D; expected DivergedSince {{ meet: [N] }} but got {:?}", other),
        }
    }

    /// Mirror of the double-decrement case: the same DAG and forced re-visit,
    /// but with subject and comparison roles swapped, so the double-count lands
    /// on the subject-head accounting and the false verdict is StrictAscends,
    /// which entity.rs turns into silently dropping the incoming event.
    ///
    /// Subject = {N, D}, comparison = {S}. Correct: DivergedSince { meet: [N] }.
    #[tokio::test]
    async fn double_decrement_falsely_reports_strict_ascends() {
        let mut retriever = MockRetriever::new();

        let ev_r = make_test_event(1, &[]);
        retriever.add_event(&ev_r);

        let ev_n = make_test_event(2, &[&ev_r]);
        let id_n = ev_n.id();
        retriever.add_event(&ev_n);

        let ev_d = make_test_event(3, &[&ev_r]);
        let id_d = ev_d.id();
        retriever.add_event(&ev_d);

        let ev_a = make_test_event(4, &[&ev_n]);
        retriever.add_event(&ev_a);

        // Same seed condition as the StrictDescends case: id(N) < id(C) makes
        // the comparison-side BFS process N (short path, via A) before C
        // re-encounters it (long path, via B).
        let ev_c =
            (10u8..=255).map(|seed| make_test_event(seed, &[&ev_n])).find(|ev| ev.id() > id_n).expect("some seed must yield id(C) > id(N)");
        retriever.add_event(&ev_c);

        let ev_b = make_test_event(5, &[&ev_c]);
        retriever.add_event(&ev_b);

        let ev_s = make_test_event(6, &[&ev_a, &ev_b]);
        let id_s = ev_s.id();
        retriever.add_event(&ev_s);

        let subject = clock!(id_n, id_d); // two concurrent tips
        let comparison = clock!(id_s);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_n.clone()], "meet should be exactly [N]");
            }
            other => panic!(
                "comparison {{S}} does NOT reach concurrent tip D, so it cannot strictly ascend; expected DivergedSince {{ meet: [N] }} but got {:?}",
                other
            ),
        }
    }

    /// Budget must be spent per event, not per path. A chain of uneven diamonds
    /// has O(levels) events but 2^levels distinct paths; per-path bookkeeping
    /// re-processes the join node of every level and cascades exponentially,
    /// exhausting even the 4x-escalated budget. Per-event bookkeeping completes
    /// within a small multiple of the event count and yields a duplicate-free
    /// chain.
    ///
    /// Each level: J -> {A, B}; A -> prev (short); B -> C -> prev (long).
    /// C's seed is searched so id(C) > id(prev), forcing prev to be re-added to
    /// the frontier after it was already processed (the V1 re-visit shape).
    #[tokio::test]
    async fn diamond_chain_completes_within_linear_budget() {
        use std::collections::BTreeSet;

        let mut retriever = MockRetriever::new();

        const LEVELS: usize = 12;
        let mut seed = 1u16;
        let mut next_seed = move || {
            let s = seed;
            seed = seed.checked_add(1).expect("seed space exhausted");
            s
        };

        let ev_r = make_test_event_u16(next_seed(), &[]);
        let id_r = ev_r.id();
        retriever.add_event(&ev_r);

        let mut prev = ev_r.clone();
        for _ in 0..LEVELS {
            let ev_a = make_test_event_u16(next_seed(), &[&prev]);
            retriever.add_event(&ev_a);

            let ev_c = loop {
                let candidate = make_test_event_u16(next_seed(), &[&prev]);
                if candidate.id() > prev.id() {
                    break candidate;
                }
            };
            retriever.add_event(&ev_c);

            let ev_b = make_test_event_u16(next_seed(), &[&ev_c]);
            retriever.add_event(&ev_b);

            let ev_j = make_test_event_u16(next_seed(), &[&ev_a, &ev_b]);
            retriever.add_event(&ev_j);
            prev = ev_j;
        }

        let total_events = 1 + 4 * LEVELS;
        let subject = clock!(prev.id()); // top join
        let comparison = clock!(id_r); // genesis

        let result = compare(retriever, &subject, &comparison, 2 * total_events).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::StrictDescends { chain } => {
                let unique: BTreeSet<_> = chain.iter().collect();
                assert_eq!(unique.len(), chain.len(), "chain must not contain duplicate events");
                assert_eq!(chain.len(), total_events - 1, "chain covers every event except the comparison head R");
            }
            other => panic!("expected StrictDescends within a 2x-event-count budget (per-event, not per-path); got {:?}", other),
        }
    }

    /// CLAIM B: heads are removed from `outstanding_heads` only at the moment a
    /// node FIRST becomes common, using the origins known at that moment. Here
    /// C2's origin reaches M one step later (via X), so C2 stays outstanding
    /// forever and check_result forces DivergedSince { meet: [] } even though
    /// the meet is plainly {M}.
    ///
    /// DAG: C1 -> M;  C2 -> X -> M;  S -> M;  M is root.
    /// Subject = {S}, comparison = {C1, C2}. Correct: DivergedSince meet [M].
    #[tokio::test]
    async fn late_origin_propagation_yields_empty_meet() {
        let mut retriever = MockRetriever::new();

        let ev_m = make_test_event(1, &[]);
        let id_m = ev_m.id();
        retriever.add_event(&ev_m);

        let ev_c1 = make_test_event(2, &[&ev_m]);
        let id_c1 = ev_c1.id();
        retriever.add_event(&ev_c1);

        let ev_x = make_test_event(3, &[&ev_m]);
        retriever.add_event(&ev_x);

        let ev_c2 = make_test_event(4, &[&ev_x]);
        let id_c2 = ev_c2.id();
        retriever.add_event(&ev_c2);

        let ev_s = make_test_event(5, &[&ev_m]);
        let id_s = ev_s.id();
        retriever.add_event(&ev_s);

        let subject = clock!(id_s);
        let comparison = clock!(id_c1, id_c2);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(
                    meet,
                    &vec![id_m.clone()],
                    "meet should be [M]; an empty meet makes entity.rs re-layer from genesis and skips head tip removal"
                );
            }
            other => panic!("expected DivergedSince {{ meet: [M] }}, got {:?}", other),
        }
    }

    /// Found during remediation A2: origins can stall at a node that is already
    /// EXPANDED but not common. Incremental retirement (including the
    /// arrival-at-common case of A2) never sees such a head reach the meet, and
    /// the per-side dedup from A1 means the traversal will not carry it further.
    /// The exhaustion-time reachability reconciliation must retire it.
    ///
    /// DAG: C1 -> K -> M;  H -> L -> K;  S -> M;  M is root.
    /// Subject = {S}, comparison = {C1, H}. The seed search forces
    /// id(K) < id(L), so K is expanded (propagating only C1's origin onward)
    /// before L delivers H's origin to K, where it stalls.
    /// Correct: DivergedSince { meet: [M] }.
    #[tokio::test]
    async fn origin_stalled_at_processed_node_still_retires_head() {
        let mut retriever = MockRetriever::new();

        let ev_m = make_test_event(1, &[]);
        let id_m = ev_m.id();
        retriever.add_event(&ev_m);

        let ev_k = make_test_event(2, &[&ev_m]);
        let id_k = ev_k.id();
        retriever.add_event(&ev_k);

        let ev_c1 = make_test_event(3, &[&ev_k]);
        let id_c1 = ev_c1.id();
        retriever.add_event(&ev_c1);

        // L: child of K on the longer path; must be expanded after K.
        let ev_l = (10u16..=u16::MAX)
            .map(|seed| make_test_event_u16(seed, &[&ev_k]))
            .find(|ev| ev.id() > id_k)
            .expect("some seed must yield id(L) > id(K)");
        retriever.add_event(&ev_l);

        let ev_h = make_test_event(4, &[&ev_l]);
        let id_h = ev_h.id();
        retriever.add_event(&ev_h);

        let ev_s = make_test_event(5, &[&ev_m]);
        let id_s = ev_s.id();
        retriever.add_event(&ev_s);

        let subject = clock!(id_s);
        let comparison = clock!(id_c1, id_h);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_m.clone()], "meet should be [M] even though H's origin stalled at K");
            }
            other => panic!("expected DivergedSince {{ meet: [M] }}, got {:?}", other),
        }
    }
}

// ============================================================================
// QUICK-CHECK SUBSET ACCEPTS DISJOINT EXTRA ROOT (adversarial verification)
// ============================================================================

#[cfg(test)]
mod quick_check_disjoint_verify {
    use super::*;

    /// CLAIM 1: the quick-check in comparison.rs (~L100) returns StrictDescends
    /// whenever `comparison_set.is_subset(&all_parents)`, where `all_parents` is
    /// the union of the parents of every subject head event. A subject head that
    /// carries an EXTRA, DISJOINT genesis root X contributes nothing to
    /// all_parents (X has no parents), so the subset test still passes on the
    /// strength of the *other* subject event and StrictDescends is returned even
    /// though X's ancestry is entirely disjoint from the comparison clock.
    ///
    /// DAG (parents point right):  A (root);  B -> A;  X (independent root)
    /// Subject (new head) = {B, X};  comparison (current head) = {A}.
    /// B alone descends from A, but X shares no ancestry with A, so the correct
    /// relation is NOT StrictDescends — it must be Diverged/Disjoint so that
    /// entity.rs does not wholesale-replace head with {B, X} and silently adopt
    /// X's disjoint lineage.
    #[tokio::test]
    async fn test_quick_check_disjoint_extra_root() {
        let mut retriever = MockRetriever::new();

        // A: genesis root of the comparison lineage
        let ev_a = make_test_event(1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);

        // B: child of A (this is the part that genuinely descends)
        let ev_b = make_test_event(2, &[&ev_a]);
        let id_b = ev_b.id();
        retriever.add_event(&ev_b);

        // X: an INDEPENDENT genesis root, no parents, unrelated to A
        let ev_x = make_test_event(3, &[]);
        let id_x = ev_x.id();
        retriever.add_event(&ev_x);

        let subject = clock!(id_b, id_x); // new head carries a disjoint extra root
        let comparison = clock!(id_a); // current head

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        // The bug: quick-check returns StrictDescends. Assert it does NOT.
        assert!(
            !matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "subject {{B, X}} contains disjoint root X; must not be StrictDescends over {{A}}, got {:?}",
            result.relation
        );
    }

    /// When the guard trips (a subject event's parents are not within the
    /// comparison set), the comparison must fall through to the full BFS and
    /// return its verdict, not error or misclassify.
    ///
    /// DAG: A (root);  B -> A;  D -> B;  C -> A.
    /// Subject = {D}, comparison = {C}: D's parent B is outside {C}, so the
    /// shortcut does not apply; BFS finds the true relation
    /// DivergedSince { meet: [A] }.
    #[tokio::test]
    async fn test_quick_check_guard_falls_through_to_bfs() {
        let mut retriever = MockRetriever::new();

        let ev_a = make_test_event(1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);

        let ev_b = make_test_event(2, &[&ev_a]);
        retriever.add_event(&ev_b);

        let ev_d = make_test_event(3, &[&ev_b]);
        let id_d = ev_d.id();
        retriever.add_event(&ev_d);

        let ev_c = make_test_event(4, &[&ev_a]);
        let id_c = ev_c.id();
        retriever.add_event(&ev_c);

        let subject = clock!(id_d);
        let comparison = clock!(id_c);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_a.clone()], "meet should be [A]");
            }
            other => panic!("expected DivergedSince {{ meet: [A] }} via BFS fallthrough, got {:?}", other),
        }
    }

    /// The single-event form of the V3 shape, found in external review: one
    /// event D whose parent clock joins a legitimate ancestor line with an
    /// independent genesis root. D's head is grounded THROUGH B, so a
    /// per-head lineage-intersection test passes, yet adopting D grafts X's
    /// foreign line into the entity's ancestry. The exploration-boundary rule
    /// blocks the fast-forward: X is a discovered root outside the
    /// comparison's ancestry, so the traversal runs to exhaustion and the
    /// merge machinery handles it as diverged.
    ///
    /// DAG: A (root); B -> A; X (independent root); D -> [B, X].
    /// Subject = {D}, comparison = {A}. Correct: DivergedSince { meet: [A] }.
    #[tokio::test]
    async fn test_single_event_with_disjoint_extra_parent_is_not_strict_descends() {
        let mut retriever = MockRetriever::new();

        let ev_a = make_test_event(1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);

        let ev_b = make_test_event(2, &[&ev_a]);
        retriever.add_event(&ev_b);

        let ev_x = make_test_event(3, &[]);
        retriever.add_event(&ev_x);

        let ev_d = make_test_event(4, &[&ev_b, &ev_x]);
        let id_d = ev_d.id();
        retriever.add_event(&ev_d);

        let subject = clock!(id_d);
        let comparison = clock!(id_a);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        assert!(
            !matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "a graft event must not fast-forward over {{A}}, got {:?}",
            result.relation
        );
        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_a.clone()], "meet should be [A]");
            }
            other => panic!("expected DivergedSince {{ meet: [A] }}, got {:?}", other),
        }
    }
}

/// V4: the StrictDescends "gap-jump" in entity.rs applies ONLY the incoming
/// event's operations and jumps the head, so a bridge batch applied out of
/// causal order silently loses the skipped ancestors' operations. The
/// remediation defense is ordering: batches are topologically sorted
/// (parents first) before application on both the producer and receiver
/// sides. The entity-level defense (replaying staged-but-unapplied ancestors
/// inside the StrictDescends arm) is deliberately NOT part of this
/// remediation; it is the B3 follow-up issue.
#[cfg(test)]
mod strict_descends_gap_jump {
    use super::*;
    use crate::entity::Entity;
    use crate::event_dag::ordering::topo_sort_events;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use ankurah_proto::Attested;

    /// Read a committed LWW property value out of the entity's serialized state.
    fn read_lww(entity: &Entity, prop: &str) -> Option<Value> {
        let state = entity.to_state().unwrap();
        let buf = state.state_buffers.0.get("lww")?;
        let backend = LWWBackend::from_state_buffer(buf).unwrap();
        backend.get(&prop.into())
    }

    /// A bridge delivering [B, X] child-first must not lose X's operations.
    /// This drives the receiver-side sequence at the unit level: stage the
    /// whole batch (add_event simulates staging), topologically sort, apply
    /// in sorted order. Without the sort, applying B first compares
    /// StrictDescends over head {A} (its ancestry resolves via staging), the
    /// head gap-jumps to {B}, and X's ops are then dropped as StrictAscends:
    /// p1 goes missing while head {B} claims X is history. On a durable node
    /// that wrong state is persisted via set_state and served as canonical.
    #[tokio::test]
    async fn test_strict_descends_gap_jump_skips_ancestor_ops() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = MockRetriever::new();

        // A: genesis create event (empty parent). Establishes head {A}.
        let ev_a = make_lww_event_with_parent(1, vec![("p0", "genesis")], &[]);
        let id_a = ev_a.id();
        retriever.add_event(&ev_a);
        assert!(ev_a.is_entity_create(), "A must be a creation event");

        // X: child of A, writes p1. This is the intermediate ancestor.
        let ev_x = make_lww_event_with_parent(2, vec![("p1", "written_by_X")], &[&ev_a]);
        let id_x = ev_x.id();
        retriever.add_event(&ev_x);

        // B: child of X, writes p2. B descends A through X.
        let ev_b = make_lww_event_with_parent(3, vec![("p2", "written_by_B")], &[&ev_x]);
        let id_b = ev_b.id();
        retriever.add_event(&ev_b);

        // Establish local head = {A}.
        assert!(entity.apply_event(&retriever, &ev_a).await.unwrap(), "A should apply");
        assert_eq!(entity.head(), Clock::from(vec![id_a.clone()]));
        assert_eq!(read_lww(&entity, "p0"), Some(Value::String("genesis".into())));

        // The wire delivers child B before its parent X.
        let batch = vec![Attested::opt(ev_b.clone(), None), Attested::opt(ev_x.clone(), None)];
        let sorted = topo_sort_events(batch).unwrap();
        let sorted_ids: Vec<EventId> = sorted.iter().map(|e| e.payload.id()).collect();
        assert_eq!(sorted_ids, vec![id_x.clone(), id_b.clone()], "sort must place parent X before child B");

        for event in &sorted {
            assert!(entity.apply_event(&retriever, &event.payload).await.unwrap(), "each event applies in causal order");
        }

        assert_eq!(entity.head(), Clock::from(vec![id_b.clone()]), "head advanced to B");
        assert_eq!(read_lww(&entity, "p2"), Some(Value::String("written_by_B".into())), "B's op (p2) applied");

        // The V4 loss: without ordered application, p1 is missing while head
        // {B} claims X is part of history.
        let p1 = read_lww(&entity, "p1");
        assert_eq!(
            p1,
            Some(Value::String("written_by_X".into())),
            "CONSISTENCY VIOLATION: head descends X but X's write (p1) is missing. p1={p1:?}"
        );
    }
}

// ============================================================================
// R1: REQUESTED-ID KEYING (D2 M5 pre-task, dispositions Q1, maintainer
// approved). The comparison keys its bookkeeping on the id it REQUESTED,
// not the id recomputed from the served payload, so a lying retriever
// yields a coherent walk over the served structure instead of a budget
// stall. In every honest run the two ids are equal (engines key rows by
// payload id at write); the behavior differs only under lying or corrupt
// storage, where a coherent walk plus the D2-4 edge checks strictly beats
// silent mis-keying, budget spin, and poisoned grounding. On the ephemeral
// CachedEventGetter lane a mid-walk peer fetch is inside amendment K's
// trust envelope (a durable node lying to an ephemeral is out of threat
// model), which is what makes trading the pre-R1 fail-stop for a coherent
// walk sound there (red-team fold item 6).
// ============================================================================

#[cfg(test)]
mod requested_id_keying {
    use super::*;

    /// R1 red (oracle brief section 1.4): one doctored payload served under
    /// the original id must complete with the TRUE verdict. Pre-R1 the
    /// mis-keyed row never leaves the frontier (frontier.remove sees the
    /// recomputed id), the walk refetches the requested id from the LRU
    /// forever, and the comparison burns to BudgetExceeded through every
    /// escalation.
    #[tokio::test]
    async fn doctored_payload_under_original_id_completes_with_true_verdict() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        for e in [&g, &a, &b] {
            retriever.add_event(e);
        }

        // Interior corruption: the walk must traverse a to ground b in g.
        let mut lying = GenCorruptedRetriever::new(retriever);
        lying.doctor(a.id(), Event { generation: 999, ..a.clone() });

        let result = compare(lying, &clock!(b.id()), &clock!(g.id()), 100).await.unwrap();
        assert!(
            matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "the true verdict is StrictDescends (b descends g through a); a generation-only lie may never \
             change the outcome, got {:?}",
            result.relation
        );
    }
}

// ============================================================================
// COMPARESTATS MECHANICS (D2 M5, dispositions Q4)
// ============================================================================

#[cfg(test)]
mod compare_stats_mechanics {
    use super::*;

    /// events_fetched counts UNDERLYING retriever fetches only; LRU hits are
    /// not fetches. budget_decrements counts processed frontier occupants,
    /// which exceeds distinct events exactly when the two sides reach an
    /// event at different steps (here: the comparison side processes g at
    /// level 1 and the subject's traversal re-processes it at level 3).
    ///
    /// Trace for chain g <- a <- b, subject {b}, comparison {g}:
    /// quick check fetches b (1; parents not within comparison, BFS runs);
    /// level 1 processes b (LRU hit) and g (fetch 2); level 2 processes a
    /// (fetch 3); level 3 processes g on the subject side (LRU hit). Totals:
    /// 3 fetches, 4 decrements, verdict StrictDescends.
    #[tokio::test]
    async fn stats_count_underlying_fetches_and_budget_decrements() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        for e in [&g, &a, &b] {
            retriever.add_event(e);
        }

        let result = compare(retriever, &clock!(b.id()), &clock!(g.id()), 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        assert_eq!(result.stats.events_fetched, 3, "b, g, a fetched once each; the level-3 re-encounter of g is an LRU hit");
        assert_eq!(result.stats.budget_decrements, 4, "b, g, a, plus g re-processed from the subject side");
        assert_eq!(result.stats.id_mismatches, 0, "honest retriever: every payload recomputes to its requested id");
        assert_eq!(result.stats.precheck_suppressions, 0, "no prechecks ran (no generation operands supplied)");
        assert_eq!(result.stats.edge_check_violations, 0);
        assert_eq!(result.stats.demotions, 0);
    }

    /// The Equal and empty-clock early returns complete without touching the
    /// retriever: their stats are all zero.
    #[tokio::test]
    async fn early_returns_carry_zero_stats() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(4, &[]);
        retriever.add_event(&g);

        let equal = compare(retriever.clone(), &clock!(g.id()), &clock!(g.id()), 100).await.unwrap();
        assert!(matches!(equal.relation, AbstractCausalRelation::Equal));
        assert_eq!(equal.stats, Default::default());

        let empty = compare(retriever, &clock!(g.id()), &Clock::default(), 100).await.unwrap();
        assert!(matches!(empty.relation, AbstractCausalRelation::DivergedSince { .. }));
        assert_eq!(empty.stats, Default::default());
    }
}

// ============================================================================
// PROPERTY TEST: state machine verdict vs brute-force reachability oracle
// ============================================================================

#[cfg(test)]
mod comparison_property {
    use super::*;
    use std::collections::BTreeSet;

    /// Deterministic xorshift32: no external dependency, and every failure is
    /// reproducible from the printed dag_seed.
    struct Rng(u32);

    impl Rng {
        fn next(&mut self) -> u32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize { (self.next() as usize) % n }
    }

    /// Inclusive ancestry over the true parent map (the oracle's reachability).
    fn ancestry(parents: &BTreeMap<EventId, Vec<EventId>>, heads: &[EventId]) -> BTreeSet<EventId> {
        let mut out = BTreeSet::new();
        let mut stack: Vec<EventId> = heads.to_vec();
        while let Some(id) = stack.pop() {
            if !out.insert(id.clone()) {
                continue;
            }
            if let Some(ps) = parents.get(&id) {
                stack.extend(ps.iter().cloned());
            }
        }
        out
    }

    /// Reduce a set of events to its maximal antichain: drop members that are
    /// proper ancestors of other members.
    fn to_antichain(parents: &BTreeMap<EventId, Vec<EventId>>, ids: &BTreeSet<EventId>) -> Vec<EventId> {
        ids.iter()
            .filter(|id| !ids.iter().any(|other| other != *id && ancestry(parents, std::slice::from_ref(other)).contains(*id)))
            .cloned()
            .collect()
    }

    #[derive(Debug)]
    enum Expected {
        Equal,
        StrictDescends,
        StrictAscends,
        DivergedMeet(Vec<EventId>),
        Disjoint,
    }

    /// Brute-force specification of the comparison verdict. Precedence mirrors
    /// the machine: Equal, then the strict completions (which fire during
    /// traversal), then the exhaustion verdicts.
    fn oracle(parents: &BTreeMap<EventId, Vec<EventId>>, subject: &[EventId], comparison: &[EventId]) -> Expected {
        let s_set: BTreeSet<EventId> = subject.iter().cloned().collect();
        let c_set: BTreeSet<EventId> = comparison.iter().cloned().collect();
        if s_set == c_set {
            return Expected::Equal;
        }

        let s_cover = ancestry(parents, subject);
        let c_cover = ancestry(parents, comparison);

        // StrictDescends: cover containment plus root containment. Every
        // genesis root in the subject's cover must lie within the comparison's
        // cover; a foreign root (juxtaposed as an extra head, or grafted in
        // through one event's multi-parent clock) blocks adoption. Root
        // containment implies the older per-head grounding condition: every
        // subject head's ancestry bottoms out in shared lineage.
        if c_cover.is_subset(&s_cover)
            && s_cover.iter().filter(|id| parents.get(*id).is_some_and(|p| p.is_empty())).all(|root| c_cover.contains(root))
        {
            return Expected::StrictDescends;
        }

        // StrictAscends is deliberately cover-containment only (it drives
        // skip-incoming decisions, where covering suffices).
        if s_cover.is_subset(&c_cover) {
            return Expected::StrictAscends;
        }

        let common: BTreeSet<EventId> = s_cover.intersection(&c_cover).cloned().collect();
        if common.is_empty() {
            return Expected::Disjoint;
        }

        // A comparison head sharing nothing with the subject's cover cannot be
        // accounted to any common node: the exhaustion gate reports the
        // conservative empty meet. (Disjoint is reserved for no shared
        // history at all.)
        if comparison.iter().any(|h| ancestry(parents, std::slice::from_ref(h)).intersection(&s_cover).next().is_none()) {
            return Expected::DivergedMeet(vec![]);
        }

        Expected::DivergedMeet(to_antichain(parents, &common))
    }

    fn verdict_matches(expected: &Expected, actual: &AbstractCausalRelation<EventId>) -> bool {
        match (expected, actual) {
            (Expected::Equal, AbstractCausalRelation::Equal) => true,
            (Expected::StrictDescends, AbstractCausalRelation::StrictDescends { .. }) => true,
            (Expected::StrictAscends, AbstractCausalRelation::StrictAscends) => true,
            (Expected::DivergedMeet(want), AbstractCausalRelation::DivergedSince { meet, .. }) => {
                let mut got = meet.clone();
                got.sort();
                let mut want = want.clone();
                want.sort();
                got == want
            }
            (Expected::Disjoint, AbstractCausalRelation::Disjoint { .. }) => true,
            _ => false,
        }
    }

    /// Base of the `dag_seed` range, overridable so a nightly or manual run can
    /// shift the window without overlapping a prior run's coverage. Default
    /// matches the historical hardcoded start (`1`) so ordinary test runs are
    /// unchanged.
    fn oracle_seed_base() -> u32 { std::env::var("ORACLE_SEED_BASE").ok().and_then(|s| s.parse().ok()).unwrap_or(1) }

    /// Count of `dag_seed` values to run, overridable for the nightly high-seed
    /// scale-out (100k+). Default matches the historical hardcoded count
    /// (`300`) so ordinary test runs are unchanged.
    fn oracle_seed_count() -> u32 { std::env::var("ORACLE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(300) }

    /// The self-contained, one-line seeded-failure artifact. Everything needed
    /// to reproduce the exact failing case from the log line alone: the
    /// dag_seed (which deterministically regenerates the DAG and all four
    /// subject/comparison pairs via the xorshift `Rng`), the mismatching
    /// verdict, and the precise command to re-run just that seed. Format
    /// mirrors C1's `SIMFAIL` line (space-separated `key=value` tokens, one
    /// line, no wrapping).
    fn artifact_line(
        dag_seed: u32,
        subject_ids: &[EventId],
        comparison_ids: &[EventId],
        expected: &Expected,
        actual: &AbstractCausalRelation<EventId>,
    ) -> String {
        format!(
            "ORACLEFAIL dag_seed={dag_seed} subject={subject_ids:?} comparison={comparison_ids:?} expected={expected:?} actual={actual:?} reproduce=\"ORACLE_SEED_BASE={dag_seed} ORACLE_SEEDS=1 cargo test -p ankurah-core --lib event_dag::tests::comparison_property::randomized_dags_match_reachability_oracle -- --exact --nocapture\""
        )
    }

    /// Randomized small DAGs (including extra genesis roots), random antichain
    /// clocks, machine verdict checked against the oracle. Clocks are built
    /// sorted since Clock does not yet normalize input order (C1).
    ///
    /// The `dag_seed` range is `ORACLE_SEED_BASE..(ORACLE_SEED_BASE +
    /// ORACLE_SEEDS)`, defaulting to `1..=300` (unchanged from before these
    /// env vars existed). The nightly scale-out (C2) sets `ORACLE_SEEDS` to
    /// 100k+; a hit prints an `ORACLEFAIL` line (see `artifact_line`) before
    /// panicking, so any divergence is reproducible from the log alone.
    #[tokio::test]
    async fn randomized_dags_match_reachability_oracle() {
        let seed_base = oracle_seed_base();
        let seed_count = oracle_seed_count();
        for dag_seed in seed_base..seed_base.saturating_add(seed_count) {
            let mut rng = Rng(dag_seed.wrapping_mul(0x9E37_79B9) | 1);
            let n_events = 5 + rng.below(6); // 5..=10

            let mut retriever = MockRetriever::new();
            let mut events: Vec<Event> = Vec::new();
            let mut ids: Vec<EventId> = Vec::new();
            let mut parents_map: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();

            for i in 0..n_events {
                // The first event is always a root; later events occasionally
                // start a fresh root to exercise disjoint and foreign-lineage
                // shapes.
                let parent_ids: Vec<EventId> = if i == 0 || rng.below(6) == 0 {
                    vec![]
                } else {
                    let mut chosen = BTreeSet::new();
                    for _ in 0..(1 + rng.below(2)) {
                        chosen.insert(ids[rng.below(ids.len())].clone());
                    }
                    // Parent clocks are antichains in honest input.
                    to_antichain(&parents_map, &chosen)
                };

                // Resolve the chosen parent ids back to their events (the
                // helper stamps generations from parent payloads).
                let parent_events: Vec<&Event> =
                    parent_ids.iter().map(|pid| events.iter().find(|e| e.id() == *pid).expect("parent id was minted above")).collect();
                let ev = make_test_event_u16(i as u16 + 1, &parent_events);
                let id = ev.id();
                parents_map.insert(id.clone(), parent_ids);
                ids.push(id);
                retriever.add_event(&ev);
                events.push(ev);
            }

            for _pair in 0..4 {
                let mut pick_clock = |rng: &mut Rng| -> Vec<EventId> {
                    let mut set = BTreeSet::new();
                    for _ in 0..(1 + rng.below(3)) {
                        set.insert(ids[rng.below(ids.len())].clone());
                    }
                    to_antichain(&parents_map, &set)
                };
                let subject_ids = pick_clock(&mut rng);
                let comparison_ids = pick_clock(&mut rng);

                let subject = Clock::from(subject_ids.clone());
                let comparison = Clock::from(comparison_ids.clone());

                let result = compare(retriever.clone(), &subject, &comparison, 4 * n_events).await.unwrap();
                let expected = oracle(&parents_map, &subject_ids, &comparison_ids);

                if !verdict_matches(&expected, &result.relation) {
                    panic!("{}", artifact_line(dag_seed, &subject_ids, &comparison_ids, &expected, &result.relation));
                }

                // Chain COMPLETENESS for grounded StrictDescends verdicts
                // (REV 4): gap replay applies the chain's events, so every
                // event strictly between the comparison cover and the
                // subject must be in it. The chain may carry extras the
                // head already incorporates (BFS overshoot); the layer
                // partition makes those harmless, so only completeness is
                // asserted.
                if let AbstractCausalRelation::StrictDescends { chain } = &result.relation {
                    let chain_set: BTreeSet<EventId> = chain.iter().cloned().collect();
                    let s_cover = ancestry(&parents_map, &subject_ids);
                    let c_cover = ancestry(&parents_map, &comparison_ids);
                    for id in s_cover.difference(&c_cover) {
                        assert!(
                            chain_set.contains(id),
                            "ORACLEFAIL chain incomplete dag_seed={dag_seed} missing={id:?} subject={subject_ids:?} comparison={comparison_ids:?} chain={chain:?} reproduce=\"ORACLE_SEED_BASE={dag_seed} ORACLE_SEEDS=1 cargo test -p ankurah-core --lib event_dag::tests::comparison_property::randomized_dags_match_reachability_oracle -- --exact --nocapture\""
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn artifact_line_format_is_self_contained() {
        let subject_ids = vec![];
        let comparison_ids = vec![];
        let expected = Expected::Disjoint;
        let actual = AbstractCausalRelation::Equal;
        let line = artifact_line(42, &subject_ids, &comparison_ids, &expected, &actual);

        assert!(line.starts_with("ORACLEFAIL "), "artifact line must start with the ORACLEFAIL marker: {line}");
        assert!(line.contains("dag_seed=42"), "artifact line must carry the exact dag_seed: {line}");
        assert!(line.contains("expected=Disjoint"), "artifact line must carry the expected verdict: {line}");
        assert!(line.contains("actual=Equal"), "artifact line must carry the actual verdict: {line}");
        assert!(
            line.contains(
                "reproduce=\"ORACLE_SEED_BASE=42 ORACLE_SEEDS=1 cargo test -p ankurah-core --lib event_dag::tests::comparison_property::randomized_dags_match_reachability_oracle -- --exact --nocapture\""
            ),
            "artifact line must carry a copy-pasteable single-seed repro command: {line}"
        );
        assert!(!line.contains('\n'), "artifact line must be a single line: {line}");
    }
}

// ============================================================================
// ENTITY CHANGE NOTIFICATION FOR MULTI-EVENT BATCHES
// ============================================================================

#[cfg(test)]
mod entity_change_batches {
    use super::*;
    use crate::changes::EntityChange;
    use crate::entity::Entity;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use ankurah_proto::Attested;

    /// Like make_lww_event_with_parent, but for a FIXED entity id:
    /// EntityChange validates event ownership, so the events must genuinely
    /// belong to the entity under test (the shared helper derives entity ids
    /// from its seed).
    fn lww_event_for(entity_id: EntityId, properties: Vec<(&str, &str)>, parents: &[&Event]) -> Event {
        let backend = LWWBackend::new();
        for (name, value) in properties {
            backend.set(name.into(), Some(Value::String(value.into())));
        }
        let ops = backend.to_operations().unwrap().unwrap();
        Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
            parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
        }
    }

    /// An ordered parent-then-child batch applies cleanly, leaving only the
    /// child in the head. The change notification must still accept the
    /// parent (it is superseded within the batch), or the applier commits
    /// both events and then fails while constructing the notification.
    #[tokio::test]
    async fn notification_accepts_batch_superseded_ancestors() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 77;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = MockRetriever::new();

        let ev_a = lww_event_for(entity_id, vec![("p0", "genesis")], &[]);
        retriever.add_event(&ev_a);
        let ev_x = lww_event_for(entity_id, vec![("p1", "from_x")], &[&ev_a]);
        retriever.add_event(&ev_x);
        let ev_b = lww_event_for(entity_id, vec![("p2", "from_b")], &[&ev_x]);
        retriever.add_event(&ev_b);

        assert!(entity.apply_event(&retriever, &ev_a).await.unwrap());
        assert!(entity.apply_event(&retriever, &ev_x).await.unwrap());
        assert!(entity.apply_event(&retriever, &ev_b).await.unwrap());
        assert_eq!(entity.head(), Clock::from(vec![ev_b.id()]));

        let batch = vec![Attested::opt(ev_x.clone(), None), Attested::opt(ev_b.clone(), None)];
        let change = EntityChange::new(entity.clone(), batch);
        assert!(change.is_ok(), "superseded ancestor X must be acceptable in a batch notification: {:?}", change.err());

        // Still rejected: an event that is neither a head tip nor superseded
        // by a later batch member.
        let stray = lww_event_for(entity_id, vec![("p9", "stray")], &[&ev_a]);
        let bad = EntityChange::new(entity, vec![Attested::opt(stray, None)]);
        assert!(bad.is_err(), "an event outside the head and unsuperseded in the batch must be rejected");
    }
}

/// D2 M1 identity pins: the generation is inside the hashed content, so an
/// event's id commits to its generation and a mis-stamped forgery cannot wear
/// the correct event's identity (266-E). These live in core because the
/// validation gate runs `-p ankurah-core`, not the proto crate's own tests.
mod generation_identity {
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;

    fn event(entity: EntityId, parent: Clock, generation: u32) -> Event {
        Event { entity_id: entity, collection: "test".into(), operations: OperationSet(BTreeMap::new()), parent, generation }
    }

    /// R-D2-1a: two events differing ONLY in generation get different ids.
    #[test]
    fn r_d2_1a_generation_differentiates_identity() {
        let entity = EntityId::from_bytes([7u8; 16]);
        let a = event(entity, Clock::default(), 1);
        let b = event(entity, Clock::default(), 2);
        assert_ne!(a.id(), b.id(), "events differing only in generation must have different ids");
    }

    /// R-D2-1b: a mis-stamped event is BUILDABLE, but its id commits to the
    /// stamped value (round-trip pins the field is hashed), so a forgery gets a
    /// distinct id from the correctly-stamped event rather than colliding with it.
    #[test]
    fn r_d2_1b_misstamped_generation_is_hashed_into_identity() {
        let entity = EntityId::from_bytes([9u8; 16]);

        // A mis-stamped genesis (claims 5, must be 1) is buildable, but its id
        // differs from the correct genesis.
        let genesis_correct = event(entity, Clock::default(), 1);
        let genesis_misstamped = event(entity, Clock::default(), 5);
        assert_ne!(genesis_correct.id(), genesis_misstamped.id(), "a mis-stamped genesis must not share the correct genesis's id");

        // The stamped value round-trips through bincode and the decoded event
        // recomputes the same id: the id commits to the stored field.
        let bytes = bincode::serialize(&genesis_misstamped).unwrap();
        let decoded: Event = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.generation, 5, "generation must survive serialization");
        assert_eq!(decoded.id(), genesis_misstamped.id(), "the id must commit to the stored generation");

        // A mis-stamped child (claims 9, must be 1 + parent) likewise gets a
        // distinct id from the correctly-stamped child.
        let parent = Clock::from(vec![genesis_correct.id()]);
        let child_correct = event(entity, parent.clone(), 2);
        let child_misstamped = event(entity, parent, 9);
        assert_ne!(child_correct.id(), child_misstamped.id(), "a mis-stamped child must not share the correct child's id");
    }
}
