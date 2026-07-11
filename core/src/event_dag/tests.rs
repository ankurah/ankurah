#![cfg(test)]

use crate::error::RetrievalError;
use crate::event_dag::EventLayer;
use crate::property::backend::lww::LWWBackend;
use crate::property::backend::PropertyBackend;
use crate::retrieval::GetEvents;
use crate::value::Value;

use super::comparison::{compare, compare_with, CompareOptions};
use super::relation::AbstractCausalRelation;
use ankurah_proto::{Clock, EntityId, Event, EventId, GClock, OperationSet};
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

/// A single deterministic fake model-definition id for these pure-wire DAG
/// fixtures (#330). These tests build events by hand and never route them
/// through a node's catalog, so any stable id works; the model field is also
/// excluded from `EventId` hashing, so it never perturbs the content-hashed
/// event ids these tests order and compare on.
fn test_model_id() -> EntityId {
    let mut bytes = [0u8; 16];
    bytes[0] = 0xEE;
    EntityId::from_bytes(bytes)
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

/// A retriever wrapper that RECORDS the sequence of requested ids (oracle
/// brief section 4.1: the fetch-order log is the test-side observable for
/// schedule changes, quick-check suppression and frontier reordering both).
/// Zero production surface; interior mutability so the log survives the
/// comparison taking ownership.
#[derive(Clone)]
struct LoggingRetriever<G> {
    inner: G,
    log: Arc<std::sync::Mutex<Vec<EventId>>>,
}

impl<G> LoggingRetriever<G> {
    fn new(inner: G) -> Self { Self { inner, log: Arc::new(std::sync::Mutex::new(Vec::new())) } }

    /// Handle to the fetch log; reads the ids requested so far, in order.
    fn log_handle(&self) -> Arc<std::sync::Mutex<Vec<EventId>>> { self.log.clone() }
}

#[async_trait]
impl<G: GetEvents + Send + Sync> GetEvents for LoggingRetriever<G> {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.log.lock().unwrap().push(event_id.clone());
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
        backend.insert(&crate::property::PropertyKey::name(text_field), 0, insert_text).unwrap();
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
        let result = backend.get_string(&crate::property::PropertyKey::name("text")).unwrap();
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

        let initial_text = backend.get_string(&crate::property::PropertyKey::name("text")).unwrap();
        assert_eq!(initial_text, "hello");

        // Now apply with event_a in already_applied and event_b in to_apply
        let event_b = make_yrs_event(2, "text", "world");
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_b];
        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Only event_b should be applied again (if it wasn't already)
        // The text should contain "world" from event_b
        let final_text = backend.get_string(&crate::property::PropertyKey::name("text")).unwrap();
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
        let result1 = backend1.get_string(&crate::property::PropertyKey::name("text")).unwrap();

        // Order 2: C, A, B
        let backend2 = YrsBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        let result2 = backend2.get_string(&crate::property::PropertyKey::name("text")).unwrap();

        // Order 3: B, C, A
        let backend3 = YrsBackend::new();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        let result3 = backend3.get_string(&crate::property::PropertyKey::name("text")).unwrap();

        // All results should be identical (CRDT convergence)
        assert_eq!(result1, result2, "Order 1 vs Order 2 should produce same result");
        assert_eq!(result2, result3, "Order 2 vs Order 3 should produce same result");
    }

    #[test]
    fn test_yrs_apply_layer_empty_to_apply() {
        let backend = YrsBackend::new();
        backend.insert(&crate::property::PropertyKey::name("text"), 0, "initial").unwrap();

        let event_a = make_yrs_event(1, "text", "hello");

        // Apply with event_a in already_applied but nothing in to_apply
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Text should be unchanged (only "initial")
        let result = backend.get_string(&crate::property::PropertyKey::name("text")).unwrap();
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

        let result = entity.apply_event(&retriever, &creation_event_1, None).await;
        assert!(result.is_ok(), "First creation event should succeed");
        assert!(result.unwrap(), "First creation event should return true (applied)");

        // Entity head should now be non-empty
        assert!(!entity.head().is_empty(), "Entity head should be non-empty after creation");

        // Second creation event (different seed = different event) should be rejected.
        // BFS detects two different roots -> Disjoint -> LineageError::Disjoint
        let creation_event_2 = make_creation_event(2);
        retriever.add_event(&creation_event_2);
        let result = entity.apply_event(&retriever, &creation_event_2, None).await;

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

        let result = entity.apply_event(&retriever, &creation_event, None).await;
        assert!(result.is_ok() && result.unwrap(), "First apply should succeed");

        // Re-deliver the SAME creation event (same content, same id)
        // Since event_stored returns false but event is at head, comparison returns Equal -> no-op
        let result = entity.apply_event(&retriever, &creation_event, None).await;
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
            accumulator.accumulate(id, &event);
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
        assert!(entity.apply_event(&retriever, &ev_a, None).await.unwrap(), "A should apply");
        assert_eq!(entity.head(), Clock::from(vec![id_a.clone()]));
        assert_eq!(read_lww(&entity, "p0"), Some(Value::String("genesis".into())));

        // The wire delivers child B before its parent X.
        let batch = vec![Attested::opt(ev_b.clone(), None), Attested::opt(ev_x.clone(), None)];
        let sorted = topo_sort_events(batch).unwrap();
        let sorted_ids: Vec<EventId> = sorted.iter().map(|e| e.payload.id()).collect();
        assert_eq!(sorted_ids, vec![id_x.clone(), id_b.clone()], "sort must place parent X before child B");

        for event in &sorted {
            assert!(entity.apply_event(&retriever, &event.payload, None).await.unwrap(), "each event applies in causal order");
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
        assert_eq!(result.stats.id_mismatches, 1, "the doctored fetch is observed exactly once (underlying fetches only)");
        assert_eq!(result.stats.events_fetched, 3, "the stall is gone: b, g, and the doctored a fetch once each");
    }

    /// The honest path is unchanged by R1: requested and recomputed ids are
    /// equal on every honest fetch, so the mismatch counter stays zero and
    /// the verdict is identical with or without the wrapper.
    #[tokio::test]
    async fn honest_retriever_observes_no_mismatches() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        for e in [&g, &a, &b] {
            retriever.add_event(e);
        }
        let wrapped = GenCorruptedRetriever::new(retriever); // no overrides
        let result = compare(wrapped, &clock!(b.id()), &clock!(g.id()), 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        assert_eq!(result.stats.id_mismatches, 0);
    }
}

// ============================================================================
// P1/P2 PRECHECKS, SUPPRESS-ONLY (D2 M5, plan D2-4, derivations section 2).
// A precheck rejection may only SUPPRESS the positive fast-path attempt (the
// StrictDescends quick check); it never selects, skips, or concludes a
// verdict path. These pins drive compare_with with adversarial operand
// values and assert the counter moved AND every BOUND observable stayed
// byte-identical to the honest baseline.
// ============================================================================

#[cfg(test)]
mod prechecks_suppress_only {
    use super::*;

    /// The B1 deflate seed (delta red-team BLOCKER 1, instantiated per the
    /// oracle brief GC-DEFLATE): honest chain g(1) <- m(2) <- u(3) <- w(4),
    /// subject {w}, comparison {m}; truth is StrictDescends. Doctored values
    /// m -> 5, u -> 2, w -> 3 arrive through BOTH channels (payloads via the
    /// lying retriever, tips via the operand GClocks, mutually consistent).
    /// On the lies P1 computes maxg(C) = 5 > maxg(S) = 3 and wrongly rejects
    /// a TRUE StrictDescends; the rejection may only suppress the attempt,
    /// and the walk must return the byte-identical verdict.
    #[tokio::test]
    async fn b1_deflate_wrong_rejection_fires_and_is_contained() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let m = make_test_event(2, &[&g]);
        let u = make_test_event(3, &[&m]);
        let w = make_test_event(4, &[&u]);
        for e in [&g, &m, &u, &w] {
            retriever.add_event(e);
        }
        let subject = clock!(w.id());
        let comparison = clock!(m.id());

        // Honest baseline: honest payloads, honest operands.
        let baseline = compare_with(
            retriever.clone(),
            &subject,
            &comparison,
            100,
            CompareOptions {
                subject_gens: Some(GClock::from((4, w.id()))),
                comparison_gens: Some(GClock::from((2, m.id()))),
                unverified: None,
            },
        )
        .await
        .unwrap();
        assert!(matches!(baseline.relation, AbstractCausalRelation::StrictDescends { .. }), "precondition: truth is StrictDescends");
        assert_eq!(baseline.stats.precheck_suppressions, 0, "honest values reject nothing on this shape");

        // Corrupted run: deflation through both channels, consistently.
        let mut lying = GenCorruptedRetriever::new(retriever);
        lying.doctor(m.id(), Event { generation: 5, ..m.clone() });
        lying.doctor(u.id(), Event { generation: 2, ..u.clone() });
        lying.doctor(w.id(), Event { generation: 3, ..w.clone() });
        let corrupted = compare_with(
            lying,
            &subject,
            &comparison,
            100,
            CompareOptions {
                subject_gens: Some(GClock::from((3, w.id()))),
                comparison_gens: Some(GClock::from((5, m.id()))),
                unverified: None,
            },
        )
        .await
        .unwrap();

        assert!(
            corrupted.stats.precheck_suppressions >= 1,
            "the corruption must BITE: P1 sees maxg(C)=5 > maxg(S)=3 and fires (suppression delta >= 1 vs baseline), got {}",
            corrupted.stats.precheck_suppressions
        );
        assert_eq!(corrupted.relation, baseline.relation, "BOUND: the wrong rejection may only suppress; verdict and chain byte-equal");
    }

    /// GC-INFLATE, one-step shape: subject {e} sits exactly one step above
    /// comparison {h} and the baseline quick check fires. Inflating the
    /// COMPARISON operand makes P1 suppress the attempt; the BFS must return
    /// the byte-identical verdict, and the fetch-order log shows the route
    /// change (the suppressed quick check never fetched the comparison
    /// head; the BFS does): a FREE delta asserted on purpose.
    #[tokio::test]
    async fn inflate_one_step_suppresses_quick_check_and_bfs_agrees() {
        let mut retriever = MockRetriever::new();
        let h = make_test_event(1, &[]);
        let e = make_test_event(2, &[&h]);
        retriever.add_event(&h);
        retriever.add_event(&e);
        let subject = clock!(e.id());
        let comparison = clock!(h.id());

        let honest_ops = || CompareOptions {
            subject_gens: Some(GClock::from((2, e.id()))),
            comparison_gens: Some(GClock::from((1, h.id()))),
            unverified: None,
        };

        let base_log_retriever = LoggingRetriever::new(retriever.clone());
        let base_log = base_log_retriever.log_handle();
        let baseline = compare_with(base_log_retriever, &subject, &comparison, 100, honest_ops()).await.unwrap();
        assert!(matches!(baseline.relation, AbstractCausalRelation::StrictDescends { .. }));
        assert_eq!(baseline.stats.precheck_suppressions, 0, "honest values suppress nothing (the quick check fires)");
        let base_fetches = base_log.lock().unwrap().clone();
        assert_eq!(base_fetches, vec![e.id()], "precondition: the quick check fetches only the subject tip");

        // Operand-channel inflation only: the retriever stays honest.
        let corr_log_retriever = LoggingRetriever::new(retriever);
        let corr_log = corr_log_retriever.log_handle();
        let corrupted = compare_with(
            corr_log_retriever,
            &subject,
            &comparison,
            100,
            CompareOptions {
                subject_gens: Some(GClock::from((2, e.id()))),
                comparison_gens: Some(GClock::from((4_000_000_000, h.id()))),
                unverified: None,
            },
        )
        .await
        .unwrap();

        assert!(
            corrupted.stats.precheck_suppressions >= 1,
            "the inflated comparison tip must suppress the quick-check attempt, got {}",
            corrupted.stats.precheck_suppressions
        );
        assert_eq!(corrupted.relation, baseline.relation, "BOUND: byte-identical verdict and chain through the BFS route");
        let corr_fetches = corr_log.lock().unwrap().clone();
        assert_ne!(corr_fetches, base_fetches, "FREE, asserted on purpose: the route change is visible in the fetch order");
    }

    /// The no-op flank (oracle brief GC-INFLATE): inflating a SUBJECT tip
    /// must not newly fire anything. Prechecks can only REJECT
    /// StrictDescends hypotheses; a wiring inversion that concluded
    /// positively from a huge subject value would show up here.
    #[tokio::test]
    async fn inflated_subject_tip_is_a_no_op_flank() {
        let mut retriever = MockRetriever::new();
        let h = make_test_event(1, &[]);
        let e = make_test_event(2, &[&h]);
        retriever.add_event(&h);
        retriever.add_event(&e);

        let result = compare_with(
            retriever,
            &clock!(e.id()),
            &clock!(h.id()),
            100,
            CompareOptions {
                subject_gens: Some(GClock::from((4_000_000_000, e.id()))),
                comparison_gens: Some(GClock::from((1, h.id()))),
                unverified: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result.stats.precheck_suppressions, 0, "an inflated subject rejects nothing (P1 compares maxg(C) > maxg(S))");
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
    }

    /// The apply-path wiring: a redelivered OLD event (true StrictAscends)
    /// walks with the resident's materialized annotation as the comparison
    /// operand and its own stamp as the subject operand; P1 fires (the old
    /// event provably cannot descend the newer head) and only suppresses.
    /// Pinned through the cfg(test) stats mirror, the only observable the
    /// bool return leaves.
    #[tokio::test]
    async fn apply_event_wires_operands_and_p1_fires_on_redelivered_ancestor() {
        use crate::entity::Entity;
        let entity_id = {
            let mut b = [0u8; 16];
            b[0] = 42;
            EntityId::from_bytes(b)
        };
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = MockRetriever::new();
        let g = make_lww_event_for_entity(entity_id, vec![("p", "g")], &[]);
        let a = make_lww_event_for_entity(entity_id, vec![("p", "a")], &[&g]);
        let b = make_lww_event_for_entity(entity_id, vec![("p", "b")], &[&a]);
        for e in [&g, &a, &b] {
            retriever.add_event(e);
        }

        assert!(entity.apply_event(&retriever, &g, None).await.unwrap());
        assert!(entity.apply_event(&retriever, &a, None).await.unwrap());
        assert!(entity.apply_event(&retriever, &b, None).await.unwrap());
        let _ = entity.take_last_compare_stats();

        // Redeliver a: gen 2 against the materialized head {b} at gen 3.
        // P1 rejects the (impossible) StrictDescends hypothesis, suppressing
        // the quick-check attempt; the walk returns StrictAscends (no-op).
        assert!(!entity.apply_event(&retriever, &a, None).await.unwrap(), "redelivered ancestor is a no-op");
        let stats = entity.take_last_compare_stats().expect("apply_event mirrored its comparison stats");
        assert!(
            stats.precheck_suppressions >= 1,
            "apply_event must feed the resident GClock and the event stamp into the prechecks; P1 fires here, got {}",
            stats.precheck_suppressions
        );
    }

    /// Helper for the apply-path pin: LWW events on a FIXED entity id (the
    /// shared helpers derive entity ids from their seed, but apply_event
    /// validates event ownership against the entity).
    fn make_lww_event_for_entity(entity_id: EntityId, properties: Vec<(&str, &str)>, parents: &[&Event]) -> Event {
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
}

// ============================================================================
// FRONTIER ORDERING (D2 M5, plan D2-4; derivations section 3): the level
// drain processes entries max-eligible-generation-first, unknowns last,
// EventId ascending tiebreak. A pure schedule choice: level membership and
// every verdict are order-independent (dispositions Q3: level-drain with
// in-level ordering is the shipped granularity).
// ============================================================================

#[cfg(test)]
mod frontier_ordering {
    use super::*;

    /// Build the ordering shape: comparison {x, y} is an honest antichain
    /// with DIFFERENT tip generations (x at 2, y at 3 via an extra step),
    /// subject {s} two steps above (so the quick check is inapplicable and
    /// the BFS runs). The seed search picks content whose hash order OPPOSES
    /// the generation order (id(y) > id(x)), so id-ascending and
    /// generation-descending schedules are observably different.
    fn ordering_shape() -> (MockRetriever, Event, Event, Event) {
        let g = make_test_event(1, &[]);
        let x = make_test_event(2, &[&g]); // gen 2
        for seed in 0..u16::MAX {
            let w = make_test_event_u16(seed, &[&g]); // gen 2
            let y = make_test_event_u16(seed.wrapping_add(1), &[&w]); // gen 3
            if y.id() > x.id() {
                let m = make_test_event(3, &[&x, &y]);
                let s = make_test_event(4, &[&m]);
                let mut retriever = MockRetriever::new();
                for e in [&g, &x, &w, &y, &m, &s] {
                    retriever.add_event(e);
                }
                return (retriever, s, x, y);
            }
        }
        unreachable!("half of all seeds order y above x");
    }

    /// With materialized tip operands, the first drained level fetches the
    /// higher-generation comparison tip FIRST even though its id sorts
    /// later: eligible generations key the schedule, ids only break ties.
    /// (Level membership is schedule-independent, so this is fetch-order
    /// observable only; the verdict is pinned identical either way.)
    #[tokio::test]
    async fn level_drain_fetches_max_eligible_generation_first() {
        let (retriever, s, x, y) = ordering_shape();
        let subject = clock!(s.id());
        let comparison = clock!(x.id(), y.id());

        let logging = LoggingRetriever::new(retriever.clone());
        let log = logging.log_handle();
        let with_operands = compare_with(
            logging,
            &subject,
            &comparison,
            100,
            CompareOptions {
                subject_gens: Some(GClock::from((4, s.id()))),
                comparison_gens: Some(GClock::new(vec![(2, x.id()), (3, y.id())])),
                unverified: None,
            },
        )
        .await
        .unwrap();

        let fetches = log.lock().unwrap().clone();
        assert_eq!(
            &fetches[..3],
            &[s.id(), y.id(), x.id()],
            "the level drain must schedule the generation-3 tip y before the generation-2 tip x \
             (operand-seeded keys, unknowns last, id tiebreak); got {fetches:?}"
        );

        // BOUND unchanged: the schedule is free, the verdict is not.
        let plain = compare(retriever, &subject, &comparison, 100).await.unwrap();
        assert_eq!(with_operands.relation, plain.relation, "ordering is a pure schedule choice; verdicts byte-equal");
    }
}

// ============================================================================
// WALK-TIME EDGE CHECKS (D2 M5, plan D2-4): when the walk holds child and
// parents, assert gen(child) == 1 + max(gen(parents)) (saturating); on
// violation WARN with a counter, demote the child to per-comparison
// ineligibility, continue. Never a retroactive rejection, never a verdict
// input.
// ============================================================================

#[cfg(test)]
mod walk_time_edge_checks {
    use super::*;

    /// A traversed doctored edge must be SEEN: the lying retriever serves
    /// interior event a with generation 7 (honest: 2), so both in-hand
    /// edges around it violate the equation (a against its parent g, child
    /// b against a). The walk warns and demotes, and the verdict is
    /// byte-identical to the honest baseline (containment).
    #[tokio::test]
    async fn traversed_doctored_edge_warns_demotes_and_is_contained() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        for e in [&g, &a, &b] {
            retriever.add_event(e);
        }
        let subject = clock!(b.id());
        let comparison = clock!(g.id());

        let baseline = compare(retriever.clone(), &subject, &comparison, 100).await.unwrap();
        assert!(matches!(baseline.relation, AbstractCausalRelation::StrictDescends { .. }));
        assert_eq!(baseline.stats.edge_check_violations, 0, "an honest corpus must produce zero violations");
        assert_eq!(baseline.stats.demotions, 0);

        let mut lying = GenCorruptedRetriever::new(retriever);
        lying.doctor(a.id(), Event { generation: 7, ..a.clone() });
        let corrupted = compare(lying, &subject, &comparison, 100).await.unwrap();

        assert!(
            corrupted.stats.edge_check_violations >= 1,
            "the walk traversed the doctored edge and must have SEEN the lie (violation counter), got {}",
            corrupted.stats.edge_check_violations
        );
        assert!(
            corrupted.stats.demotions >= 1,
            "a violating child is demoted to per-comparison ineligibility, got {}",
            corrupted.stats.demotions
        );
        assert_eq!(corrupted.relation, baseline.relation, "BOUND: warn and demote, verdict unchanged");
    }

    /// Edge-honest SATURATED chains pass the check: the expected value uses
    /// the same saturating arithmetic as the stamp, so a parent at u32::MAX
    /// with a child at u32::MAX is equation-consistent, not a violation.
    /// The base is a deep-history stand-in genesis carrying an explicit
    /// near-ceiling claim (a genesis has no edges, so nothing in-walk can or
    /// should check its absolute claim; that is admission's law): every
    /// CHECKABLE edge here is equation-consistent under saturation.
    #[tokio::test]
    async fn honest_saturated_edges_do_not_warn() {
        let mut retriever = MockRetriever::new();
        // Explicit claims, equation-consistent per edge: the sanctioned
        // adversarial-construction form (registry-ban ruling).
        let near = Event { generation: u32::MAX - 1, ..make_test_event(2, &[]) };
        let at = Event { generation: u32::MAX, ..make_test_event(3, &[&near]) };
        let still = Event { generation: u32::MAX, ..make_test_event(4, &[&at]) };
        let mut add = |e: &Event| retriever.add_event(e);
        add(&near);
        add(&at);
        add(&still);

        let result = compare(retriever, &clock!(still.id()), &clock!(near.id()), 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        assert_eq!(
            result.stats.edge_check_violations, 0,
            "saturating expected values keep honest ceiling chains violation-free (266-C.iv direction)"
        );
    }
}

// ============================================================================
// CHAIN CANONICALIZATION AT EMISSION (D2 M5, dispositions Q2 as corrected by
// the red-team fold item 3: the topo sorter is FIFO Kahn whose tie order
// derives from input order, so canonical output requires sorted-by-id input)
// ============================================================================

#[cfg(test)]
mod chain_canonicalization {
    use super::*;

    /// Two-branch diamond: g <- a, g <- b, {a,b} <- m. The subject BFS visits
    /// the {a,b} level in ascending id order and emits the reversed visit
    /// order, so the raw chain carries the level DESCENDING. Q2 requires the
    /// emitted chain to be the canonical topological order (topo_sort_ids
    /// over the chain set with BTree-seeded ties): ascending within the
    /// level, parents before children.
    #[tokio::test]
    async fn strict_descends_chain_is_emitted_canonical() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&g]);
        let m = make_test_event(4, &[&a, &b]);
        for e in [&g, &a, &b, &m] {
            retriever.add_event(e);
        }

        let (lo, hi) = if a.id() < b.id() { (a.id(), b.id()) } else { (b.id(), a.id()) };

        let result = compare(retriever, &clock!(m.id()), &clock!(g.id()), 100).await.unwrap();
        match &result.relation {
            AbstractCausalRelation::StrictDescends { chain } => {
                assert_eq!(
                    chain,
                    &vec![lo.clone(), hi.clone(), m.id()],
                    "the emitted chain must be the canonical topo order of its set (id-ascending within a level)"
                );
            }
            other => panic!("expected StrictDescends, got {other:?}"),
        }
    }

    /// The DivergedSince chains get the same canonical ORDER. Same diamond
    /// plus a divergent comparison branch c under g: subject {m} and
    /// comparison {c} diverge at meet {g}.
    ///
    /// Chain CONTENT is deliberately not pinned exactly: build_forward_chain
    /// trims at the last-encountered meet member in visit order and drops
    /// genuine divergent-region members when the meet member is
    /// dual-processed mid-level (the recorded defect, red-team fold item 2;
    /// with the maintainer, no production consumer reads these chains).
    /// Under the M5 in-level schedule this shape genuinely loses one of
    /// {a, b} whenever g's id sorts between theirs, so DivergedSince chain
    /// content is FREE-but-valid: what Q2 pins is that whatever members are
    /// emitted arrive in the canonical order of THEIR set, drawn from the
    /// side's divergent region, with the divergent tip present.
    #[tokio::test]
    async fn diverged_since_chains_are_emitted_canonical() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&g]);
        let m = make_test_event(4, &[&a, &b]);
        let c = make_test_event(5, &[&g]);
        for e in [&g, &a, &b, &m, &c] {
            retriever.add_event(e);
        }

        let result = compare(retriever, &clock!(m.id()), &clock!(c.id()), 100).await.unwrap();
        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } => {
                assert_eq!(meet, &vec![g.id()], "precondition: the diamond diverges from c at g");
                // Q2, the canonical-order half: byte-equal to the canonical
                // topo order of the emitted member set (permutation-stable
                // by construction of canonicalize_chain; re-derive it from
                // the true parent map here).
                let mut dag = BTreeMap::new();
                for e in [&g, &a, &b, &m, &c] {
                    dag.insert(e.id(), e.parent.as_slice().to_vec());
                }
                let expected_order = crate::event_dag::ordering::canonicalize_chain(subject_chain.clone(), &dag);
                assert_eq!(subject_chain, &expected_order, "the emitted members must arrive in the canonical order of their set");
                // FREE-but-valid content: members lie in the subject's
                // divergent region (never the meet), and the divergent tip
                // is present.
                let divergent: std::collections::BTreeSet<EventId> = [a.id(), b.id(), m.id()].into_iter().collect();
                assert!(subject_chain.iter().all(|id| divergent.contains(id)), "chain members stay within the divergent region");
                assert!(subject_chain.contains(&m.id()), "the divergent tip is present");
                assert_eq!(other_chain, &vec![c.id()], "the single-branch other chain is exact (no trim ambiguity)");
            }
            other => panic!("expected DivergedSince, got {other:?}"),
        }
    }
}

// ============================================================================
// APPLIED-SET CAP-THRASH CHARACTERIZATION (plan D2-5: M5 characterizes the
// cliff empirically). The applied-set's FIFO eviction turns an O(1) skip
// back into a comparison walk for redeliveries older than the cap window;
// the cliff is the fetch-count jump at the eviction boundary. Cost changes,
// outcome never does (R-D2-3c's law, re-asserted here at every sample).
// ============================================================================

#[cfg(test)]
mod applied_set_cap_thrash {
    use super::*;
    use crate::entity::Entity;

    fn lww_event_for(entity_id: EntityId, title: &str, parents: &[&Event]) -> Event {
        let backend = LWWBackend::new();
        backend.set("title".into(), Some(Value::String(title.into())));
        let ops = backend.to_operations().unwrap().unwrap();
        Event {
            entity_id,
            collection: "test".into(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
            parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
        }
    }

    /// Build a CHAIN entity with cap-bounded applied rows, then redeliver at
    /// sample positions across the eviction boundary and measure walk
    /// fetches. Inside the window: zero fetches (the O(1) skip). Outside:
    /// the comparison walk, whose fetch count grows with the redelivered
    /// event's depth below the head (the thrash cost is a FULL backward
    /// walk per evicted redelivery, not a constant). The printed table is
    /// the D2-5 empirical characterization for the PR evidence; run with
    /// --nocapture to see it.
    #[tokio::test]
    async fn redelivery_cost_cliff_at_the_cap_boundary() {
        const N: usize = 256;
        const CAP: usize = 64;
        let entity_id = {
            let mut b = [0u8; 16];
            b[0] = 99;
            EntityId::from_bytes(b)
        };
        let entity = Entity::create_with_applied_cap(entity_id, "test".into(), CAP);

        let mut retriever = MockRetriever::new();
        let mut chain: Vec<Event> = Vec::with_capacity(N);
        for i in 0..N {
            let ev = match chain.last() {
                None => lww_event_for(entity_id, &format!("t{i}"), &[]),
                Some(prev) => lww_event_for(entity_id, &format!("t{i}"), &[prev]),
            };
            retriever.add_event(&ev);
            chain.push(ev);
        }
        for ev in &chain {
            assert!(entity.apply_event(&retriever, ev, None).await.unwrap(), "the chain applies in order");
            // The post-persist hook's insertion half, simulated: each apply
            // is followed by a completed persist covering the event.
            entity.mark_applied([ev.id()]);
        }

        // FIFO eviction: only the last CAP ids remain members.
        assert!(!entity.applied_contains(&chain[N - CAP - 1].id()), "precondition: the id just below the window was evicted");
        assert!(entity.applied_contains(&chain[N - CAP].id()), "precondition: the oldest window member is retained");

        println!("D2-5 cap-thrash characterization: N={N} chain events, cap={CAP}");
        println!("position-from-head | member | walk fetches (per redelivery)");
        let samples = [1usize, CAP / 2, CAP - 1, CAP, CAP + 1, 2 * CAP, N / 2, N - 1];
        for depth_from_head in samples {
            let idx = N - 1 - depth_from_head;
            let ev = &chain[idx];
            let member = entity.applied_contains(&ev.id());
            let logging = LoggingRetriever::new(retriever.clone());
            let log = logging.log_handle();
            let applied = entity.apply_event(&logging, ev, None).await.unwrap();
            let fetches = log.lock().unwrap().len();
            assert!(!applied, "outcome invariance: a redelivery is a no-op regardless of membership (R-D2-3c)");
            if member {
                assert_eq!(fetches, 0, "an applied-set member redelivery is the O(1) skip: zero fetches");
            } else {
                assert!(fetches > 0, "an evicted redelivery walks (cost, never outcome)");
                assert!(
                    fetches >= depth_from_head.min(N),
                    "the walk grounds the redelivered event against the head: fetch count scales with its depth \
                     ({depth_from_head} below head), got {fetches}"
                );
            }
            println!("{depth_from_head:>18} | {member:>6} | {fetches}");
        }
    }
}

// ============================================================================
// THE BUDGET INVARIANT (red-team fold item 7): the walk decrements budget at
// most TWICE per distinct event under ANY schedule (once per side;
// Side::processed makes per-side expansion idempotent and the level snapshot
// deduplicates). The immunity oracle's non-binding-budget guarantee (budget
// = 4x corpus, neither run may BudgetExceed) rests on this bound; a frontier
// rewrite that breaks per-side once-only processing would turn
// BudgetExceeded into a schedule-dependent oracle artifact.
// ============================================================================

#[cfg(test)]
mod budget_invariant {
    use super::*;

    /// Staggered divergence maximizes dual-side re-encounters: subject
    /// {u} over m over g; comparison {v} over g. The two traversals reach g
    /// at DIFFERENT levels (comparison at level 2, subject at level 3), so
    /// g is decremented once per side; everything else once. Exact totals
    /// pin the accounting; the bound pins the law.
    #[tokio::test]
    async fn decrements_are_at_most_two_per_event_and_exact_on_the_staggered_seed() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let m = make_test_event(2, &[&g]);
        let u = make_test_event(3, &[&m]);
        let v = make_test_event(4, &[&g]);
        for e in [&g, &m, &u, &v] {
            retriever.add_event(e);
        }
        let n_events = 4;

        let result = compare(retriever, &clock!(u.id()), &clock!(v.id()), 2 * n_events).await.unwrap();
        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => assert_eq!(meet, &vec![g.id()]),
            other => panic!("expected DivergedSince at meet {{g}}, got {other:?}"),
        }
        assert_eq!(result.stats.budget_decrements, 5, "u, v, m once each; g once per side (the staggered dual re-encounter); nothing else");
        assert!(result.stats.budget_decrements <= 2 * n_events, "the <= 2 per event law");
        assert_eq!(result.stats.events_fetched, 4, "the re-encounter is an LRU hit, not a fetch");
    }

    /// PER-SIDE once-only processing is what carries the bound: a node
    /// reachable by two paths of DIFFERENT lengths within ONE side is
    /// decremented once for that side, because the longer path's late
    /// arrival finds it in the processed set and must not re-queue it.
    /// Shape: s parents {p, w}; p child of z; w child of x; x child of z;
    /// z child of g. The subject walk reaches z at level 3 via p and again
    /// at level 4 via x (the seed search fixes the in-level order so x is
    /// processed after z, the arrival order that would re-queue). Exact
    /// decrement count pins it: 6 events, one dual-side re-encounter (g),
    /// total exactly 7.
    #[tokio::test]
    async fn longer_path_re_arrival_does_not_requeue_a_processed_node() {
        let g = make_test_event(1, &[]);
        let z = make_test_event(2, &[&g]);
        let p = make_test_event(3, &[&z]);
        for seed in 0..u16::MAX {
            let x = make_test_event_u16(seed, &[&z]);
            if x.id() > z.id() {
                let w = make_test_event_u16(seed.wrapping_add(1), &[&x]);
                let s = make_test_event(4, &[&p, &w]);
                let mut retriever = MockRetriever::new();
                for e in [&g, &z, &p, &x, &w, &s] {
                    retriever.add_event(e);
                }
                let result = compare(retriever, &clock!(s.id()), &clock!(g.id()), 24).await.unwrap();
                assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
                assert_eq!(
                    result.stats.budget_decrements, 7,
                    "s, p, w, z, x once each on the subject side; g once per side; the level-4 \
                     re-arrival at z via x must not re-queue it"
                );
                assert!(result.stats.budget_decrements <= 2 * 6, "the <= 2 per event law");
                return;
            }
        }
        unreachable!("half of all seeds order x above z");
    }

    /// The same-level dual encounter costs ONE decrement: with equal-depth
    /// divergence (subject {x}, comparison {y}, both children of g) the two
    /// frontiers hold g in the same level and the deduplicated snapshot
    /// processes it once with both flags.
    #[tokio::test]
    async fn same_level_dual_encounter_decrements_once() {
        let mut retriever = MockRetriever::new();
        let g = make_test_event(1, &[]);
        let x = make_test_event(2, &[&g]);
        let y = make_test_event(3, &[&g]);
        for e in [&g, &x, &y] {
            retriever.add_event(e);
        }

        let result = compare(retriever, &clock!(x.id()), &clock!(y.id()), 6).await.unwrap();
        assert!(matches!(&result.relation, AbstractCausalRelation::DivergedSince { meet, .. } if meet == &vec![g.id()]));
        assert_eq!(result.stats.budget_decrements, 3, "x, y, and ONE dual-flag processing of g");
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
    pub(super) struct Rng(pub(super) u32);

    impl Rng {
        pub(super) fn next(&mut self) -> u32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            self.0 = x;
            x
        }

        pub(super) fn below(&mut self, n: usize) -> usize { (self.next() as usize) % n }
    }

    /// Inclusive ancestry over the true parent map (the oracle's reachability).
    pub(super) fn ancestry(parents: &BTreeMap<EventId, Vec<EventId>>, heads: &[EventId]) -> BTreeSet<EventId> {
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
    pub(super) fn to_antichain(parents: &BTreeMap<EventId, Vec<EventId>>, ids: &BTreeSet<EventId>) -> Vec<EventId> {
        ids.iter()
            .filter(|id| !ids.iter().any(|other| other != *id && ancestry(parents, std::slice::from_ref(other)).contains(*id)))
            .cloned()
            .collect()
    }

    #[derive(Debug)]
    pub(super) enum Expected {
        Equal,
        StrictDescends,
        StrictAscends,
        DivergedMeet(Vec<EventId>),
        Disjoint,
    }

    /// Brute-force specification of the comparison verdict. Precedence mirrors
    /// the machine: Equal, then the strict completions (which fire during
    /// traversal), then the exhaustion verdicts.
    pub(super) fn oracle(parents: &BTreeMap<EventId, Vec<EventId>>, subject: &[EventId], comparison: &[EventId]) -> Expected {
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

    pub(super) fn verdict_matches(expected: &Expected, actual: &AbstractCausalRelation<EventId>) -> bool {
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
// THE GEN-CORRUPTION IMMUNITY ORACLE (D2 M5 arm ii; oracle brief +
// red-team fold). Executable form of the 5b-ii theorem's quantifier: for
// fixed ids and fixed parent edges, ANY assignment of generation values
// observed by the comparison leaves verdicts, meets, and the layer
// partition identical. Corruption is injected through BOTH channels the
// consuming code reads (the fold's two-channel matrix): the PAYLOAD
// channel (a lying retriever serving doctored payloads under original
// ids) and the MATERIALIZATION channel (doctored operand GClocks feeding
// the prechecks and schedule seeds), plus cross-mismatch shapes where the
// channels disagree.
//
// BOUND observables (byte-identical or STOP-AND-REPORT): relation
// variant; the StrictDescends chain semantic core plus its completeness
// law; the DivergedSince meet, immediate-children sets, and full layer
// partition; the Disjoint variant. FREE-but-valid (may differ; targeted
// arms assert deltas on purpose): suppression, violation, demotion,
// fetch and decrement counters, fetch order, Disjoint root identities
// (first-reached, fold item 2), and DivergedSince chain content (the
// recorded build_forward_chain trim defect; canonical order and validity
// still hold per run). BudgetExceeded in ANY run is a harness bug, never
// tolerated: the budget is 4x corpus and the walk decrements at most
// twice per event.
//
// Corpus WIDTH: the randomized default and large tiers draw at most two
// parents per event and at most three clock tips, so they never build a
// wide frontier. The GC-WIDE-ANTICHAIN named seed (width 64, the
// event_dag bench's detonation shape) and the wide randomized tier
// (full-current-tip joins) close that class: the M5 edge-check
// registration explosion (fixed in ba1e5e52) hid exactly there, through
// nineteen commits of narrow-corpus soundness testing.
// ============================================================================

#[cfg(test)]
mod gen_corruption_immunity {
    use super::comparison_property::{ancestry, Rng};
    use super::*;
    use crate::event_dag::stats::CompareStats;
    use std::collections::BTreeSet;

    // ---- Corpus ----

    pub(super) struct Corpus {
        pub retriever: MockRetriever,
        pub events: Vec<Event>,
        pub ids: Vec<EventId>,
        pub parents_map: BTreeMap<EventId, Vec<EventId>>,
    }

    impl Corpus {
        pub fn from_events(events: Vec<Event>) -> Self {
            let mut retriever = MockRetriever::new();
            let mut ids = Vec::new();
            let mut parents_map = BTreeMap::new();
            for e in &events {
                retriever.add_event(e);
                ids.push(e.id());
                parents_map.insert(e.id(), e.parent.as_slice().to_vec());
            }
            Self { retriever, events, ids, parents_map }
        }

        pub fn event(&self, id: &EventId) -> &Event { self.events.iter().find(|e| e.id() == *id).expect("id minted from this corpus") }

        pub fn gen_of(&self, id: &EventId) -> u32 { self.event(id).generation }

        /// Honest operand annotation for a clock's tips (the materialized
        /// GClock a resident or wire state would carry).
        pub fn operand(&self, tips: &[EventId]) -> GClock {
            GClock::new(tips.iter().map(|t| (self.gen_of(t), t.clone())).collect::<Vec<_>>())
        }

        pub fn size(&self) -> usize { self.events.len() }
    }

    // ---- Doctoring (one corrupted run's worth of lies) ----

    #[derive(Default, Clone)]
    pub(super) struct Doctoring {
        /// Payload channel: original id -> corrupt generation.
        pub payloads: Vec<(EventId, u32)>,
        /// Materialization channel: per-tip operand overrides.
        pub subject_tips: Vec<(EventId, u32)>,
        pub comparison_tips: Vec<(EventId, u32)>,
    }

    impl Doctoring {
        pub fn is_empty(&self) -> bool { self.payloads.is_empty() && self.subject_tips.is_empty() && self.comparison_tips.is_empty() }

        /// The (id, original, corrupted) triples for the artifact line.
        pub fn triples(&self, corpus: &Corpus) -> String {
            let mut parts = Vec::new();
            for (id, corrupt) in &self.payloads {
                parts.push(format!("payload({id},{},{corrupt})", corpus.gen_of(id)));
            }
            for (id, corrupt) in &self.subject_tips {
                parts.push(format!("subject_tip({id},{},{corrupt})", corpus.gen_of(id)));
            }
            for (id, corrupt) in &self.comparison_tips {
                parts.push(format!("comparison_tip({id},{},{corrupt})", corpus.gen_of(id)));
            }
            format!("[{}]", parts.join(","))
        }

        /// The lying retriever plus the doctored-payload-id -> original-id
        /// map (layer events recompute doctored ids; captures map back).
        pub fn retriever(&self, corpus: &Corpus) -> (GenCorruptedRetriever, HashMap<EventId, EventId>) {
            let mut lying = GenCorruptedRetriever::new(corpus.retriever.clone());
            let mut back = HashMap::new();
            for (id, corrupt) in &self.payloads {
                let doctored = Event { generation: *corrupt, ..corpus.event(id).clone() };
                back.insert(doctored.id(), id.clone());
                lying.doctor(id.clone(), doctored);
            }
            (lying, back)
        }

        /// Operand GClocks with this doctoring's tip overrides applied.
        pub fn operands(&self, corpus: &Corpus, subject: &[EventId], comparison: &[EventId]) -> (GClock, GClock) {
            let apply = |tips: &[EventId], overrides: &[(EventId, u32)]| {
                GClock::new(
                    tips.iter()
                        .map(|t| {
                            let g = overrides.iter().find(|(id, _)| id == t).map(|(_, g)| *g).unwrap_or_else(|| corpus.gen_of(t));
                            (g, t.clone())
                        })
                        .collect::<Vec<_>>(),
                )
            };
            (apply(subject, &self.subject_tips), apply(comparison, &self.comparison_tips))
        }
    }

    // ---- Captured observables ----

    /// The BOUND set (byte-comparable between runs).
    #[derive(Debug, Clone, PartialEq)]
    pub(super) enum Bound {
        Equal,
        StrictAscends,
        StrictDescends {
            /// chain intersected with s_cover \ c_cover, in emitted
            /// (canonical) order.
            chain_core: Vec<EventId>,
        },
        DivergedSince {
            meet: Vec<EventId>,
            subject_children: Vec<EventId>,
            other_children: Vec<EventId>,
            /// Per layer: (sorted to_apply, sorted already_applied), ids
            /// mapped back to originals.
            layers: Vec<(Vec<EventId>, Vec<EventId>)>,
        },
        Disjoint,
    }

    pub(super) struct Capture {
        pub bound: Bound,
        pub stats: CompareStats,
        pub fetches: Vec<EventId>,
    }

    /// Drive one comparison and capture its observables, asserting the
    /// per-run validity laws (which hold for corrupted runs too: validity
    /// is about structure, which generation doctoring never touches).
    /// Panics with `context` on any harness-guarantee violation
    /// (BudgetExceeded, broken validity), which is a STOP-AND-REPORT
    /// artifact, not an acceptable outcome.
    pub(super) async fn run<G: GetEvents + Send + Sync>(
        getter: G,
        corpus: &Corpus,
        subject_ids: &[EventId],
        comparison_ids: &[EventId],
        subject_gens: GClock,
        comparison_gens: GClock,
        unverified: Option<&crate::ingest::UnverifiedEvents>,
        back_map: &HashMap<EventId, EventId>,
        context: &str,
    ) -> Capture {
        let subject = Clock::from(subject_ids.to_vec());
        let comparison = Clock::from(comparison_ids.to_vec());
        let budget = 4 * corpus.size();

        let logging = LoggingRetriever::new(getter);
        let log = logging.log_handle();
        let opts = CompareOptions { subject_gens: Some(subject_gens), comparison_gens: Some(comparison_gens), unverified };
        let result = compare_with(logging, &subject, &comparison, budget, opts).await.expect("oracle corpora are fully fetchable");

        let stats = result.stats.clone();
        assert!(
            stats.budget_decrements <= 2 * corpus.size(),
            "{context}: the <=2-decrements-per-event law broke (got {} over {} events); the oracle budget math is void",
            stats.budget_decrements,
            corpus.size()
        );

        let s_cover = ancestry(&corpus.parents_map, subject_ids);
        let c_cover = ancestry(&corpus.parents_map, comparison_ids);

        let bound = match &result.relation {
            AbstractCausalRelation::Equal => Bound::Equal,
            AbstractCausalRelation::StrictAscends => Bound::StrictAscends,
            AbstractCausalRelation::BudgetExceeded { .. } => {
                panic!(
                    "{context}: BudgetExceeded with budget 4x corpus is a HARNESS BUG (stop and report); never raise the budget in place"
                )
            }
            AbstractCausalRelation::StrictDescends { chain } => {
                let chain_set: BTreeSet<EventId> = chain.iter().cloned().collect();
                // The arm (i) completeness law, NOT weakened (fold item 2):
                // every event strictly between the covers is in the chain.
                for id in s_cover.difference(&c_cover) {
                    assert!(chain_set.contains(id), "{context}: StrictDescends chain completeness law broke: missing {id}");
                }
                // Validity: emitted order is a topological order over the
                // honest edges (generation doctoring preserves structure).
                assert_topo_order(chain, &corpus.parents_map, context);
                // The semantic core, presented CANONICALLY over the core set
                // itself: the emitted order restricted to the core is not
                // schedule-stable (overshoot extras can delay a core
                // member's Kahn readiness and reorder ties), so the byte
                // comparison binds the core SET in its own canonical order,
                // the sorted-set tier the oracle brief's chain ruling
                // admits. With the completeness law above, the core set
                // always equals s_cover minus c_cover, so this is defense
                // in depth.
                let core: Vec<EventId> = chain.iter().filter(|id| s_cover.contains(*id) && !c_cover.contains(*id)).cloned().collect();
                let core = crate::event_dag::ordering::canonicalize_chain(core, &corpus.parents_map);
                Bound::StrictDescends { chain_core: core }
            }
            AbstractCausalRelation::Disjoint { subject_root, other_root, .. } => {
                // Roots are FREE-but-valid (first-reached, fold item 2):
                // each reported root is genuinely parentless and reachable
                // from its side, and the two differ.
                for (root, cover, side) in [(subject_root, &s_cover, "subject"), (other_root, &c_cover, "comparison")] {
                    assert!(
                        corpus.parents_map.get(root).is_some_and(|p| p.is_empty()),
                        "{context}: Disjoint {side} root {root} is not parentless"
                    );
                    assert!(cover.contains(root), "{context}: Disjoint {side} root {root} is outside its side's cover");
                }
                assert_ne!(subject_root, other_root, "{context}: Disjoint roots must differ");
                Bound::Disjoint
            }
            AbstractCausalRelation::DivergedSince { meet, subject: s_children, other: o_children, subject_chain, other_chain } => {
                // Chains are FREE-but-valid: canonical order over honest
                // edges, members within their side's cover.
                for (chain, cover, side) in [(subject_chain, &s_cover, "subject"), (other_chain, &c_cover, "comparison")] {
                    assert_topo_order(chain, &corpus.parents_map, context);
                    for id in chain {
                        assert!(cover.contains(id), "{context}: DivergedSince {side} chain member {id} outside its cover");
                    }
                }
                let meet_bound = meet.clone();
                let s_children_bound = s_children.clone();
                let o_children_bound = o_children.clone();
                // The layer partition downstream of the meet is BOUND:
                // byte-identical sequence, ids mapped back to originals.
                let mut layers_bound = Vec::new();
                let mut layers = result.into_layers(comparison_ids.to_vec()).expect("DivergedSince always yields layers");
                while let Some(layer) = layers.next().await.expect("layer iteration fetches from the same corpus") {
                    let map_back = |events: &[Event]| {
                        let mut mapped: Vec<EventId> =
                            events.iter().map(|e| back_map.get(&e.id()).cloned().unwrap_or_else(|| e.id())).collect();
                        mapped.sort();
                        mapped
                    };
                    layers_bound.push((map_back(&layer.to_apply), map_back(&layer.already_applied)));
                }
                Bound::DivergedSince {
                    meet: meet_bound,
                    subject_children: s_children_bound,
                    other_children: o_children_bound,
                    layers: layers_bound,
                }
            }
        };

        let fetches = log.lock().unwrap().clone();
        Capture { bound, stats, fetches }
    }

    fn assert_topo_order(chain: &[EventId], parents_map: &BTreeMap<EventId, Vec<EventId>>, context: &str) {
        let mut seen: BTreeSet<EventId> = BTreeSet::new();
        let in_chain: BTreeSet<EventId> = chain.iter().cloned().collect();
        for id in chain {
            if let Some(parents) = parents_map.get(id) {
                for p in parents {
                    if in_chain.contains(p) {
                        assert!(seen.contains(p), "{context}: chain is not a topological order: {id} precedes its parent {p}");
                    }
                }
            }
            seen.insert(id.clone());
        }
    }

    /// The extended ORACLEFAIL artifact line (oracle brief 3.4): one line,
    /// self-contained, carrying the shape, the corruption seed, the
    /// doctored triples, and the exact single-seed repro command.
    pub(super) fn gen_artifact_line(
        dag_seed: u32,
        shape: &str,
        corruption_seed: u32,
        subject: &[EventId],
        comparison: &[EventId],
        triples: &str,
        baseline: &Bound,
        corrupted: &Bound,
    ) -> String {
        format!(
            "ORACLEFAIL kind=gen_corruption dag_seed={dag_seed} shape={shape} corruption_seed={corruption_seed} subject={subject:?} comparison={comparison:?} doctored={triples} baseline={baseline:?} corrupted={corrupted:?} reproduce=\"GEN_ORACLE_SEED_BASE={dag_seed} GEN_ORACLE_SEEDS=1 GEN_ORACLE_SHAPE={shape} cargo test -p ankurah-core --lib event_dag::tests::gen_corruption_immunity::randomized_corpora_are_immune_to_generation_corruption -- --exact --nocapture\""
        )
    }

    /// Assert the corrupted run's BOUND observables equal the baseline's,
    /// panicking with the full artifact line on ANY divergence
    /// (STOP-AND-REPORT; there is no acceptable divergence and no
    /// fix-forward).
    pub(super) fn assert_bound_identical(
        dag_seed: u32,
        shape: &str,
        corruption_seed: u32,
        subject: &[EventId],
        comparison: &[EventId],
        triples: &str,
        baseline: &Capture,
        corrupted: &Capture,
    ) {
        if baseline.bound != corrupted.bound {
            panic!(
                "{}",
                gen_artifact_line(dag_seed, shape, corruption_seed, subject, comparison, triples, &baseline.bound, &corrupted.bound)
            );
        }
    }

    // ---- Named seed corpora (derivations sections 4.1 and 4.3) ----

    /// GC-SEED-41: the derivations 4.1 counterexample DAG. Returns the
    /// corpus plus (subject tip s, comparison tip c, c1, m) for meet pins.
    pub(super) fn seed_41() -> (Corpus, EventId, EventId, EventId, EventId) {
        let g = make_test_event(1, &[]);
        let x = make_test_event(2, &[&g]);
        let y = make_test_event(3, &[&x]);
        let z = make_test_event(4, &[&y]);
        let c1 = make_test_event(5, &[&z]);
        let m = make_test_event(6, &[&g]);
        let u = make_test_event(7, &[&m]);
        let v = make_test_event(8, &[&m]);
        let s = make_test_event(9, &[&c1, &u]);
        let c = make_test_event(10, &[&c1, &v]);
        let (s_id, c_id, c1_id, m_id) = (s.id(), c.id(), c1.id(), m.id());
        (Corpus::from_events(vec![g, x, y, z, c1, m, u, v, s, c]), s_id, c_id, c1_id, m_id)
    }

    /// GC-SEED-43: the derivations 4.3 self-refutation DAG. Returns the
    /// corpus plus (A, B, f) for the meet pin.
    pub(super) fn seed_43() -> (Corpus, EventId, EventId, EventId) {
        let g = make_test_event(1, &[]);
        let m = make_test_event(2, &[&g]);
        let c = make_test_event(3, &[&m]);
        let f = make_test_event(4, &[&c]);
        let a = make_test_event(5, &[&f, &m]);
        let b = make_test_event(6, &[&f, &m]);
        let (a_id, b_id, f_id) = (a.id(), b.id(), f.id());
        (Corpus::from_events(vec![g, m, c, f, a, b]), a_id, b_id, f_id)
    }

    /// GC-WIDE-ANTICHAIN width: the event_dag bench's detonation width
    /// (compare/wide_antichain/64), where the pre-ba1e5e52 edge-check
    /// registration explosion took one comparison past 11 GB RSS.
    pub(super) const WIDE_ANTICHAIN_WIDTH: usize = 64;

    /// GC-WIDE-ANTICHAIN: one root, WIDE_ANTICHAIN_WIDTH concurrent
    /// children, one join naming all of them as parents. The mandatory
    /// wide-frontier seed (blind-spot closure): no other corpus in this
    /// module walks a frontier wider than three, so nothing here ever
    /// saturated the blocked-edge-check bookkeeping until this seed.
    /// Returns the corpus plus (join, root, child_a, child_b) for the
    /// three pinned pairs: join vs root (the walk holds every join edge
    /// blocked at once), sibling vs sibling (wide divergence meeting at
    /// the root), and the full child antichain as a width-64 CLOCK vs
    /// root (wide operand GClocks and frontier initialization).
    pub(super) fn seed_wide_antichain() -> (Corpus, EventId, EventId, EventId, EventId) {
        let root = make_test_event_u16(1, &[]);
        let children: Vec<Event> = (0..WIDE_ANTICHAIN_WIDTH).map(|i| make_test_event_u16(i as u16 + 2, &[&root])).collect();
        let join = make_test_event_u16(WIDE_ANTICHAIN_WIDTH as u16 + 2, &children.iter().collect::<Vec<_>>());
        let (join_id, root_id, ca_id, cb_id) = (join.id(), root.id(), children[0].id(), children[1].id());
        let mut events = vec![root];
        events.extend(children);
        events.push(join);
        (Corpus::from_events(events), join_id, root_id, ca_id, cb_id)
    }

    /// Every child of the wide-antichain corpus's root, as a width-64
    /// antichain clock (the merged-head shape a resident would carry).
    pub(super) fn wide_antichain_children(corpus: &Corpus, root: &EventId) -> Vec<EventId> {
        corpus.ids.iter().filter(|id| corpus.parents_map[*id].as_slice() == std::slice::from_ref(root)).cloned().collect()
    }

    /// The 4.1 baseline meet is EXACTLY {c1, m}: the DAG on which the RFC's
    /// generation-threshold stop rule would have wrongly concluded {c1},
    /// missing the low-generation common branch head m (derivations 4.1).
    /// Pinned BEFORE any corruption runs against this corpus.
    #[tokio::test]
    async fn gc_seed_41_baseline_meet_is_c1_and_m() {
        let (corpus, s, c, c1, m) = seed_41();
        let (sg, cg) = Doctoring::default().operands(&corpus, std::slice::from_ref(&s), std::slice::from_ref(&c));
        let capture = run(corpus.retriever.clone(), &corpus, &[s], &[c], sg, cg, None, &HashMap::new(), "gc_seed_41 baseline").await;
        match &capture.bound {
            Bound::DivergedSince { meet, .. } => {
                let mut expected = vec![c1, m];
                expected.sort();
                assert_eq!(meet, &expected, "the 4.1 ground truth: meet EXACTLY {{c1, m}}");
            }
            other => panic!("gc_seed_41 baseline must be DivergedSince, got {other:?}"),
        }
        assert_eq!(capture.stats.edge_check_violations, 0, "the honest build stamps cleanly");
    }

    /// The 4.3 baseline meet is EXACTLY {f}: m is evicted by its common
    /// child c, which the all-frontier-common stop rule could not see
    /// (derivations 4.3). Pinned BEFORE any corruption runs.
    #[tokio::test]
    async fn gc_seed_43_baseline_meet_is_f() {
        let (corpus, a, b, f) = seed_43();
        let (sg, cg) = Doctoring::default().operands(&corpus, std::slice::from_ref(&a), std::slice::from_ref(&b));
        let capture = run(corpus.retriever.clone(), &corpus, &[a], &[b], sg, cg, None, &HashMap::new(), "gc_seed_43 baseline").await;
        match &capture.bound {
            Bound::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![f], "the 4.3 ground truth: meet EXACTLY {{f}} (m evicted by its common child)");
            }
            other => panic!("gc_seed_43 baseline must be DivergedSince, got {other:?}"),
        }
        assert_eq!(capture.stats.edge_check_violations, 0, "the honest build stamps cleanly");
    }

    /// The wide-antichain baselines, pinned BEFORE any corruption runs:
    /// join vs root is a StrictDescends whose backward walk fetches the
    /// join first and holds all 64 of its edges blocked at once (one such
    /// comparison exceeded 11 GB RSS before ba1e5e52; run()'s budget and
    /// validity laws now bound it), sibling vs sibling diverges with meet
    /// EXACTLY {root}, and the full 64-tip child antichain as a CLOCK
    /// strictly descends the root.
    #[tokio::test]
    async fn gc_wide_antichain_baselines_descend_and_meet_at_root() {
        let (corpus, join, root, ca, cb) = seed_wide_antichain();

        let (sg, cg) = Doctoring::default().operands(&corpus, std::slice::from_ref(&join), std::slice::from_ref(&root));
        let capture = run(
            corpus.retriever.clone(),
            &corpus,
            &[join.clone()],
            &[root.clone()],
            sg,
            cg,
            None,
            &HashMap::new(),
            "gc_wide_antichain descent baseline",
        )
        .await;
        assert!(matches!(capture.bound, Bound::StrictDescends { .. }), "join vs root must be StrictDescends, got {:?}", capture.bound);
        assert_eq!(capture.stats.edge_check_violations, 0, "the honest build stamps cleanly");

        let (sg, cg) = Doctoring::default().operands(&corpus, std::slice::from_ref(&ca), std::slice::from_ref(&cb));
        let capture = run(
            corpus.retriever.clone(),
            &corpus,
            &[ca.clone()],
            &[cb.clone()],
            sg,
            cg,
            None,
            &HashMap::new(),
            "gc_wide_antichain sibling baseline",
        )
        .await;
        match &capture.bound {
            Bound::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![root.clone()], "wide siblings meet EXACTLY at the root");
            }
            other => panic!("gc_wide_antichain sibling baseline must be DivergedSince, got {other:?}"),
        }
        assert_eq!(capture.stats.edge_check_violations, 0, "the honest build stamps cleanly");

        let antichain = wide_antichain_children(&corpus, &root);
        assert_eq!(antichain.len(), WIDE_ANTICHAIN_WIDTH, "shape check: the full child antichain");
        let (sg, cg) = Doctoring::default().operands(&corpus, &antichain, std::slice::from_ref(&root));
        let capture = run(
            corpus.retriever.clone(),
            &corpus,
            &antichain,
            &[root.clone()],
            sg,
            cg,
            None,
            &HashMap::new(),
            "gc_wide_antichain clock baseline",
        )
        .await;
        assert!(
            matches!(capture.bound, Bound::StrictDescends { .. }),
            "the width-64 clock must strictly descend the root, got {:?}",
            capture.bound
        );
        assert_eq!(capture.stats.edge_check_violations, 0, "the honest build stamps cleanly");
    }

    /// GC-CONTROL, the alarm-rings canary (harness self-test): STRUCTURAL
    /// corruption (a served payload with a dropped parent) genuinely
    /// changes the outcome, and the harness must DETECT the divergence.
    /// Guards against a vacuously green oracle (a doctor map that never
    /// engages, or a comparison of a run against itself). The detected
    /// divergence is asserted, then swallowed.
    #[tokio::test]
    async fn gc_control_canary_detects_structural_divergence() {
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        let corpus = Corpus::from_events(vec![g.clone(), a.clone(), b.clone()]);
        let (sg, cg) = Doctoring::default().operands(&corpus, &[b.id()], &[g.id()]);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &[b.id()], &[g.id()], sg.clone(), cg.clone(), None, &HashMap::new(), "canary baseline")
                .await;
        assert!(matches!(baseline.bound, Bound::StrictDescends { .. }), "precondition: honest verdict is StrictDescends");

        // Structural lie: a served with an EMPTY parent clock (g dropped).
        // Post-R1 the walk adopts the served structure, so b's ancestry
        // bottoms out at a and the sides are disjoint. NOTE: the doctored
        // event changes STRUCTURE, so the corpus validity laws do not
        // apply; drive compare_with directly rather than through run().
        let mut lying = GenCorruptedRetriever::new(corpus.retriever.clone());
        lying.doctor(a.id(), Event { parent: Clock::default(), ..a.clone() });
        let corrupted = compare_with(
            lying,
            &clock!(b.id()),
            &clock!(g.id()),
            4 * corpus.size(),
            CompareOptions { subject_gens: Some(sg), comparison_gens: Some(cg), unverified: None },
        )
        .await
        .unwrap();

        let diverged = !matches!(corrupted.relation, AbstractCausalRelation::StrictDescends { .. });
        assert!(
            diverged,
            "THE CANARY IS DEAD: structural corruption produced the baseline verdict; the harness cannot detect \
             divergence and every green immunity result is vacuous. Got {:?}",
            corrupted.relation
        );
    }

    // ---- Corruption shapes (oracle brief section 2, per channel where
    // meaningful; the fold's two-channel matrix) ----

    /// All randomized sub-arm shapes. Payload shapes corrupt what the lying
    /// retriever serves; gclock shapes corrupt the operand annotations the
    /// prechecks and schedule seeds read; cross corrupts both independently
    /// (the channels then usually DISAGREE, the materialization-drift shape
    /// amendment K's bookkeeping worries about).
    pub(super) const SHAPES: &[&str] = &[
        "gc-rand-payload",
        "gc-rand-gclock",
        "gc-rand-cross",
        "gc-deflate-meet",
        "gc-inflate-tip",
        "gc-sat-interior",
        "gc-sat-tips",
        "gc-genesis",
    ];

    /// The biased adversarial value mix (oracle brief GC-RAND).
    fn corrupt_value(rng: &mut Rng, truth: u32) -> u32 {
        match rng.below(5) {
            0 => 0,
            1 => 1,
            2 => {
                let delta = 1 + rng.below(3) as u32;
                if rng.below(2) == 0 {
                    truth.saturating_add(delta)
                } else {
                    truth.saturating_sub(delta)
                }
            }
            3 => 4_000_000_000u32.saturating_add(rng.below(1000) as u32),
            _ => u32::MAX,
        }
    }

    /// Build one shape's doctoring for a (corpus, pair). Returns None when
    /// the shape is not meaningful on this pair (e.g. deflate-meet on a
    /// non-diverged baseline), which the caller skips.
    fn shape_doctoring(
        shape: &str,
        rng: &mut Rng,
        corpus: &Corpus,
        subject: &[EventId],
        comparison: &[EventId],
        baseline: &Bound,
    ) -> Option<Doctoring> {
        let mut d = Doctoring::default();
        match shape {
            "gc-rand-payload" => {
                for id in &corpus.ids {
                    if rng.below(2) == 0 {
                        let truth = corpus.gen_of(id);
                        d.payloads.push((id.clone(), corrupt_value(rng, truth)));
                    }
                }
                if d.payloads.is_empty() {
                    let id = corpus.ids[rng.below(corpus.ids.len())].clone();
                    let truth = corpus.gen_of(&id);
                    d.payloads.push((id, corrupt_value(rng, truth)));
                }
            }
            "gc-rand-gclock" => {
                for tip in subject.iter() {
                    if rng.below(2) == 0 {
                        d.subject_tips.push((tip.clone(), corrupt_value(rng, corpus.gen_of(tip))));
                    }
                }
                for tip in comparison.iter() {
                    if rng.below(2) == 0 {
                        d.comparison_tips.push((tip.clone(), corrupt_value(rng, corpus.gen_of(tip))));
                    }
                }
                if d.is_empty() {
                    let tip = &comparison[rng.below(comparison.len())];
                    d.comparison_tips.push((tip.clone(), corrupt_value(rng, corpus.gen_of(tip))));
                }
            }
            "gc-rand-cross" => {
                // Both channels, independent draws: where they hit the same
                // tip they usually disagree (the drift shape).
                let id = corpus.ids[rng.below(corpus.ids.len())].clone();
                let truth = corpus.gen_of(&id);
                d.payloads.push((id, corrupt_value(rng, truth)));
                for tip in subject.iter().chain(comparison.iter()) {
                    if rng.below(2) == 0 {
                        d.payloads.push((tip.clone(), corrupt_value(rng, corpus.gen_of(tip))));
                    }
                    if rng.below(2) == 0 {
                        let over = corrupt_value(rng, corpus.gen_of(tip));
                        if subject.contains(tip) {
                            d.subject_tips.push((tip.clone(), over));
                        } else {
                            d.comparison_tips.push((tip.clone(), over));
                        }
                    }
                }
                if d.subject_tips.is_empty() && d.comparison_tips.is_empty() {
                    let tip = &subject[rng.below(subject.len())];
                    d.subject_tips.push((tip.clone(), corrupt_value(rng, corpus.gen_of(tip))));
                }
            }
            "gc-deflate-meet" => {
                // Meet-boundary deflation (the delta red-team BLOCKER 1
                // recipe generalized): meet members and their immediate
                // children doctored below their parents' values.
                let Bound::DivergedSince { meet, subject_children, other_children, .. } = baseline else {
                    return None;
                };
                for id in meet.iter().chain(subject_children.iter()).chain(other_children.iter()) {
                    d.payloads.push((id.clone(), if rng.below(2) == 0 { 0 } else { 1 }));
                }
                if d.payloads.is_empty() {
                    return None;
                }
            }
            "gc-inflate-tip" => {
                // Operand-channel inflation of one comparison tip: the
                // wrong-suppression direction, materialization channel.
                let tip = &comparison[rng.below(comparison.len())];
                d.comparison_tips.push((tip.clone(), 4_000_000_000));
            }
            "gc-sat-interior" => {
                // Interior u32::MAX with tips left honest: a shape honest
                // stamping cannot produce; tip eligibility stays ENABLED
                // over a corrupt-saturated interior, so soundness must come
                // from suppress-only wiring alone.
                let tips: BTreeSet<&EventId> = subject.iter().chain(comparison.iter()).collect();
                for id in &corpus.ids {
                    if !tips.contains(id) {
                        d.payloads.push((id.clone(), u32::MAX));
                    }
                }
                if d.payloads.is_empty() {
                    return None; // every event is a tip; no interior exists
                }
            }
            "gc-sat-tips" => {
                // Tips at MAX through BOTH channels: the disable rule must
                // fire (prechecks self-disable; never a suppression) and the
                // run must equal baseline.
                for tip in subject.iter() {
                    d.payloads.push((tip.clone(), u32::MAX));
                    d.subject_tips.push((tip.clone(), u32::MAX));
                }
                for tip in comparison.iter() {
                    d.payloads.push((tip.clone(), u32::MAX));
                    d.comparison_tips.push((tip.clone(), u32::MAX));
                }
            }
            "gc-genesis" => {
                // Genesis-not-1: the genesis rule is ADMISSION's law; the
                // comparison detects roots by empty parent clocks and must
                // not care what a genesis claims.
                for id in &corpus.ids {
                    if corpus.parents_map[id].is_empty() {
                        let v = match rng.below(3) {
                            0 => 0,
                            1 => 7,
                            _ => u32::MAX,
                        };
                        d.payloads.push((id.clone(), v));
                    }
                }
                if d.payloads.is_empty() {
                    return None;
                }
            }
            other => panic!("unknown corruption shape {other}"),
        }
        Some(d)
    }

    // ---- Randomized corpus generation (shared with the plain property
    // test's idiom: xorshift, occasional extra roots, antichain clocks) ----

    /// `wide_joins: false` is the original tier: one or two parents per
    /// event, so no wide frontier ever forms (the blind spot the M5
    /// edge-check defect hid in). `wide_joins: true` is the wide tier:
    /// single-parent draws let tips accumulate, and an occasional event
    /// joins ALL current tips (a full-frontier join, width bounded only by
    /// how many tips the band accrued; routinely 8..=20 at the wide
    /// tier's sizes). Tips of a DAG are pairwise incomparable, so the
    /// full-tip parent set is already an antichain.
    fn generate_corpus(rng: &mut Rng, n_events: usize, wide_joins: bool) -> Corpus {
        let mut events: Vec<Event> = Vec::new();
        let mut ids: Vec<EventId> = Vec::new();
        let mut parents_map: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
        let mut tips: BTreeSet<EventId> = BTreeSet::new();
        for i in 0..n_events {
            let parent_ids: Vec<EventId> = if i == 0 || rng.below(6) == 0 {
                vec![]
            } else if wide_joins && tips.len() >= 2 && rng.below(12) == 0 {
                tips.iter().cloned().collect()
            } else {
                let mut chosen = BTreeSet::new();
                let draws = if wide_joins { 1 } else { 1 + rng.below(2) };
                for _ in 0..draws {
                    chosen.insert(ids[rng.below(ids.len())].clone());
                }
                super::comparison_property::to_antichain(&parents_map, &chosen)
            };
            let parent_events: Vec<&Event> =
                parent_ids.iter().map(|pid| events.iter().find(|e| e.id() == *pid).expect("parent id was minted above")).collect();
            let ev = make_test_event_u16(i as u16 + 1, &parent_events);
            for p in &parent_ids {
                tips.remove(p);
            }
            tips.insert(ev.id());
            parents_map.insert(ev.id(), parent_ids);
            ids.push(ev.id());
            events.push(ev);
        }
        Corpus::from_events(events)
    }

    fn pick_clock(rng: &mut Rng, corpus: &Corpus) -> Vec<EventId> {
        let mut set = BTreeSet::new();
        for _ in 0..(1 + rng.below(3)) {
            set.insert(corpus.ids[rng.below(corpus.ids.len())].clone());
        }
        super::comparison_property::to_antichain(&corpus.parents_map, &set)
    }

    // ---- Env knobs (dedicated, oracle brief section 6: the immunity arm
    // scales independently of the plain oracle) ----

    fn gen_seed_base() -> u32 { std::env::var("GEN_ORACLE_SEED_BASE").ok().and_then(|s| s.parse().ok()).unwrap_or(1) }
    fn gen_seed_count() -> u32 { std::env::var("GEN_ORACLE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(300) }
    fn gen_large_seed_count() -> u32 { std::env::var("GEN_ORACLE_LARGE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(20) }
    fn gen_wide_seed_count() -> u32 { std::env::var("GEN_ORACLE_WIDE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(10) }
    fn gen_shape_filter() -> Option<String> { std::env::var("GEN_ORACLE_SHAPE").ok() }

    /// Documented corruption-seed mix: one xorshift stream per
    /// (dag_seed, shape), so shapes are independent and each is
    /// reproducible from the artifact line alone.
    fn corruption_seed(dag_seed: u32, shape_index: usize) -> u32 {
        dag_seed.wrapping_mul(0x9E37_79B9) ^ ((shape_index as u32 + 1).wrapping_mul(0x517C_C1B7)) | 1
    }

    /// Drive every shape of the two-channel matrix over one generated
    /// corpus size band. Returns how many corrupted runs showed any FREE
    /// delta (the GC-RAND aggregate non-vacuity numerator) and the
    /// corpus's maximum parent degree (the wide tier's own non-vacuity
    /// observable: proof its bands actually contain wide joins).
    async fn drive_seed_band(
        dag_seed: u32,
        n_min: usize,
        n_span: usize,
        shape_filter: &Option<String>,
        wide_joins: bool,
    ) -> (usize, usize) {
        let mut rng = Rng(dag_seed.wrapping_mul(0x9E37_79B9) | 1);
        let n_events = n_min + rng.below(n_span);
        let corpus = generate_corpus(&mut rng, n_events, wide_joins);
        let max_parent_degree = corpus.parents_map.values().map(Vec::len).max().unwrap_or(0);
        let mut free_deltas = 0usize;

        for _pair in 0..4 {
            let subject = pick_clock(&mut rng, &corpus);
            let comparison = pick_clock(&mut rng, &corpus);

            // Honest baseline. Its own laws: the builder stamps honestly
            // (zero edge violations), the verdict matches the brute-force
            // reachability oracle (arm i for compare_with), and every
            // payload generation equals its brute-force depth (arm iv).
            let (sg, cg) = Doctoring::default().operands(&corpus, &subject, &comparison);
            let baseline =
                run(corpus.retriever.clone(), &corpus, &subject, &comparison, sg, cg, None, &HashMap::new(), "immunity baseline").await;
            assert_eq!(
                baseline.stats.edge_check_violations, 0,
                "dag_seed={dag_seed}: the corpus builder must stamp honestly (its own correctness check)"
            );
            let expected = super::comparison_property::oracle(&corpus.parents_map, &subject, &comparison);
            assert!(
                bound_matches_expected(&expected, &baseline.bound),
                "dag_seed={dag_seed}: arm (i): the accelerated baseline diverged from the brute-force oracle: expected {expected:?}, got {:?}",
                baseline.bound
            );
            assert_depths_equal_generations(&corpus, dag_seed);

            for (shape_index, shape) in SHAPES.iter().enumerate() {
                if let Some(filter) = shape_filter {
                    if filter != shape {
                        continue;
                    }
                }
                let mut shape_rng = Rng(corruption_seed(dag_seed, shape_index));
                let Some(doctoring) = shape_doctoring(shape, &mut shape_rng, &corpus, &subject, &comparison, &baseline.bound) else {
                    continue;
                };
                let (lying, back) = doctoring.retriever(&corpus);
                let (sg, cg) = doctoring.operands(&corpus, &subject, &comparison);
                let corrupted =
                    run(lying, &corpus, &subject, &comparison, sg, cg, None, &back, &format!("shape {shape} dag_seed {dag_seed}")).await;
                assert_bound_identical(
                    dag_seed,
                    shape,
                    corruption_seed(dag_seed, shape_index),
                    &subject,
                    &comparison,
                    &doctoring.triples(&corpus),
                    &baseline,
                    &corrupted,
                );
                if corrupted.stats != baseline.stats || corrupted.fetches != baseline.fetches {
                    free_deltas += 1;
                }
            }
        }
        (free_deltas, max_parent_degree)
    }

    fn bound_matches_expected(expected: &super::comparison_property::Expected, bound: &Bound) -> bool {
        use super::comparison_property::Expected;
        match (expected, bound) {
            (Expected::Equal, Bound::Equal) => true,
            (Expected::StrictDescends, Bound::StrictDescends { .. }) => true,
            (Expected::StrictAscends, Bound::StrictAscends) => true,
            (Expected::DivergedMeet(want), Bound::DivergedSince { meet, .. }) => {
                let mut got = meet.clone();
                got.sort();
                let mut want = want.clone();
                want.sort();
                got == want
            }
            (Expected::Disjoint, Bound::Disjoint) => true,
            _ => false,
        }
    }

    /// Arm (iv): every honest payload generation equals its brute-force
    /// topological depth over the true parent map (a transient
    /// assertion-time walk, not a store).
    fn assert_depths_equal_generations(corpus: &Corpus, dag_seed: u32) {
        fn depth(id: &EventId, parents: &BTreeMap<EventId, Vec<EventId>>, memo: &mut HashMap<EventId, u32>) -> u32 {
            if let Some(d) = memo.get(id) {
                return *d;
            }
            let ps = &parents[id];
            let d = if ps.is_empty() { 1 } else { 1 + ps.iter().map(|p| depth(p, parents, memo)).max().expect("nonempty") };
            memo.insert(id.clone(), d);
            d
        }
        let mut memo = HashMap::new();
        for id in &corpus.ids {
            let expected = depth(id, &corpus.parents_map, &mut memo);
            assert_eq!(corpus.gen_of(id), expected, "dag_seed={dag_seed}: arm (iv): honest stamp diverged from brute-force depth for {id}");
        }
    }

    /// ARM (ii), the randomized two-channel matrix at gate defaults
    /// (GEN_ORACLE_SEED_BASE / GEN_ORACLE_SEEDS / GEN_ORACLE_SHAPE scale it
    /// out; the nightly runs 25k seeds). Every corrupted run must be
    /// BOUND-identical to its honest baseline; across the whole arm at
    /// least one run must show a FREE delta (aggregate non-vacuity: the
    /// corruption BITES, the oracle is not comparing a run to itself).
    #[tokio::test]
    async fn randomized_corpora_are_immune_to_generation_corruption() {
        let base = gen_seed_base();
        let count = gen_seed_count();
        let filter = gen_shape_filter();
        let mut free_deltas = 0usize;
        for dag_seed in base..base.saturating_add(count) {
            free_deltas += drive_seed_band(dag_seed, 5, 6, &filter, false).await.0;
        }
        assert!(
            free_deltas > 0,
            "aggregate non-vacuity failed: no corrupted run showed even a FREE delta; the doctor map cannot be engaging"
        );
    }

    /// The larger-DAG variant (oracle brief section 3.1): 20..=40 events
    /// exercise frontier ordering far harder than the 5..=10 default. A
    /// token count rides the gate (GEN_ORACLE_LARGE_SEEDS, default 20);
    /// the nightly scales it.
    #[tokio::test]
    async fn randomized_large_corpora_are_immune_to_generation_corruption() {
        let base = gen_seed_base();
        let count = gen_large_seed_count();
        let filter = gen_shape_filter();
        for dag_seed in base..base.saturating_add(count) {
            drive_seed_band(dag_seed.wrapping_add(0x4000_0000), 20, 21, &filter, false).await;
        }
    }

    /// The WIDE-frontier variant (blind-spot closure, with ba1e5e52):
    /// corpora whose generator emits full-current-tip joins over 24..=48
    /// events, so the walk's blocked-edge bookkeeping and the frontier
    /// ordering see wide parent sets nobody hand-picked. A token count
    /// rides the gate (GEN_ORACLE_WIDE_SEEDS, default 10: ten bands of
    /// four pairs each, roughly a second; the deterministic width-64 pin
    /// is gc_wide_antichain_is_immune_under_every_shape); the nightly
    /// scales it. The band-wide max parent degree is asserted so the tier
    /// can never silently degenerate back to narrow corpora.
    #[tokio::test]
    async fn randomized_wide_corpora_are_immune_to_generation_corruption() {
        let base = gen_seed_base();
        let count = gen_wide_seed_count();
        let filter = gen_shape_filter();
        let mut widest = 0usize;
        for dag_seed in base..base.saturating_add(count) {
            widest = widest.max(drive_seed_band(dag_seed.wrapping_add(0x2000_0000), 24, 25, &filter, true).await.1);
        }
        assert!(widest >= 8, "the wide tier must actually generate wide joins (widest parent set seen: {widest}); the generator's full-tip join path cannot be engaging");
    }

    // ---- Named deterministic sub-arm pins (always-on; never env-scaled) ----

    /// Every shape of the matrix against the 4.1 counterexample DAG, whose
    /// meet {c1, m} is structurally delicate (the RFC stop rule broke on
    /// it). All shapes, both channels, BOUND-identical.
    #[tokio::test]
    async fn gc_seed_41_is_immune_under_every_shape() {
        let (corpus, s, c, _c1, _m) = seed_41();
        assert_seed_immune(&corpus, &[s], &[c], "gc_seed_41").await;
    }

    /// Every shape against the 4.3 self-refutation DAG (meet {f}, where a
    /// wrong stop rule kept the evicted m).
    #[tokio::test]
    async fn gc_seed_43_is_immune_under_every_shape() {
        let (corpus, a, b, _f) = seed_43();
        assert_seed_immune(&corpus, &[a], &[b], "gc_seed_43").await;
    }

    /// Every shape against the width-64 antichain, on all three pinned
    /// pairs (descent across the join, wide-sibling divergence, and the
    /// width-64 clock). The arm-ii closure of the wide-frontier blind
    /// spot: corruption on a shape whose walk saturates the blocked-edge
    /// bookkeeping (and whose operand GClocks carry 64 tips) must still
    /// move nothing BOUND.
    #[tokio::test]
    async fn gc_wide_antichain_is_immune_under_every_shape() {
        let (corpus, join, root, ca, cb) = seed_wide_antichain();
        assert_seed_immune(&corpus, std::slice::from_ref(&join), std::slice::from_ref(&root), "gc_wide_antichain descent").await;
        assert_seed_immune(&corpus, &[ca], &[cb], "gc_wide_antichain siblings").await;
        let antichain = wide_antichain_children(&corpus, &root);
        assert_seed_immune(&corpus, &antichain, &[root], "gc_wide_antichain clock").await;
    }

    async fn assert_seed_immune(corpus: &Corpus, subject: &[EventId], comparison: &[EventId], name: &str) {
        let (sg, cg) = Doctoring::default().operands(corpus, subject, comparison);
        let baseline =
            run(corpus.retriever.clone(), corpus, subject, comparison, sg, cg, None, &HashMap::new(), &format!("{name} baseline")).await;
        for (shape_index, shape) in SHAPES.iter().enumerate() {
            let mut shape_rng = Rng(corruption_seed(0xD2D2_0041, shape_index));
            let Some(doctoring) = shape_doctoring(shape, &mut shape_rng, corpus, subject, comparison, &baseline.bound) else {
                continue;
            };
            let (lying, back) = doctoring.retriever(corpus);
            let (sg, cg) = doctoring.operands(corpus, subject, comparison);
            let corrupted = run(lying, corpus, subject, comparison, sg, cg, None, &back, &format!("{name} shape {shape}")).await;
            assert_bound_identical(
                0xD2D2_0041,
                shape,
                corruption_seed(0xD2D2_0041, shape_index),
                subject,
                comparison,
                &doctoring.triples(corpus),
                &baseline,
                &corrupted,
            );
        }
    }

    /// GC-SAT shape (a) named pin: interior saturation with honest-low tips
    /// keeps the prechecks ENABLED (tip eligibility sees non-MAX values)
    /// while the interior lies; soundness comes from suppress-only wiring
    /// alone, and the traversed saturated edges are SEEN (violation
    /// counter: a FREE delta asserted on purpose).
    #[tokio::test]
    async fn gc_sat_interior_is_contained_and_detected() {
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let b = make_test_event(3, &[&a]);
        let c = make_test_event(4, &[&b]);
        let corpus = Corpus::from_events(vec![g.clone(), a.clone(), b.clone(), c.clone()]);
        let subject = [c.id()];
        let comparison = [g.id()];

        let (sg, cg) = Doctoring::default().operands(&corpus, &subject, &comparison);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &subject, &comparison, sg, cg, None, &HashMap::new(), "gc-sat-a baseline").await;

        let doctoring = Doctoring { payloads: vec![(a.id(), u32::MAX), (b.id(), u32::MAX)], subject_tips: vec![], comparison_tips: vec![] };
        let (lying, back) = doctoring.retriever(&corpus);
        let (sg, cg) = doctoring.operands(&corpus, &subject, &comparison);
        let corrupted = run(lying, &corpus, &subject, &comparison, sg, cg, None, &back, "gc-sat-a corrupted").await;

        assert_bound_identical(41, "gc-sat-interior", 0, &subject, &comparison, &doctoring.triples(&corpus), &baseline, &corrupted);
        assert!(
            corrupted.stats.edge_check_violations >= 1,
            "the walk traverses the corrupt-saturated interior and must SEE it, got {}",
            corrupted.stats.edge_check_violations
        );
    }

    /// GC-SAT shape (b) named pin: tips at MAX through BOTH channels make
    /// the prechecks self-DISABLE (disabled is not suppressed: the counter
    /// stays at baseline zero and the quick check still runs), and the run
    /// equals baseline byte-for-byte.
    #[tokio::test]
    async fn gc_sat_tips_disable_the_prechecks_without_suppressing() {
        let h = make_test_event(1, &[]);
        let e = make_test_event(2, &[&h]);
        let corpus = Corpus::from_events(vec![h.clone(), e.clone()]);
        let subject = [e.id()];
        let comparison = [h.id()];

        let (sg, cg) = Doctoring::default().operands(&corpus, &subject, &comparison);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &subject, &comparison, sg, cg, None, &HashMap::new(), "gc-sat-b baseline").await;
        assert_eq!(baseline.stats.precheck_suppressions, 0, "precondition: the honest one-step shape suppresses nothing");

        let doctoring = Doctoring {
            payloads: vec![(e.id(), u32::MAX), (h.id(), u32::MAX)],
            subject_tips: vec![(e.id(), u32::MAX)],
            comparison_tips: vec![(h.id(), u32::MAX)],
        };
        let (lying, back) = doctoring.retriever(&corpus);
        let (sg, cg) = doctoring.operands(&corpus, &subject, &comparison);
        let corrupted = run(lying, &corpus, &subject, &comparison, sg, cg, None, &back, "gc-sat-b corrupted").await;

        assert_bound_identical(43, "gc-sat-tips", 0, &subject, &comparison, &doctoring.triples(&corpus), &baseline, &corrupted);
        assert_eq!(
            corrupted.stats.precheck_suppressions, 0,
            "saturated operands DISABLE the precheck (266-C.iv); disabling must never read as a suppression"
        );
    }

    /// GC-GENESIS named pin: geneses doctored to {0, 7, MAX} through the
    /// payload channel change nothing BOUND, on a descent shape and on a
    /// two-root Disjoint shape (roots are detected by empty parent clocks,
    /// never by gen == 1).
    #[tokio::test]
    async fn gc_genesis_claims_do_not_steer_roots_or_verdicts() {
        // Descent shape.
        let g = make_test_event(1, &[]);
        let a = make_test_event(2, &[&g]);
        let corpus = Corpus::from_events(vec![g.clone(), a.clone()]);
        let (sg, cg) = Doctoring::default().operands(&corpus, &[a.id()], &[g.id()]);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &[a.id()], &[g.id()], sg, cg, None, &HashMap::new(), "gc-genesis descent baseline")
                .await;
        for claim in [0u32, 7, u32::MAX] {
            let doctoring = Doctoring { payloads: vec![(g.id(), claim)], subject_tips: vec![], comparison_tips: vec![] };
            let (lying, back) = doctoring.retriever(&corpus);
            let (sg, cg) = doctoring.operands(&corpus, &[a.id()], &[g.id()]);
            let corrupted =
                run(lying, &corpus, &[a.id()], &[g.id()], sg, cg, None, &back, &format!("gc-genesis descent claim {claim}")).await;
            assert_bound_identical(1, "gc-genesis", claim, &[a.id()], &[g.id()], &doctoring.triples(&corpus), &baseline, &corrupted);
        }

        // Disjoint shape: two independent roots.
        let r1 = make_test_event(10, &[]);
        let r2 = make_test_event(11, &[]);
        let d1 = make_test_event(12, &[&r1]);
        let d2 = make_test_event(13, &[&r2]);
        let corpus = Corpus::from_events(vec![r1.clone(), r2.clone(), d1.clone(), d2.clone()]);
        let (sg, cg) = Doctoring::default().operands(&corpus, &[d1.id()], &[d2.id()]);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &[d1.id()], &[d2.id()], sg, cg, None, &HashMap::new(), "gc-genesis disjoint baseline")
                .await;
        assert!(matches!(baseline.bound, Bound::Disjoint), "precondition: two roots are Disjoint");
        let doctoring = Doctoring { payloads: vec![(r1.id(), u32::MAX), (r2.id(), 7)], subject_tips: vec![], comparison_tips: vec![] };
        let (lying, back) = doctoring.retriever(&corpus);
        let (sg, cg) = doctoring.operands(&corpus, &[d1.id()], &[d2.id()]);
        let corrupted = run(lying, &corpus, &[d1.id()], &[d2.id()], sg, cg, None, &back, "gc-genesis disjoint corrupted").await;
        assert_bound_identical(2, "gc-genesis", 0, &[d1.id()], &[d2.id()], &doctoring.triples(&corpus), &baseline, &corrupted);
    }

    /// Cross-mismatch named pin (the fold's materialization-drift shape):
    /// the retriever and the operand GClock DISAGREE about one tip. Both
    /// lies flow, the channels contradict each other, and nothing BOUND
    /// moves.
    #[tokio::test]
    async fn gc_cross_mismatch_between_channels_is_contained() {
        let g = make_test_event(1, &[]);
        let m = make_test_event(2, &[&g]);
        let w = make_test_event(3, &[&m]);
        let corpus = Corpus::from_events(vec![g.clone(), m.clone(), w.clone()]);
        let subject = [w.id()];
        let comparison = [m.id()];

        let (sg, cg) = Doctoring::default().operands(&corpus, &subject, &comparison);
        let baseline =
            run(corpus.retriever.clone(), &corpus, &subject, &comparison, sg, cg, None, &HashMap::new(), "gc-cross baseline").await;

        // The retriever serves m at 9; the comparison operand claims 5;
        // the subject operand deflates w to 3. Every consumer sees a
        // different world and none of it may matter.
        let doctoring = Doctoring { payloads: vec![(m.id(), 9)], subject_tips: vec![(w.id(), 3)], comparison_tips: vec![(m.id(), 5)] };
        let (lying, back) = doctoring.retriever(&corpus);
        let (sg, cg) = doctoring.operands(&corpus, &subject, &comparison);
        let corrupted = run(lying, &corpus, &subject, &comparison, sg, cg, None, &back, "gc-cross corrupted").await;

        assert_bound_identical(3, "gc-cross", 0, &subject, &comparison, &doctoring.triples(&corpus), &baseline, &corrupted);
        assert!(
            corrupted.stats.precheck_suppressions >= 1,
            "the corruption must BITE: the drifted operands (maxg(C)=5 > maxg(S)=3) fire P1, got {}",
            corrupted.stats.precheck_suppressions
        );
    }

    /// ARM (iii), synthetic saturation, expressly distinct from GC-SAT: the
    /// corpus is HONESTLY saturated (every checkable edge equation-consistent
    /// under saturating arithmetic; the base is a deep-history stand-in
    /// genesis at the ceiling), BOTH runs see the same honest values, and
    /// the disable rule preserves verdicts: the operand-annotated run equals
    /// the operand-free plain walk byte-for-byte with zero suppressions and
    /// zero violations.
    #[tokio::test]
    async fn synthetic_saturation_disable_path_preserves_verdicts() {
        let base = Event { generation: u32::MAX - 1, ..make_test_event(1, &[]) };
        let mid = Event { generation: u32::MAX, ..make_test_event(2, &[&base]) };
        let tip = Event { generation: u32::MAX, ..make_test_event(3, &[&mid]) };
        let sibling = Event { generation: u32::MAX, ..make_test_event(4, &[&mid]) };
        let corpus = Corpus::from_events(vec![base.clone(), mid.clone(), tip.clone(), sibling.clone()]);

        for (subject, comparison, name) in
            [(vec![tip.id()], vec![base.id()], "descent at the ceiling"), (vec![tip.id()], vec![sibling.id()], "divergence at the ceiling")]
        {
            let (sg, cg) = Doctoring::default().operands(&corpus, &subject, &comparison);
            let annotated = run(corpus.retriever.clone(), &corpus, &subject, &comparison, sg, cg, None, &HashMap::new(), name).await;
            let plain =
                compare(corpus.retriever.clone(), &Clock::from(subject.clone()), &Clock::from(comparison.clone()), 4 * corpus.size())
                    .await
                    .unwrap();
            let plain_variant_matches = match (&annotated.bound, &plain.relation) {
                (Bound::StrictDescends { .. }, AbstractCausalRelation::StrictDescends { .. }) => true,
                (Bound::DivergedSince { meet, .. }, AbstractCausalRelation::DivergedSince { meet: pm, .. }) => meet == pm,
                _ => false,
            };
            assert!(plain_variant_matches, "{name}: the disable path must preserve the plain walk's verdict");
            assert_eq!(annotated.stats.precheck_suppressions, 0, "{name}: saturated operands disable, never suppress");
            assert_eq!(annotated.stats.edge_check_violations, 0, "{name}: an honestly saturated corpus is violation-free");
        }
    }

    /// Format pin for the extended artifact line (mirrors
    /// artifact_line_format_is_self_contained).
    #[test]
    fn gen_artifact_line_format_is_self_contained() {
        let line = gen_artifact_line(41, "gc-deflate", 7, &[], &[], "[payload(x,2,9)]", &Bound::Equal, &Bound::StrictAscends);
        assert!(line.starts_with("ORACLEFAIL "), "must carry the ORACLEFAIL marker: {line}");
        assert!(line.contains("kind=gen_corruption"), "must self-identify the oracle: {line}");
        assert!(line.contains("dag_seed=41"), "must carry the dag seed: {line}");
        assert!(line.contains("shape=gc-deflate"), "must carry the corruption shape: {line}");
        assert!(line.contains("corruption_seed=7"), "must carry the corruption seed: {line}");
        assert!(line.contains("doctored=[payload(x,2,9)]"), "must carry the doctored triples: {line}");
        assert!(
            line.contains("reproduce=\"GEN_ORACLE_SEED_BASE=41 GEN_ORACLE_SEEDS=1 GEN_ORACLE_SHAPE=gc-deflate cargo test"),
            "must carry a copy-pasteable single-seed repro: {line}"
        );
        assert!(!line.contains('\n'), "must be a single line: {line}");
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

        assert!(entity.apply_event(&retriever, &ev_a, None).await.unwrap());
        assert!(entity.apply_event(&retriever, &ev_x, None).await.unwrap());
        assert!(entity.apply_event(&retriever, &ev_b, None).await.unwrap());
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
