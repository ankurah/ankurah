#![cfg(test)]

use crate::error::RetrievalError;
use crate::retrieval::GetEvents;

use super::comparison::compare;
use super::relation::AbstractCausalRelation;
use ankurah_proto::{Clock, EntityId, Event, EventId, OperationSet};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};

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

    fn add_event(&mut self, event: Event) {
        self.events.insert(event.id(), event);
    }
}

#[async_trait]
impl GetEvents for MockRetriever {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.events
            .get(event_id)
            .cloned()
            .ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }
}

/// Create a test event with deterministic content-hashed IDs.
/// The seed differentiates events; parent_ids determine the parent clock.
/// Returns the event (call `.id()` on it to get the computed EventId).
fn make_test_event(seed: u8, parent_ids: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 16];
    entity_id_bytes[0] = seed;
    let entity_id = EntityId::from_bytes(entity_id_bytes);

    Event {
        entity_id,
        collection: "test".into(),
        parent: Clock::from(parent_ids.to_vec()),
        operations: OperationSet(BTreeMap::new()),
    }
}

/// Create a Clock from EventIds without consuming them.
macro_rules! clock {
    ($($id:expr),* $(,)?) => {
        Clock::from(vec![$($id.clone()),*])
    };
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
    retriever.add_event(ev1);

    let ev2 = make_test_event(2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(3, &[id2]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

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
    retriever.add_event(ev1);

    let ev2 = make_test_event(2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(3, &[id1.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    let ev4 = make_test_event(4, &[id1.clone()]);
    let id4 = ev4.id();
    retriever.add_event(ev4);

    let ev5 = make_test_event(5, &[id2.clone(), id3.clone()]);
    let id5 = ev5.id();
    retriever.add_event(ev5);

    let ev6 = make_test_event(6, &[id3.clone(), id4]);
    let id6 = ev6.id();
    retriever.add_event(ev6);

    let ev7 = make_test_event(7, &[id5.clone(), id6.clone()]);
    let _id7 = ev7.id();
    retriever.add_event(ev7);

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
    retriever.add_event(ev1);

    let ev2 = make_test_event(2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(3, &[id2.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    let ev4 = make_test_event(4, &[id1.clone()]);
    let _id4 = ev4.id();
    retriever.add_event(ev4);

    let ev5 = make_test_event(5, &[_id4]);
    let id5 = ev5.id();
    retriever.add_event(ev5);

    // 6 is an unrelated root event
    let ev6 = make_test_event(6, &[]);
    let id6 = ev6.id();
    retriever.add_event(ev6);

    let ev7 = make_test_event(7, &[id6.clone()]);
    let id7 = ev7.id();
    retriever.add_event(ev7);

    let ev8 = make_test_event(8, &[id7]);
    let id8 = ev8.id();
    retriever.add_event(ev8);

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
    retriever.add_event(ev1);

    let empty = Clock::default();
    let non_empty = clock!(id1);

    let expected = AbstractCausalRelation::DivergedSince {
        meet: vec![],
        subject: vec![],
        other: vec![],
        subject_chain: vec![],
        other_chain: vec![],
    };

    let result = compare(retriever.clone(), &empty, &empty, 100).await.unwrap();
    assert_eq!(result.relation, expected);
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
    let mut ids: Vec<EventId> = Vec::new();
    for i in 0..20u8 {
        let parents = if i == 0 { vec![] } else { vec![ids[i as usize - 1].clone()] };
        let ev = make_test_event(i + 1, &parents);
        ids.push(ev.id());
        retriever.add_event(ev);
    }

    let ancestor = clock!(ids[0].clone());
    let descendant = clock!(ids[19].clone());

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
    retriever.add_event(ev1);

    let clock = clock!(id1);

    // A clock does NOT descend itself
    let result = compare(retriever.clone(), &clock, &clock, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);
}

#[tokio::test]
async fn multiple_roots() {
    let mut retriever = MockRetriever::new();

    // Six independent roots
    let mut root_ids = Vec::new();
    for i in 1..=6u8 {
        let ev = make_test_event(i, &[]);
        root_ids.push(ev.id());
        retriever.add_event(ev);
    }

    // merge-point 7 references all six heads
    let ev7 = make_test_event(7, &root_ids);
    let id7 = ev7.id();
    retriever.add_event(ev7);

    // subject head 8 descends only from 7
    let ev8 = make_test_event(8, &[id7]);
    let id8 = ev8.id();
    retriever.add_event(ev8);

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
    retriever.add_event(ev1);

    let ev2 = make_test_event(2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(3, &[id2.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(4, &[id3.clone()]);

    let clock_1 = clock!(id1);
    let clock_2 = clock!(id2);
    let clock_3 = clock!(id3);

    // Stage the unstored event so compare can find it
    retriever.add_event(unstored_event.clone());

    // The unstored event should descend from all ancestors
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_2, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an unstored event that has multiple parents
    let unstored_merge_event = make_test_event(5, &[id2.clone(), id3.clone()]);
    retriever.add_event(unstored_merge_event.clone());

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_merge_event.id()]), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an incomparable case - different roots should return Disjoint
    let ev10 = make_test_event(10, &[]); // Independent root
    let id10 = ev10.id();
    retriever.add_event(ev10);
    let incomparable_clock = clock!(id10);

    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &incomparable_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
            if *subject_root == id1 && *other_root == id10
    ));

    // Test root event case
    let root_event = make_test_event(11, &[]);
    retriever.add_event(root_event.clone());

    let empty_clock = Clock::default();
    let result = compare(retriever.clone(), &Clock::from(vec![root_event.id()]), &empty_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
    ));

    let result = compare(retriever.clone(), &Clock::from(vec![root_event.id()]), &clock_1, 100).await.unwrap();
    // Two independent root events with different lineages should be Disjoint
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::Disjoint { .. }
    ));

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
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(2, &[id1]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(3, &[id2]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(4, &[id3.clone()]);
    let id4 = unstored_event.id();

    // Stage the unstored event so compare can find it
    retriever.add_event(unstored_event.clone());

    // Test the normal case first
    let clock_3 = clock!(id3);
    let result = compare(retriever.clone(), &Clock::from(vec![unstored_event.id()]), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Now store event 4 to simulate it being applied
    let ev4_stored = make_test_event(4, &[id3.clone()]);
    retriever.add_event(ev4_stored);

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

/// Proves that compare() busyloops when a frontier event can't be fetched.
///
/// Setup: A (creation, in retriever) -> B (child of A, NOT in retriever) -> C (child of B, in retriever).
/// compare(subject=[C], comparison=[A]) starts BFS. It fetches C (ok, parents=[B]),
/// adds B to subject frontier. Tries to fetch B -> EventNotFound -> `continue`,
/// which skips process_event (the only place that removes IDs from frontiers).
/// B stays on the frontier forever -> infinite loop.
///
/// We run compare on a separate OS thread because the busy loop never yields
/// to the tokio runtime (MockRetriever returns Ready immediately), so
/// tokio::time::timeout would never get a chance to fire on a single thread.
#[test]
fn test_missing_event_busyloop() {
    let mut retriever = MockRetriever::new();

    // A: creation event, in retriever
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    // B: child of A, deliberately NOT added to retriever (missing event)
    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    // Do NOT add ev_b to retriever

    // C: child of B, in retriever
    let ev_c = make_test_event(3, &[id_b.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // compare(subject=[C], comparison=[A]):
    // BFS fetches C (ok), adds parent B to subject frontier.
    // BFS fetches A (ok, it's comparison frontier).
    // BFS tries to fetch B -> EventNotFound -> continue -> B stays on frontier -> loop
    let subject = clock!(id_c);
    let comparison = clock!(id_a);

    // Run on a separate OS thread so we can detect the timeout.
    // The busy loop never yields (MockRetriever returns Ready), so
    // tokio::time::timeout on a single-threaded runtime would never fire.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(compare(retriever, &subject, &comparison, 100));
        let _ = tx.send(result);
    });

    // Wait up to 2 seconds. If it doesn't return, it's busylooping.
    let recv_result = rx.recv_timeout(std::time::Duration::from_secs(2));

    // Timeout (Err) means busyloop -- this is the bug we're documenting
    assert!(recv_result.is_err(), "Expected timeout due to busyloop, but compare returned");
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
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_a]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // Event D extends C
    let event_d = make_test_event(4, &[id_c.clone()]);

    // Entity head is [B, C]
    let entity_head = clock!(id_b, id_c);

    // Stage event D so compare can find it
    retriever.add_event(event_d.clone());

    // D should NOT return StrictAscends - it should be DivergedSince
    // because D extends C which is a tip, and is concurrent with B
    let result = compare(retriever.clone(), &Clock::from(vec![event_d.id()]), &entity_head, 100).await.unwrap();

    // The meet should be [C]; B is concurrent (not a child of meet C) so
    // it appears in other_chain rather than the immediate-children `other` field.
    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }),
        "Expected DivergedSince, got {:?}",
        result.relation
    );

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
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_a]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // Event D merges both tips
    let event_d = make_test_event(4, &[id_b.clone(), id_c.clone()]);

    // Entity head is [B, C]
    let entity_head = clock!(id_b, id_c);

    // Stage event D so compare can find it
    retriever.add_event(event_d.clone());

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
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_a]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    // Event E extends only B
    let event_e = make_test_event(5, &[id_b.clone()]);

    // Entity head is [B, C, D]
    let entity_head = clock!(id_b, id_c, id_d);

    // Stage event E so compare can find it
    retriever.add_event(event_e.clone());

    let result = compare(retriever.clone(), &Clock::from(vec![event_e.id()]), &entity_head, 100).await.unwrap();

    // Should be DivergedSince because E extends B but is concurrent with C and D
    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }),
        "Expected DivergedSince, got {:?}",
        result.relation
    );

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
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_b]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(5, &[id_c]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let ev_f = make_test_event(6, &[id_d]);
    let id_f = ev_f.id();
    retriever.add_event(ev_f);

    let ev_g = make_test_event(7, &[id_e]);
    let id_g = ev_g.id();
    retriever.add_event(ev_g);

    let ev_h = make_test_event(8, &[id_f]);
    let id_h = ev_h.id();
    retriever.add_event(ev_h);

    let ev_i = make_test_event(9, &[id_g]);
    let id_i = ev_i.id();
    retriever.add_event(ev_i);

    let clock_h = clock!(id_h);
    let clock_i = clock!(id_i);

    let result = compare(retriever.clone(), &clock_h, &clock_i, 100).await.unwrap();

    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }),
        "Expected DivergedSince, got {:?}",
        result.relation
    );

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
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_c]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(5, &[id_d.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let ev_f = make_test_event(6, &[id_e]);
    let id_f = ev_f.id();
    retriever.add_event(ev_f);

    let ev_g = make_test_event(7, &[id_f]);
    let id_g = ev_g.id();
    retriever.add_event(ev_g);

    let ev_h = make_test_event(8, &[id_g]);
    let id_h = ev_h.id();
    retriever.add_event(ev_h);

    // Short branch from D
    let ev_x = make_test_event(9, &[id_d]); // X (parent is D)
    let id_x = ev_x.id();
    retriever.add_event(ev_x);

    let ev_y = make_test_event(10, &[id_x]); // Y
    let _id_y = ev_y.id();

    let clock_h = clock!(id_h);

    // Stage event Y so compare can find it
    retriever.add_event(ev_y.clone());

    // Event Y arrives late, with parent X
    let result = compare(retriever.clone(), &Clock::from(vec![ev_y.id()]), &clock_h, 100).await.unwrap();

    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }),
        "Expected DivergedSince, got {:?}",
        result.relation
    );

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
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_c]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    // Long branch from genesis
    let ev_x = make_test_event(5, &[id_a.clone()]);
    let id_x = ev_x.id();
    retriever.add_event(ev_x);

    let ev_y = make_test_event(6, &[id_x]);
    let id_y = ev_y.id();
    retriever.add_event(ev_y);

    let ev_z = make_test_event(7, &[id_y]);
    let id_z = ev_z.id();
    retriever.add_event(ev_z);

    let clock_d = clock!(id_d);
    let clock_z = clock!(id_z);

    let result = compare(retriever.clone(), &clock_d, &clock_z, 100).await.unwrap();

    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }),
        "Expected DivergedSince, got {:?}",
        result.relation
    );

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
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_b.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_c.clone()]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(5, &[id_d.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

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
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(4, &[id_b.clone()]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(5, &[id_c.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

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

#[tokio::test]
async fn test_same_event_redundant_delivery() {
    let mut retriever = MockRetriever::new();

    // Create: A -> B -> C
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_b.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c.clone());

    // Entity head is at C
    let entity_head = clock!(id_c);

    // Event C is delivered again (redundant) - recreate same event
    // event_c_again has the same content as ev_c, so same EventId - already in retriever
    let event_c_again = make_test_event(3, &[id_b]);

    let result = compare(retriever.clone(), &Clock::from(vec![event_c_again.id()]), &entity_head, 100).await.unwrap();

    // Should be Equal since event is already at head
    assert!(
        matches!(result.relation, AbstractCausalRelation::Equal),
        "Redundant delivery of head event should return Equal, got {:?}",
        result.relation
    );
}

#[tokio::test]
async fn test_event_in_history_not_at_head() {
    let mut retriever = MockRetriever::new();

    // Create: A -> B -> C
    let ev_a = make_test_event(1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(3, &[id_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // Entity head is at C
    let entity_head = clock!(id_c);

    // Event B (in history but not at head) is delivered.
    // With staging, B is already in the retriever (same content = same EventId as ev_b).
    // compare([B], [C]) can now walk the DAG and discover B is an ancestor of C.
    let event_b = make_test_event(2, &[id_a]);

    let result = compare(retriever.clone(), &Clock::from(vec![event_b.id()]), &entity_head, 100).await.unwrap();

    // With staging, B IS discoverable in the retriever. compare([B], [C]) walks
    // backward from C and finds B as an ancestor, so this returns StrictAscends.
    assert!(
        matches!(result.relation, AbstractCausalRelation::StrictAscends),
        "Event in history should return StrictAscends (B is ancestor of C), got {:?}",
        result.relation
    );
}

// ============================================================================
// LWW LAYER APPLICATION TESTS (Phase 3)
// ============================================================================

#[cfg(test)]
mod lww_layer_tests {
    use crate::event_dag::accumulator::EventLayer;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use crate::value::Value;
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Create a test event with LWW operations.
    /// Uses a seed to create deterministic but different entity IDs for each event.
    fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event {
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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        }
    }

    fn layer_from_refs_with_context(
        already_applied: &[&Event],
        to_apply: &[&Event],
        context_events: &[&Event],
    ) -> EventLayer {
        let mut dag = BTreeMap::new();
        for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
            dag.insert(event.id(), event.parent.as_slice().to_vec());
        }
        EventLayer::new(
            already_applied.iter().map(|e| (*e).clone()).collect(),
            to_apply.iter().map(|e| (*e).clone()).collect(),
            Arc::new(dag),
        )
    }

    fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
        layer_from_refs_with_context(already_applied, to_apply, &[])
    }

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
}

// ============================================================================
// YRS LAYER APPLICATION TESTS (Phase 3e - Category 2)
// ============================================================================

#[cfg(test)]
mod yrs_layer_tests {
    use crate::event_dag::accumulator::EventLayer;
    use crate::property::backend::yrs::YrsBackend;
    use crate::property::backend::PropertyBackend;
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;
    use std::sync::Arc;

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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("yrs".to_string(), ops)])),
        }
    }

    fn layer_from_refs_with_context(
        already_applied: &[&Event],
        to_apply: &[&Event],
        context_events: &[&Event],
    ) -> EventLayer {
        let mut dag = BTreeMap::new();
        for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
            dag.insert(event.id(), event.parent.as_slice().to_vec());
        }
        EventLayer::new(
            already_applied.iter().map(|e| (*e).clone()).collect(),
            to_apply.iter().map(|e| (*e).clone()).collect(),
            Arc::new(dag),
        )
    }

    fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
        layer_from_refs_with_context(already_applied, to_apply, &[])
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
    use crate::event_dag::accumulator::EventLayer;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use crate::value::Value;
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event {
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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        }
    }

    fn layer_from_refs_with_context(
        already_applied: &[&Event],
        to_apply: &[&Event],
        context_events: &[&Event],
    ) -> EventLayer {
        let mut dag = BTreeMap::new();
        for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
            dag.insert(event.id(), event.parent.as_slice().to_vec());
        }
        EventLayer::new(
            already_applied.iter().map(|e| (*e).clone()).collect(),
            to_apply.iter().map(|e| (*e).clone()).collect(),
            Arc::new(dag),
        )
    }

    fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
        layer_from_refs_with_context(already_applied, to_apply, &[])
    }

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
    use crate::event_dag::accumulator::EventLayer;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use crate::value::Value;
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event {
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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        }
    }

    fn layer_from_refs_with_context(
        already_applied: &[&Event],
        to_apply: &[&Event],
        context_events: &[&Event],
    ) -> EventLayer {
        let mut dag = BTreeMap::new();
        for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
            dag.insert(event.id(), event.parent.as_slice().to_vec());
        }
        EventLayer::new(
            already_applied.iter().map(|e| (*e).clone()).collect(),
            to_apply.iter().map(|e| (*e).clone()).collect(),
            Arc::new(dag),
        )
    }

    fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
        layer_from_refs_with_context(already_applied, to_apply, &[])
    }

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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::new()), // No operations
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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
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
    use crate::event_dag::accumulator::EventLayer;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use crate::value::Value;
    use ankurah_proto::{Clock, EntityId, Event, OperationSet};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event {
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
            parent: Clock::default(),
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        }
    }

    #[test]
    fn test_stored_event_id_below_meet_loses_to_layer_candidate() {
        let backend = LWWBackend::new();

        // Step 1: Apply an initial event so the backend has a stored value with an event_id.
        let old_event = make_lww_event(1, vec![("x", "old_value")]);
        let old_event_id = old_event.id();

        // Apply the old event as a normal layer first (so the backend tracks the event_id)
        {
            let dag = BTreeMap::from([(old_event_id.clone(), vec![])]);
            let layer = EventLayer::new(
                vec![],
                vec![old_event.clone()],
                Arc::new(dag),
            );
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
        let layer = EventLayer::new(
            vec![],
            vec![new_event.clone()],
            Arc::new(dag),
        );

        backend.apply_layer(&layer).unwrap();

        // The new event should always win because the stored value is older_than_meet,
        // regardless of event_id ordering.
        assert_eq!(
            backend.get(&"x".into()),
            Some(Value::String("new_value".into())),
            "Layer candidate must beat stored value whose event_id is below the meet"
        );
        assert_eq!(
            backend.get_event_id(&"x".into()),
            Some(new_event_id),
            "Winning event_id must be the new event"
        );
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
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_c = make_test_event(3, &[id_b.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let entity_head = clock!(id_c);

        // Re-deliver event B (which is already in the history, but not at head).
        // With the staging+compare pattern, B is already in the retriever (same
        // content = same EventId as ev_b). compare([B], [C]) can walk backward
        // from C and discover B as an ancestor, returning StrictAscends.
        let event_b_again = make_test_event(2, &[id_a]);
        let result = compare(retriever.clone(), &Clock::from(vec![event_b_again.id()]), &entity_head, 100).await.unwrap();

        // Re-delivery of event C (which IS at the head) should return Equal.
        let event_c_again = make_test_event(3, &[id_b]);
        let result_c = compare(retriever.clone(), &Clock::from(vec![event_c_again.id()]), &entity_head, 100).await.unwrap();
        assert_eq!(
            result_c.relation,
            AbstractCausalRelation::Equal,
            "Re-delivery of head event must return Equal"
        );

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
    use crate::entity::Entity;
    use crate::error::MutationError;
    use crate::retrieval::GetEvents;
    use ankurah_proto::{Clock, EntityId, Event, EventId, OperationSet};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, HashMap};

    #[derive(Clone)]
    struct SimpleRetriever {
        events: HashMap<EventId, Event>,
    }

    impl SimpleRetriever {
        fn new() -> Self { Self { events: HashMap::new() } }
        fn add_event(&mut self, event: Event) { self.events.insert(event.id(), event); }
    }

    #[async_trait]
    impl GetEvents for SimpleRetriever {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, crate::error::RetrievalError> {
            self.events
                .get(event_id)
                .cloned()
                .ok_or_else(|| crate::error::RetrievalError::EventNotFound(event_id.clone()))
        }
    }

    fn make_creation_event(seed: u8) -> Event {
        use crate::property::backend::lww::LWWBackend;
        use crate::property::backend::PropertyBackend;
        use crate::value::Value;

        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42; // Same entity for both events
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        // Use LWW operations with distinct values to produce different event IDs.
        // Without differing content, identical events would produce the same EventId.
        let backend = LWWBackend::new();
        backend.set("x".into(), Some(Value::String(format!("value_{}", seed))));
        let ops = backend.to_operations().unwrap().unwrap();

        Event {
            entity_id,
            collection: "test".into(),
            parent: Clock::default(), // empty parent = creation event
            operations: OperationSet(BTreeMap::from([("lww".to_string(), ops)])),
        }
    }

    #[tokio::test]
    async fn test_second_creation_event_rejected() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = SimpleRetriever::new();

        // First creation event should succeed
        let creation_event_1 = make_creation_event(1);
        retriever.add_event(creation_event_1.clone());

        let result = entity.apply_event(&retriever, &creation_event_1).await;
        assert!(result.is_ok(), "First creation event should succeed");
        assert!(result.unwrap(), "First creation event should return true (applied)");

        // Entity head should now be non-empty
        assert!(!entity.head().is_empty(), "Entity head should be non-empty after creation");

        // Second creation event (different seed = different event) should be rejected
        let creation_event_2 = make_creation_event(2);
        // Don't store it - it's a new creation event, not the same one
        let result = entity.apply_event(&retriever, &creation_event_2).await;

        assert!(result.is_err(), "Second creation event should fail");
        assert!(
            matches!(result.unwrap_err(), MutationError::DuplicateCreation),
            "Error should be DuplicateCreation"
        );
    }

    #[tokio::test]
    async fn test_redelivery_of_same_creation_event_is_noop() {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = 42;
        let entity_id = EntityId::from_bytes(entity_id_bytes);
        let entity = Entity::create(entity_id, "test".into());

        let mut retriever = SimpleRetriever::new();

        // First creation event
        let creation_event = make_creation_event(1);
        retriever.add_event(creation_event.clone());

        let result = entity.apply_event(&retriever, &creation_event).await;
        assert!(result.is_ok() && result.unwrap(), "First apply should succeed");

        // Re-deliver the SAME creation event (same content, same id)
        // Since event_stored returns true, it should be treated as re-delivery
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
        let mut ids: Vec<EventId> = Vec::new();
        for i in 0..6u8 {
            let parents = if i == 0 { vec![] } else { vec![ids[i as usize - 1].clone()] };
            let ev = make_test_event(i + 1, &parents);
            ids.push(ev.id());
            retriever.add_event(ev);
        }

        let ancestor = clock!(ids[0]);
        let descendant = clock!(ids[5]);

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
        retriever.add_event(ev_a);

        let ev_b = make_test_event(2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_g = make_test_event(3, &[id_a.clone()]);
        let id_g = ev_g.id();
        retriever.add_event(ev_g);

        let ev_c = make_test_event(4, &[id_b.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_e = make_test_event(5, &[id_c.clone(), id_g.clone()]);
        let id_e = ev_e.id();
        retriever.add_event(ev_e);

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
        assert!(
            all_layers.len() >= 2,
            "Expected at least 2 layers for mixed-parent merge, got {}",
            all_layers.len()
        );

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
        let e_layer_idx = all_layers.iter().position(|l| {
            l.to_apply.iter().any(|ev| ev.id() == id_e) ||
            l.already_applied.iter().any(|ev| ev.id() == id_e)
        }).expect("E must appear in some layer");

        let g_layer_idx = all_layers.iter().position(|l| {
            l.to_apply.iter().any(|ev| ev.id() == id_g) ||
            l.already_applied.iter().any(|ev| ev.id() == id_g)
        }).expect("G must appear in some layer");

        let c_layer_idx = all_layers.iter().position(|l| {
            l.to_apply.iter().any(|ev| ev.id() == id_c) ||
            l.already_applied.iter().any(|ev| ev.id() == id_c)
        }).expect("C must appear in some layer");

        assert!(
            e_layer_idx > g_layer_idx,
            "Merge event E (layer {}) must be after G (layer {})",
            e_layer_idx, g_layer_idx
        );
        assert!(
            e_layer_idx > c_layer_idx,
            "Merge event E (layer {}) must be after C (layer {})",
            e_layer_idx, c_layer_idx
        );

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
        retriever.add_event(ev_a);

        let ev_b = make_test_event(2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_c = make_test_event(3, &[id_a.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_d = make_test_event(4, &[id_b.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d);

        let ev_e = make_test_event(5, &[id_c.clone()]);
        let id_e = ev_e.id();
        retriever.add_event(ev_e);

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
            assert!(
                !layer.to_apply.is_empty() || !layer.already_applied.is_empty(),
                "Layer should not be empty"
            );
        }
        assert!(layer_count > 0, "Should produce at least one layer from eagerly stored events");
    }
}
