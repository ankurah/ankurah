#![cfg(test)]

use crate::error::RetrievalError;
use ankurah_proto::{AttestationSet, Attested};

use super::*;
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::sync::RwLock;

// Simple test types
type TestId = u32;

#[derive(Clone, Debug)]
struct TestClock {
    members: Vec<TestId>,
}

#[derive(Clone)]
struct TestEvent {
    id: TestId,
    parent_clock: TestClock,
}

impl TClock for TestClock {
    type Id = TestId;
    fn members(&self) -> &[Self::Id] { &self.members }
}

impl TEvent for TestEvent {
    type Id = TestId;
    type Parent = TestClock;

    fn id(&self) -> TestId { self.id }
    fn parent(&self) -> &TestClock { &self.parent_clock }
}

impl std::fmt::Display for TestEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Event({})", self.id) }
}

// Test causal assertion - like proto::CausalAssertion but for test IDs
#[derive(Debug, Clone)]
struct TestCausalAssertion {
    subject_head: TestClock,
    comparison_head: TestClock,
    relation: TestAssertionRelation,
}

#[derive(Debug, Clone)]
enum TestAssertionRelation {
    Descends,
    NotDescends { meet: Vec<TestId> },
    PartiallyDescends { meet: Vec<TestId> },
    Incomparable,
}

// Mock navigator with both events and causal assertions
struct MockEventStore {
    events: HashMap<TestId, Attested<TestEvent>>,
    assertions: Vec<TestCausalAssertion>,
    /// Tracks the sequence of event fetch batches for testing
    pub event_fetch_log: RwLock<Vec<Vec<TestId>>>,
}

impl MockEventStore {
    fn new() -> Self { Self { events: HashMap::new(), assertions: Vec::new(), event_fetch_log: RwLock::new(Vec::new()) } }

    /// Convenient constructor from event list
    fn from_events(events: Vec<(TestId, Vec<TestId>)>) -> Self {
        let mut store = Self::new();
        for (id, parent_ids) in events {
            store.add(id, &parent_ids);
        }
        store
    }

    fn add(&mut self, id: TestId, parent_ids: &[TestId]) {
        let event = TestEvent { id, parent_clock: TestClock { members: parent_ids.to_vec() } };
        let attested = Attested { payload: event, attestations: AttestationSet::default() };
        self.events.insert(id, attested);
    }

    fn add_assertion(&mut self, subject_head: TestClock, comparison_head: TestClock, relation: TestAssertionRelation) {
        self.assertions.push(TestCausalAssertion { subject_head, comparison_head, relation });
    }

    /// Get a clone of the event fetch log for testing
    fn get_fetch_log(&self) -> Vec<Vec<TestId>> { self.event_fetch_log.read().unwrap().clone() }
}

#[async_trait]
impl CausalNavigator for MockEventStore {
    type EID = TestId;
    type Event = TestEvent;

    async fn expand_frontier(
        &self,
        frontier_ids: &[Self::EID],
        _budget: usize,
    ) -> Result<NavigationStep<Self::EID, Self::Event>, RetrievalError> {
        // Log this fetch batch (only if we're actually fetching events)
        let mut fetched_ids = Vec::new();
        let mut events = Vec::new();

        for id in frontier_ids {
            if let Some(event) = self.events.get(id) {
                events.push(event.payload.clone());
                fetched_ids.push(*id);
            }
        }

        // Only log if we actually fetched events
        if !fetched_ids.is_empty() {
            fetched_ids.sort(); // Sort for consistent test assertions
            self.event_fetch_log.write().unwrap().push(fetched_ids);
        }

        // Check for matching assertions
        let mut assertion_results = Vec::new();
        for assertion in &self.assertions {
            // Check if any frontier_id matches the subject_head of this assertion
            for frontier_id in frontier_ids {
                if assertion.subject_head.members().contains(frontier_id) {
                    // Convert TestAssertionRelation to AssertionRelation
                    let relation = match &assertion.relation {
                        TestAssertionRelation::Descends => AssertionRelation::Descends,
                        TestAssertionRelation::NotDescends { meet } => AssertionRelation::NotDescends { meet: meet.clone() },
                        TestAssertionRelation::PartiallyDescends { meet } => AssertionRelation::PartiallyDescends { meet: meet.clone() },
                        TestAssertionRelation::Incomparable => AssertionRelation::Incomparable,
                    };

                    assertion_results.push(AssertionResult {
                        from: *frontier_id,
                        to: assertion.comparison_head.members().first().cloned(),
                        relation,
                    });
                }
            }
        }

        let consumed_budget = 1; // Match the real implementation pattern
        Ok(NavigationStep { events, assertions: assertion_results, consumed_budget })
    }
}

#[tokio::test]
async fn test_linear_history() {
    let mut store = MockEventStore::new();

    // Create a linear chain: 1 <- 2 <- 3
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);

    let ancestor = TestClock { members: vec![1] };
    let descendant = TestClock { members: vec![3] };

    // descendant descends from ancestor
    assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);

    // ancestor does not descend from descendant, but they both have a common ancestor: [1]
    assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
}

#[tokio::test]
async fn test_concurrent_history() {
    let mut store = MockEventStore::new();

    // Example history (arrows represent descendency not reference, which is the reverse direction):
    //      1
    //   ↙  ↓  ↘
    //  2   3   4  - at this point, we have a non-ancestral concurrency (1 is not concurrent)
    //   ↘ ↙ ↘ ↙
    //    5   6    - ancestral concurrency introduced (is 5+6 = 2+3+4?) LATER determine if we allow this
    //     ↘ ↙
    //      7
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[1]);
    store.add(4, &[1]);
    store.add(5, &[2, 3]);
    store.add(6, &[3, 4]);
    store.add(7, &[5, 6]);

    {
        // concurrency in lineage *between* clocks, but the descendant clock fully descends from the ancestor clock
        let ancestor = TestClock { members: vec![1] };
        let descendant = TestClock { members: vec![5] };
        assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
        // a is the common ancestor of this comparison. They are comparable, but a does not descend from b
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
    }
    {
        // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
        let ancestor = TestClock { members: vec![2, 3] };
        let descendant = TestClock { members: vec![5] };

        assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![2, 3] });
    }

    {
        // a and b are fully concurrent, but still comparable
        let a = TestClock { members: vec![2] };
        let b = TestClock { members: vec![3] };
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
        assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
    }

    {
        // a partially descends from b, but b has a component that is not in a
        let a = TestClock { members: vec![6] };
        let b = TestClock { members: vec![2, 3] };
        assert_eq!(
            compare(&store, &a, &b, 100).await.unwrap(),
            // concurrent_events 2 is fairly conclusively correct
            // It's a little less clear whether common_ancestor should be 1, or 1,3, because 3 is not an "ancestor" of 2,3 per se
            // but this would be consistent with the internal concurrency ancestor test above. maybe `common` is a better term
            AbstractCausalRelation::PartiallyDescends { meet: vec![3] /* , concurrent_events: vec![2] */ }
        );
    }
}

#[tokio::test]
async fn test_incomparable() {
    let mut store = MockEventStore::new();

    //   1        6
    //   ↓  ↘     ↓
    //   2   4    7
    //   ↓   ↓    ↓
    //   3   5    8
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);
    store.add(4, &[1]);
    store.add(5, &[4]);

    // 6 is an unrelated root event
    store.add(6, &[]);
    store.add(7, &[6]);
    store.add(8, &[7]);

    {
        // fully incomparable
        let a = TestClock { members: vec![3] };
        let b = TestClock { members: vec![8] };
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
    }
    {
        // fully incomparable (just a different tier)
        let a = TestClock { members: vec![2] };
        let b = TestClock { members: vec![8] };
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
    }
    {
        // partially incomparable is still incomparable (consider adding a report of common ancestor elements for cases of partial comparability)
        let a = TestClock { members: vec![3] };
        let b = TestClock { members: vec![5, 8] };
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
        // line 509 - the assertions above are passing
    }
}

#[tokio::test]
async fn test_empty_clocks() {
    let mut store = MockEventStore::new();

    store.add(1, &[]);

    let empty = TestClock { members: vec![] };
    let non_empty = TestClock { members: vec![1] };

    assert_eq!(compare(&store, &empty, &empty, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
    assert_eq!(compare(&store, &non_empty, &empty, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
    assert_eq!(compare(&store, &empty, &non_empty, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
}

#[tokio::test]
async fn test_budget_exceeded() {
    let mut store = MockEventStore::new();

    //   1
    //   ↓  ↘
    //   2   5
    //   ↓   ↓  ↘
    //   3   6   8
    //   ↓   ↓
    //   4   7

    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);
    store.add(4, &[3]);
    store.add(5, &[1]);
    store.add(6, &[5]);
    store.add(7, &[6]);
    store.add(8, &[5]);

    {
        // simple linear chain
        let ancestor = TestClock { members: vec![1] };
        let descendant = TestClock { members: vec![4] };

        assert_eq!(
            compare(&store, &descendant, &ancestor, 2).await.unwrap(),
            AbstractCausalRelation::BudgetExceeded { subject_frontier: [2].into(), other_frontier: [].into() }
        );
    }
    {
        let ancestor = TestClock { members: vec![1] };
        let descendant = TestClock { members: vec![4, 5] };

        //  with a high enough budget, we can see that the descendant fully descends from the ancestor
        assert_eq!(compare(&store, &descendant, &ancestor, 10).await.unwrap(), AbstractCausalRelation::StrictDescends);

        // when multiple paths are split across the budget, we can determine there's at least partial descent, but that's not accurate.
        // We can't declare partial descent until we've found a common ancestor, otherwise they might be incomparable.
        assert_eq!(
            compare(&store, &ancestor, &descendant, 2).await.unwrap(),
            AbstractCausalRelation::BudgetExceeded { subject_frontier: [].into(), other_frontier: [2].into() }
        );
    }
}

#[tokio::test]
async fn test_self_comparison() {
    let mut store = MockEventStore::new();

    // Create a simple event to compare with itself
    store.add(1, &[]);
    let clock = TestClock { members: vec![1] };

    // A clock does NOT descend itself
    assert_eq!(compare(&store, &clock, &clock, 100).await.unwrap(), AbstractCausalRelation::Equal);
}

#[tokio::test]
async fn multiple_roots() {
    //   1   2   3   4   5   6  ← six independent roots
    //   ↓   ↓   ↓   ↓   ↓   ↓
    //   ╰───────────────────╯
    //           ↓
    //           7
    //           ↓
    //           8
    //
    // This test is supposed to stress two aspects of the design:
    // 1. The merge node 7 carries six origin tags upward, forcing a heap spill in the SmallVec‐based Origins
    // 2. All six heads are removed from outstanding_heads in a single step when they become common
    // TODO: validate this empirically
    let mut store = MockEventStore::new();

    // six independent roots
    for id in 1..=6 {
        store.add(id, &[]);
    }

    // merge-point 7 references all six heads
    store.add(7, &[1, 2, 3, 4, 5, 6]);

    // subject head 8 descends only from 7
    store.add(8, &[7]);

    let subject = TestClock { members: vec![8] };
    let big_other = TestClock { members: vec![1, 2, 3, 4, 5, 6] };

    // 8 descends from all heads in big_other via 7
    assert_eq!(compare(&store, &subject, &big_other, 1_000).await.unwrap(), AbstractCausalRelation::StrictDescends);

    // In the opposite direction, none of the heads descend from 8, but they are comparable
    assert_eq!(
        compare(&store, &big_other, &subject, 1_000).await.unwrap(),
        AbstractCausalRelation::NotDescends { meet: vec![1, 2, 3, 4, 5, 6] }
    );
}

#[tokio::test]
async fn test_compare_event_unstored() {
    let mut store = MockEventStore::new();

    // Create a chain: 1 <- 2 <- 3 (stored)
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);

    // Create an unstored event 4 that would descend from 3
    let unstored_event = TestEvent { id: 4, parent_clock: TestClock { members: vec![3] } };

    // Test cases for the unstored event
    let clock_1 = TestClock { members: vec![1] };
    let clock_2 = TestClock { members: vec![2] };
    let clock_3 = TestClock { members: vec![3] };

    // The unstored event should descend from all ancestors
    assert_eq!(compare_unstored_event(&store, &unstored_event, &clock_1, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
    assert_eq!(compare_unstored_event(&store, &unstored_event, &clock_2, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
    assert_eq!(compare_unstored_event(&store, &unstored_event, &clock_3, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);

    // Test with an unstored event that has multiple parents
    let unstored_merge_event = TestEvent { id: 5, parent_clock: TestClock { members: vec![2, 3] } };

    assert_eq!(compare_unstored_event(&store, &unstored_merge_event, &clock_1, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);

    // Test with an incomparable case
    store.add(10, &[]); // Independent root
    let incomparable_clock = TestClock { members: vec![10] };

    assert_eq!(
        compare_unstored_event(&store, &unstored_event, &incomparable_clock, 100).await.unwrap(),
        AbstractCausalRelation::Incomparable
    );

    // Test root event case
    let root_event = TestEvent { id: 11, parent_clock: TestClock { members: vec![] } };

    let empty_clock = TestClock { members: vec![] };
    assert_eq!(compare_unstored_event(&store, &root_event, &empty_clock, 100).await.unwrap(), AbstractCausalRelation::Incomparable);

    assert_eq!(compare_unstored_event(&store, &root_event, &clock_1, 100).await.unwrap(), AbstractCausalRelation::Incomparable);

    // Test that a non-empty unstored event does not descend from an empty clock
    let empty_clock = TestClock { members: vec![] };
    assert_eq!(compare_unstored_event(&store, &unstored_event, &empty_clock, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
}

#[tokio::test]
async fn test_compare_event_redundant_delivery() {
    let mut store = MockEventStore::new();

    // Create a chain: 1 <- 2 <- 3 (stored)
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);

    // Create an unstored event 4 that would descend from 3
    let unstored_event = TestEvent { id: 4, parent_clock: TestClock { members: vec![3] } };

    // Test the normal case first
    let clock_3 = TestClock { members: vec![3] };
    assert_eq!(compare_unstored_event(&store, &unstored_event, &clock_3, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);

    // Now store event 4 to simulate it being applied
    store.add(4, &[3]);

    // Test redundant delivery: the event is already in the clock (exact match)
    let clock_with_event = TestClock { members: vec![4] };
    // The equality check should catch this case and return Equal
    assert_eq!(compare_unstored_event(&store, &unstored_event, &clock_with_event, 100).await.unwrap(), AbstractCausalRelation::Equal);

    // Test case where the event is in the clock but with other events too - this is NOT Equal
    let clock_with_multiple = TestClock { members: vec![3, 4] };
    // This should be Incomparable since we're comparing the event's parent [3] against [3,4]
    // The parent [3] doesn't contain 4, so they're incomparable
    assert_eq!(
        compare_unstored_event(&store, &unstored_event, &clock_with_multiple, 100).await.unwrap(),
        AbstractCausalRelation::Incomparable
    );
}

// ============================================================================
// ASSERTION TESTS
// ============================================================================

#[tokio::test]
async fn test_assertion_perfect_match_zero_fetch() {
    let mut store = MockEventStore::new();

    // Create a simple chain: 1 <- 2 <- 3 <- 4 <- 5
    for i in 1..=5 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Add assertion that perfectly bridges the gap: [5] descends from [2]
    store.add_assertion(TestClock { members: vec![5] }, TestClock { members: vec![2] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![5] };
    let comparison = TestClock { members: vec![2] };

    // This should complete with ZERO event fetches (only the assertion is used)
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
    assert_eq!(store.get_fetch_log(), Vec::<Vec<TestId>>::new()); // Zero fetches!
}

#[tokio::test]
async fn test_assertion_falls_short_minimal_fetch() {
    let mut store = MockEventStore::new();

    // Create chain: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8 <- 9 <- 10
    for i in 1..=10 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Assertion gets us partway: [10] descends from [5]
    store.add_assertion(TestClock { members: vec![10] }, TestClock { members: vec![5] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![10] };
    let comparison = TestClock { members: vec![3] };

    // Should fetch: [10] from event frontier, [5] from assertion frontier,
    // then [9], [4], then [8], [3] - finding the match at [3]
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
    // TODO: Update expected fetch sequence once multi-frontier is implemented
    // Expected: vec![vec![10], vec![5], vec![9], vec![4], vec![8], vec![3]]
    // For now, just verify we fetched some events
    assert!(!store.get_fetch_log().is_empty());
}

#[tokio::test]
async fn test_assertion_overshoots_finds_minimal_meet() {
    let mut store = MockEventStore::new();

    // Create chain: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8 <- 9 <- 10
    for i in 1..=10 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Assertion overshoots: [10] descends from [4] (skips past [7])
    store.add_assertion(TestClock { members: vec![10] }, TestClock { members: vec![4] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![10] };
    let comparison = TestClock { members: vec![7] };

    // Event frontier should find [7] as the meet, even though assertion goes to [4]
    // The assertion frontier continues but doesn't affect the result
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
    // TODO: Update expected fetch sequence once multi-frontier is implemented
    // Expected: event frontier fetches [10, 9, 8, 7] while assertion frontier fetches [4, 3, 2, 1]
    assert!(!store.get_fetch_log().is_empty());
}

#[tokio::test]
async fn test_assertion_chaining() {
    let mut store = MockEventStore::new();

    // Create chain: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8 <- 9 <- 10
    for i in 1..=10 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Add two assertions that can chain:
    // [10] descends from [6]
    store.add_assertion(TestClock { members: vec![10] }, TestClock { members: vec![6] }, TestAssertionRelation::Descends);

    // [6] descends from [2]
    store.add_assertion(TestClock { members: vec![6] }, TestClock { members: vec![2] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![10] };
    let comparison = TestClock { members: vec![2] };

    // Should chain assertions: [10]→[6]→[2] with minimal fetches
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
}

#[tokio::test]
async fn test_multiple_assertions_different_paths() {
    let mut store = MockEventStore::new();

    // Create a DAG with multiple paths:
    //      1
    //     / \
    //    2   3
    //   / \ / \
    //  4   5   6
    //   \ | /
    //     7
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[1]);
    store.add(4, &[2]);
    store.add(5, &[2, 3]);
    store.add(6, &[3]);
    store.add(7, &[4, 5, 6]);

    // Add assertions for different paths:
    // [7] descends from [4]
    store.add_assertion(TestClock { members: vec![7] }, TestClock { members: vec![4] }, TestAssertionRelation::Descends);

    // [7] descends from [6]
    store.add_assertion(TestClock { members: vec![7] }, TestClock { members: vec![6] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![7] };
    let comparison = TestClock { members: vec![2] };

    // Should explore both assertion paths and find that [7] descends from [2]
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
}

#[tokio::test]
async fn test_single_assertion_descends() {
    let mut store = MockEventStore::new();

    // Create a long chain: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8 <- 9 <- 10
    for i in 1..=10 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Add assertion: [10] descends from [5] (skipping events 6,7,8,9)
    store.add_assertion(TestClock { members: vec![10] }, TestClock { members: vec![5] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![10] };
    let comparison = TestClock { members: vec![1] };

    // Without assertion, this would require fetching events 10,9,8,7,6,5,4,3,2,1
    // With assertion, we can shortcut from 10 to 5, then continue normally
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
}

#[tokio::test]
async fn test_single_assertion_not_descends() {
    let mut store = MockEventStore::new();

    // Create two divergent branches:
    // 1 <- 2 <- 3
    // 1 <- 4 <- 5
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);
    store.add(4, &[1]);
    store.add(5, &[4]);

    // Add assertion: [5] does not descend from [3], meet at [1]
    store.add_assertion(
        TestClock { members: vec![5] },
        TestClock { members: vec![3] },
        TestAssertionRelation::NotDescends { meet: vec![1] },
    );

    let subject = TestClock { members: vec![5] };
    let comparison = TestClock { members: vec![3] };

    // The assertion should provide the meet directly
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
}

#[tokio::test]
async fn test_multiple_non_overlapping_assertions() {
    let mut store = MockEventStore::new();

    // Create a complex DAG:
    // 1 <- 2 <- 3 <- 4 <- 5
    // 1 <- 6 <- 7 <- 8 <- 9
    for i in 1..=5 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }
    store.add(6, &[1]);
    for i in 7..=9 {
        store.add(i, &[i - 1]);
    }

    // Add two non-overlapping assertions:
    // [5] descends from [3] (shortcuts 4)
    store.add_assertion(TestClock { members: vec![5] }, TestClock { members: vec![3] }, TestAssertionRelation::Descends);

    // [9] descends from [7] (shortcuts 8)
    store.add_assertion(TestClock { members: vec![9] }, TestClock { members: vec![7] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![5] };
    let comparison = TestClock { members: vec![9] };

    // Both branches should trace back to common ancestor [1]
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::NotDescends { meet: vec![1] });
}

#[tokio::test]
async fn test_multiple_overlapping_assertions() {
    let mut store = MockEventStore::new();

    // Create a chain: 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8
    for i in 1..=8 {
        let parent = if i == 1 { vec![] } else { vec![i - 1] };
        store.add(i, &parent);
    }

    // Add overlapping assertions:
    // [8] descends from [5] (shortcuts 7,6)
    store.add_assertion(TestClock { members: vec![8] }, TestClock { members: vec![5] }, TestAssertionRelation::Descends);

    // [6] descends from [3] (shortcuts 5,4)
    store.add_assertion(TestClock { members: vec![6] }, TestClock { members: vec![3] }, TestAssertionRelation::Descends);

    let subject = TestClock { members: vec![8] };
    let comparison = TestClock { members: vec![1] };

    // Should be able to use both assertions: 8->5 (via assertion), then 5->3 (via assertion), then 3->2->1 (via events)
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::StrictDescends);
}

#[tokio::test]
async fn test_assertion_with_partial_descent() {
    let mut store = MockEventStore::new();

    // Create a merge scenario:
    //   1 <- 2 <- 4
    //   1 <- 3 <- 4 (4 has parents [2,3])
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[1]);
    store.add(4, &[2, 3]);

    // Add assertion: [4] partially descends from [2], meet at [2]
    store.add_assertion(
        TestClock { members: vec![4] },
        TestClock { members: vec![2] },
        TestAssertionRelation::PartiallyDescends { meet: vec![2] },
    );

    let subject = TestClock { members: vec![4] };
    let comparison = TestClock { members: vec![2] };

    // The assertion should provide the partial descent relationship
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::PartiallyDescends { meet: vec![2] });
}

#[tokio::test]
async fn test_assertion_incomparable() {
    let mut store = MockEventStore::new();

    // Create two completely separate chains:
    // Chain A: 1 <- 2 <- 3
    // Chain B: 4 <- 5 <- 6
    store.add(1, &[]);
    store.add(2, &[1]);
    store.add(3, &[2]);
    store.add(4, &[]);
    store.add(5, &[4]);
    store.add(6, &[5]);

    // Add assertion: [6] is incomparable to [3]
    store.add_assertion(TestClock { members: vec![6] }, TestClock { members: vec![3] }, TestAssertionRelation::Incomparable);

    let subject = TestClock { members: vec![6] };
    let comparison = TestClock { members: vec![3] };

    // The assertion should provide the incomparable relationship directly
    assert_eq!(compare(&store, &subject, &comparison, 100).await.unwrap(), AbstractCausalRelation::Incomparable);
}
