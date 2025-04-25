use crate::{error::RetrievalError, storage::StorageCollection};
use ankurah_proto::{Attested, Clock, EventId};
use async_trait::async_trait;
use std::collections::BTreeSet;

/// a trait for events and eventlike things that can be descended
pub trait TEvent {
    type Id: Eq + PartialEq + Clone;
    type Parent: TClock<Id = Self::Id>;

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self::Parent;
}

pub trait TClock {
    type Id: Eq + PartialEq + Clone;
    fn members(&self) -> &[Self::Id];
}

pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id>;
    type Error;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store, returning the budget consumed by this operation and the events retrieved
    async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), Self::Error>;
}

impl TClock for Clock {
    type Id = EventId;
    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for ankurah_proto::Event {
    type Id = ankurah_proto::EventId;
    type Parent = Clock;

    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &Clock { &self.parent }
}

impl<T: StorageCollection> GetEvents for T {
    type Id = EventId;
    type Event = ankurah_proto::Event;
    type Error = RetrievalError;

    async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), Self::Error> {
        // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
        Ok((1, self.get_events(event_ids).await?))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Ordering<Id> {
    Equal,

    /// subject fully descends from comparison (all events in comparison are in subject's history)
    /// This means there exists a path in the DAG from each event in comparison to every event in subject
    Descends,

    /// subject does not descend from comparison, but there exists a path to a common ancestor
    /// we don't care right now, but notably: when the common ancestor equals the subject, it can be said to Ascend/Precede the comparison
    NotDescends {
        /// The closest common full(identical) ancestor event ids
        common_ancestors: Vec<Id>,
    },

    /// subject and comparison have no common ancestor whatsoever
    Incomparable,

    /// subject partially descends from comparison (some but not all events in comparison are in subject's history)
    /// This means there exists a path from some events in comparison to some (but not all) events in subject
    PartiallyDescends {
        /// The closest common full(identical) ancestor event ids
        common_ancestors: Vec<Id>,
        // The immediate descendents of the common ancestor that are in b's history but not in a's history
        // concurrent_events: Vec<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        // what exactly do we need to resume the comparison?
        // we want the smallest set of events that would allow us to resume the comparison
    },
}

/// Compares two clocks to determine their relationship in the directed acyclic graph (DAG).
///
/// This function determines whether the subject clock descends from, shares ancestry with,
/// or is incomparable to the comparison clock by traversing the event history.
pub async fn compare<G, C>(getter: &G, subject: &C, other: &C, budget: usize) -> Result<Ordering<G::Id>, G::Error>
where
    G: GetEvents,
    G::Event: TEvent<Id = G::Id>,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord,
{
    // REQUIREMENTS:
    // 0. Keep it simple as possible. We want to make this code elegant, efficient, correct, and easy to understand.
    // 1. Do not implement any visit tracking for the purposes of detecting cycles. The actual use case for this involves content-based ids, which cannot be cyclical.
    // For the test case, the budget mechanism can be a sufficient backstop against infinite loops.
    // 2. Travel light. Try to process things on a streaming basis as much as possible.
    // Event histories will tend to be fairly linear, but with occasional concurrencies which may remain concurrent for several generations.
    // 3. When you detect that any of the events in the comparison have no common history, you can return Incomparable immediately.
    // 4. when the budget is exceeded, return BudgetExceeded, even if we've found a partial answer. We need to give the Correct answer or nothing (BudgetExceeded).
    // 5. Equality is not descendency
    // 6. Early return whenever a conclusive determination can be made.
    // 7. Stretch goal: when the budget is exceeded, think about what we might be able to include in the BudgetExceeded result to help the caller resume the comparison later if they choose to. Can that be feed into subject and comparison clocks?

    // bail out right away for the obvious cases
    if subject.members().is_empty() || other.members().is_empty() {
        return Ok(Ordering::Incomparable);
    }

    if subject.members() == other.members() {
        return Ok(Ordering::Equal);
    }

    let mut comparison = Comparison::new(getter, subject, other, budget);

    loop {
        if let Some(ordering) = comparison.step().await? {
            return Ok(ordering);
        }
    }
}

pub struct Comparison<'a, G, C>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    /// The event provider for retrieving event data during traversal
    getter: &'a G,

    /// Current subject events frontier that changes as we traverse to parents
    subject_frontier: BTreeSet<C::Id>,

    /// Current other events frontier that changes as we traverse to parents
    other_frontier: BTreeSet<C::Id>,

    /// Remaining budget for event lookups before we stop traversal
    remaining_budget: usize,

    /// Original "other" clock events, preserved to determine descendency
    original_other_events: BTreeSet<C::Id>,

    /// All events in other's causal set, including discovered ancestors
    other_set: BTreeSet<C::Id>,

    /// All events in subject's causal set, including discovered ancestors
    subject_set: BTreeSet<C::Id>,
}

impl<'a, G, C> Comparison<'a, G, C>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    pub fn new(getter: &'a G, subject: &C, other: &C, budget: usize) -> Self {
        println!("Creating comparison with subject: {:?}, other: {:?}", subject.members(), other.members());
        // Initialize working sets with the initial events
        let subject_frontier = subject.members().iter().cloned().collect();
        let other_frontier = other.members().iter().cloned().collect();
        let original_other_events: BTreeSet<C::Id> = other.members().iter().cloned().collect();

        // Initialize causal sets with initial events
        let other_set: BTreeSet<C::Id> = other.members().iter().cloned().collect();
        let subject_set: BTreeSet<C::Id> = subject.members().iter().cloned().collect();

        Self { getter, subject_frontier, other_frontier, remaining_budget: budget, original_other_events, other_set, subject_set }
    }

    // runs one step of the comparison, returning Some(ordering) if a conclusive determination can be made, or None if it needs more steps
    pub async fn step(&mut self) -> Result<Option<Ordering<G::Id>>, G::Error> {
        println!("Step called - subject_ws: {:?}, other_ws: {:?}", self.subject_frontier, self.other_frontier);

        // Create an iterator over both frontiers
        let ids: Vec<G::Id> = self.subject_frontier.union(&self.other_frontier).cloned().collect();
        let (cost, events) = self.getter.get_events(ids).await?;
        self.remaining_budget = self.remaining_budget.saturating_sub(cost);

        self.process_events(&events);
        if let Some(ordering) = self.check_result() {
            return Ok(Some(ordering));
        }

        Ok(None)
    }

    // Process events in a single pass - update causal sets and regress frontiers
    fn process_events(&mut self, events: &[Attested<G::Event>]) {
        for event in events {
            let event_id = event.payload.id();
            let parent_ids = event.payload.parent().members();

            // Handle subject-side processing
            if self.subject_frontier.remove(&event_id) {
                // Add parent events to frontier and the set
                self.subject_frontier.extend(parent_ids.iter().cloned());
                self.subject_set.extend(parent_ids.iter().cloned());
            }

            // Handle other-side processing
            if self.other_frontier.remove(&event_id) {
                // Add parent events to frontier and the set
                self.other_frontier.extend(parent_ids.iter().cloned());
                self.other_set.extend(parent_ids.iter().cloned());
            }
        }

        println!("After regression - subject frontier: {:?}, other frontier: {:?}", self.subject_frontier, self.other_frontier);
    }

    // Check if we can make a conclusive ordering determination
    fn check_result(&self) -> Option<Ordering<G::Id>> {
        // Descends can be determined at any time - it's our happy path
        if self.original_other_events.is_subset(&self.subject_set) {
            println!("Subject descends from all elements in other");
            return Some(Ordering::Descends);
        }

        // All other orderings require complete exploration
        if self.subject_frontier.is_empty() && self.other_frontier.is_empty() {
            // Both frontiers are exhausted - time to pick a consolation prize

            let common_ancestors: Vec<G::Id> = self.subject_set.intersection(&self.other_set).cloned().collect();
            if common_ancestors.len() > 0 {
                if self.original_other_events.intersection(&self.subject_set).count() > 0 {
                    Some(Ordering::PartiallyDescends { common_ancestors })
                } else {
                    Some(Ordering::NotDescends { common_ancestors })
                }
            } else {
                Some(Ordering::Incomparable)
            }
        } else {
            // Keep exploring
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use ankurah_proto::AttestationSet;
    use async_trait::async_trait;

    use super::*;
    use std::collections::HashMap;

    // Simple test types
    type TestId = u32;

    #[derive(Clone)]
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

    // Mock event store for testing
    struct MockEventStore {
        events: HashMap<TestId, Attested<TestEvent>>,
    }

    impl MockEventStore {
        fn new() -> Self { Self { events: HashMap::new() } }

        fn add(&mut self, id: TestId, parent_ids: &[TestId]) {
            let event = TestEvent { id, parent_clock: TestClock { members: parent_ids.to_vec() } };
            let attested = Attested { payload: event, attestations: AttestationSet::default() };
            self.events.insert(id, attested);
        }
    }

    impl GetEvents for MockEventStore {
        type Id = TestId;
        type Event = TestEvent;
        type Error = ();

        async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), Self::Error> {
            let mut result = Vec::new();
            for id in event_ids {
                if let Some(event) = self.events.get(&id) {
                    result.push(event.clone());
                }
            }
            Ok((1, result))
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
        assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), Ordering::Descends);

        println!("\n\n\n");

        // LEFT OFF HERE - THink about whether we need to store all the ancestors to determine if there are any common ancestors
        // or if we can do it in a streaming manner. At present it's failing to detect that 1 and 3 have a common ancestor: [1]

        // ancestor does not descend from descendant, but they both have a common ancestor: [1]
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestors: vec![1] });
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
            assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), Ordering::Descends);
            // a is the common ancestor of this comparison. They are comparable, but a does not descend from b
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestors: vec![1] });
        }
        {
            // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
            let ancestor = TestClock { members: vec![2, 3] };
            let descendant = TestClock { members: vec![5] };

            assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), Ordering::Descends);
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestors: vec![2, 3] });
        }

        {
            // a and b are fully concurrent, but still comparable
            let a = TestClock { members: vec![2] };
            let b = TestClock { members: vec![3] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::NotDescends { common_ancestors: vec![1] });
            assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), Ordering::NotDescends { common_ancestors: vec![1] });
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
                Ordering::PartiallyDescends { common_ancestors: vec![1] /* , concurrent_events: vec![2] */ }
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
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::Incomparable);
        }
        {
            // fully incomparable (just a different tier)
            let a = TestClock { members: vec![2] };
            let b = TestClock { members: vec![8] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::Incomparable);
        }
        {
            // partially incomparable is still incomparable (consider adding a report of common ancestor elements for cases of partial comparability)
            let a = TestClock { members: vec![3] };
            let b = TestClock { members: vec![5, 8] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::Incomparable);
        }
    }

    #[tokio::test]
    async fn test_empty_clocks() {
        let mut store = MockEventStore::new();

        store.add(1, &[]);

        let empty = TestClock { members: vec![] };
        let non_empty = TestClock { members: vec![1] };

        assert_eq!(compare(&store, &empty, &empty, 100).await.unwrap(), Ordering::Incomparable);
        assert_eq!(compare(&store, &non_empty, &empty, 100).await.unwrap(), Ordering::Incomparable);
        assert_eq!(compare(&store, &empty, &non_empty, 100).await.unwrap(), Ordering::Incomparable);
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

            assert_eq!(compare(&store, &descendant, &ancestor, 2).await.unwrap(), Ordering::BudgetExceeded {});
        }
        {
            let ancestor = TestClock { members: vec![1] };
            let descendant = TestClock { members: vec![4, 5] };

            //  with a high enough budget, we can see that the descendant fully descends from the ancestor
            assert_eq!(compare(&store, &descendant, &ancestor, 10).await.unwrap(), Ordering::Descends);

            // when multiple paths are split across the budget, we can determine there's at least partial descent, but that's not accurate.
            // We can't declare partial descent until we've found a common ancestor, otherwise they might be incomparable.
            assert_eq!(compare(&store, &ancestor, &descendant, 2).await.unwrap(), Ordering::BudgetExceeded {});
        }
    }

    #[tokio::test]
    async fn test_self_comparison() {
        let mut store = MockEventStore::new();

        // Create a simple event to compare with itself
        store.add(1, &[]);
        let clock = TestClock { members: vec![1] };

        // A clock does NOT descend itself
        assert_eq!(compare(&store, &clock, &clock, 100).await.unwrap(), Ordering::Equal);
    }
}
