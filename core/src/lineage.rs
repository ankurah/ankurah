use crate::{error::RetrievalError, storage::StorageCollectionWrapper};
use ankurah_proto::{Attested, Clock, Event, EventId};
use async_trait::async_trait;
use smallvec::SmallVec;
use std::collections::{BTreeSet, HashMap, HashSet};

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

#[async_trait]
pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id>;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store, returning the budget consumed by this operation and the events retrieved
    async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError>;
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

#[async_trait]
impl GetEvents for StorageCollectionWrapper {
    type Id = EventId;
    type Event = ankurah_proto::Event;

    async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
        Ok((1, self.0.get_events(event_ids).await?))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Ordering<Id> {
    Equal,

    /// This means there exists a path in the DAG from each event in comparison to every event in subject
    Descends,

    /// subject does not descend from comparison, but there exists a path to a common ancestor
    NotDescends {
        /// The greatest lower bound of the two sets
        meet: Vec<Id>,
    },

    /// subject and comparison have no common ancestor whatsoever
    Incomparable,

    /// subject partially descends from comparison (some but not all events in comparison are in subject's history)
    /// This means there exists a path from some events in comparison to some (but not all) events in subject
    PartiallyDescends {
        /// The greatest lower bound of the two sets
        meet: Vec<Id>,
        // LATER: The immediate descendents of the common ancestor that are in b's history but not in a's history
        // difference: Vec<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        // what exactly do we need to resume the comparison?
        subject_frontier: BTreeSet<Id>,
        other_frontier: BTreeSet<Id>,
    },
}

pub async fn compare_event(
    getter: &StorageCollectionWrapper,
    subject: &Event,
    other: &Clock,
    budget: usize,
) -> Result<Ordering<EventId>, RetrievalError> {
    if subject.parent().members() == other.members() {
        return Ok(Ordering::Descends);
    }
    let subject = subject.id().into();
    compare(getter, &subject, other, budget).await
}

/// Compares two Clocks, traversing the event history to classify their
/// causal relationship.
///
/// The function performs a **simultaneous, breadth-first walk** from the head
/// sets of `subject` and `other`, fetching parents in batches.
///
/// `budget` reflects whatever appetite we have for traversal, which is costly
/// In practice, the node may decline an event with too high of a comparison cost.
///
/// As it walks it records which side first reached each node, incrementally
/// constructs the set of **minimal common ancestors** (the "meet"), and keeps a
/// checklist so it can decide without a second pass whether every event
/// in 'other' found a common ancestor.
///
/// The moment the relationship is clear, it returns an Ordering.
///
/// If the budget is exhausted before a definite answer is reached, the partially explored
/// frontiers are returned in `BudgetExceeded`, allowing the caller to resume
/// later with a higher budget. (This bit is under-baked, and needs to be revisited)
///
/// The intention is for this to operate on a streaming basis, storing the minimal state
/// required to make a conclusive comparison. I think we can probably purge the `state`
/// map for a given entity once visited by both sides, and the id is removed from the checklist
/// but this needs a bit more thought.
pub async fn compare<G, C>(getter: &G, subject: &C, other: &C, budget: usize) -> Result<Ordering<G::Id>, RetrievalError>
where
    G: GetEvents,
    G::Event: TEvent<Id = G::Id>,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Display,
{
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

#[derive(Debug, Clone, Default)]
struct Origins<Id>(SmallVec<[Id; 8]>);

impl<Id> Origins<Id> {
    fn new() -> Self { Self(SmallVec::new()) }
}

impl<Id: Clone + PartialEq> Origins<Id> {
    fn add(&mut self, id: Id) {
        if !self.0.contains(&id) {
            self.0.push(id);
        }
    }

    fn augment(&mut self, other: &Self) {
        for h in other.0.iter() {
            if !self.0.contains(h) {
                self.0.push(h.clone());
            }
        }
    }
}

impl<Id> std::ops::Deref for Origins<Id> {
    type Target = [Id];
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone)]
struct State<Id> {
    seen_from_subject: bool,
    seen_from_other: bool,
    common_child_count: usize,
    origins: Origins<Id>,
}

impl<Id> Default for State<Id> {
    fn default() -> Self { Self { seen_from_subject: false, seen_from_other: false, common_child_count: 0, origins: Origins::new() } }
}

impl<Id> State<Id>
where Id: Clone + PartialEq
{
    fn is_common(&self) -> bool { self.seen_from_subject && self.seen_from_other }

    fn mark_seen_from(&mut self, from_subject: bool, from_other: bool) {
        if from_subject {
            self.seen_from_subject = true;
        }
        if from_other {
            self.seen_from_other = true;
        }
    }
}

// TODOs
// [ ] benchmark and audit this
// [ ] consider moving `fn compare` into a static Comparison struct method
// [ ] the way Origin tracking works is pretty goofy, and we're doing more hashmap lookups than we need to
// [ ] implement skip links with bloom filters so we can traverse longer histories with a smaller budget
// [ ] replace StorageCollectionWrapper with an EventRetriever that can retrive from local or remote storage
pub(crate) struct Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    /// The event store to fetch events from
    getter: &'a G,

    /// The original set of `other` events
    original_other_events: BTreeSet<G::Id>, // immutable snapshot

    /// The set of `other` heads still looking for a common ancestor
    outstanding_heads: BTreeSet<G::Id>,

    /// The remaining budget for fetching events
    remaining_budget: usize,

    /* search frontiers */
    subject_frontier: BTreeSet<G::Id>,
    other_frontier: BTreeSet<G::Id>,

    /* per-node bookkeeping */
    states: HashMap<G::Id, State<G::Id>>,

    /* incremental meet construction */
    meet_candidates: BTreeSet<G::Id>,

    /* enum-decision flags */
    unseen_other_heads: usize,
    head_overlap: bool,
    any_common: bool,
}

impl<'a, G> Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug + std::fmt::Display,
{
    pub fn new<C: TClock<Id = G::Id>>(getter: &'a G, subject: &C, other: &C, budget: usize) -> Self {
        let subject_frontier: BTreeSet<_> = subject.members().iter().cloned().collect();
        let other: BTreeSet<_> = other.members().iter().cloned().collect();
        let original_other_events = other.clone();

        Self {
            getter,

            unseen_other_heads: other.len(),

            subject_frontier,
            other_frontier: other.clone(),
            remaining_budget: budget,
            original_other_events,

            head_overlap: false,
            any_common: false,

            states: HashMap::new(),
            meet_candidates: BTreeSet::new(),
            outstanding_heads: other,
        }
    }

    // runs one step of the comparison, returning Some(ordering) if a conclusive determination can be made, or None if it needs more steps
    pub async fn step(&mut self) -> Result<Option<Ordering<G::Id>>, RetrievalError> {
        // look up events in both frontiers
        let ids: Vec<G::Id> = self.subject_frontier.union(&self.other_frontier).cloned().collect();
        // TODO: create a NewType(HashSet) and impl ToSql for the postgres storage method
        // so we can pass the HashSet as a borrow and don't have to alloc this twice
        let mut result_checklist: HashSet<G::Id> = ids.iter().cloned().collect();
        // info!("step -> get_events {:?}", ids);
        let (cost, events) = self.getter.get_events(ids).await?;
        // info!("step -> get_events result {:?}", events.iter().map(|e| e.payload.id()).collect::<Vec<_>>());
        self.remaining_budget = self.remaining_budget.saturating_sub(cost);

        for event in events {
            if result_checklist.remove(&event.payload.id()) {
                self.process_event(event.payload.id(), event.payload.parent().members());
            }
        }
        if !result_checklist.is_empty() {
            return Err(RetrievalError::StorageError(format!("Events not found: {:?}", result_checklist).into()));
        }

        if let Some(ordering) = self.check_result() {
            return Ok(Some(ordering));
        }

        // keep going
        Ok(None)
    }
    fn process_event(&mut self, id: G::Id, parents: &[G::Id]) {
        let from_subject = self.subject_frontier.remove(&id);
        let from_other = self.other_frontier.remove(&id);

        // Process the current node and capture relevant state
        let (is_common, origins) = {
            let node_state = self.states.entry(id.clone()).or_default();
            node_state.mark_seen_from(from_subject, from_other);

            // Handle origins for "other" heads
            if from_other && self.original_other_events.contains(&id) {
                node_state.origins.add(id.clone());
            }

            // Capture state before dropping borrow
            (node_state.is_common(), node_state.origins.clone())
        };

        // Handle common node and parent updates
        if is_common && self.meet_candidates.insert(id.clone()) {
            self.any_common = true;

            // remove satisfied heads from the checklist
            for h in origins.iter() {
                self.outstanding_heads.remove(h);
            }

            // Update common child count and propagate origins in one pass over parents
            for p in parents {
                let parent_state = self.states.entry(p.clone()).or_default();
                if from_other {
                    parent_state.origins.augment(&origins);
                }
                parent_state.common_child_count += 1;
            }
        } else if from_other {
            // Just propagate origins if not a common node
            for p in parents {
                let parent_state = self.states.entry(p.clone()).or_default();
                parent_state.origins.augment(&origins);
            }
        }

        // Extend frontiers
        if from_subject {
            self.subject_frontier.extend(parents.iter().cloned());

            if self.original_other_events.contains(&id) {
                self.unseen_other_heads -= 1;
                self.head_overlap = true;
            }
        }
        if from_other {
            self.other_frontier.extend(parents.iter().cloned());
        }
    }

    fn check_result(&mut self) -> Option<Ordering<G::Id>> {
        if self.unseen_other_heads == 0 {
            return Some(Ordering::Descends);
        }

        if self.subject_frontier.is_empty() && self.other_frontier.is_empty() {
            // prune to minimal common ancestors
            let meet: Vec<_> = self
                .meet_candidates
                .iter()
                .filter(|id| self.states.get(*id).map_or(0, |state| state.common_child_count) == 0)
                .cloned()
                .collect();

            if !self.any_common {
                return Some(Ordering::Incomparable);
            }
            if !self.outstanding_heads.is_empty() {
                return Some(Ordering::Incomparable); // ≥ 1 head stayed isolated
            }
            if self.head_overlap {
                Some(Ordering::PartiallyDescends { meet })
            } else {
                Some(Ordering::NotDescends { meet })
            }
        } else if self.remaining_budget == 0 {
            Some(Ordering::BudgetExceeded { subject_frontier: self.subject_frontier.clone(), other_frontier: self.other_frontier.clone() })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use ankurah_proto::AttestationSet;

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

    #[async_trait]
    impl GetEvents for MockEventStore {
        type Id = TestId;
        type Event = TestEvent;

        async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
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

        // ancestor does not descend from descendant, but they both have a common ancestor: [1]
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { meet: vec![1] });
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
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { meet: vec![1] });
        }
        {
            // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
            let ancestor = TestClock { members: vec![2, 3] };
            let descendant = TestClock { members: vec![5] };

            assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), Ordering::Descends);
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { meet: vec![2, 3] });
        }

        {
            // a and b are fully concurrent, but still comparable
            let a = TestClock { members: vec![2] };
            let b = TestClock { members: vec![3] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::NotDescends { meet: vec![1] });
            assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), Ordering::NotDescends { meet: vec![1] });
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
                Ordering::PartiallyDescends { meet: vec![3] /* , concurrent_events: vec![2] */ }
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
            // line 509 - the assertions above are passing
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

            assert_eq!(
                compare(&store, &descendant, &ancestor, 2).await.unwrap(),
                Ordering::BudgetExceeded { subject_frontier: [2].into(), other_frontier: [].into() }
            );
        }
        {
            let ancestor = TestClock { members: vec![1] };
            let descendant = TestClock { members: vec![4, 5] };

            //  with a high enough budget, we can see that the descendant fully descends from the ancestor
            assert_eq!(compare(&store, &descendant, &ancestor, 10).await.unwrap(), Ordering::Descends);

            // when multiple paths are split across the budget, we can determine there's at least partial descent, but that's not accurate.
            // We can't declare partial descent until we've found a common ancestor, otherwise they might be incomparable.
            assert_eq!(
                compare(&store, &ancestor, &descendant, 2).await.unwrap(),
                Ordering::BudgetExceeded { subject_frontier: [].into(), other_frontier: [2].into() }
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
        assert_eq!(compare(&store, &clock, &clock, 100).await.unwrap(), Ordering::Equal);
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
        assert_eq!(compare(&store, &subject, &big_other, 1_000).await.unwrap(), Ordering::Descends);

        // In the opposite direction, none of the heads descend from 8, but they are comparable
        assert_eq!(compare(&store, &big_other, &subject, 1_000).await.unwrap(), Ordering::NotDescends { meet: vec![1, 2, 3, 4, 5, 6] });
    }
}
