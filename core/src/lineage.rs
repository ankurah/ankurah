use crate::{error::RetrievalError, storage::StorageCollection};
use ankurah_proto::{Attested, Clock, EventId};
use async_trait::async_trait;
use smallvec::SmallVec;
use std::collections::{BTreeSet, HashMap};

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
    type Error;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
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

#[async_trait]
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
        // The immediate descendents of the common ancestor that are in b's history but not in a's history
        // concurrent_events: Vec<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        // what exactly do we need to resume the comparison?
        subject_frontier: BTreeSet<Id>,
        other_frontier: BTreeSet<Id>,
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
    println!("[DEBUG] comparing {:?} and {:?}", subject.members(), other.members());

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

type Origins<Id> = SmallVec<[Id; 8]>;

pub(crate) struct Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    /* external inputs */
    getter: &'a G,
    original_other_events: BTreeSet<G::Id>, // immutable snapshot

    /* mutable checklist: heads still looking for a common ancestor */
    outstanding_heads: BTreeSet<G::Id>,

    remaining_budget: usize,

    /* search frontiers */
    subject_frontier: BTreeSet<G::Id>,
    other_frontier: BTreeSet<G::Id>,

    /* per-node bookkeeping: (seen_from_subject, seen_from_other, origin heads) */
    seen: HashMap<G::Id, (bool, bool)>,
    origins: HashMap<G::Id, Origins<G::Id>>,

    /* incremental meet construction */
    meet_candidates: BTreeSet<G::Id>,
    common_child_cnt: HashMap<G::Id, usize>,

    /* enum-decision flags */
    unseen_other_heads: usize,
    head_overlap: bool,
    any_common: bool,
}

impl<'a, G> Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    pub fn new<C: TClock<Id = G::Id>>(getter: &'a G, subject: &C, other: &C, budget: usize) -> Self {
        // println!("Creating comparison with subject: {:?}, other: {:?}", subject.members(), other.members());
        // Initialize working sets with the initial events
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

            seen: HashMap::new(),
            origins: HashMap::new(),
            outstanding_heads: other,
            meet_candidates: BTreeSet::new(),
            common_child_cnt: HashMap::new(),
        }
    }

    // runs one step of the comparison, returning Some(ordering) if a conclusive determination can be made, or None if it needs more steps
    pub async fn step(&mut self) -> Result<Option<Ordering<G::Id>>, G::Error> {
        // println!("Step called - subject_ws: {:?}, other_ws: {:?}", self.subject_frontier, self.other_frontier);

        // look up events in both frontiers
        let ids: Vec<G::Id> = self.subject_frontier.union(&self.other_frontier).cloned().collect();
        let (cost, events) = self.getter.get_events(ids).await?;
        self.remaining_budget = self.remaining_budget.saturating_sub(cost);

        self.process_events(&events);

        if let Some(ordering) = self.check_result() {
            return Ok(Some(ordering));
        }

        // keep going
        Ok(None)
    }
    fn process_events(&mut self, events: &[Attested<G::Event>]) {
        for event in events {
            let id = event.payload.id();
            let parents = event.payload.parent().members().to_vec();
            /* ── get (or insert) entries in the two maps ───────────────────── */

            let origins_entry = self.origins.entry(id.clone()).or_insert(Origins::<G::Id>::new());

            /* ── mark which side(s) we came from ───────────────────────────── */
            let from_subject = self.subject_frontier.remove(&id);
            let from_other = self.other_frontier.remove(&id);

            let seen_entry = {
                let seen_entry = self.seen.entry(id.clone()).or_insert((false, false));

                if from_subject {
                    seen_entry.0 = true;
                }
                if from_other {
                    seen_entry.1 = true;
                }
                seen_entry.clone()
            };
            /* ── add the head itself to the origin list if this is an “other” head ─ */
            if from_other && self.original_other_events.contains(&id) {
                if !origins_entry.contains(&id) {
                    origins_entry.push(id.clone());
                    println!("origins_entry: {:?}", origins_entry);
                }
            }

            /* grab the tag slice and drop the mutable borrow before looping parents */
            let tag_slice = origins_entry.clone();

            /* ── propagate origin tags *only* when the visit was via OTHER side ─── */
            if from_other {
                for p in parents.iter() {
                    let parent_orig = self.origins.entry(p.clone()).or_insert(Origins::<G::Id>::new());
                    // let mut added = false;
                    for h in tag_slice.iter() {
                        if !parent_orig.contains(h) {
                            parent_orig.push(h.clone());
                            println!("parent_orig: {:?}", parent_orig);
                        }
                    }
                }
            }

            /* ── extend the frontier(s) we came from ───────────────────────── */
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

            /* ── did this node just become common? ─────────────────────────── */
            if seen_entry.0 && seen_entry.1 && self.meet_candidates.insert(id.clone()) {
                eprintln!(
                    "[DEBUG] COMMON node {:?}: origins = {:?}; outstanding_heads left = {}",
                    id,
                    tag_slice,
                    self.outstanding_heads.len()
                );
                self.any_common = true;

                // remove satisfied heads from the checklist
                for h in tag_slice.iter() {
                    self.outstanding_heads.remove(h);
                }

                for p in parents {
                    *self.common_child_cnt.entry(p.clone()).or_default() += 1;
                }
            }
        }
    }

    /*───────────────────────────────────────────────────────────────*/
    /* 2.  decide once queues drain (unchanged from earlier)         */
    /*───────────────────────────────────────────────────────────────*/
    fn check_result(&mut self) -> Option<Ordering<G::Id>> {
        if self.unseen_other_heads == 0 {
            return Some(Ordering::Descends);
        }

        if self.subject_frontier.is_empty() && self.other_frontier.is_empty() {
            // prune to minimal common ancestors
            let meet: Vec<_> =
                self.meet_candidates.iter().filter(|id| self.common_child_cnt.get(*id).copied().unwrap_or(0) == 0).cloned().collect();

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
    ///
    // DAG structure
    //
    //   1   2   3   4   5   6       ← six independent heads (other-clock)
    //   │   │   │   │   │   │
    //   ╰─┬─┴─┬─┴─┬─┴─┬─┴─┬─╯
    //     │   │   │   │   │
    //     7 (fan-in node)
    //     │
    //     8       ← subject head (descends only from 1)
    //
    //  meet({8},{2-6})  = [1]
    //  direction 8→{2-6}:  NotDescends
    //  direction {2-6}→8:  PartiallyDescends
    ///
    #[tokio::test]
    async fn test_many_heads_meet_at_merge() {
        // Why this stresses the design
        // The merge node 7 carries six origin tags upward, forcing a heap spill in the SmallVec‐based Origins.
        // All six heads are removed from outstanding_heads in a single step when node 1‥6 first become common, stressing the “check-list” logic.
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

        // 8 shares ancestor 7 with every head in big_other, but does not contain them.
        assert_eq!(compare(&store, &subject, &big_other, 1_000).await.unwrap(), Ordering::Descends);

        // In the opposite direction, big_other partially descends from subject’s history
        // because one of its heads (1) is on the path to 8, while the others are not.
        assert_eq!(compare(&store, &big_other, &subject, 1_000).await.unwrap(), Ordering::NotDescends { meet: vec![1, 2, 3, 4, 5, 6] });
    }
}
