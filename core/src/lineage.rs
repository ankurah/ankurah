use crate::{error::RetrievalError, storage::StorageCollection};
use ankurah_proto::{Attested, Clock, Event, EventId};
use async_trait::async_trait;

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
    type Id: Eq + PartialEq + Clone;
    type Event: TEvent<Id = Self::Id>;
    type Error;
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
        let events = self.get_events(event_ids).await?;
        Ok((1, events))
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
        /// The closest common full(identical) ancestor of comparison and subject
        common_ancestor: Vec<Id>,
    },

    /// subject and comparison have no common ancestor whatsoever
    Incomparable,

    /// subject partially descends from comparison (some but not all events in comparison are in subject's history)
    /// This means there exists a path from some events in comparison to some (but not all) events in subject
    PartiallyDescends {
        /// The closest common full(identical) ancestor of a and b
        common_ancestor: Vec<Id>,
        /// The immediate descendents of the common ancestor that are in b's history but not in a's history
        concurrent_events: Vec<Id>,
    },

    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        // what exactly do we need to resume the comparison?
        // we want the smallest set of events that would allow us to resume the comparison
    },
}

pub async fn compare<G, C>(getter: &G, subject: &C, comparison: &C, budget: usize) -> Result<Ordering<G::Id>, G::Error>
where
    G: GetEvents,
    G::Event: TEvent<Id = G::Id>,
    C: TClock<Id = G::Id>,
{
    todo!("implement this")
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
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestor: vec![1] });
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
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestor: vec![1] });
        }
        {
            // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
            let ancestor = TestClock { members: vec![2, 3] };
            let descendant = TestClock { members: vec![5] };

            assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), Ordering::Descends);
            assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), Ordering::NotDescends { common_ancestor: vec![2, 3] });
        }

        {
            // a and b are fully concurrent, but still comparable
            let a = TestClock { members: vec![2] };
            let b = TestClock { members: vec![3] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::NotDescends { common_ancestor: vec![1] });
            assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), Ordering::NotDescends { common_ancestor: vec![1] });
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
                Ordering::PartiallyDescends { common_ancestor: vec![1], concurrent_events: vec![2] }
            );
        }
    }

    #[tokio::test]
    async fn test_incomparable() {
        let mut store = MockEventStore::new();

        //   1        4
        //   ↓        ↓
        //   2        5
        //   ↓        ↓
        //   3        6
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[2]);

        store.add(4, &[]);
        store.add(5, &[4]);
        store.add(6, &[5]);

        {
            let a = TestClock { members: vec![3] };
            let b = TestClock { members: vec![6] };
            assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), Ordering::Incomparable);
        }
        {
            let a = TestClock { members: vec![2] };
            let b = TestClock { members: vec![6] };
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

        // linear chain that exceeds budget
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[2]);
        store.add(4, &[3]);

        let ancestor = TestClock { members: vec![1] };
        let descendant = TestClock { members: vec![4] };

        match compare(&store, &ancestor, &descendant, 2).await.unwrap() {
            Ordering::BudgetExceeded {} => {
                assert!(true); // Haven't found 1 yet when budget exceeded
            }
            _ => panic!("Expected BudgetExceeded"),
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
