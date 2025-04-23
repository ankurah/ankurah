use crate::{error::RetrievalError, storage::StorageCollection};
use ankurah_proto::{AttestationSet, Attested, Clock, Event, EventId};
use async_trait::async_trait;
use std::{
    collections::{HashSet, VecDeque},
    hash::Hash,
};

/// a trait for events and eventlike things that can be descended
pub trait TEvent {
    type Id: Eq + PartialEq + Clone + Hash;
    type Parent: TClock<Id = Self::Id>;

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self::Parent;
}

pub trait TClock {
    type Id: Eq + PartialEq + Clone + Hash;
    fn members(&self) -> &[Self::Id];
}
#[async_trait]
pub trait GetEvents {
    type Event: TEvent;
    type Error;
    async fn get_events(&self, event_ids: Vec<<Self::Event as TEvent>::Id>) -> Result<Vec<Attested<Self::Event>>, Self::Error>;
}

impl TClock for Clock {
    type Id = EventId;
    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for Event {
    type Id = EventId;
    type Parent = Clock;

    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &Clock { &self.parent }
}

#[async_trait]
impl<T: StorageCollection> GetEvents for T {
    type Event = Event;
    type Error = RetrievalError;

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> { self.get_events(event_ids).await }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ComparisonResult<Id> {
    /// b fully descends from a (all events in a are in b's history)
    /// This means there exists a path in the DAG from each event in b to every event in a
    Descends,
    /// b partially descends from a (some but not all events in a are in b's history)
    /// This means there exists a path from some events in b to some (but not all) events in a
    PartiallyDescends {
        /// Events from a found in b's history
        found_events: Vec<Id>,
    },
    /// The clocks are incomparable (b does not descend from a)
    /// This means either:
    /// - We found no events from a in b's history
    /// - We hit root events without finding all of a's events
    Incomparable,
    /// Recursion budget was exceeded before a determination could be made
    BudgetExceeded {
        /// Events from a found in b's history so far
        found_events: Vec<Id>,
    },
}

/// Compare two clocks a and b to determine if b descends from a.
///
/// This function searches backwards through b's history to find events from a.
/// The search starts at b's events and follows parent links until either:
/// 1. All of a's events are found (Descends)
/// 2. Some but not all of a's events are found (PartiallyDescends)
/// 3. No events from a are found, or root events are hit (Incomparable)
/// 4. The recursion budget is exceeded (BudgetExceeded)
pub async fn compare<T>(
    getter: &T,
    a: &<T::Event as TEvent>::Parent,
    b: &<T::Event as TEvent>::Parent,
    max_recursion: usize,
) -> Result<ComparisonResult<<T::Event as TEvent>::Id>, T::Error>
where
    T: GetEvents,
    T::Event: TEvent,
{
    if max_recursion == 0 {
        return Ok(ComparisonResult::BudgetExceeded { found_events: Vec::new() });
    }

    let a_members: HashSet<_> = a.members().iter().cloned().collect();
    let b_members = b.members();

    // Empty clocks are incomparable
    if a_members.is_empty() || b_members.is_empty() {
        return Ok(ComparisonResult::Incomparable);
    }

    // Track visited events to avoid cycles
    let mut visited = HashSet::new();
    // Track recursion depth
    let mut recursion_count = 0;
    // Track found events from a
    let mut found_events = Vec::new();
    // Track if we've hit root events for each branch
    let mut hit_roots = HashSet::new();

    // Start at b's events and search backwards through parents
    let mut to_check = VecDeque::from(b_members.to_vec());

    while let Some(event_id) = to_check.pop_front() {
        // Check recursion budget
        if recursion_count >= max_recursion {
            return Ok(ComparisonResult::BudgetExceeded { found_events });
        }
        recursion_count += 1;

        // Skip already visited events
        if !visited.insert(event_id.clone()) {
            continue;
        }

        // Get the event's parents and continue searching
        let events = getter.get_events(vec![event_id.clone()]).await?;
        if events.is_empty() {
            continue;
        }
        let event = &events[0].payload;

        // If this event is in a's clock, track it
        if a_members.contains(&event_id) {
            found_events.push(event_id.clone());
            // If we've found all events from a, we're done
            if found_events.len() == a_members.len() {
                return Ok(ComparisonResult::Descends);
            }
            // Continue searching parents to ensure we find all paths
        }

        let parent_clock = event.parent();

        // If we hit a root event, mark it and continue
        if parent_clock.members().is_empty() {
            hit_roots.insert(event_id);
            continue;
        }

        // Add parent events to check
        for parent_id in parent_clock.members() {
            if !visited.contains(parent_id) {
                to_check.push_back(parent_id.clone());
            }
        }
    }

    // If we found some but not all events from a, it's a partial descent
    if !found_events.is_empty() {
        return Ok(ComparisonResult::PartiallyDescends { found_events });
    }

    // If we hit root events without finding any events from a, they're incomparable
    if !hit_roots.is_empty() {
        return Ok(ComparisonResult::Incomparable);
    }

    // Otherwise, keep searching (this shouldn't happen in practice)
    Ok(ComparisonResult::Incomparable)
}

#[cfg(test)]
mod tests {
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
        type Event = TestEvent;
        type Error = ();

        async fn get_events(&self, event_ids: Vec<TestId>) -> Result<Vec<Attested<Self::Event>>, Self::Error> {
            let mut result = Vec::new();
            for id in event_ids {
                if let Some(event) = self.events.get(&id) {
                    result.push(event.clone());
                }
            }
            Ok(result)
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

        // Check that 3 descends from 1
        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), ComparisonResult::Descends);
        // Check that 1 and 3 are incomparable (we don't care about ascendency)
        assert_eq!(compare(&store, &descendant, &ancestor, 100).await.unwrap(), ComparisonResult::Incomparable);
    }

    #[tokio::test]
    async fn test_concurrent_history() {
        let mut store = MockEventStore::new();

        // Create a concurrent history:
        //    2
        // 1     4
        //    3
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[1]);
        store.add(4, &[2, 3]);

        let ancestor = TestClock { members: vec![2, 3] };
        let descendant = TestClock { members: vec![4] };

        assert_eq!(compare(&store, &ancestor, &descendant, 100).await.unwrap(), ComparisonResult::Descends);
    }

    #[tokio::test]
    async fn test_partial_descent() {
        let mut store = MockEventStore::new();

        // Create a partial descent:
        // [] <- 1 <- 2
        //   <- 3 <- 4
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[]);
        store.add(4, &[3]);

        let clock1 = TestClock { members: vec![2, 4] };
        let clock2 = TestClock { members: vec![2] };

        match compare(&store, &clock1, &clock2, 100).await.unwrap() {
            ComparisonResult::PartiallyDescends { found_events } => {
                assert_eq!(found_events, vec![2]);
            }
            _ => panic!("Expected PartiallyDescends"),
        }
    }

    #[tokio::test]
    async fn test_root_incomparable() {
        let mut store = MockEventStore::new();

        // Create two unrelated branches from root:
        // [] <- 1 <- 2
        // [] <- 3 <- 4
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[]);
        store.add(4, &[3]);

        let clock1 = TestClock { members: vec![2] };
        let clock2 = TestClock { members: vec![4] };

        assert_eq!(compare(&store, &clock1, &clock2, 100).await.unwrap(), ComparisonResult::Incomparable);
    }

    #[tokio::test]
    async fn test_empty_clocks() {
        let store = MockEventStore::new();

        let empty = TestClock { members: vec![] };
        let non_empty = TestClock { members: vec![1] };

        assert_eq!(compare(&store, &empty, &empty, 100).await.unwrap(), ComparisonResult::Incomparable);
        assert_eq!(compare(&store, &non_empty, &empty, 100).await.unwrap(), ComparisonResult::Incomparable);
        assert_eq!(compare(&store, &empty, &non_empty, 100).await.unwrap(), ComparisonResult::Incomparable);
    }

    #[tokio::test]
    async fn test_budget_exceeded() {
        let mut store = MockEventStore::new();

        // Create a deep chain that exceeds budget
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[2]);

        let ancestor = TestClock { members: vec![1] };
        let descendant = TestClock { members: vec![3] };

        match compare(&store, &ancestor, &descendant, 2).await.unwrap() {
            ComparisonResult::BudgetExceeded { found_events } => {
                assert!(found_events.is_empty()); // Haven't found 1 yet when budget exceeded
            }
            _ => panic!("Expected BudgetExceeded"),
        }
    }

    #[tokio::test]
    async fn test_partial_descent_multiple_branches() {
        let mut store = MockEventStore::new();

        // Create a history where some (but not all) events in b are ancestors of events in a:
        //    2 <- 4
        // 1
        //    3 <- 5
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[1]);
        store.add(4, &[2]);
        store.add(5, &[3]);

        let a = TestClock { members: vec![4, 5] };
        let b = TestClock { members: vec![2, 3] };

        // When comparing b to a, we should find that event 2 is an ancestor of event 4,
        // making this a partial descent relationship
        match compare(&store, &b, &a, 100).await.unwrap() {
            ComparisonResult::PartiallyDescends { found_events } => {
                assert_eq!(found_events, vec![2]);
            }
            _ => panic!("Expected PartiallyDescends"),
        }
    }

    #[tokio::test]
    async fn test_diamond_pattern() {
        let mut store = MockEventStore::new();

        // Create a diamond pattern:
        //    2
        // 1     4
        //    3
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[1]);
        store.add(4, &[2, 3]);

        let a = TestClock { members: vec![1] };
        let b = TestClock { members: vec![4] };

        // Both paths should lead to the same result
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), ComparisonResult::Descends);
    }

    #[tokio::test]
    async fn test_multiple_roots() {
        let mut store = MockEventStore::new();

        // Create a history with multiple roots in the same clock:
        // [] <- 1    [] <- 3
        //    <- 2       <- 4
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[]);
        store.add(4, &[3]);

        let a = TestClock { members: vec![2, 4] };
        let b = TestClock { members: vec![1, 3] };

        // Neither clock should descend from the other
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), ComparisonResult::Incomparable);
        assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), ComparisonResult::Incomparable);
    }

    #[tokio::test]
    async fn test_partial_overlap() {
        let mut store = MockEventStore::new();

        // Create a history with partial overlap:
        // 1 <- 2 <- 4
        //    <- 3 <- 5
        store.add(1, &[]);
        store.add(2, &[1]);
        store.add(3, &[1]);
        store.add(4, &[2]);
        store.add(5, &[3]);

        let a = TestClock { members: vec![4] };
        let b = TestClock { members: vec![5] };

        // Events share history but neither descends from the other
        assert_eq!(compare(&store, &a, &b, 100).await.unwrap(), ComparisonResult::Incomparable);
        assert_eq!(compare(&store, &b, &a, 100).await.unwrap(), ComparisonResult::Incomparable);
    }

    #[tokio::test]
    async fn test_self_comparison() {
        let mut store = MockEventStore::new();

        // Create a simple event to compare with itself
        store.add(1, &[]);
        store.add(2, &[1]);

        let clock = TestClock { members: vec![2] };

        // A clock should fully descend from itself
        assert_eq!(compare(&store, &clock, &clock, 100).await.unwrap(), ComparisonResult::Descends);
    }
}
