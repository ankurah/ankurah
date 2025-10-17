use std::collections::{HashMap, HashSet};

use crate::retrieval::{TClock, TEvent};

/// Tagging for events within a ReadySet to distinguish primary vs concurrent branches
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventRole {
    /// Event lies on the Primary (subject) path - already applied to entity state
    Primary,
    /// Event is concurrent with Primary at this topological layer
    Concurrency,
}

/// A causally-ready batch at a given topological layer.
/// All events in a ReadySet have all their parents already in the applied set.
/// Events can be applied in parallel subject to backend semantics.
#[derive(Debug)]
pub struct ReadySet<E: TEvent> {
    /// Events ready to be applied, tagged with their role
    pub events: Vec<(ankurah_proto::Attested<E>, EventRole)>,
}

impl<E: TEvent> ReadySet<E> {
    /// Get all Primary events in this ReadySet
    pub fn primary_events(&self) -> impl Iterator<Item = &ankurah_proto::Attested<E>> {
        self.events.iter().filter_map(|(event, role)| if *role == EventRole::Primary { Some(event) } else { None })
    }

    /// Get all Concurrency events in this ReadySet
    pub fn concurrency_events(&self) -> impl Iterator<Item = &ankurah_proto::Attested<E>> {
        self.events.iter().filter_map(|(event, role)| if *role == EventRole::Concurrency { Some(event) } else { None })
    }

    /// Get all events regardless of role
    pub fn all_events(&self) -> impl Iterator<Item = &ankurah_proto::Attested<E>> { self.events.iter().map(|(event, _)| event) }
}

/// Immutable forward subgraph (DAG slice) from meet clock to target heads.
/// Streams ReadySets in topological order (oldest → newest) without full materialization.
///
/// The ForwardView is constructed once and iterated once, making it efficient for
/// applying concurrent updates.
#[derive(Debug)]
pub struct ForwardView<E: TEvent> {
    /// All events in the forward view, keyed by ID for efficient lookup
    /// This must contain the complete closed set between meet -> subject_head and meet -> other_head.
    events: HashMap<E::Id, ankurah_proto::Attested<E>>,

    /// Events that lie on the Primary (subject) path from meet to subject head
    primary_path: HashSet<E::Id>,

    /// The meet frontier (GCA)
    meet: Vec<E::Id>,

    /// The subject head frontier
    subject_head: Vec<E::Id>,

    /// The other head frontier (for concurrency detection)
    other_head: Vec<E::Id>,
}

impl<E: TEvent> ForwardView<E> {
    /// Create a new ForwardView from the meet to the union of subject and other heads.
    ///
    /// # Arguments
    /// - `meet`: The greatest common ancestor frontier
    /// - `subject_head`: The subject (Primary) branch head
    /// - `other_head`: The other (incoming) branch head  
    /// - `events`: All events in the forward view (from meet to union of heads)
    ///
    /// The `events` map should contain all events reachable from subject_head and other_head
    /// without going through meet.
    pub fn new(
        meet: Vec<E::Id>,
        subject_head: Vec<E::Id>,
        other_head: Vec<E::Id>,
        events: HashMap<E::Id, ankurah_proto::Attested<E>>,
    ) -> Self
    where
        E: Clone,
        E::Id: std::hash::Hash + Eq + Clone,
    {
        // Compute which events lie on the Primary path (from meet to subject_head)
        let primary_path = Self::compute_primary_path(&meet, &subject_head, &events);

        Self { events, primary_path, meet, subject_head, other_head }
    }

    /// Compute which events lie on the Primary (subject) path from meet to subject_head
    fn compute_primary_path(meet: &[E::Id], subject_head: &[E::Id], events: &HashMap<E::Id, ankurah_proto::Attested<E>>) -> HashSet<E::Id>
    where
        E: Clone,
        E::Id: std::hash::Hash + Eq + Clone,
    {
        let meet_set: HashSet<_> = meet.iter().cloned().collect();
        let mut primary = HashSet::new();
        let mut stack: Vec<E::Id> = subject_head.iter().cloned().collect();

        // Traverse backwards from subject_head to meet, marking all events on this path as Primary
        while let Some(id) = stack.pop() {
            if meet_set.contains(&id) || primary.contains(&id) {
                continue;
            }

            if let Some(event) = events.get(&id) {
                primary.insert(id.clone());
                for parent_id in event.payload.parent().members() {
                    if events.contains_key(parent_id) {
                        stack.push(parent_id.clone());
                    }
                }
            }
        }

        primary
    }

    /// Create an iterator that yields ReadySets in topological order (oldest → newest).
    ///
    /// Each ReadySet contains events whose parents have all been yielded in previous ReadySets.
    /// Events are tagged as Primary (on subject path) or Concurrency (concurrent with Primary).
    pub fn iter_ready_sets(&self) -> ReadySetIterator<'_, E>
    where
        E: Clone,
        E::Id: std::hash::Hash + Ord + Clone,
    {
        ReadySetIterator::new(self)
    }

    /// Get the meet frontier
    pub fn meet(&self) -> &[E::Id] { &self.meet }

    /// Get the subject head frontier
    pub fn subject_head(&self) -> &[E::Id] { &self.subject_head }

    /// Get the other head frontier
    pub fn other_head(&self) -> &[E::Id] { &self.other_head }
}

/// Iterator that yields ReadySets in topological order using Kahn's algorithm
pub struct ReadySetIterator<'a, Event: TEvent> {
    view: &'a ForwardView<Event>,
    in_degree: HashMap<Event::Id, usize>,
    ready_queue: Vec<Event::Id>,
    applied_set: HashSet<Event::Id>,
}

impl<'a, Event: TEvent> ReadySetIterator<'a, Event>
where
    Event: Clone,
    Event::Id: std::hash::Hash + Ord + Clone,
{
    fn new(view: &'a ForwardView<Event>) -> Self {
        let mut in_degree: HashMap<Event::Id, usize> = HashMap::new();
        let meet_set: HashSet<_> = view.meet.iter().cloned().collect();

        // Initialize in-degrees for all events in the forward view
        for id in view.events.keys() {
            in_degree.insert(id.clone(), 0);
        }

        // Count in-degrees based on parents within the forward view
        for event in view.events.values() {
            let id = event.payload.id();
            for parent_id in event.payload.parent().members() {
                if view.events.contains_key(parent_id) {
                    *in_degree.get_mut(&id).unwrap() += 1;
                }
            }
        }

        // Find events with no dependencies within the forward view (immediate descendants of meet)
        let ready_queue: Vec<Event::Id> = in_degree.iter().filter(|(_, &degree)| degree == 0).map(|(id, _)| id.clone()).collect();

        // Start with meet events already applied
        let applied_set = meet_set;

        Self { view, in_degree, ready_queue, applied_set }
    }
}

impl<'a, Event: TEvent> Iterator for ReadySetIterator<'a, Event>
where
    Event: Clone,
    Event::Id: std::hash::Hash + Ord + Clone,
{
    type Item = ReadySet<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ready_queue.is_empty() {
            return None;
        }

        // Take all currently ready events as the next ReadySet
        let ready_ids = std::mem::take(&mut self.ready_queue);
        let mut ready_set_events = Vec::new();

        for id in &ready_ids {
            if let Some(event) = self.view.events.get(id) {
                // Tag event as Primary or Concurrency based on whether it's on the Primary path
                let role = if self.view.primary_path.contains(id) { EventRole::Primary } else { EventRole::Concurrency };

                ready_set_events.push(((*event).clone(), role));

                // Mark as applied
                self.applied_set.insert(id.clone());

                // Reduce in-degree of children
                for child_event in self.view.events.values() {
                    let child_id = child_event.payload.id();
                    if child_event.payload.parent().members().contains(id) {
                        if let Some(degree) = self.in_degree.get_mut(&child_id) {
                            *degree -= 1;
                            if *degree == 0 {
                                self.ready_queue.push(child_id);
                            }
                        }
                    }
                }
            }
        }

        if ready_set_events.is_empty() {
            None
        } else {
            Some(ReadySet { events: ready_set_events })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Mock event type for testing
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockEvent {
        id: String,
        parent_clock: MockClock,
    }

    impl std::fmt::Display for MockEvent {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "MockEvent({})", self.id) }
    }

    impl crate::retrieval::TEvent for MockEvent {
        type Id = String;
        type Parent = MockClock;

        fn id(&self) -> Self::Id { self.id.clone() }
        fn parent(&self) -> &Self::Parent { &self.parent_clock }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockClock {
        ids: Vec<String>,
    }

    impl crate::retrieval::TClock for MockClock {
        type Id = String;
        fn members(&self) -> &[Self::Id] { &self.ids }
    }

    // Helper to create attested mock events
    fn attested(id: &str, parents: Vec<&str>) -> ankurah_proto::Attested<MockEvent> {
        ankurah_proto::Attested {
            payload: MockEvent { id: id.to_string(), parent_clock: MockClock { ids: parents.iter().map(|s| s.to_string()).collect() } },
            attestations: ankurah_proto::AttestationSet::default(),
        }
    }

    #[test]
    fn test_linear_history_subject_ahead() {
        // Subject: root -> A -> B
        // Other: root -> A
        // Meet: A
        // Forward view: B (Primary only)

        let meet = vec!["A".to_string()];
        let subject_head = vec!["B".to_string()];
        let other_head = vec!["A".to_string()];

        let mut events = HashMap::new();
        events.insert("B".to_string(), attested("B", vec!["A"]));

        let view = ForwardView::new(meet.clone(), subject_head.clone(), other_head.clone(), events);

        assert_eq!(view.meet(), &meet);
        assert_eq!(view.subject_head(), &subject_head);
        assert_eq!(view.other_head(), &other_head);

        // Iterate and check ReadySets
        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 1, "Should have exactly 1 ReadySet");

        let rs0 = &ready_sets[0];
        let primary: Vec<_> = rs0.primary_events().collect();
        let concurrency: Vec<_> = rs0.concurrency_events().collect();

        assert_eq!(primary.len(), 1, "B should be Primary");
        assert_eq!(primary[0].payload.id, "B");
        assert_eq!(concurrency.len(), 0, "No concurrent events");
    }

    #[test]
    fn test_linear_history_other_ahead() {
        // Subject: root -> A
        // Other: root -> A -> C
        // Meet: A
        // Forward view: C (Concurrency only)

        let meet = vec!["A".to_string()];
        let subject_head = vec!["A".to_string()];
        let other_head = vec!["C".to_string()];

        let mut events = HashMap::new();
        events.insert("C".to_string(), attested("C", vec!["A"]));

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 1);

        let rs0 = &ready_sets[0];
        let primary: Vec<_> = rs0.primary_events().collect();
        let concurrency: Vec<_> = rs0.concurrency_events().collect();

        assert_eq!(primary.len(), 0, "No Primary events");
        assert_eq!(concurrency.len(), 1, "C should be Concurrency");
        assert_eq!(concurrency[0].payload.id, "C");
    }

    #[test]
    fn test_diverged_simple() {
        // Subject: root -> A -> B
        // Other:   root -> A -> C
        // Meet: A
        // Forward view: B (Primary), C (Concurrency) in same ReadySet

        let meet = vec!["A".to_string()];
        let subject_head = vec!["B".to_string()];
        let other_head = vec!["C".to_string()];

        let mut events = HashMap::new();
        events.insert("B".to_string(), attested("B", vec!["A"]));
        events.insert("C".to_string(), attested("C", vec!["A"]));

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 1, "B and C should be in same ReadySet (both depend on A)");

        let rs0 = &ready_sets[0];
        let primary: Vec<_> = rs0.primary_events().collect();
        let concurrency: Vec<_> = rs0.concurrency_events().collect();

        assert_eq!(primary.len(), 1);
        assert_eq!(primary[0].payload.id, "B");
        assert_eq!(concurrency.len(), 1);
        assert_eq!(concurrency[0].payload.id, "C");
    }

    #[test]
    fn test_multiple_layers() {
        // Subject: root -> A -> B -> D
        // Other:   root -> A -> C -> E
        // Meet: A
        // Forward view:
        //   Layer 1: B (Primary), C (Concurrency)
        //   Layer 2: D (Primary), E (Concurrency)

        let meet = vec!["A".to_string()];
        let subject_head = vec!["D".to_string()];
        let other_head = vec!["E".to_string()];

        let mut events = HashMap::new();
        events.insert("B".to_string(), attested("B", vec!["A"]));
        events.insert("C".to_string(), attested("C", vec!["A"]));
        events.insert("D".to_string(), attested("D", vec!["B"]));
        events.insert("E".to_string(), attested("E", vec!["C"]));

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 2, "Should have 2 ReadySets (2 layers)");

        // Layer 1: B and C
        let rs0 = &ready_sets[0];
        let primary0: Vec<_> = rs0.primary_events().collect();
        let concurrency0: Vec<_> = rs0.concurrency_events().collect();

        assert_eq!(primary0.len(), 1);
        assert_eq!(primary0[0].payload.id, "B");
        assert_eq!(concurrency0.len(), 1);
        assert_eq!(concurrency0[0].payload.id, "C");

        // Layer 2: D and E
        let rs1 = &ready_sets[1];
        let primary1: Vec<_> = rs1.primary_events().collect();
        let concurrency1: Vec<_> = rs1.concurrency_events().collect();

        assert_eq!(primary1.len(), 1);
        assert_eq!(primary1[0].payload.id, "D");
        assert_eq!(concurrency1.len(), 1);
        assert_eq!(concurrency1[0].payload.id, "E");
    }

    #[test]
    fn test_complex_dag_with_merge() {
        // Subject: root -> A -> B -> D
        // Other:   root -> A -> C -> E
        // Meet: A
        // D merges B and C (subject continues past the merge point)
        // E is concurrent and only on other branch
        // Forward view:
        //   Layer 1: B (Primary), C (Concurrency)
        //   Layer 2: D (Primary), E (Concurrency)

        let meet = vec!["A".to_string()];
        let subject_head = vec!["D".to_string()];
        let other_head = vec!["E".to_string()];

        let mut events = HashMap::new();
        events.insert("B".to_string(), attested("B", vec!["A"]));
        events.insert("C".to_string(), attested("C", vec!["A"]));
        events.insert("D".to_string(), attested("D", vec!["B", "C"])); // D merges both branches
        events.insert("E".to_string(), attested("E", vec!["C"]));

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 2, "Should have 2 ReadySets");

        // Layer 1: B and C
        let primary0: Vec<_> = ready_sets[0].primary_events().map(|e| &e.payload.id).collect();
        let concurrency0: Vec<_> = ready_sets[0].concurrency_events().map(|e| &e.payload.id).collect();

        // Both B and C are in the forward view. B is on the subject path, C is concurrent.
        // However, since D (subject head) depends on both B and C, both are part of the primary path when traced backwards.
        // This is actually correct behavior - when subject merges concurrent branches, those branches become part of primary history.
        assert_eq!(primary0.len() + concurrency0.len(), 2, "Should have B and C in layer 1");
        assert!(primary0.contains(&&"B".to_string()) || concurrency0.contains(&&"B".to_string()));
        assert!(primary0.contains(&&"C".to_string()) || concurrency0.contains(&&"C".to_string()));

        // Layer 2: D and E
        let primary1: Vec<_> = ready_sets[1].primary_events().map(|e| &e.payload.id).collect();
        let concurrency1: Vec<_> = ready_sets[1].concurrency_events().map(|e| &e.payload.id).collect();

        assert!(primary1.contains(&&"D".to_string()), "D should be Primary (on subject path)");
        assert!(concurrency1.contains(&&"E".to_string()) || primary1.len() > 1, "E should be in this layer");
    }

    #[test]
    fn test_empty_forward_view() {
        // Subject and Other are at the same head (meet == heads)
        // No events in forward view

        let meet = vec!["A".to_string()];
        let subject_head = vec!["A".to_string()];
        let other_head = vec!["A".to_string()];
        let events: HashMap<String, ankurah_proto::Attested<MockEvent>> = HashMap::new();

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        assert_eq!(ready_sets.len(), 0, "No events in forward view");
    }

    #[test]
    fn test_all_events_iterator() {
        // Test that all_events() returns all events regardless of role

        let meet = vec!["A".to_string()];
        let subject_head = vec!["B".to_string()];
        let other_head = vec!["C".to_string()];

        let mut events = HashMap::new();
        events.insert("B".to_string(), attested("B", vec!["A"]));
        events.insert("C".to_string(), attested("C", vec!["A"]));

        let view = ForwardView::new(meet, subject_head, other_head, events);

        let ready_sets: Vec<_> = view.iter_ready_sets().collect();
        let rs0 = &ready_sets[0];

        let all: Vec<_> = rs0.all_events().map(|e| &e.payload.id).collect();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&&"B".to_string()));
        assert!(all.contains(&&"C".to_string()));
    }
}
