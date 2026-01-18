//! Layer computation for concurrent event application.
//!
//! This module provides the [`EventLayer`] type and [`compute_layers`] function
//! for organizing events into concurrency layers for unified backend application.

use super::traits::{EventId, TClock, TEvent};
use crate::error::RetrievalError;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// A layer of concurrent events for unified backend application.
///
/// All events in a layer are mutually concurrent (same causal depth from the meet point).
/// The layer is partitioned into:
/// - `already_applied`: Events already in the current state (for context)
/// - `to_apply`: Events that need to be processed
#[derive(Debug, Clone)]
pub struct EventLayer<Id, E> {
    /// Events at this layer already in our state (for context/comparison).
    pub already_applied: Vec<E>,
    /// Events at this layer we need to process.
    pub to_apply: Vec<E>,
    events: Arc<BTreeMap<Id, E>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}

impl<Id, E> EventLayer<Id, E>
where
    Id: EventId,
    E: TEvent<Id = Id> + Clone,
    E::Parent: TClock<Id = Id>,
{
    /// Create a new empty layer.
    pub fn new(events: Arc<BTreeMap<Id, E>>) -> Self { Self { already_applied: Vec::new(), to_apply: Vec::new(), events } }

    pub fn new_with_context(already_applied: Vec<E>, to_apply: Vec<E>, events: Arc<BTreeMap<Id, E>>) -> Self {
        Self { already_applied, to_apply, events }
    }

    /// Check if this layer has any events to apply.
    pub fn has_work(&self) -> bool { !self.to_apply.is_empty() }

    /// Total number of events in this layer.
    pub fn len(&self) -> usize { self.already_applied.len() + self.to_apply.len() }

    /// Check if layer is empty.
    pub fn is_empty(&self) -> bool { self.already_applied.is_empty() && self.to_apply.is_empty() }

    /// Compare two event IDs using accumulated DAG context.
    /// Returns Err if relation cannot be proven with available data.
    pub fn compare(&self, a: &Id, b: &Id) -> Result<CausalRelation, RetrievalError> {
        if a == b {
            return Ok(CausalRelation::Descends);
        }

        let a_ancestry = collect_ancestry(&self.events, a)?;
        let b_ancestry = collect_ancestry(&self.events, b)?;

        if a_ancestry.contains(b) {
            return Ok(CausalRelation::Descends);
        }
        if b_ancestry.contains(a) {
            return Ok(CausalRelation::Ascends);
        }

        Ok(CausalRelation::Concurrent)
    }
}

impl<Id, E> Default for EventLayer<Id, E>
where
    Id: EventId,
    E: TEvent<Id = Id> + Clone,
    E::Parent: TClock<Id = Id>,
{
    fn default() -> Self { Self::new(Arc::new(BTreeMap::new())) }
}

/// Compute concurrency layers from a set of events.
///
/// Given:
/// - `events`: Map of all events in the merged DAG (from both branches)
/// - `meet`: The meet point (common ancestor frontier)
/// - `current_head_ancestry`: Set of event IDs that are ancestors of (or equal to) current head
///
/// Returns layers in causal order (layer 0 is closest to meet).
/// Each layer contains events partitioned into `already_applied` and `to_apply`.
///
/// # Algorithm
///
/// Uses frontier expansion from the meet point:
/// 1. Start with immediate children of meet as the frontier
/// 2. Partition frontier into already_applied / to_apply based on current_head_ancestry
/// 3. Create layer if there are events to apply
/// 4. Advance frontier to children whose parents are all processed
/// 5. Repeat until frontier is empty
pub fn compute_layers<Id, E>(
    events: &BTreeMap<Id, E>,
    meet: &[Id],
    current_head_ancestry: &BTreeSet<Id>,
) -> Vec<EventLayer<Id, E>>
where
    Id: EventId,
    E: TEvent<Id = Id> + Clone,
    E::Parent: TClock<Id = Id>,
{
    let mut layers = Vec::new();
    let events = Arc::new(events.clone());
    let meet_set: BTreeSet<Id> = meet.iter().cloned().collect();
    let mut processed: BTreeSet<Id> = meet_set.clone();

    // Start with immediate children of meet
    let mut frontier: BTreeSet<Id> = meet.iter().flat_map(|m| children_of(&events, m)).collect();

    while !frontier.is_empty() {
        // Partition frontier into already_applied vs to_apply
        let mut already_applied = Vec::new();
        let mut to_apply = Vec::new();

        for event_id in &frontier {
            if let Some(event) = events.get(event_id) {
                if current_head_ancestry.contains(event_id) {
                    already_applied.push(event.clone());
                } else {
                    to_apply.push(event.clone());
                }
            }
        }

        // Only create layer if there's something to apply
        if !to_apply.is_empty() {
            layers.push(EventLayer::new_with_context(already_applied, to_apply, Arc::clone(&events)));
        }

        // Mark frontier as processed
        processed.extend(frontier.iter().cloned());

        // Advance to next frontier: children whose parents are all processed
        frontier = frontier
            .iter()
            .flat_map(|id| children_of(&events, id))
            .filter(|id| !processed.contains(id))
            .filter(|id| {
                // Only include if all parents are processed
                if let Some(event) = events.get(id) {
                    event.parent().members().iter().all(|p| processed.contains(p))
                } else {
                    false
                }
            })
            .collect();
    }

    layers
}

/// Find children of an event in the event map.
///
/// A child is an event whose parent clock contains the given event ID.
fn children_of<Id, E>(events: &BTreeMap<Id, E>, parent: &Id) -> Vec<Id>
where
    Id: EventId,
    E: TEvent<Id = Id>,
    E::Parent: TClock<Id = Id>,
{
    events.iter().filter(|(_, e)| e.parent().members().contains(parent)).map(|(id, _)| id.clone()).collect()
}

/// Compute the ancestry set for a given head.
///
/// Returns all event IDs that are ancestors of (or equal to) any event in the head.
/// This is used to partition events into already_applied vs to_apply.
pub fn compute_ancestry<Id, E>(events: &BTreeMap<Id, E>, head: &[Id]) -> BTreeSet<Id>
where
    Id: EventId,
    E: TEvent<Id = Id>,
    E::Parent: TClock<Id = Id>,
{
    let mut ancestry = BTreeSet::new();
    let mut frontier: BTreeSet<Id> = head.iter().cloned().collect();

    while !frontier.is_empty() {
        let mut next_frontier = BTreeSet::new();

        for id in frontier {
            if ancestry.insert(id.clone()) {
                // Newly added - explore parents
                if let Some(event) = events.get(&id) {
                    for parent in event.parent().members() {
                        next_frontier.insert(parent.clone());
                    }
                }
            }
        }

        frontier = next_frontier;
    }

    ancestry
}

fn collect_ancestry<Id, E>(events: &BTreeMap<Id, E>, start: &Id) -> Result<BTreeSet<Id>, RetrievalError>
where
    Id: EventId,
    E: TEvent<Id = Id>,
    E::Parent: TClock<Id = Id>,
{
    let mut ancestry = BTreeSet::new();
    let mut frontier: Vec<Id> = vec![start.clone()];

    while let Some(id) = frontier.pop() {
        if !ancestry.insert(id.clone()) {
            continue;
        }
        let event = events
            .get(&id)
            .ok_or_else(|| RetrievalError::Other(format!("missing event for ancestry lookup: {}", id)))?;
        for parent in event.parent().members() {
            if !ancestry.contains(parent) {
                frontier.push(parent.clone());
            }
        }
    }

    Ok(ancestry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock types for testing
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct MockId(u64);

    impl std::fmt::Display for MockId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "E{}", self.0) }
    }

    impl EventId for MockId {}

    #[derive(Debug, Clone)]
    struct MockClock(Vec<MockId>);

    impl TClock for MockClock {
        type Id = MockId;
        fn members(&self) -> &[Self::Id] { &self.0 }
    }

    #[derive(Debug, Clone)]
    struct MockEvent {
        id: MockId,
        parent: MockClock,
    }

    impl std::fmt::Display for MockEvent {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Event({})", self.id) }
    }

    impl TEvent for MockEvent {
        type Id = MockId;
        type Parent = MockClock;

        fn id(&self) -> Self::Id { self.id.clone() }

        fn parent(&self) -> &Self::Parent { &self.parent }
    }

    fn mock_event(id: u64, parents: &[u64]) -> MockEvent {
        MockEvent { id: MockId(id), parent: MockClock(parents.iter().map(|&p| MockId(p)).collect()) }
    }

    #[test]
    fn test_simple_two_branch() {
        // DAG:
        //     A (meet)
        //    / \
        //   B   C
        //
        // Current head: [B]
        // Incoming: [C]
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),  // A - genesis/meet
            (MockId(2), mock_event(2, &[1])), // B - already applied
            (MockId(3), mock_event(3, &[1])), // C - to apply
        ]
        .into_iter()
        .collect();

        let meet = vec![MockId(1)];
        let current_ancestry: BTreeSet<_> = [MockId(1), MockId(2)].into_iter().collect();

        let layers = compute_layers(&events, &meet, &current_ancestry);

        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].already_applied.len(), 1);
        assert_eq!(layers[0].to_apply.len(), 1);
        assert_eq!(layers[0].already_applied[0].id, MockId(2)); // B
        assert_eq!(layers[0].to_apply[0].id, MockId(3)); // C
    }

    #[test]
    fn test_multi_layer() {
        // DAG:
        //     A (meet)
        //    / \
        //   B   C
        //   |   |
        //   D   E
        //
        // Current head: [D]
        // Incoming: [E]
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),  // A
            (MockId(2), mock_event(2, &[1])), // B
            (MockId(3), mock_event(3, &[1])), // C
            (MockId(4), mock_event(4, &[2])), // D
            (MockId(5), mock_event(5, &[3])), // E
        ]
        .into_iter()
        .collect();

        let meet = vec![MockId(1)];
        let current_ancestry: BTreeSet<_> = [MockId(1), MockId(2), MockId(4)].into_iter().collect();

        let layers = compute_layers(&events, &meet, &current_ancestry);

        assert_eq!(layers.len(), 2);

        // Layer 0: B (already), C (to_apply)
        assert_eq!(layers[0].already_applied.len(), 1);
        assert_eq!(layers[0].to_apply.len(), 1);

        // Layer 1: D (already), E (to_apply)
        assert_eq!(layers[1].already_applied.len(), 1);
        assert_eq!(layers[1].to_apply.len(), 1);
    }

    #[test]
    fn test_strict_descends_no_concurrency() {
        // DAG:
        //   A (meet/current head)
        //   |
        //   B
        //   |
        //   C (incoming)
        //
        // All events are to_apply (none already applied except meet)
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),  // A
            (MockId(2), mock_event(2, &[1])), // B
            (MockId(3), mock_event(3, &[2])), // C
        ]
        .into_iter()
        .collect();

        let meet = vec![MockId(1)];
        let current_ancestry: BTreeSet<_> = [MockId(1)].into_iter().collect();

        let layers = compute_layers(&events, &meet, &current_ancestry);

        assert_eq!(layers.len(), 2);

        // Layer 0: B (to_apply only)
        assert_eq!(layers[0].already_applied.len(), 0);
        assert_eq!(layers[0].to_apply.len(), 1);

        // Layer 1: C (to_apply only)
        assert_eq!(layers[1].already_applied.len(), 0);
        assert_eq!(layers[1].to_apply.len(), 1);
    }

    #[test]
    fn test_three_way_concurrency() {
        // DAG:
        //       A (meet)
        //      /|\
        //     B C D
        //
        // Current head: [B]
        // Incoming: [C, D]
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),  // A
            (MockId(2), mock_event(2, &[1])), // B
            (MockId(3), mock_event(3, &[1])), // C
            (MockId(4), mock_event(4, &[1])), // D
        ]
        .into_iter()
        .collect();

        let meet = vec![MockId(1)];
        let current_ancestry: BTreeSet<_> = [MockId(1), MockId(2)].into_iter().collect();

        let layers = compute_layers(&events, &meet, &current_ancestry);

        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].already_applied.len(), 1); // B
        assert_eq!(layers[0].to_apply.len(), 2); // C, D
    }

    #[test]
    fn test_diamond_reconvergence() {
        // DAG:
        //     A (meet)
        //    / \
        //   B   C
        //    \ /
        //     D
        //
        // Current head: [B]
        // Incoming: [D] (via C)
        // Note: D requires both B and C as parents
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),     // A
            (MockId(2), mock_event(2, &[1])),    // B
            (MockId(3), mock_event(3, &[1])),    // C
            (MockId(4), mock_event(4, &[2, 3])), // D (parents: B, C)
        ]
        .into_iter()
        .collect();

        let meet = vec![MockId(1)];
        let current_ancestry: BTreeSet<_> = [MockId(1), MockId(2)].into_iter().collect();

        let layers = compute_layers(&events, &meet, &current_ancestry);

        // Layer 0: B (already), C (to_apply)
        // Layer 1: D (to_apply) - only after both B and C are processed
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0].already_applied.len(), 1);
        assert_eq!(layers[0].to_apply.len(), 1);
        assert_eq!(layers[1].already_applied.len(), 0);
        assert_eq!(layers[1].to_apply.len(), 1);
    }

    #[test]
    fn test_compute_ancestry() {
        let events: BTreeMap<MockId, MockEvent> = [
            (MockId(1), mock_event(1, &[])),
            (MockId(2), mock_event(2, &[1])),
            (MockId(3), mock_event(3, &[1])),
            (MockId(4), mock_event(4, &[2])),
        ]
        .into_iter()
        .collect();

        // Ancestry of [4] should be {1, 2, 4}
        let ancestry = compute_ancestry(&events, &[MockId(4)]);
        assert!(ancestry.contains(&MockId(1)));
        assert!(ancestry.contains(&MockId(2)));
        assert!(ancestry.contains(&MockId(4)));
        assert!(!ancestry.contains(&MockId(3)));

        // Ancestry of [4, 3] should be {1, 2, 3, 4}
        let ancestry = compute_ancestry(&events, &[MockId(4), MockId(3)]);
        assert!(ancestry.contains(&MockId(1)));
        assert!(ancestry.contains(&MockId(2)));
        assert!(ancestry.contains(&MockId(3)));
        assert!(ancestry.contains(&MockId(4)));
    }
}
