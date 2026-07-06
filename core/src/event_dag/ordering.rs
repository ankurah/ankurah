//! Topological ordering of event batches for application.
//!
//! EventBridge batches arrive as a set discovered by backward BFS; neither the
//! discovery order nor the sender is trusted to provide a causal order.
//! Applying a child before its staged parent makes the child compare
//! StrictDescends over the current head (its parents resolve via staging), so
//! the head jumps past the parent and the parent's operations are then
//! silently dropped as StrictAscends even though the event is committed to
//! storage (V4).

use crate::error::MutationError;
use ankurah_proto::{Attested, Event, EventId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Sort a batch of events parents-first (Kahn's algorithm), considering only
/// parent edges within the batch: parents outside it are below the bridge
/// floor and assumed already applied or resolvable by the receiver.
///
/// Duplicate events (by id) are collapsed to their first occurrence. Ids are
/// content-addressed, so cycles are impossible in honest input; a cycle means
/// a malformed or malicious batch and returns an error.
pub fn topo_sort_events(events: Vec<Attested<Event>>) -> Result<Vec<Attested<Event>>, MutationError> {
    let mut by_id: BTreeMap<EventId, Attested<Event>> = BTreeMap::new();
    for event in events {
        by_id.entry(event.payload.id()).or_insert(event);
    }

    let nodes: Vec<(EventId, Vec<EventId>)> =
        by_id.iter().map(|(id, event)| (id.clone(), event.payload.parent.as_slice().to_vec())).collect();
    let order = topo_sort_ids(nodes)?;

    Ok(order.into_iter().map(|id| by_id.remove(&id).expect("sorted id came from by_id")).collect())
}

/// The Kahn core over explicit (id, parent ids) pairs, considering only
/// parent edges within the given set. Queue seeding iterates ids in BTree
/// order and discovery appends in that order, so the result is deterministic
/// for a given input set regardless of input ordering.
///
/// Also used by the ingest planner, which orders staged event ids without
/// materializing the events.
pub(crate) fn topo_sort_ids(nodes: Vec<(EventId, Vec<EventId>)>) -> Result<Vec<EventId>, MutationError> {
    let ids: BTreeSet<EventId> = nodes.iter().map(|(id, _)| id.clone()).collect();

    let mut indegree: BTreeMap<EventId, usize> = BTreeMap::new();
    let mut children: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
    let mut seen: BTreeSet<EventId> = BTreeSet::new();
    for (id, parents) in nodes {
        // Duplicate nodes collapse to their first occurrence, matching the
        // by-id dedup the event wrapper performs.
        if !seen.insert(id.clone()) {
            continue;
        }
        indegree.entry(id.clone()).or_insert(0);
        // Deduplicate parent ids: a malformed parent clock must not skew the
        // in-degree accounting. A self-parent stays in and reads as a cycle,
        // matching the pre-refactor behavior for malformed input.
        let parents: BTreeSet<EventId> = parents.into_iter().filter(|p| ids.contains(p)).collect();
        for parent in parents {
            *indegree.entry(id.clone()).or_insert(0) += 1;
            children.entry(parent).or_default().push(id.clone());
        }
    }

    let mut queue: VecDeque<EventId> = indegree.iter().filter(|(_, d)| **d == 0).map(|(id, _)| id.clone()).collect();
    let mut sorted: Vec<EventId> = Vec::with_capacity(indegree.len());

    while let Some(id) = queue.pop_front() {
        if let Some(kids) = children.get(&id) {
            for kid in kids {
                let degree = indegree.get_mut(kid).expect("child in-degree was initialized");
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(kid.clone());
                }
            }
        }
        sorted.push(id);
    }

    if sorted.len() != indegree.len() {
        return Err(MutationError::InvalidUpdate("event batch contains a parent cycle"));
    }

    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::{Clock, EntityId, OperationSet};

    /// Distinct, deterministic EventIds. Content-addressing means raw ids
    /// cannot be fabricated; these are real hashes of synthetic content. The
    /// parent edges passed to topo_sort_ids are explicit pairs, so shapes
    /// impossible for honest content (cycles) remain constructible here.
    fn tid(n: u8) -> EventId {
        EventId::from_parts(&EntityId::from_bytes([n; 16]), &OperationSet(Default::default()), &Clock::from(Vec::new()))
    }

    #[test]
    fn ids_sort_parents_first() {
        // Linear chain forces a unique topological order regardless of how
        // the hashed ids happen to sort: c <- b <- a, presented child-first.
        let (a, b, c) = (tid(1), tid(2), tid(3));
        let nodes = vec![(c.clone(), vec![b.clone()]), (b.clone(), vec![a.clone()]), (a.clone(), vec![])];
        assert_eq!(topo_sort_ids(nodes).unwrap(), vec![a, b, c]);
    }

    #[test]
    fn out_of_set_parents_are_ignored() {
        let (a, b) = (tid(1), tid(2));
        let mut got = topo_sort_ids(vec![(b.clone(), vec![tid(9)]), (a.clone(), vec![tid(8)])]).unwrap();
        got.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(got, expected);
    }

    #[test]
    fn a_cycle_is_rejected_not_reordered() {
        let (a, b) = (tid(1), tid(2));
        let nodes = vec![(a.clone(), vec![b.clone()]), (b, vec![a])];
        assert!(matches!(topo_sort_ids(nodes), Err(MutationError::InvalidUpdate(_))));
    }

    #[test]
    fn duplicate_ids_collapse() {
        let (a, b) = (tid(1), tid(2));
        let nodes = vec![(a.clone(), vec![]), (a.clone(), vec![]), (b.clone(), vec![a.clone()])];
        assert_eq!(topo_sort_ids(nodes).unwrap(), vec![a, b]);
    }
}
