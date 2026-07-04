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
pub(crate) fn topo_sort_events(events: Vec<Attested<Event>>) -> Result<Vec<Attested<Event>>, MutationError> {
    let mut by_id: BTreeMap<EventId, Attested<Event>> = BTreeMap::new();
    for event in events {
        by_id.entry(event.payload.id()).or_insert(event);
    }

    let mut indegree: BTreeMap<EventId, usize> = BTreeMap::new();
    let mut children: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();
    for (id, event) in &by_id {
        indegree.entry(id.clone()).or_insert(0);
        // Deduplicate parent ids: a malformed parent clock must not skew the
        // in-degree accounting.
        let parents: BTreeSet<&EventId> = event.payload.parent.as_slice().iter().filter(|p| by_id.contains_key(*p)).collect();
        for parent in parents {
            *indegree.entry(id.clone()).or_insert(0) += 1;
            children.entry(parent.clone()).or_default().push(id.clone());
        }
    }

    let mut queue: VecDeque<EventId> = indegree.iter().filter(|(_, d)| **d == 0).map(|(id, _)| id.clone()).collect();
    let mut sorted: Vec<Attested<Event>> = Vec::with_capacity(by_id.len());

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
        sorted.push(by_id.remove(&id).expect("queued id is present"));
    }

    if !by_id.is_empty() {
        return Err(MutationError::InvalidUpdate("event batch contains a parent cycle"));
    }

    Ok(sorted)
}
