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
/// parent edges within the given set. DETERMINISM CONTRACT (corrected at D2
/// M5; the red-team fold item 3 caught the older comment overclaiming
/// input-order independence): only the queue SEED iterates in BTree order;
/// discovery appends children in the order their parents appear in the
/// input, so ties between order-unconstrained nodes follow INPUT order. The
/// result is deterministic for a given input SEQUENCE; callers wanting one
/// canonical order for a given input SET must sort the input by id first
/// (`canonicalize_chain` below does exactly that).
///
/// Also used by the ingest planner, which orders staged event ids without
/// materializing the events (its nodes come from a BTreeMap iteration, so
/// its ties are id-ordered by construction).
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

/// Canonicalize an emitted comparison chain (D2 M5, dispositions Q2): the
/// unique topological order of the chain SET under `topo_sort_ids` with
/// sorted-by-id input, edges restricted to the set (drawn from the
/// comparison's accumulated dag). Two emissions of the same set, however
/// scheduled or routed (quick check vs BFS, any frontier order), produce
/// byte-identical chains; this is what lets the immunity oracle bind the
/// chain's semantic core byte-for-byte, and it costs one sort plus one Kahn
/// pass over an already-small vector.
///
/// The input MUST be sorted before the Kahn pass: tie order follows input
/// order (see the contract above), and the chain arrives in visit order,
/// which is schedule-carried.
///
/// Hostile-input note: chain members are drawn from a walk over served
/// parent lists, and a lying retriever can serve lists that form a cycle
/// within the set (impossible for honest content-addressed events). A cycle
/// would make the Kahn pass reject; canonicalization then falls back to the
/// plain id-sorted order (deterministic, never panics) and the walk-time
/// edge checks remain the sanctioned detector for such corruption.
pub(crate) fn canonicalize_chain(mut chain: Vec<EventId>, dag: &BTreeMap<EventId, Vec<EventId>>) -> Vec<EventId> {
    chain.sort();
    chain.dedup();
    let nodes: Vec<(EventId, Vec<EventId>)> = chain.iter().map(|id| (id.clone(), dag.get(id).cloned().unwrap_or_default())).collect();
    match topo_sort_ids(nodes) {
        Ok(sorted) => sorted,
        Err(_) => {
            tracing::warn!("comparison chain contains a served parent cycle; emitting id-sorted order");
            chain
        }
    }
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
        EventId::from_parts(&EntityId::from_bytes([n; 16]), &OperationSet(Default::default()), &Clock::from(Vec::new()), 1)
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

    /// The corrected determinism contract: ties follow INPUT order, so two
    /// permutations of one set may legally differ. This is the fact that
    /// makes sorted input a REQUIREMENT for canonical output (red-team fold
    /// item 3), pinned here so nobody restores the old
    /// input-order-independent doc claim without noticing.
    #[test]
    fn topo_ties_follow_input_order() {
        let (p, x, y) = (tid(1), tid(2), tid(3));
        let fwd = topo_sort_ids(vec![(p.clone(), vec![]), (x.clone(), vec![p.clone()]), (y.clone(), vec![p.clone()])]).unwrap();
        let rev = topo_sort_ids(vec![(p.clone(), vec![]), (y.clone(), vec![p.clone()]), (x.clone(), vec![p.clone()])]).unwrap();
        assert_eq!(fwd, vec![p.clone(), x.clone(), y.clone()]);
        assert_eq!(rev, vec![p, y, x], "tie order derives from input order; the sorter is NOT internally canonical");
    }

    /// Q2 permutation pin (red-team fold item 3): canonicalize_chain yields
    /// byte-identical output for every input permutation of one set, because
    /// it sorts by id before the Kahn pass.
    #[test]
    fn canonicalize_chain_is_permutation_stable() {
        let (p, x, y, m) = (tid(1), tid(2), tid(3), tid(4));
        let mut dag = BTreeMap::new();
        dag.insert(p.clone(), vec![]);
        dag.insert(x.clone(), vec![p.clone()]);
        dag.insert(y.clone(), vec![p.clone()]);
        dag.insert(m.clone(), vec![x.clone(), y.clone()]);

        let ids = [p.clone(), x.clone(), y.clone(), m.clone()];
        // Every permutation of the 4-element set, generated by repeated
        // rotations and swaps; exhaustive enumeration is cheap at this size.
        let mut perms: Vec<Vec<EventId>> = Vec::new();
        for i in 0..4 {
            for j in 0..4 {
                for k in 0..4 {
                    for l in 0..4 {
                        let idx = [i, j, k, l];
                        let mut seen = [false; 4];
                        if idx.iter().all(|&n| !std::mem::replace(&mut seen[n], true)) {
                            perms.push(idx.iter().map(|&n| ids[n].clone()).collect());
                        }
                    }
                }
            }
        }
        assert_eq!(perms.len(), 24);

        let canonical = canonicalize_chain(perms[0].clone(), &dag);
        // A valid topo order: p first, m last, {x, y} ascending between.
        let (lo, hi) = if x < y { (x, y) } else { (y, x) };
        assert_eq!(canonical, vec![p, lo, hi, m]);
        for perm in perms {
            assert_eq!(canonicalize_chain(perm.clone(), &dag), canonical, "shuffled input {perm:?} must reach the same bytes");
        }
    }

    /// The hostile-cycle fallback: a lying retriever can serve parent lists
    /// forming a cycle within the chain set (impossible for honest
    /// content-addressed events). Canonicalization must not panic or loop;
    /// it falls back to the deterministic id-sorted order.
    #[test]
    fn canonicalize_chain_survives_served_cycles() {
        let (a, b) = (tid(1), tid(2));
        let mut dag = BTreeMap::new();
        dag.insert(a.clone(), vec![b.clone()]);
        dag.insert(b.clone(), vec![a.clone()]);
        let mut expected = vec![a.clone(), b.clone()];
        expected.sort();
        assert_eq!(canonicalize_chain(vec![b, a], &dag), expected);
    }
}
