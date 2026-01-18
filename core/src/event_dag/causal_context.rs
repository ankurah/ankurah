//! Causal context for event relationship queries.
//!
//! This module provides the [`CausalContext`] trait which allows backends
//! to query causal relationships between events using accumulated DAG information.

use ankurah_proto::EventId;
use std::collections::{BTreeMap, BTreeSet};

/// Provides causal relationship information from accumulated DAG.
///
/// This trait is implemented by types that accumulate DAG structure during
/// event comparison, allowing backends to determine causal relationships
/// between events without additional retrieval.
pub trait CausalContext: Send + Sync {
    /// Check if `descendant` is a causal descendant of `ancestor`.
    ///
    /// Returns:
    /// - `Some(true)`: descendant strictly descends from ancestor (descendant > ancestor)
    /// - `Some(false)`: descendant does NOT strictly descend from ancestor
    ///   (they may be equal, concurrent, or ancestor > descendant)
    /// - `None`: cannot determine with available DAG info
    fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool>;

    /// Check if an event is known in the accumulated DAG.
    fn contains(&self, event_id: &EventId) -> bool;
}

/// A simple causal context backed by a DAG map.
///
/// This is used when we have accumulated DAG structure (event_id -> parent_ids)
/// and need to answer causal queries.
pub struct DagCausalContext<'a> {
    /// DAG structure: event_id -> parent event_ids
    dag: &'a BTreeMap<EventId, Vec<EventId>>,
}

impl<'a> DagCausalContext<'a> {
    pub fn new(dag: &'a BTreeMap<EventId, Vec<EventId>>) -> Self {
        Self { dag }
    }
}

impl<'a> CausalContext for DagCausalContext<'a> {
    fn is_descendant(&self, descendant: &EventId, ancestor: &EventId) -> Option<bool> {
        if descendant == ancestor {
            return Some(false); // Equal, not strictly descends
        }

        // BFS backward from descendant looking for ancestor
        let mut frontier = vec![descendant.clone()];
        let mut visited = BTreeSet::new();

        while let Some(id) = frontier.pop() {
            if visited.contains(&id) {
                continue;
            }
            visited.insert(id.clone());

            if &id == ancestor {
                return Some(true); // Found ancestor in descendant's past
            }

            // Get parents from DAG
            match self.dag.get(&id) {
                Some(parents) => {
                    for parent in parents {
                        if !visited.contains(parent) {
                            frontier.push(parent.clone());
                        }
                    }
                }
                None => {
                    // Unknown event in the path - can't determine relationship
                    // unless we've already visited this event
                    if &id != descendant {
                        return None;
                    }
                    // The descendant itself is not in the DAG - can't determine
                    return None;
                }
            }
        }

        // Exhausted search without finding ancestor
        Some(false)
    }

    fn contains(&self, event_id: &EventId) -> bool {
        self.dag.contains_key(event_id)
    }
}

/// A no-op causal context that always returns None (unknown).
///
/// Used for backends that don't need causal context (e.g., CRDTs).
pub struct NoCausalContext;

impl CausalContext for NoCausalContext {
    fn is_descendant(&self, _descendant: &EventId, _ancestor: &EventId) -> Option<bool> {
        None
    }

    fn contains(&self, _event_id: &EventId) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event_id(n: u8) -> EventId {
        let mut bytes = [0u8; 32];
        bytes[0] = n;
        EventId::from_bytes(bytes)
    }

    #[test]
    fn test_is_descendant_linear_chain() {
        // A -> B -> C
        let a = make_event_id(1);
        let b = make_event_id(2);
        let c = make_event_id(3);

        let mut dag = BTreeMap::new();
        dag.insert(a.clone(), vec![]); // A is genesis
        dag.insert(b.clone(), vec![a.clone()]); // B's parent is A
        dag.insert(c.clone(), vec![b.clone()]); // C's parent is B

        let ctx = DagCausalContext::new(&dag);

        // C descends from B
        assert_eq!(ctx.is_descendant(&c, &b), Some(true));
        // C descends from A
        assert_eq!(ctx.is_descendant(&c, &a), Some(true));
        // B descends from A
        assert_eq!(ctx.is_descendant(&b, &a), Some(true));

        // A does not descend from B
        assert_eq!(ctx.is_descendant(&a, &b), Some(false));
        // A does not descend from C
        assert_eq!(ctx.is_descendant(&a, &c), Some(false));
        // B does not descend from C
        assert_eq!(ctx.is_descendant(&b, &c), Some(false));

        // Equal is not descends
        assert_eq!(ctx.is_descendant(&a, &a), Some(false));
    }

    #[test]
    fn test_is_descendant_concurrent() {
        //     A
        //    / \
        //   B   C
        let a = make_event_id(1);
        let b = make_event_id(2);
        let c = make_event_id(3);

        let mut dag = BTreeMap::new();
        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![a.clone()]);

        let ctx = DagCausalContext::new(&dag);

        // B does not descend from C (concurrent)
        assert_eq!(ctx.is_descendant(&b, &c), Some(false));
        // C does not descend from B (concurrent)
        assert_eq!(ctx.is_descendant(&c, &b), Some(false));
    }

    #[test]
    fn test_is_descendant_diamond() {
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let a = make_event_id(1);
        let b = make_event_id(2);
        let c = make_event_id(3);
        let d = make_event_id(4);

        let mut dag = BTreeMap::new();
        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);
        dag.insert(c.clone(), vec![a.clone()]);
        dag.insert(d.clone(), vec![b.clone(), c.clone()]); // D merges B and C

        let ctx = DagCausalContext::new(&dag);

        // D descends from everything
        assert_eq!(ctx.is_descendant(&d, &a), Some(true));
        assert_eq!(ctx.is_descendant(&d, &b), Some(true));
        assert_eq!(ctx.is_descendant(&d, &c), Some(true));

        // B and C are concurrent
        assert_eq!(ctx.is_descendant(&b, &c), Some(false));
        assert_eq!(ctx.is_descendant(&c, &b), Some(false));
    }

    #[test]
    fn test_is_descendant_unknown_event() {
        let a = make_event_id(1);
        let b = make_event_id(2);
        let unknown = make_event_id(99);

        let mut dag = BTreeMap::new();
        dag.insert(a.clone(), vec![]);
        dag.insert(b.clone(), vec![a.clone()]);

        let ctx = DagCausalContext::new(&dag);

        // Unknown event as descendant
        assert_eq!(ctx.is_descendant(&unknown, &a), None);
        // Unknown event as ancestor - we can determine B doesn't descend from unknown
        // by exhausting B's ancestry without finding unknown
        assert_eq!(ctx.is_descendant(&b, &unknown), Some(false));
    }

    #[test]
    fn test_is_descendant_missing_parent() {
        // B's parent is A, but A is not in the DAG
        let a = make_event_id(1);
        let b = make_event_id(2);
        let c = make_event_id(3);

        let mut dag = BTreeMap::new();
        dag.insert(b.clone(), vec![a.clone()]); // B has parent A, but A not in DAG
        dag.insert(c.clone(), vec![b.clone()]);

        let ctx = DagCausalContext::new(&dag);

        // C descends from B (we can verify this)
        assert_eq!(ctx.is_descendant(&c, &b), Some(true));
        // C descends from A - we can verify this by following C → B → A
        // We find A as the target ancestor, so we know C descends from A
        assert_eq!(ctx.is_descendant(&c, &a), Some(true));

        // To test the "unknown in path" case, we need an event whose parent
        // is missing while we're still searching for a deeper ancestor
        let mut dag2 = BTreeMap::new();
        dag2.insert(c.clone(), vec![b.clone()]); // C's parent is B
        // B is NOT in dag2, so when traversing C looking for A, we hit unknown B
        let ctx2 = DagCausalContext::new(&dag2);

        // C descends from A? We can't determine because B (intermediate) is missing
        assert_eq!(ctx2.is_descendant(&c, &a), None);
    }
}
