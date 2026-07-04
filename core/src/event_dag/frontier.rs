//! Frontier management for DAG traversal.

use std::collections::BTreeSet;

/// A frontier is a set of event IDs representing a boundary in the DAG.
///
/// During comparison, we maintain separate frontiers for subject and comparison,
/// walking them backward simultaneously until they converge or diverge.
#[derive(Debug, Clone)]
pub(crate) struct Frontier<Id> {
    /// Current set of event IDs at this frontier boundary.
    pub(crate) ids: BTreeSet<Id>,
}

impl<Id: Ord> Frontier<Id> {
    pub(crate) fn new(ids: impl IntoIterator<Item = Id>) -> Self { Self { ids: ids.into_iter().collect() } }

    pub(crate) fn is_empty(&self) -> bool { self.ids.is_empty() }

    /// Remove an ID from the frontier (when processing an event).
    pub(crate) fn remove(&mut self, id: &Id) -> bool { self.ids.remove(id) }

    /// Add IDs to the frontier (parents of processed events).
    pub(crate) fn extend(&mut self, ids: impl IntoIterator<Item = Id>) { self.ids.extend(ids); }
}
