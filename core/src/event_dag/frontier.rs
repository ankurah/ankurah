//! Frontier management for DAG traversal.

use std::collections::BTreeSet;

/// A frontier is a set of event IDs representing a boundary in the DAG.
///
/// During comparison, we maintain separate frontiers for subject and comparison,
/// walking them backward simultaneously until they converge or diverge.
#[derive(Debug, Clone)]
pub struct Frontier<Id> {
    /// Current set of event IDs at this frontier boundary.
    pub ids: BTreeSet<Id>,

    /// State tracking whether this frontier has encountered non-descending paths.
    pub state: FrontierState,
}

/// Tracks the exploration state of a frontier.
#[derive(Debug, Clone, PartialEq)]
pub enum FrontierState {
    /// Still exploring paths normally.
    Exploring,

    /// Found a non-descending assertion - frontier is tainted.
    /// Even if other paths succeed, best result is PartiallyDescends.
    Tainted { reason: TaintReason },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaintReason {
    NotDescends,
    Incomparable,
    PartiallyDescends,
}

impl<Id: Ord> Frontier<Id> {
    pub fn new(ids: impl IntoIterator<Item = Id>) -> Self { Self { ids: ids.into_iter().collect(), state: FrontierState::Exploring } }

    pub fn is_empty(&self) -> bool { self.ids.is_empty() }

    pub fn is_tainted(&self) -> bool { matches!(self.state, FrontierState::Tainted { .. }) }

    pub fn taint(&mut self, reason: TaintReason) { self.state = FrontierState::Tainted { reason }; }

    /// Remove an ID from the frontier (when processing an event).
    pub fn remove(&mut self, id: &Id) -> bool { self.ids.remove(id) }

    /// Add IDs to the frontier (parents of processed events).
    pub fn extend(&mut self, ids: impl IntoIterator<Item = Id>) { self.ids.extend(ids); }

    /// Add a single ID to the frontier.
    pub fn insert(&mut self, id: Id) -> bool { self.ids.insert(id) }
}
