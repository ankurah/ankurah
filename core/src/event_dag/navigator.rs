//! Trait for navigating causal event DAGs with assertion-based shortcuts.

use super::{EventId, TEvent};
use crate::error::RetrievalError;
use async_trait::async_trait;

/// Provides navigation through event DAG lineage, walking backwards from newer to older events.
///
/// Implementations may optimize traversal using:
/// - Pre-validated causal assertions (zero-cost shortcuts or path terminators)
/// - Batch event fetching from local or remote storage
/// - Caching of frequently accessed events
#[async_trait]
pub trait CausalNavigator {
    type EID: EventId;
    type Event: TEvent<Id = Self::EID>;

    /// Expands the frontier backward by one step.
    ///
    /// Given a set of event IDs (the frontier), this method:
    /// 1. Fetches events at those IDs (costs budget)
    /// 2. Checks for matching causal assertions (zero cost)
    /// 3. Returns both fetched events and assertion results
    ///
    /// The caller processes results to update frontiers and detect convergence.
    async fn expand_frontier(
        &self,
        frontier_ids: &[Self::EID],
        budget: usize,
    ) -> Result<NavigationStep<Self::EID, Self::Event>, RetrievalError>;
}

/// Temporary result container for a single frontier expansion.
///
/// This is just a structured return value (a "DTO"), not a meaningful domain object.
/// It exists only to avoid returning a complex tuple from `expand_frontier`.
pub struct NavigationStep<Id, Ev> {
    /// Events fetched from storage (costs budget).
    pub events: Vec<Ev>,

    /// Assertion-based results that may shortcut or terminate paths.
    pub assertions: Vec<AssertionResult<Id>>,

    /// Budget consumed by event fetches.
    pub consumed_budget: usize,
}

/// Result of applying a causal assertion to the frontier.
pub struct AssertionResult<Id> {
    /// The frontier ID that matched this assertion's subject.
    pub from: Id,

    /// The target ID from the assertion (if relation allows continuation).
    pub to: Option<Id>,

    /// The causal relation asserted between from and to.
    pub relation: AssertionRelation<Id>,
}

/// Simplified assertion relations for navigation purposes.
#[derive(Debug, Clone, PartialEq)]
pub enum AssertionRelation<Id> {
    /// Subject descends from target - continue exploration via shortcut
    Descends,
    /// Subject does not descend - terminate this path  
    NotDescends { meet: Vec<Id> },
    /// Partial descent - taint frontier but continue
    PartiallyDescends { meet: Vec<Id> },
    /// No relation - terminate this path
    Incomparable,
}

impl<Id, Ev> NavigationStep<Id, Ev> {
    pub fn empty() -> Self { Self { events: Vec::new(), assertions: Vec::new(), consumed_budget: 0 } }
}
