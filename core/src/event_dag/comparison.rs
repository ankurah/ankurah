//! Core comparison logic for determining causal relationships in event DAGs.
//!
//! ## Algorithm: Backward Breadth-First Search
//!
//! This module implements a **backward breadth-first search** from two clock heads
//! simultaneously, walking toward their common ancestors (if any) to classify their
//! causal relationship.
//!
//! ## Direction: Backward (Newer → Older)
//!
//! The comparison walks **backward** from newer heads toward older ancestors:
//! ```text
//! Timeline:   Root → A → B → C (head)
//!                    ↑   ↑   ↑
//! Search:        older ← ← ← newer (we walk this direction)
//! ```
//!
//! ## Process
//!
//! 1. **Initialize**: Start with two frontiers = {subject_head} and {other_head}
//! 2. **Expand**: Ask `CausalNavigator` to fetch events at current frontier positions
//! 3. **Process**: For each returned event:
//!    - Remove it from its frontier
//!    - Add its parents to the frontier (moving backward)
//!    - Track which frontier(s) have seen it (subject, other, or both)
//! 4. **Detect**: Check if we've determined the relationship (Descends, NotDescends, etc.)
//! 5. **Repeat** until: relationship determined, frontiers empty, or budget exhausted

use super::{
    frontier::{Frontier, FrontierState, TaintReason},
    navigator::{AssertionRelation, CausalNavigator, NavigationStep},
    AbstractCausalRelation, EventId, TClock, TEvent,
};
use crate::error::RetrievalError;
use std::collections::{BTreeSet, HashMap};

/// Compare two clocks to determine their causal relationship.
///
/// Performs a simultaneous backward breadth-first search from both heads,
/// looking for common ancestors or divergence.
pub async fn compare<N, C>(
    navigator: &N,
    subject: &C,
    comparison: &C,
    budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
where
    N: CausalNavigator,
    C: TClock<Id = N::EID>,
    N::EID: std::hash::Hash + Ord + Clone,
{
    // Early exit for obvious cases
    if subject.members().is_empty() || comparison.members().is_empty() {
        return Ok(AbstractCausalRelation::Incomparable);
    }

    if subject.members() == comparison.members() {
        return Ok(AbstractCausalRelation::Equal);
    }

    let mut comparison = Comparison::new(navigator, subject, comparison, budget);

    loop {
        if let Some(relation) = comparison.step().await? {
            return Ok(relation);
        }
    }
}

/// Compare an unstored event against a stored clock.
///
/// This is a special case where we start from the event's parent clock
/// rather than the event itself (which isn't stored yet).
pub async fn compare_unstored_event<N, E>(
    navigator: &N,
    event: &E,
    comparison: &E::Parent,
    budget: usize,
) -> Result<AbstractCausalRelation<N::EID>, RetrievalError>
where
    N: CausalNavigator,
    E: TEvent<Id = N::EID>,
    E::Parent: TClock<Id = N::EID>,
    N::EID: std::hash::Hash + Ord + Clone,
{
    // Special case: redundant delivery check
    if comparison.members().len() == 1 && comparison.members()[0] == event.id() {
        return Ok(AbstractCausalRelation::Equal);
    }

    // Compare event's parent against the comparison clock
    let result = compare(navigator, event.parent(), comparison, budget).await?;

    // If parent equals comparison, then event descends from comparison
    Ok(match result {
        AbstractCausalRelation::Equal => AbstractCausalRelation::StrictDescends,
        other => other,
    })
}

/// Internal state machine for the comparison algorithm.
struct Comparison<'a, N: CausalNavigator> {
    navigator: &'a N,

    // Frontiers being explored
    subject_frontier: Frontier<N::EID>,
    comparison_frontier: Frontier<N::EID>,

    // Original comparison heads (for tracking)
    original_comparison: BTreeSet<N::EID>,

    // Tracking state
    states: HashMap<N::EID, NodeState<N::EID>>,
    meet_candidates: BTreeSet<N::EID>,
    outstanding_heads: BTreeSet<N::EID>,

    // Progress tracking
    remaining_budget: usize,
    unseen_comparison_heads: usize,
    head_overlap: bool,
    any_common: bool,
}

#[derive(Clone)]
struct NodeState<Id: Clone> {
    seen_from_subject: bool,
    seen_from_comparison: bool,
    common_child_count: usize,
    origins: Vec<Id>, // Tracks which comparison heads reach this node
}

impl<Id: Clone> NodeState<Id> {
    fn new() -> Self { Self { seen_from_subject: false, seen_from_comparison: false, common_child_count: 0, origins: Vec::new() } }

    fn is_common(&self) -> bool { self.seen_from_subject && self.seen_from_comparison }

    fn mark_seen_from(&mut self, from_subject: bool, from_comparison: bool) {
        if from_subject {
            self.seen_from_subject = true;
        }
        if from_comparison {
            self.seen_from_comparison = true;
        }
    }
}

impl<'a, N> Comparison<'a, N>
where
    N: CausalNavigator,
    N::EID: std::hash::Hash + Ord + Clone,
{
    fn new<C: TClock<Id = N::EID>>(navigator: &'a N, subject: &C, comparison: &C, budget: usize) -> Self {
        let subject_ids: BTreeSet<_> = subject.members().iter().cloned().collect();
        let comparison_ids: BTreeSet<_> = comparison.members().iter().cloned().collect();

        Self {
            navigator,
            subject_frontier: Frontier::new(subject_ids),
            comparison_frontier: Frontier::new(comparison_ids.clone()),
            original_comparison: comparison_ids.clone(),
            states: HashMap::new(),
            meet_candidates: BTreeSet::new(),
            outstanding_heads: comparison_ids.clone(),
            remaining_budget: budget,
            unseen_comparison_heads: comparison_ids.len(),
            head_overlap: false,
            any_common: false,
        }
    }

    async fn step(&mut self) -> Result<Option<AbstractCausalRelation<N::EID>>, RetrievalError> {
        // Collect frontier IDs from both frontiers (preserving original budget behavior)
        let mut all_frontier_ids = Vec::new();
        all_frontier_ids.extend(self.subject_frontier.ids.iter().cloned());
        all_frontier_ids.extend(self.comparison_frontier.ids.iter().cloned());

        // Ask navigator to expand - this should match the original budget consumption
        let NavigationStep { events, assertions, consumed_budget } =
            self.navigator.expand_frontier(&all_frontier_ids, self.remaining_budget).await?;

        // Deduct budget for fetched events
        self.remaining_budget = self.remaining_budget.saturating_sub(consumed_budget);

        // Process fetched events
        for event in events {
            self.process_event(event.id(), event.parent().members());
        }

        // Process assertion results
        for assertion in assertions {
            self.process_assertion(assertion.from, assertion.to, assertion.relation);
        }

        // Check if we have a result
        Ok(self.check_result())
    }

    fn process_event(&mut self, id: N::EID, parents: &[N::EID]) {
        let from_subject = self.subject_frontier.remove(&id);
        let from_comparison = self.comparison_frontier.remove(&id);

        // Update node state
        let (is_common, origins) = {
            let state = self.states.entry(id.clone()).or_insert_with(NodeState::new);
            state.mark_seen_from(from_subject, from_comparison);

            // Track origins for comparison heads
            if from_comparison && self.original_comparison.contains(&id) {
                state.origins.push(id.clone());
            }

            (state.is_common(), state.origins.clone())
        };

        // Handle common nodes
        if is_common && self.meet_candidates.insert(id.clone()) {
            self.any_common = true;

            // Remove satisfied heads from checklist
            for origin in &origins {
                self.outstanding_heads.remove(origin);
            }

            // Update parent states
            for parent in parents {
                let parent_state = self.states.entry(parent.clone()).or_insert_with(NodeState::new);
                if from_comparison {
                    parent_state.origins.extend(origins.clone());
                }
                parent_state.common_child_count += 1;
            }
        } else if from_comparison {
            // Propagate origins
            for parent in parents {
                self.states.entry(parent.clone()).or_insert_with(NodeState::new).origins.extend(origins.clone());
            }
        }

        // Extend frontiers with parents
        if from_subject {
            self.subject_frontier.extend(parents.iter().cloned());

            if self.original_comparison.contains(&id) {
                self.unseen_comparison_heads -= 1;
                self.head_overlap = true;
            }
        }

        if from_comparison {
            self.comparison_frontier.extend(parents.iter().cloned());
        }
    }

    fn process_assertion(&mut self, from: N::EID, to: Option<N::EID>, relation: AssertionRelation<N::EID>) {
        // Determine which frontier(s) contain this ID
        let in_subject = self.subject_frontier.ids.contains(&from);
        let in_comparison = self.comparison_frontier.ids.contains(&from);

        match relation {
            AssertionRelation::Descends => {
                // Add shortcut target to appropriate frontier(s)
                if let Some(target) = to {
                    if in_subject {
                        self.subject_frontier.insert(target.clone());
                    }
                    if in_comparison {
                        self.comparison_frontier.insert(target);
                    }
                }
            }
            AssertionRelation::NotDescends { .. } => {
                // Taint the frontier(s) containing this ID
                if in_subject {
                    self.subject_frontier.taint(TaintReason::NotDescends);
                }
                if in_comparison {
                    self.comparison_frontier.taint(TaintReason::NotDescends);
                }
            }
            AssertionRelation::PartiallyDescends { meet } => {
                // Taint but continue exploration
                if in_subject {
                    self.subject_frontier.taint(TaintReason::PartiallyDescends);
                }
                if in_comparison {
                    self.comparison_frontier.taint(TaintReason::PartiallyDescends);
                }
                // Add meet candidates from assertion
                for meet_id in meet {
                    self.meet_candidates.insert(meet_id);
                }
                // Still add target if provided
                if let Some(target) = to {
                    if in_subject {
                        self.subject_frontier.insert(target.clone());
                    }
                    if in_comparison {
                        self.comparison_frontier.insert(target);
                    }
                }
            }
            AssertionRelation::Incomparable => {
                // Taint and stop exploration
                if in_subject {
                    self.subject_frontier.taint(TaintReason::Incomparable);
                }
                if in_comparison {
                    self.comparison_frontier.taint(TaintReason::Incomparable);
                }
            }
        }
    }

    fn check_result(&mut self) -> Option<AbstractCausalRelation<N::EID>> {
        // Check for assertion-based tainting first (takes precedence)
        if self.subject_frontier.is_tainted() {
            match &self.subject_frontier.state {
                FrontierState::Tainted { reason: TaintReason::PartiallyDescends } => {
                    // Return PartiallyDescends with meet candidates from assertions
                    let meet: Vec<_> = self.meet_candidates.iter().cloned().collect();
                    return Some(AbstractCausalRelation::PartiallyDescends { meet });
                }
                FrontierState::Tainted { reason: TaintReason::NotDescends } => {
                    return Some(AbstractCausalRelation::NotDescends { meet: vec![] });
                }
                FrontierState::Tainted { reason: TaintReason::Incomparable } => {
                    return Some(AbstractCausalRelation::Incomparable);
                }
                _ => {}
            }
        }

        // Check for complete descent (only if not tainted)
        if self.unseen_comparison_heads == 0 {
            return Some(AbstractCausalRelation::StrictDescends);
        }

        // Check if frontiers are exhausted
        if self.subject_frontier.is_empty() && self.comparison_frontier.is_empty() {
            // Compute minimal common ancestors
            let meet: Vec<_> =
                self.meet_candidates.iter().filter(|id| self.states.get(*id).map_or(0, |s| s.common_child_count) == 0).cloned().collect();

            // Determine final relation based on frontier states and findings
            if !self.any_common {
                return Some(AbstractCausalRelation::Incomparable);
            }

            if !self.outstanding_heads.is_empty() {
                return Some(AbstractCausalRelation::Incomparable);
            }

            // Check for tainting
            let subject_tainted = self.subject_frontier.is_tainted();
            let comparison_tainted = self.comparison_frontier.is_tainted();

            if subject_tainted || comparison_tainted || self.head_overlap {
                Some(AbstractCausalRelation::PartiallyDescends { meet })
            } else {
                Some(AbstractCausalRelation::NotDescends { meet })
            }
        } else if self.remaining_budget == 0 {
            // Budget exhausted
            Some(AbstractCausalRelation::BudgetExceeded {
                subject_frontier: self.subject_frontier.ids.clone(),
                other_frontier: self.comparison_frontier.ids.clone(),
            })
        } else {
            // Continue exploration
            None
        }
    }
}
