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
//! 2. **Expand**: Fetch events at current frontier positions via `EventAccumulator`
//! 3. **Process**: For each returned event:
//!    - Remove it from its frontier
//!    - Add its parents to the frontier (moving backward)
//!    - Track which frontier(s) have seen it (subject, other, or both)
//! 4. **Detect**: Check if we've determined the relationship (Descends, NotDescends, etc.)
//! 5. **Repeat** until: relationship determined, frontiers empty, or budget exhausted

use super::{
    accumulator::{ComparisonResult, EventAccumulator},
    frontier::Frontier,
    relation::AbstractCausalRelation,
};
use crate::error::RetrievalError;
use crate::retrieval::GetEvents;
use ankurah_proto::{Clock, EventId};
use std::collections::{BTreeSet, HashMap};

/// Compare two clocks to determine their causal relationship.
///
/// Performs a simultaneous backward breadth-first search from both heads,
/// looking for common ancestors or divergence.
///
/// Takes ownership of the retriever, wrapping it in an `EventAccumulator`.
/// Returns a `ComparisonResult` containing both the relation and the accumulated DAG.
///
/// Budget escalation: if the initial budget is exhausted, retries internally
/// with up to 4x the initial budget before returning `BudgetExceeded`.
pub async fn compare<E: GetEvents>(
    event_getter: E,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<E>, RetrievalError> {
    // Early exit for obvious cases - empty clocks are disjoint
    if subject.as_slice().is_empty() || comparison.as_slice().is_empty() {
        let accumulator = EventAccumulator::new(event_getter);
        return Ok(ComparisonResult::new(
            AbstractCausalRelation::DivergedSince {
                meet: vec![],
                subject: vec![],
                other: vec![],
                subject_chain: vec![],
                other_chain: vec![],
            },
            accumulator,
        ));
    }

    if subject.as_slice() == comparison.as_slice() {
        let accumulator = EventAccumulator::new(event_getter);
        return Ok(ComparisonResult::new(AbstractCausalRelation::Equal, accumulator));
    }

    let mut accumulator = EventAccumulator::new(event_getter);

    // Quick check: if every comparison head appears as a parent of some subject
    // event, then subject strictly descends from comparison (one step away).
    // This avoids BFS entirely for the common case of a new event whose parents
    // ARE the current head -- and crucially avoids fetching the comparison events
    // themselves, which may not be in local storage (ephemeral node scenario).
    {
        let comparison_set: BTreeSet<EventId> = comparison.as_slice().iter().cloned().collect();
        let mut all_parents: BTreeSet<EventId> = BTreeSet::new();
        let mut all_fetched = true;

        for id in subject.as_slice() {
            match accumulator.get_event(id).await {
                Ok(event) => {
                    accumulator.accumulate(&event);
                    all_parents.extend(event.parent.as_slice().iter().cloned());
                }
                Err(_) => {
                    all_fetched = false;
                    break;
                }
            }
        }

        if all_fetched && comparison_set.is_subset(&all_parents) {
            // Subject's parents cover all comparison heads -> StrictDescends
            let chain: Vec<EventId> = subject.as_slice().to_vec();
            return Ok(ComparisonResult::new(
                AbstractCausalRelation::StrictDescends { chain },
                accumulator,
            ));
        }
    }

    let initial_budget = budget;
    let max_budget = initial_budget * 4;
    let mut current_budget = initial_budget;

    loop {
        let mut comp = Comparison::new(&mut accumulator, subject, comparison, current_budget);
        let relation = loop {
            if let Some(relation) = comp.step().await? {
                break relation;
            }
        };

        match &relation {
            AbstractCausalRelation::BudgetExceeded { .. } if current_budget < max_budget => {
                current_budget = (current_budget * 4).min(max_budget);
                // accumulator retains its DAG and cache - warm start for retry
                continue;
            }
            _ => {
                return Ok(ComparisonResult::new(relation, accumulator));
            }
        }
    }
}

/// Internal state machine for the comparison algorithm.
struct Comparison<'a, E: GetEvents> {
    accumulator: &'a mut EventAccumulator<E>,

    // Frontiers being explored
    subject_frontier: Frontier<EventId>,
    comparison_frontier: Frontier<EventId>,

    // Original heads (for tracking strict descent/ascent)
    original_subject: BTreeSet<EventId>,
    original_comparison: BTreeSet<EventId>,

    // Tracking state
    states: HashMap<EventId, NodeState>,
    meet_candidates: BTreeSet<EventId>,
    outstanding_heads: BTreeSet<EventId>,

    // Progress tracking
    remaining_budget: usize,
    /// Count of comparison heads not yet seen by subject's traversal (for StrictDescends)
    unseen_comparison_heads: usize,
    /// Count of subject heads not yet seen by comparison's traversal (for StrictAscends)
    unseen_subject_heads: usize,
    any_common: bool,

    // Chain tracking (for forward chain accumulation)
    /// Events visited from subject's traversal (child→parent order during traversal)
    subject_visited: Vec<EventId>,
    /// Events visited from comparison's traversal (child→parent order during traversal)
    other_visited: Vec<EventId>,

    // Root tracking (for Disjoint detection)
    /// Genesis event found in subject's traversal (event with empty parents)
    subject_root: Option<EventId>,
    /// Genesis event found in comparison's traversal (event with empty parents)
    other_root: Option<EventId>,
}

#[derive(Clone)]
struct NodeState {
    seen_from_subject: bool,
    seen_from_comparison: bool,
    common_child_count: usize,
    origins: Vec<EventId>,          // Tracks which comparison heads reach this node
    subject_children: Vec<EventId>, // Children from subject's traversal
    other_children: Vec<EventId>,   // Children from comparison's traversal
}

impl NodeState {
    fn new() -> Self {
        Self {
            seen_from_subject: false,
            seen_from_comparison: false,
            common_child_count: 0,
            origins: Vec::new(),
            subject_children: Vec::new(),
            other_children: Vec::new(),
        }
    }

    fn is_common(&self) -> bool { self.seen_from_subject && self.seen_from_comparison }

    fn mark_seen_from(&mut self, from_subject: bool, from_comparison: bool) {
        if from_subject {
            self.seen_from_subject = true;
        }
        if from_comparison {
            self.seen_from_comparison = true;
        }
    }

    fn add_child(&mut self, child: EventId, from_subject: bool, from_comparison: bool) {
        if from_subject {
            self.subject_children.push(child.clone());
        }
        if from_comparison {
            self.other_children.push(child);
        }
    }
}

impl<'a, E: GetEvents> Comparison<'a, E> {
    fn new(accumulator: &'a mut EventAccumulator<E>, subject: &Clock, comparison: &Clock, budget: usize) -> Self {
        let subject_ids: BTreeSet<EventId> = subject.as_slice().iter().cloned().collect();
        let comparison_ids: BTreeSet<EventId> = comparison.as_slice().iter().cloned().collect();

        Self {
            accumulator,
            subject_frontier: Frontier::new(subject_ids.clone()),
            comparison_frontier: Frontier::new(comparison_ids.clone()),
            original_subject: subject_ids.clone(),
            original_comparison: comparison_ids.clone(),
            states: HashMap::new(),
            meet_candidates: BTreeSet::new(),
            outstanding_heads: comparison_ids.clone(),
            remaining_budget: budget,
            unseen_comparison_heads: comparison_ids.len(),
            unseen_subject_heads: subject_ids.len(),
            any_common: false,
            subject_visited: Vec::new(),
            other_visited: Vec::new(),
            subject_root: None,
            other_root: None,
        }
    }

    async fn step(&mut self) -> Result<Option<AbstractCausalRelation<EventId>>, RetrievalError> {
        // Collect frontier IDs from both frontiers
        let mut all_frontier_ids = Vec::new();
        all_frontier_ids.extend(self.subject_frontier.ids.iter().cloned());
        all_frontier_ids.extend(self.comparison_frontier.ids.iter().cloned());

        // Fetch events individually via accumulator (replaces batch expand_frontier)
        for id in &all_frontier_ids {
            // Skip events already processed (e.g. an ID that appeared in both
            // frontiers gets collected twice but was already handled on its
            // first encounter).
            if !self.subject_frontier.ids.contains(id) && !self.comparison_frontier.ids.contains(id) {
                continue;
            }

            let event = match self.accumulator.get_event(id).await {
                Ok(event) => event,
                Err(RetrievalError::EventNotFound(_)) => {
                    // Event is unfetchable. If it's currently on both frontiers,
                    // it's a common ancestor — process with empty parents to
                    // terminate the traversal at this point. This handles the
                    // ephemeral node scenario where the meet point event is
                    // unreachable from local storage.
                    // Note: we check at processing time, not collection time,
                    // because process_event for earlier events in this loop may
                    // have added IDs to frontiers.
                    if self.subject_frontier.ids.contains(id) && self.comparison_frontier.ids.contains(id) {
                        self.process_event(id.clone(), &[]);
                        continue;
                    }
                    return Err(RetrievalError::EventNotFound(id.clone()));
                }
                Err(e) => return Err(e),
            };
            self.accumulator.accumulate(&event);
            self.remaining_budget = self.remaining_budget.saturating_sub(1);
            self.process_event(event.id(), event.parent.as_slice());
        }

        // Check if we have a result
        Ok(self.check_result())
    }

    fn process_event(&mut self, id: EventId, parents: &[EventId]) {
        let from_subject = self.subject_frontier.remove(&id);
        let from_comparison = self.comparison_frontier.remove(&id);

        // Track visited events for chain building (in child→parent order during traversal)
        if from_subject {
            self.subject_visited.push(id.clone());
        }
        if from_comparison {
            self.other_visited.push(id.clone());
        }

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

        // Register this event as a child of each of its parents (for immediate children tracking)
        for parent in parents {
            let parent_state = self.states.entry(parent.clone()).or_insert_with(NodeState::new);
            parent_state.add_child(id.clone(), from_subject, from_comparison);
        }

        // Detect genesis events (empty parents) for Disjoint detection
        if parents.is_empty() {
            if from_subject && self.subject_root.is_none() {
                self.subject_root = Some(id.clone());
            }
            if from_comparison && self.other_root.is_none() {
                self.other_root = Some(id.clone());
            }
        }

        // Extend frontiers with parents
        if from_subject {
            self.subject_frontier.extend(parents.iter().cloned());

            // Check if subject's traversal reached a comparison head
            if self.original_comparison.contains(&id) {
                self.unseen_comparison_heads = self.unseen_comparison_heads.saturating_sub(1);
            }
        }

        if from_comparison {
            self.comparison_frontier.extend(parents.iter().cloned());

            // Check if comparison's traversal reached a subject head
            if self.original_subject.contains(&id) {
                self.unseen_subject_heads = self.unseen_subject_heads.saturating_sub(1);
            }
        }
    }

    /// Build forward chain from visited events, trimmed to start after meet.
    /// Visited is in child→parent order, so we reverse and filter.
    fn build_forward_chain(&self, visited: &[EventId], meet: &BTreeSet<EventId>) -> Vec<EventId> {
        // Reverse to get parent→child (causal) order
        let mut chain: Vec<_> = visited.iter().rev().cloned().collect();

        // Find first event after meet (drop meet and everything before it)
        if !meet.is_empty() {
            if let Some(pos) = chain.iter().position(|id| meet.contains(id)) {
                // Keep everything after the meet event
                chain = chain.into_iter().skip(pos + 1).collect();
            }
        }

        chain
    }

    /// Collect immediate children of the meet nodes toward subject and other sides.
    /// Returns (subject_immediate, other_immediate) - events whose parent is a meet node.
    fn collect_immediate_children(&self, meet: &[EventId]) -> (Vec<EventId>, Vec<EventId>) {
        let mut subject_immediate = BTreeSet::new();
        let mut other_immediate = BTreeSet::new();

        for meet_id in meet {
            if let Some(state) = self.states.get(meet_id) {
                for child in &state.subject_children {
                    subject_immediate.insert(child.clone());
                }
                for child in &state.other_children {
                    other_immediate.insert(child.clone());
                }
            }
        }

        // Remove any children that appear in both sets (they're common, not divergent)
        // Also remove the meet nodes themselves if they appear
        let meet_set: BTreeSet<_> = meet.iter().cloned().collect();
        let common: BTreeSet<_> = subject_immediate.intersection(&other_immediate).cloned().collect();
        subject_immediate.retain(|id| !common.contains(id) && !meet_set.contains(id));
        other_immediate.retain(|id| !common.contains(id) && !meet_set.contains(id));

        (subject_immediate.into_iter().collect(), other_immediate.into_iter().collect())
    }

    fn check_result(&mut self) -> Option<AbstractCausalRelation<EventId>> {
        // Check for complete descent (only if not tainted)
        // StrictDescends: subject's traversal has seen all comparison heads
        if self.unseen_comparison_heads == 0 {
            // Build forward chain from comparison head to subject head
            // The subject_visited contains events from subject head backward
            // We need the forward chain (older to newer), excluding the comparison heads themselves
            let chain: Vec<_> = self.subject_visited.iter().rev().filter(|id| !self.original_comparison.contains(id)).cloned().collect();
            return Some(AbstractCausalRelation::StrictDescends { chain });
        }

        // StrictAscends: comparison's traversal has seen all subject heads
        // This means subject is older (strictly before comparison)
        if self.unseen_subject_heads == 0 {
            return Some(AbstractCausalRelation::StrictAscends);
        }

        // Check if frontiers are exhausted
        if self.subject_frontier.is_empty() && self.comparison_frontier.is_empty() {
            // Compute minimal common ancestors
            let meet: Vec<_> =
                self.meet_candidates.iter().filter(|id| self.states.get(*id).map_or(0, |s| s.common_child_count) == 0).cloned().collect();

            // Determine final relation based on frontier states and findings
            if !self.any_common || !self.outstanding_heads.is_empty() {
                // No common ancestors found - check if we have different roots (Disjoint)
                if let (Some(subject_root), Some(other_root)) = (&self.subject_root, &self.other_root) {
                    if subject_root != other_root {
                        // Proven different genesis events
                        return Some(AbstractCausalRelation::Disjoint {
                            gca: None,
                            subject_root: subject_root.clone(),
                            other_root: other_root.clone(),
                        });
                    }
                }
                // Couldn't prove disjoint - return DivergedSince with empty meet
                return Some(AbstractCausalRelation::DivergedSince {
                    meet: vec![],
                    subject: vec![],
                    other: vec![],
                    subject_chain: vec![],
                    other_chain: vec![],
                });
            }

            // Build forward chains from meet to tips
            let meet_set: BTreeSet<_> = meet.iter().cloned().collect();
            let subject_chain = self.build_forward_chain(&self.subject_visited, &meet_set);
            let other_chain = self.build_forward_chain(&self.other_visited, &meet_set);

            // Collect immediate children of meet toward each side
            let (subject_immediate, other_immediate) = self.collect_immediate_children(&meet);

            // We have common ancestors - this is DivergedSince (true concurrency)
            Some(AbstractCausalRelation::DivergedSince {
                meet,
                subject: subject_immediate,
                other: other_immediate,
                subject_chain,
                other_chain,
            })
        } else if self.remaining_budget == 0 {
            // Budget exhausted
            Some(AbstractCausalRelation::BudgetExceeded {
                subject: self.subject_frontier.ids.clone(),
                other: self.comparison_frontier.ids.clone(),
            })
        } else {
            // Continue exploration
            None
        }
    }
}
