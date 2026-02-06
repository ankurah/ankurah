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
use crate::retrieval::Retrieve;
use ankurah_proto::{Clock, Event, EventId};
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
pub async fn compare<R: Retrieve>(
    retriever: R,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<R>, RetrievalError> {
    // Early exit for obvious cases - empty clocks are disjoint
    if subject.as_slice().is_empty() || comparison.as_slice().is_empty() {
        let accumulator = EventAccumulator::new(retriever);
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
        let accumulator = EventAccumulator::new(retriever);
        return Ok(ComparisonResult::new(AbstractCausalRelation::Equal, accumulator));
    }

    let initial_budget = budget;
    let max_budget = initial_budget * 4;
    let mut current_budget = initial_budget;
    let mut accumulator = EventAccumulator::new(retriever);

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

/// Compare an unstored event against a stored clock.
///
/// This is a special case where we start from the event's parent clock
/// rather than the event itself (which isn't stored yet).
pub async fn compare_unstored_event<R: Retrieve>(
    retriever: R,
    event: &Event,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<R>, RetrievalError> {
    // Special case: redundant delivery check - event already in comparison head
    // This handles both single-head (comparison == [event_id]) and multi-head cases
    if comparison.as_slice().contains(&event.id()) {
        let accumulator = EventAccumulator::new(retriever);
        return Ok(ComparisonResult::new(AbstractCausalRelation::Equal, accumulator));
    }

    // Compare event's parent against the comparison clock
    let result = compare(retriever, &event.parent, comparison, budget).await?;

    // Destructure to get relation and accumulator separately
    let (relation, accumulator) = result.into_parts();

    // Transform the result based on what it means for event application
    let new_relation = match relation {
        AbstractCausalRelation::Equal => {
            // Parent equals comparison => event directly extends the head
            // Chain is just this single event
            AbstractCausalRelation::StrictDescends { chain: vec![event.id()] }
        }
        AbstractCausalRelation::StrictDescends { mut chain } => {
            // Parent strictly descends comparison => event also descends
            // Add this event to the end of the chain
            chain.push(event.id());
            AbstractCausalRelation::StrictDescends { chain }
        }
        AbstractCausalRelation::StrictAscends => {
            // Parent is "older" than comparison (Past(parent) ⊂ Past(comparison))
            // For an UNSTORED event, this means the event is CONCURRENT with the head,
            // not older - because the event itself is new, only its parent is older.
            //
            // Example 1: Entity head = [B, C], Event parent = [C]
            // compare([C], [B, C]) returns StrictAscends because Past([C]) ⊂ Past([B, C])
            // The event extends C, which is a tip - DivergedSince with meet at [C]
            //
            // Example 2: Entity head = [B], Event parent = [A] where A→B
            // compare([A], [B]) returns StrictAscends because Past([A]) ⊂ Past([B])
            // The event creates a concurrent branch from A - DivergedSince with meet at [A]
            let parent_members: BTreeSet<_> = event.parent.as_slice().iter().cloned().collect();
            let comparison_members: BTreeSet<_> = comparison.as_slice().iter().cloned().collect();

            // Meet is the parent clock (where this event's branch diverges from the head)
            let meet: Vec<_> = parent_members.iter().cloned().collect();
            // Other is the head tips that this event's parent doesn't include
            let other: Vec<_> = comparison_members.difference(&parent_members).cloned().collect();

            // Note: other_chain is empty because we haven't traversed from meet to
            // comparison head. LWW resolution handles this conservatively: when
            // other_chain is empty and current value has an event_id, current wins.
            // This is correct because:
            // 1. The incoming event is from an older branch point (parent < head)
            // 2. Head has more recent events that should take precedence
            // 3. Only values with no event_id (untracked) will be overwritten
            AbstractCausalRelation::DivergedSince {
                meet,
                subject: vec![], // Immediate children would need additional tracking
                other,
                subject_chain: vec![event.id()], // Just this incoming event
                other_chain: vec![],             // Empty - see note above
            }
        }
        AbstractCausalRelation::DivergedSince { meet, subject, other, mut subject_chain, other_chain } => {
            // Parent is concurrent with comparison
            // Add this event to the subject chain
            subject_chain.push(event.id());
            AbstractCausalRelation::DivergedSince { meet, subject, other, subject_chain, other_chain }
        }
        other => other,
    };

    Ok(ComparisonResult::new(new_relation, accumulator))
}

/// Internal state machine for the comparison algorithm.
struct Comparison<'a, R: Retrieve> {
    accumulator: &'a mut EventAccumulator<R>,

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

impl<'a, R: Retrieve> Comparison<'a, R> {
    fn new(accumulator: &'a mut EventAccumulator<R>, subject: &Clock, comparison: &Clock, budget: usize) -> Self {
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
            let event = match self.accumulator.get_event(id).await {
                Ok(event) => event,
                Err(RetrievalError::EventNotFound(_)) => continue,
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
