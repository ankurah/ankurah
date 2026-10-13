//! Core comparison logic for determining causal relationships in event DAGs.
//!
//! ## Algorithm: Backward Search in Generation Order
//!
//! This module searches backward from two clock heads
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
//! Before the two-sided traversal, `compare` tries to trace the subject's
//! parents down to the current head. A complete extension returns
//! `StrictDescends` without fetching the comparison events themselves.
//!
//! 1. **Initialize**: Start with two frontiers = {subject_head} and {other_head}
//! 2. **Expand**: Read the original heads together, then prefer the greatest queued generation
//! 3. **Process**: For each returned event:
//!    - Remove it from its frontier
//!    - Add its parents to the frontier (moving backward)
//!    - Track which frontier(s) have seen it (subject, other, or both)
//! 4. **Detect**: Check if we've determined the relationship (Descends, NotDescends, etc.)
//! 5. **Repeat** until the relation is known, all remaining paths lie below one
//!    common node, or the budget is exhausted. Unresolved multiple meet nodes
//!    keep walking; saturation and a failed budget attempt use breadth-first search.

use super::{
    accumulator::{compute_ancestry_from_dag, ComparisonResult, EventAccumulator},
    frontier::Frontier,
    relation::AbstractCausalRelation,
};
use crate::error::RetrievalError;
use crate::retrieval::GetEvents;
use ankurah_proto::{Clock, EventId};
use std::collections::{BTreeSet, HashMap};

/// Compare two clocks to determine their causal relationship.
///
/// Walks backward from both heads, preferring greater generations after the
/// original heads are read, looking for common ancestors or divergence.
///
/// Takes ownership of the retriever, wrapping it in an `EventAccumulator`.
/// Returns a `ComparisonResult` containing both the relation and the accumulated DAG.
/// Rejects known generation contradictions in subject tips and the history read, without additional reads.
///
/// Budget escalation: if the initial budget is exhausted, retries internally
/// with up to 4x the initial budget before returning `BudgetExceeded`.
pub async fn compare<E: GetEvents>(
    event_getter: E,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<ComparisonResult<E>, RetrievalError> {
    let mut accumulator = EventAccumulator::new(event_getter);
    let relation = compare_ancestry(&mut accumulator, subject, comparison, budget).await?;
    accumulator.validate_generation_claims(subject, comparison)?;
    Ok(ComparisonResult { relation, accumulator })
}

async fn compare_ancestry<E: GetEvents>(
    accumulator: &mut EventAccumulator<E>,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Result<AbstractCausalRelation<EventId>, RetrievalError> {
    if subject.ids().eq(comparison.ids()) {
        return Ok(AbstractCausalRelation::Equal);
    }

    // A clock with no history shares nothing with a non-empty one: diverged
    // with an empty meet.
    if subject.is_empty() || comparison.is_empty() {
        return Ok(AbstractCausalRelation::DivergedSince {
            meet: vec![],
            subject: vec![],
            other: vec![],
            subject_chain: vec![],
            other_chain: vec![],
        });
    }

    // Prove complete extensions without reading the comparison head's events:
    // an ephemeral node may have their state without retaining their payloads.
    if let Some(chain) = descendant_chain(accumulator, subject, comparison, budget).await {
        return Ok(AbstractCausalRelation::StrictDescends { chain });
    }

    let initial_budget = budget;
    let max_budget = initial_budget * 4;
    let mut current_budget = initial_budget;
    let mut generation_order = subject.max_generation() != Some(u32::MAX) && comparison.max_generation() != Some(u32::MAX);

    loop {
        let mut comp = Comparison::new(accumulator, subject, comparison, current_budget, generation_order);
        let relation = loop {
            if let Some(relation) = comp.step().await? {
                break relation;
            }
        };

        match &relation {
            AbstractCausalRelation::BudgetExceeded { .. } if generation_order => {
                // The breadth-first walk can finish a whole frontier beyond its budget.
                // Retain that behavior when generation ordering does not settle sooner.
                generation_order = false;
                current_budget = initial_budget;
            }
            AbstractCausalRelation::BudgetExceeded { .. } if current_budget < max_budget => {
                current_budget = (current_budget * 4).min(max_budget);
                // accumulator retains its DAG and cache - warm start for retry
                continue;
            }
            _ => {
                return Ok(relation);
            }
        }
    }
}

/// Follow subject parents down to the comparison clock. Every path must land
/// there, and every comparison tip must be covered; foreign branches cannot hitch a ride.
async fn descendant_chain<E: GetEvents>(
    accumulator: &mut EventAccumulator<E>,
    subject: &Clock,
    comparison: &Clock,
    budget: usize,
) -> Option<Vec<EventId>> {
    let oldest_tip = comparison.entries().iter().map(|(generation, _)| *generation).min()?;
    let mut pending: BTreeSet<_> = subject.entries().iter().cloned().collect();
    let mut covered = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut chain = Vec::new();
    while let Some((_, id)) = pending.pop_last() {
        if comparison.contains(&id) {
            covered.insert(id);
            continue;
        }
        if !visited.insert(id.clone()) {
            continue;
        }
        if visited.len() > budget.max(subject.entries().len()) {
            return None;
        }
        let event = accumulator.get_event(&id).await.ok()?;
        accumulator.accumulate(&event);
        // Older or saturated parents need the two-sided traversal. Generation
        // only limits this shortcut; the links themselves must prove descent.
        if event.parent.is_empty()
            || event
                .parent
                .entries()
                .iter()
                .any(|(generation, parent)| !comparison.contains(parent) && (*generation <= oldest_tip || *generation == u32::MAX))
        {
            return None;
        }
        chain.push(id);
        pending.extend(event.parent.entries().iter().cloned());
    }
    if covered.len() != comparison.entries().len() {
        return None;
    }
    chain.reverse();
    Some(chain)
}

/// Internal state machine for the comparison algorithm.
struct Comparison<'a, E: GetEvents> {
    accumulator: &'a mut EventAccumulator<E>,

    /// The subject clock's backward traversal.
    subject: Side,
    /// The comparison clock's backward traversal.
    comparison: Side,

    // Shared discovery state
    states: HashMap<EventId, NodeState>,
    /// Nodes reached by both traversals, in first-discovery order.
    meet_candidates: BTreeSet<EventId>,
    any_common: bool,

    remaining_budget: usize,
    generation_order: bool,
    queue: BTreeSet<(u32, EventId)>,
    unfetchable: BTreeSet<EventId>,
}

/// One clock's backward traversal. Both sides are instances of this type so
/// their bookkeeping cannot drift apart: the historical false-verdict bugs in
/// this module came from hand-duplicated per-side accounting.
struct Side {
    /// This side's original head set.
    original: BTreeSet<EventId>,
    /// Ids queued for processing.
    frontier: Frontier<EventId>,
    /// Nodes already expanded from this side. A node reachable by more than
    /// one path can be re-encountered; this set makes expansion idempotent so
    /// budget is spent per event rather than per path and `visited` stays
    /// duplicate-free.
    processed: BTreeSet<EventId>,
    /// Heads of the OPPOSITE clock reached by this traversal. A set, not a
    /// counter, so revisits cannot double-count the same head. Seeing every
    /// opposite head means this side's cover contains the opposite clock.
    opposite_heads_seen: BTreeSet<EventId>,
    /// Events this traversal processed, in child-to-parent encounter order
    /// (reversed for forward chains).
    visited: Vec<EventId>,
    /// First genesis event this traversal reached (for Disjoint detection).
    root: Option<EventId>,
}

impl Side {
    fn new(heads: BTreeSet<EventId>) -> Self {
        Self {
            frontier: Frontier::new(heads.clone()),
            original: heads,
            processed: BTreeSet::new(),
            opposite_heads_seen: BTreeSet::new(),
            visited: Vec::new(),
            root: None,
        }
    }

    /// This side's share of processing `id`: record the visit and genesis,
    /// expand the frontier once per node, and track opposite-head sightings.
    ///
    /// Expansion filters out parents already processed on this side: a longer
    /// path arriving later must not re-queue a finished node, which would
    /// re-spend budget and duplicate visited/chain entries.
    fn absorb(&mut self, id: &EventId, parents: &Clock, opposite_original: &BTreeSet<EventId>) {
        self.visited.push(id.clone());
        if parents.is_empty() && self.root.is_none() {
            self.root = Some(id.clone());
        }
        if self.processed.insert(id.clone()) {
            self.frontier.extend(parents.ids().filter(|p| !self.processed.contains(*p)).cloned());
            if opposite_original.contains(id) {
                self.opposite_heads_seen.insert(id.clone());
            }
        }
    }

    /// Whether this side's traversal has seen every head of the opposite
    /// clock, i.e. this side's cover contains the opposite clock. The seen
    /// set only ever holds members of the opposite head set, so length
    /// equality means set equality.
    fn covers(&self, opposite: &Side) -> bool { self.opposite_heads_seen.len() == opposite.original.len() }
}

#[derive(Clone)]
struct NodeState {
    seen_from_subject: bool,
    seen_from_comparison: bool,
    common_child_count: usize,
    subject_children: Vec<EventId>, // Children from subject's traversal
    other_children: Vec<EventId>,   // Children from comparison's traversal
    /// The generation currently placing this node in the fetch queue.
    queued_generation: Option<u32>,
    /// Every path currently queuing this node has crossed common history.
    below_meet: bool,
}

impl NodeState {
    fn new() -> Self {
        Self {
            seen_from_subject: false,
            seen_from_comparison: false,
            common_child_count: 0,
            subject_children: Vec::new(),
            other_children: Vec::new(),
            queued_generation: None,
            below_meet: false,
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
    fn new(accumulator: &'a mut EventAccumulator<E>, subject: &Clock, comparison: &Clock, budget: usize, generation_order: bool) -> Self {
        let mut result = Self {
            accumulator,
            subject: Side::new(subject.ids().cloned().collect()),
            comparison: Side::new(comparison.ids().cloned().collect()),
            states: HashMap::new(),
            meet_candidates: BTreeSet::new(),
            any_common: false,
            remaining_budget: budget,
            generation_order,
            queue: BTreeSet::new(),
            unfetchable: BTreeSet::new(),
        };
        if generation_order {
            for (generation, id) in subject.into_iter().chain(comparison) {
                result.enqueue(id, *generation, false);
            }
        }
        result
    }

    async fn step(&mut self) -> Result<Option<AbstractCausalRelation<EventId>>, RetrievalError> {
        // Resolve the original heads together before following their annotations.
        if self.generation_order && (!self.subject.processed.is_empty() || !self.comparison.processed.is_empty()) {
            return self.step_generation().await;
        }
        // Collect frontier IDs from both frontiers
        let mut all_frontier_ids = Vec::new();
        all_frontier_ids.extend(self.subject.frontier.ids.iter().cloned());
        all_frontier_ids.extend(self.comparison.frontier.ids.iter().cloned());

        for id in &all_frontier_ids {
            // Skip events already processed (e.g. an ID that appeared in both
            // frontiers gets collected twice but was already handled on its
            // first encounter).
            if !self.subject.frontier.ids.contains(id) && !self.comparison.frontier.ids.contains(id) {
                continue;
            }

            let event = match self.accumulator.get_event(id).await {
                Ok(event) => event,
                Err(RetrievalError::EventNotFound(_)) => {
                    // Event is unfetchable. If it's currently on both frontiers,
                    // it's a common ancestor -- process with empty parents to
                    // terminate the traversal at this point. This handles the
                    // ephemeral node scenario where the meet point event is
                    // unreachable from local storage.
                    // Note: we check at processing time, not collection time,
                    // because process_event for earlier events in this loop may
                    // have added IDs to frontiers.
                    if self.subject.frontier.ids.contains(id) && self.comparison.frontier.ids.contains(id) {
                        self.process_event(id.clone(), &Clock::default());
                        continue;
                    }
                    return Err(RetrievalError::EventNotFound(id.clone()));
                }
                Err(e) => return Err(e),
            };
            self.accumulator.accumulate(&event);
            if event.generation() == u32::MAX {
                self.generation_order = false;
            }
            self.remaining_budget = self.remaining_budget.saturating_sub(1);
            self.process_event(event.id(), &event.parent);
        }

        // Check if we have a result
        Ok(self.check_result())
    }

    async fn step_generation(&mut self) -> Result<Option<AbstractCausalRelation<EventId>>, RetrievalError> {
        if let Some(id) =
            self.unfetchable.iter().find(|id| self.subject.frontier.ids.contains(id) && self.comparison.frontier.ids.contains(id)).cloned()
        {
            self.unfetchable.remove(&id);
            self.process_event(id, &Clock::default());
            return Ok(self.check_result());
        }
        while let Some((queued_generation, id)) = self.queue.pop_last() {
            let event = match self.accumulator.get_event(&id).await {
                Ok(event) => event,
                Err(RetrievalError::EventNotFound(_)) => {
                    if self.subject.frontier.ids.contains(&id) && self.comparison.frontier.ids.contains(&id) {
                        self.process_event(id, &Clock::default());
                        return Ok(self.check_result());
                    }
                    self.unfetchable.insert(id);
                    continue;
                }
                Err(error) => return Err(error),
            };
            self.accumulator.accumulate(&event);
            let generation = event.generation();
            if generation < queued_generation {
                // Every queued node has a discovery state.
                self.states.get_mut(&id).unwrap().queued_generation = Some(generation);
                self.queue.insert((generation, id));
                continue;
            }
            // Unreliable ordering or saturation keeps the full comparison history.
            // Known contradictory annotations are still rejected by compare's validator.
            if generation > queued_generation || generation == u32::MAX {
                self.generation_order = false;
            }
            self.remaining_budget = self.remaining_budget.saturating_sub(1);
            self.process_event(id, &event.parent);
            return Ok(self.check_result());
        }
        if let Some(id) = self.unfetchable.iter().next() {
            return Err(RetrievalError::EventNotFound(id.clone()));
        }
        Ok(self.check_result())
    }

    fn enqueue(&mut self, id: &EventId, generation: u32, below_meet: bool) {
        let state = self.states.entry(id.clone()).or_insert_with(NodeState::new);
        let queued = state.queued_generation.is_some() || self.unfetchable.contains(id);
        // One path bypassing common history keeps this node active.
        state.below_meet = if queued { state.below_meet && below_meet } else { below_meet };
        if !queued {
            state.queued_generation = Some(generation);
            self.queue.insert((generation, id.clone()));
        }
    }

    fn settled(&self) -> bool {
        if self.subject.frontier.is_empty() && self.comparison.frontier.is_empty() {
            return true;
        }
        if !self.generation_order {
            return false;
        }
        // Generations may contain unverified claims. Multiple common nodes could
        // still descend one another through unread history, so only graph links
        // proving a single maximal common node permit skipping its ancestors.
        self.meet_candidates.iter().filter(|id| self.states[*id].common_child_count == 0).count() == 1
            && self
                .subject
                .frontier
                .ids
                .iter()
                .chain(&self.comparison.frontier.ids)
                .all(|id| self.states.get(id).is_some_and(|state| state.below_meet))
    }

    fn process_event(&mut self, id: EventId, parents: &Clock) {
        let from_subject = self.subject.frontier.remove(&id);
        let from_comparison = self.comparison.frontier.remove(&id);

        // Update shared node state
        let is_common = {
            let state = self.states.entry(id.clone()).or_insert_with(NodeState::new);
            if let Some(order) = state.queued_generation.take() {
                self.queue.remove(&(order, id.clone()));
            }
            state.mark_seen_from(from_subject, from_comparison);
            state.is_common()
        };

        // First discovery of a common node: a meet candidate. Its parents
        // gain a common child, which disqualifies them from the final meet
        // (an ancestor of shared history is not its edge).
        if is_common && self.meet_candidates.insert(id.clone()) {
            self.any_common = true;
            for parent in parents.ids() {
                self.states.entry(parent.clone()).or_insert_with(NodeState::new).common_child_count += 1;
            }
        }

        // Register this event as a child of each of its parents (for immediate children tracking)
        for parent in parents.ids() {
            self.states.entry(parent.clone()).or_insert_with(NodeState::new).add_child(id.clone(), from_subject, from_comparison);
        }

        if from_subject {
            self.subject.absorb(&id, parents, &self.comparison.original);
        }
        if from_comparison {
            self.comparison.absorb(&id, parents, &self.subject.original);
        }
        if self.generation_order {
            let below_meet = is_common || self.states[&id].below_meet;
            for (generation, parent) in parents {
                if self.subject.frontier.ids.contains(parent) || self.comparison.frontier.ids.contains(parent) {
                    self.enqueue(parent, *generation, below_meet);
                }
            }
        }
    }

    /// Build forward chain from visited events, trimmed to start after meet.
    /// Visited is in child-to-parent order, so we reverse and filter.
    fn build_forward_chain(visited: &[EventId], meet: &BTreeSet<EventId>) -> Vec<EventId> {
        // Reverse to get parent-to-child (causal) order
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
        // StrictDescends: the subject's traversal has seen every comparison
        // head, proving cover containment. The adoption semantics of
        // StrictDescends require more: the subject must introduce no lineage
        // foreign to the comparison. A subject clock can smuggle a foreign
        // line either as an extra head (V3: {B, X} over {A}) or through a
        // single event's multi-parent clock (a graft event whose parents join
        // a legitimate ancestor with an independent root). Such shapes must
        // not fire here; left to run, the traversal exhausts and yields the
        // diverged verdict, so the foreign line merges through layers instead
        // of being adopted wholesale.
        //
        // The test is the subject's exploration boundary: every genesis root
        // its traversal has DISCOVERED, and every id still on its frontier
        // (the undiscovered remainder), must lie within the comparison's
        // ancestry. Honest shapes pass exactly when they used to: a deep
        // linear extension fires with its unexplored remainder below the
        // comparison surface, and a sibling tip bottoms out at the shared
        // ancestor, which is inside the comparison ancestry. Ancestry here
        // includes parent ids referenced by explored events even when not yet
        // fetched; those edges are real DAG structure, so testing against
        // them is sound and preserves early termination.
        if self.subject.covers(&self.comparison) {
            let dag = self.accumulator.dag();
            let comparison_heads: Vec<EventId> = self.comparison.original.iter().cloned().collect();
            let comparison_ancestry = compute_ancestry_from_dag(dag, &comparison_heads);
            let roots_grounded = self
                .subject
                .processed
                .iter()
                .filter(|id| dag.get(*id).is_some_and(|parents| parents.is_empty()))
                .all(|root| comparison_ancestry.contains(root));
            let frontier_grounded = self.subject.frontier.ids.iter().all(|id| comparison_ancestry.contains(id));
            if roots_grounded && frontier_grounded {
                // Forward chain (older to newer), excluding the comparison
                // heads themselves.
                let chain: Vec<_> =
                    self.subject.visited.iter().rev().filter(|id| !self.comparison.original.contains(id)).cloned().collect();
                return Some(AbstractCausalRelation::StrictDescends { chain });
            }
        }

        // StrictAscends: the comparison's traversal has seen every subject
        // head, so the subject is already covered by the comparison.
        // Deliberately cover-only, no grounding: this verdict drives
        // skip-the-incoming decisions, for which covering is sufficient.
        if self.comparison.covers(&self.subject) {
            return Some(AbstractCausalRelation::StrictAscends);
        }

        // Stop once the remaining shared history cannot change the meet.
        if self.settled() {
            // No shared history discovered: the clocks are disjoint when the
            // two traversals bottomed out at different genesis events. (Both
            // roots are always found by exhaustion; the fallthrough is
            // defensive.)
            if !self.any_common {
                if let (Some(subject_root), Some(other_root)) = (&self.subject.root, &self.comparison.root) {
                    if subject_root != other_root {
                        return Some(AbstractCausalRelation::Disjoint {
                            gca: None,
                            subject_root: subject_root.clone(),
                            other_root: other_root.clone(),
                        });
                    }
                }
                return Some(AbstractCausalRelation::DivergedSince {
                    meet: vec![],
                    subject: vec![],
                    other: vec![],
                    subject_chain: vec![],
                    other_chain: vec![],
                });
            }

            // Shared history exists. Every comparison head must be accounted
            // for: some common node reachable backward from it over the
            // accumulated DAG, which includes every path above the settled meet.
            // A head sharing nothing with the subject forces the
            // conservative empty meet, so the merge machinery re-layers
            // rather than pretending a partial meet covers the whole clock.
            let dag = self.accumulator.dag();
            let meet_candidates = &self.meet_candidates;
            let all_heads_accounted = self
                .comparison
                .original
                .iter()
                .all(|head| compute_ancestry_from_dag(dag, std::slice::from_ref(head)).iter().any(|id| meet_candidates.contains(id)));
            if !all_heads_accounted {
                return Some(AbstractCausalRelation::DivergedSince {
                    meet: vec![],
                    subject: vec![],
                    other: vec![],
                    subject_chain: vec![],
                    other_chain: vec![],
                });
            }

            // The meet: common nodes with no common children, i.e. the
            // maximal antichain of the shared region.
            let meet: Vec<_> =
                self.meet_candidates.iter().filter(|id| self.states.get(*id).map_or(0, |s| s.common_child_count) == 0).cloned().collect();

            // Build forward chains from meet to tips
            let meet_set: BTreeSet<_> = meet.iter().cloned().collect();
            let subject_chain = Self::build_forward_chain(&self.subject.visited, &meet_set);
            let other_chain = Self::build_forward_chain(&self.comparison.visited, &meet_set);

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
                subject: self.subject.frontier.ids.clone(),
                other: self.comparison.frontier.ids.clone(),
            })
        } else {
            // Continue exploration
            None
        }
    }
}
