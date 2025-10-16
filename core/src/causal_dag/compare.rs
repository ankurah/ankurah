use std::collections::{BTreeSet, HashMap, HashSet};

use smallvec::SmallVec;

use super::relation::CausalRelation;
use crate::causal_dag::misc::EventAccumulator;
use crate::error::RetrievalError;
use crate::retrieval::{GetEvents, TClock, TEvent};
/// Causal relation with forward replay chain and ForwardView
#[derive(Debug)]
pub struct RelationAndChain<Event: TEvent> {
    pub relation: CausalRelation<Event::Id>,
    /// Forward chain from meet to subject (oldest → newest), if applicable (legacy)
    pub forward_chain: Vec<ankurah_proto::Attested<Event>>,
    /// ForwardView for iterating through ReadySets (new API)
    pub forward_view: Option<crate::causal_dag::forward_view::ForwardView<Event>>,
}

impl<Event: TEvent> RelationAndChain<Event> {
    pub fn new(relation: CausalRelation<Event::Id>, forward_chain: Vec<ankurah_proto::Attested<Event>>) -> Self {
        Self { relation, forward_chain, forward_view: None }
    }

    pub fn with_view(
        relation: CausalRelation<Event::Id>,
        forward_chain: Vec<ankurah_proto::Attested<Event>>,
        forward_view: crate::causal_dag::forward_view::ForwardView<Event>,
    ) -> Self {
        Self { relation, forward_chain, forward_view: Some(forward_view) }
    }

    pub fn simple(relation: CausalRelation<Event::Id>) -> Self { Self { relation, forward_chain: Vec::new(), forward_view: None } }
}

/// Compares an unstored event against a stored clock by starting the comparison
/// from the event's parent clock and checking if the other clock is reachable.
///
/// # Assumptions
///
/// This function assumes that all events in the `other` clock are already stored
/// and can be retrieved via the `getter`. This is typically true when `other`
/// represents an entity's current head, since all events in an entity's head
/// should be previously applied (and thus stored) events.
///
/// The `subject` event itself may not be stored, as it represents a new event
/// being compared for potential application.
pub async fn compare_unstored_event<G, E, C>(
    getter: &G,
    subject: &E,
    other: &C,
    budget: usize,
) -> Result<RelationAndChain<G::Event>, RetrievalError>
where
    G: GetEvents,
    G::Event: TEvent<Id = G::Id> + Clone,
    E: TEvent<Id = G::Id, Parent = C>,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Display,
{
    // Handle redundant delivery: if the other clock contains exactly this event,
    // return Equal immediately. Without this check, comparing the event's parent
    // against a clock containing the event itself would incorrectly return
    // NotDescends instead of the semantically correct Equal.
    if other.members().len() == 1 && other.members()[0] == subject.id() {
        return Ok(RelationAndChain::simple(CausalRelation::Equal));
    }

    let subject_parent = subject.parent();

    // Compare the subject's parent clock with the other clock
    // If parent equals other, then subject descends from other
    // Otherwise, the relationship is the same as between parent and other
    let result = compare(getter, subject_parent, other, budget).await?;
    Ok(match result.relation {
        CausalRelation::Equal => {
            // Parent equals other, so subject strictly descends from other
            // Forward view is empty since parent is already at other head
            // Caller must apply the subject event itself
            if let Some(fv) = result.forward_view {
                RelationAndChain::with_view(CausalRelation::StrictDescends, result.forward_chain, fv)
            } else {
                RelationAndChain::new(CausalRelation::StrictDescends, result.forward_chain)
            }
        }
        other_relation => {
            // Relation between parent and other is the same as between subject and other
            if let Some(fv) = result.forward_view {
                RelationAndChain::with_view(other_relation, result.forward_chain, fv)
            } else {
                RelationAndChain::new(other_relation, result.forward_chain)
            }
        }
    })
}

/// Compares two Clocks, traversing the event history to classify their
/// causal relationship. Returns both the ordering and the forward chain for replay.
///
/// The function performs a **simultaneous, breadth-first walk** from the head
/// sets of `subject` and `other`, fetching parents in batches.
///
/// `budget` reflects whatever appetite we have for traversal, which is costly
/// In practice, the node may decline an event with too high of a comparison cost.
///
/// As it walks it records which side first reached each node, incrementally
/// constructs the set of **minimal common ancestors** (the "meet"), and keeps a
/// checklist so it can decide without a second pass whether every event
/// in 'other' found a common ancestor.
///
/// The moment the relationship is clear, it returns an OrderingWithChain containing:
/// - The causal relationship (Equal, Descends, NotDescends, etc.)
/// - The forward chain (oldest → newest) for replay when applicable
///
/// If the budget is exhausted before a definite answer is reached, the partially explored
/// frontiers are returned in `BudgetExceeded`, allowing the caller to resume
/// later with a higher budget. (This bit is under-baked, and needs to be revisited)
///
/// The intention is for this to operate on a streaming basis, storing the minimal state
/// required to make a conclusive comparison. I think we can probably purge the `state`
/// map for a given entity once visited by both sides, and the id is removed from the checklist
/// but this needs a bit more thought.
pub async fn compare<G, C>(getter: &G, subject: &C, other: &C, budget: usize) -> Result<RelationAndChain<G::Event>, RetrievalError>
where
    G: GetEvents,
    G::Event: TEvent<Id = G::Id> + Clone,
    C: TClock<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Display,
{
    // bail out right away for the obvious case
    if subject.members() == other.members() {
        return Ok(RelationAndChain::simple(CausalRelation::Equal));
    }

    let mut comparison = Comparison::new(getter, subject, other, budget);
    let subject_head: Vec<_> = subject.members().to_vec();

    loop {
        if let Some(ordering) = comparison.step().await? {
            // Build forward chain (legacy) and ForwardView (new)
            let (forward_chain, forward_view) = match &ordering {
                CausalRelation::StrictDescends => {
                    let meet = other.members();
                    let chain = comparison.build_forward_chain(meet, &subject_head);
                    let view = comparison.build_forward_view(meet, &subject_head, other.members());
                    (chain, Some(view))
                }
                CausalRelation::DivergedSince { meet, subject, other } => {
                    let chain = comparison.build_forward_chain(meet, &subject_head);
                    let view = comparison.build_forward_view(meet, subject, other);
                    (chain, Some(view))
                }
                CausalRelation::StrictAscends => (Vec::new(), None), // No forward chain or view for ascends
                _ => (Vec::new(), None),
            };

            return Ok(if let Some(view) = forward_view {
                RelationAndChain::with_view(ordering, forward_chain, view)
            } else {
                RelationAndChain::new(ordering, forward_chain)
            });
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Origins<Id>(SmallVec<[Id; 8]>);

impl<Id> Origins<Id> {
    fn new() -> Self { Self(SmallVec::new()) }
}

impl<Id: Clone + PartialEq> Origins<Id> {
    fn add(&mut self, id: Id) {
        if !self.0.contains(&id) {
            self.0.push(id);
        }
    }

    fn augment(&mut self, other: &Self) {
        for h in other.0.iter() {
            if !self.0.contains(h) {
                self.0.push(h.clone());
            }
        }
    }
}

impl<Id> std::ops::Deref for Origins<Id> {
    type Target = [Id];
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone)]
struct State<Id> {
    seen_from_subject: bool,
    seen_from_other: bool,
    common_child_count: usize,
    origins: Origins<Id>,
}

impl<Id> Default for State<Id> {
    fn default() -> Self { Self { seen_from_subject: false, seen_from_other: false, common_child_count: 0, origins: Origins::new() } }
}

impl<Id> State<Id>
where Id: Clone + PartialEq
{
    fn is_common(&self) -> bool { self.seen_from_subject && self.seen_from_other }

    fn mark_seen_from(&mut self, from_subject: bool, from_other: bool) {
        if from_subject {
            self.seen_from_subject = true;
        }
        if from_other {
            self.seen_from_other = true;
        }
    }
}

// FIXME - rename to CausalTraversal or something like that
pub(crate) struct Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id>,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug,
{
    /// The event store to fetch events from
    getter: &'a G,

    /// The original set of `other` events
    original_other_events: BTreeSet<G::Id>, // immutable snapshot

    /// The set of `other` heads still looking for a common ancestor
    outstanding_heads: BTreeSet<G::Id>,

    /// The remaining budget for fetching events
    remaining_budget: usize,

    /* search frontiers */
    subject_frontier: BTreeSet<G::Id>,
    other_frontier: BTreeSet<G::Id>,

    /// Track initial heads for Disjoint relation
    initial_subject_heads: Vec<G::Id>,
    initial_other_heads: Vec<G::Id>,

    /* per-node bookkeeping */
    states: HashMap<G::Id, State<G::Id>>,

    /* incremental meet construction */
    meet_candidates: BTreeSet<G::Id>,

    /* enum-decision flags */
    unseen_other_heads: usize,
    head_overlap: bool,
    // True when the initial head sets are identical; allows early Equal
    initial_heads_equal: bool,
    any_common: bool,

    /* event accumulator for building event bridges */
    subject_event_accumulator: Option<EventAccumulator<ankurah_proto::Attested<G::Event>>>,

    /* cached events for ForwardView construction */
    /// All fetched events (both subject and other sides) for building ForwardView after meet is known
    events_by_id: HashMap<G::Id, ankurah_proto::Attested<G::Event>>,
    /// Cached parent relationships for efficient reverse/forward walks
    parents_by_id: HashMap<G::Id, SmallVec<[G::Id; 4]>>,
}

impl<'a, G> Comparison<'a, G>
where
    G: GetEvents + 'a,
    G::Event: TEvent<Id = G::Id> + Clone,
    G::Id: std::hash::Hash + Ord + std::fmt::Debug + std::fmt::Display,
{
    pub fn new<C: TClock<Id = G::Id>>(getter: &'a G, subject: &C, other: &C, budget: usize) -> Self {
        Self::new_with_accumulator(getter, subject, other, budget, None)
    }

    pub fn new_with_accumulator<C: TClock<Id = G::Id>>(
        getter: &'a G,
        subject: &C,
        other: &C,
        budget: usize,
        subject_event_accumulator: Option<EventAccumulator<ankurah_proto::Attested<G::Event>>>,
    ) -> Self {
        let subject_heads: Vec<_> = subject.members().to_vec();
        let other_heads: Vec<_> = other.members().to_vec();

        let subject_frontier: BTreeSet<_> = subject_heads.iter().cloned().collect();
        let other: BTreeSet<_> = other_heads.iter().cloned().collect();
        let original_other_events = other.clone();

        // Early signal: if initial head sets are identical, we can short-circuit Equal
        let initial_heads_equal = subject_frontier == other;
        let head_overlap = initial_heads_equal;

        Self {
            getter,

            unseen_other_heads: other.len(),

            subject_frontier,
            other_frontier: other.clone(),
            initial_subject_heads: subject_heads,
            initial_other_heads: other_heads,
            remaining_budget: budget,
            original_other_events,

            head_overlap,
            initial_heads_equal,
            any_common: false,

            states: HashMap::new(),
            meet_candidates: BTreeSet::new(),
            outstanding_heads: other,
            subject_event_accumulator,
            events_by_id: HashMap::new(),
            parents_by_id: HashMap::new(),
        }
    }

    pub fn take_accumulated_events(self) -> Option<Vec<ankurah_proto::Attested<G::Event>>> {
        self.subject_event_accumulator.map(|acc| acc.take_events())
    }

    /// Constructs the forward chain from meet to subject in causal order (oldest → newest).
    /// This is the chain of events that need to be applied when ascending from the meet.
    fn build_forward_chain(&self, meet: &[G::Id], subject_head: &[G::Id]) -> Vec<ankurah_proto::Attested<G::Event>> {
        let meet_set: HashSet<_> = meet.iter().cloned().collect();
        let _subject_head_set: HashSet<_> = subject_head.iter().cloned().collect();

        // First, find all events reachable from subject_head without going through meet
        let mut reachable = HashSet::new();
        let mut stack: Vec<G::Id> = subject_head.iter().cloned().collect();

        while let Some(id) = stack.pop() {
            if meet_set.contains(&id) || reachable.contains(&id) {
                continue;
            }

            if let Some(parents) = self.parents_by_id.get(&id) {
                reachable.insert(id.clone());
                for parent_id in parents.iter() {
                    if self.events_by_id.contains_key(parent_id) {
                        stack.push(parent_id.clone());
                    }
                }
            }
        }

        // Now build topological order using Kahn's algorithm on reachable events
        let mut in_degree: HashMap<G::Id, usize> = HashMap::new();
        let mut children: HashMap<G::Id, Vec<G::Id>> = HashMap::new();

        // Initialize for reachable events
        for id in &reachable {
            in_degree.insert(id.clone(), 0);
            children.insert(id.clone(), Vec::new());
        }

        // Count in-degrees and build children map
        for id in &reachable {
            if let Some(parents) = self.parents_by_id.get(id) {
                for parent_id in parents.iter() {
                    if reachable.contains(parent_id) {
                        *in_degree.get_mut(id).unwrap() += 1;
                        children.entry(parent_id.clone()).or_default().push(id.clone());
                    }
                }
            }
        }

        // Start with events that have no dependencies in the reachable set
        let mut queue: Vec<G::Id> = in_degree.iter().filter(|(_, &degree)| degree == 0).map(|(id, _)| id.clone()).collect();

        let mut result = Vec::new();

        // Process events in topological order
        while let Some(id) = queue.pop() {
            if let Some(event) = self.events_by_id.get(&id) {
                result.push(event.clone());

                // Reduce in-degree of children
                if let Some(child_ids) = children.get(&id) {
                    for child_id in child_ids {
                        if let Some(degree) = in_degree.get_mut(child_id) {
                            *degree -= 1;
                            if *degree == 0 {
                                queue.push(child_id.clone());
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Filter events to the closed set between meet → {subject, other} and build ForwardView
    pub fn build_forward_view(
        &self,
        meet: &[G::Id],
        subject_head: &[G::Id],
        other_head: &[G::Id],
    ) -> crate::causal_dag::forward_view::ForwardView<G::Event>
    where
        G::Id: std::hash::Hash + Eq + Clone,
    {
        // Reverse-walk from both heads back to meet to find the closed set
        let meet_set: HashSet<_> = meet.iter().cloned().collect();
        let mut closed_set = HashSet::new();
        let mut stack: Vec<G::Id> = subject_head.iter().chain(other_head.iter()).cloned().collect();

        while let Some(id) = stack.pop() {
            if meet_set.contains(&id) || closed_set.contains(&id) {
                continue;
            }

            if let Some(parents) = self.parents_by_id.get(&id) {
                closed_set.insert(id.clone());
                for parent_id in parents.iter() {
                    if self.events_by_id.contains_key(parent_id) {
                        stack.push(parent_id.clone());
                    }
                }
            }
        }

        // Build the events map for ForwardView (only events in the closed set)
        let events_map: HashMap<G::Id, ankurah_proto::Attested<G::Event>> =
            closed_set.iter().filter_map(|id| self.events_by_id.get(id).map(|e| (id.clone(), e.clone()))).collect();

        crate::causal_dag::forward_view::ForwardView::new(meet.to_vec(), subject_head.to_vec(), other_head.to_vec(), events_map)
    }

    // runs one step of the comparison, returning Some(ordering) if a conclusive determination can be made, or None if it needs more steps
    pub async fn step(&mut self) -> Result<Option<CausalRelation<G::Id>>, RetrievalError> {
        // Early short-circuit: if the initial head sets are identical, we are Equal.
        // IMPORTANT: We only use the initial-heads predicate here; we intentionally
        // do NOT short-circuit on incidental frontier equality mid-traversal, because
        // in Descends scenarios both traversals can temporarily land on the same
        // ancestors (frontiers match) before the subject continues past them.
        if self.initial_heads_equal {
            return Ok(Some(CausalRelation::Equal));
        }

        // look up events in both frontiers
        let ids: Vec<G::Id> = self.subject_frontier.union(&self.other_frontier).cloned().collect();
        // TODO: create a NewType(HashSet) and impl ToSql for the postgres storage method
        // so we can pass the HashSet as a borrow and don't have to alloc this twice
        let mut result_checklist: HashSet<G::Id> = ids.iter().cloned().collect();
        let (cost, events) = self.getter.retrieve_event(ids).await?;
        self.remaining_budget = self.remaining_budget.saturating_sub(cost);

        for event in events {
            if result_checklist.remove(&event.payload.id()) {
                self.process_event(&event);
            }
        }
        if !result_checklist.is_empty() {
            return Err(RetrievalError::StorageError(format!("Events not found: {:?}", result_checklist).into()));
        }

        if let Some(ordering) = self.check_result() {
            return Ok(Some(ordering));
        }

        // keep going
        Ok(None)
    }
    fn process_event(&mut self, event: &ankurah_proto::Attested<G::Event>) {
        let id = event.payload.id();
        let from_subject = self.subject_frontier.remove(&id);
        let from_other = self.other_frontier.remove(&id);

        // Cache all events (both subject and other sides) for ForwardView construction
        self.events_by_id.insert(id.clone(), event.clone());

        // Cache parent relationships for efficient traversal
        let parents: SmallVec<[G::Id; 4]> = event.payload.parent().members().iter().cloned().collect();
        self.parents_by_id.insert(id.clone(), parents.clone());

        // Process the current node and capture relevant state
        // We do this BEFORE accumulation to track seen_from state
        let (is_common, origins) = {
            let node_state = self.states.entry(id.clone()).or_default();
            node_state.mark_seen_from(from_subject, from_other);

            // Accumulate events from the subject side for event bridge building
            // Only accumulate events that are:
            // 1. Currently being processed from subject side
            // 2. NOT in the original other heads (we don't need to send the known heads)
            // 3. NOT common (haven't been seen from both sides at any point)
            if from_subject && !self.original_other_events.contains(&id) && !node_state.is_common() {
                if let Some(ref mut accumulator) = self.subject_event_accumulator {
                    accumulator.add(&event); // Accumulate the full Attested<Event>
                }
            }

            // Handle origins for "other" heads
            if from_other && self.original_other_events.contains(&id) {
                node_state.origins.add(id.clone());
            }

            // Capture state before dropping borrow
            (node_state.is_common(), node_state.origins.clone())
        };

        // Handle common node and parent updates
        if is_common && self.meet_candidates.insert(id.clone()) {
            self.any_common = true;

            // remove satisfied heads from the checklist
            for h in origins.iter() {
                self.outstanding_heads.remove(h);
            }

            // Update common child count and propagate origins in one pass over parents
            for p in &parents {
                let parent_state = self.states.entry(p.clone()).or_default();
                if from_other {
                    parent_state.origins.augment(&origins);
                }
                parent_state.common_child_count += 1;
            }
        } else if from_other {
            // Just propagate origins if not a common node
            for p in &parents {
                let parent_state = self.states.entry(p.clone()).or_default();
                parent_state.origins.augment(&origins);
            }
        }

        // Extend frontiers
        if from_subject {
            self.subject_frontier.extend(parents.iter().cloned());

            if self.original_other_events.contains(&id) {
                self.unseen_other_heads = self.unseen_other_heads.saturating_sub(1);
                self.head_overlap = true;
            }
        }
        if from_other {
            self.other_frontier.extend(parents.iter().cloned());
        }
    }

    fn check_result(&mut self) -> Option<CausalRelation<G::Id>> {
        // Budget exhausted - can't continue
        if self.remaining_budget == 0 {
            return Some(CausalRelation::BudgetExceeded { subject: self.subject_frontier.clone(), other: self.other_frontier.clone() });
        }

        // Both frontiers exhausted - we have complete information
        if self.subject_frontier.is_empty() && self.other_frontier.is_empty() {
            return Some(self.determine_final_ordering());
        }

        // Early determination: if we've found the meet and all other heads are accounted for,
        // we can determine NotDescends/PartiallyDescends without traversing to root
        if self.any_common && self.outstanding_heads.is_empty() && self.unseen_other_heads > 0 {
            return Some(self.compute_not_descends_ordering());
        }

        // Need more steps
        None
    }

    fn determine_final_ordering(&self) -> CausalRelation<G::Id> {
        // Subject has seen all of other's heads
        if self.unseen_other_heads == 0 {
            return if self.initial_heads_equal { CausalRelation::Equal } else { CausalRelation::StrictDescends };
        }

        // Subject hasn't seen all of other's heads - check for common ancestors
        if !self.any_common || !self.outstanding_heads.is_empty() {
            //  If either side is empty, handle specially
            if self.initial_subject_heads.is_empty() {
                // Subject is empty - it came before other (Ascends)
                return CausalRelation::StrictAscends;
            }
            if self.initial_other_heads.is_empty() {
                // Other is empty - subject came after (Descends)
                return CausalRelation::StrictDescends;
            }

            // Both sides have events, no common ancestor - Disjoint
            // TODO: Track actual roots during traversal
            let subject_root = self.initial_subject_heads[0].clone();
            let other_root = self.initial_other_heads[0].clone();

            return CausalRelation::Disjoint {
                gca: if self.any_common { Some(self.meet_candidates.iter().cloned().collect()) } else { None },
                subject_root,
                other_root,
            };
        }

        self.compute_not_descends_ordering()
    }

    fn compute_not_descends_ordering(&self) -> CausalRelation<G::Id> {
        let meet: Vec<_> = self
            .meet_candidates
            .iter()
            .filter(|id| self.states.get(*id).map_or(0, |state| state.common_child_count) == 0)
            .cloned()
            .collect();

        // If meet is empty, there's no common ancestor - return Disjoint
        if meet.is_empty() {
            // TODO: Track actual roots during traversal - for now use placeholder values
            let subject_root = self
                .initial_subject_heads
                .first()
                .or_else(|| self.subject_frontier.iter().next())
                .cloned()
                .unwrap_or_else(|| self.original_other_events.iter().next().cloned().unwrap());
            let other_root = self
                .initial_other_heads
                .first()
                .or_else(|| self.other_frontier.iter().next())
                .cloned()
                .unwrap_or_else(|| self.original_other_events.iter().next().cloned().unwrap());

            return CausalRelation::Disjoint { gca: None, subject_root, other_root };
        }

        // Classify as DivergedSince (true concurrency) if there's a meet,
        // indicating both sides have changes the other doesn't
        // TODO: Populate subject/other frontiers after meet
        let subject = self.subject_frontier.iter().cloned().collect();
        let other = self.other_frontier.iter().cloned().collect();

        if self.head_overlap {
            CausalRelation::DivergedSince { meet, subject, other }
        } else {
            // No overlap means subject doesn't have other's changes
            // This could be StrictAscends (other descends from subject) or DivergedSince
            // For now, conservatively return DivergedSince - refinement to StrictAscends
            // requires additional traversal to check if other descends from subject
            CausalRelation::DivergedSince { meet, subject, other }
        }
    }
}
