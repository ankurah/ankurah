//! The planner: order and membership for one entity's staged events.
//!
//! The planner is LEAN by design: it decides WHICH staged events apply and in
//! WHAT order, from cheap graph facts only. Causal verdicts stay where they
//! live today, inside `apply_event`'s compare-and-retry under the TOCTOU
//! discipline; precomputing them here would either duplicate every comparison
//! or trust verdicts that go stale across the retry window. The head passed
//! in is therefore advisory: it seeds classification, and the executor's
//! per-event application re-checks everything that must hold at mutation
//! time.
//!
//! The planner is write-pure: it takes `GetEvents` plus a read view of the
//! staging area, so it provably cannot commit or persist. On ephemeral nodes
//! the getter may fetch remotely mid-plan exactly as comparison does today.
//!
//! Membership has three sources:
//! 1. The delivered batch.
//! 2. The staging closure (B3): staged-but-unapplied ancestors of batch
//!    events, walked by parent edges within the staging area. These are
//!    events a previous delivery staged but never applied; scheduling them
//!    ahead of their descendants removes the gap-jump loss class
//!    structurally.
//! 3. Descendant re-drive: staged events from previous deliveries whose
//!    parents all become satisfied by this plan (or are already committed or
//!    in the head), found through the staging area's reverse index, seeded
//!    from the scheduled set and the current head. This is how a buffered
//!    orphan integrates when the thing it was waiting for finally arrives,
//!    whether that thing came as an event or as a state adoption; an
//!    ancestors-only walk can never find it.

use std::collections::{BTreeSet, VecDeque};

use ankurah_proto::{Clock, EventId};

use super::outcome::{IngestOutcome, SkipReason};
use super::staging::StagingArea;
use crate::error::MutationError;
use crate::event_dag::ordering::topo_sort_ids;
use crate::retrieval::GetEvents;

/// The application order and plan-time outcomes for one entity.
#[derive(Debug)]
pub(crate) struct IngestPlan {
    /// Staged event ids to apply, parents-first. Bodies live in the staging
    /// area; the executor sources them from there.
    pub schedule: Vec<EventId>,
    /// Outcomes decided at plan time without touching the entity:
    /// already-committed redeliveries and events that cannot apply before
    /// the entity has state. Events with a NeedsState outcome remain staged.
    pub preresolved: Vec<(EventId, IngestOutcome)>,
    /// The members that arrived in THIS delivery's batch, as opposed to
    /// closure/re-drive carryover buffered by earlier deliveries. The
    /// executor's retention sweep needs the distinction: an unresolved batch
    /// member may leave the area (its item error drives the sender's retry),
    /// but an unresolved carryover was acked back when it buffered, and
    /// nobody will redeliver it.
    pub batch: BTreeSet<EventId>,
}

/// Plan the application of `batch` (plus whatever the staging area makes
/// schedulable) against an entity whose current head is `head`.
///
/// Contract: every id in `batch` has already been staged by intake. A batch
/// id missing from staging (evicted between intake and planning under cap
/// pressure) is skipped here; the sender's retry re-delivers it.
pub(crate) async fn plan_entity<G: GetEvents + Send + Sync>(
    head: &Clock,
    batch: &[EventId],
    staging: &StagingArea,
    getter: &G,
) -> Result<IngestPlan, MutationError> {
    // 1 + 2: batch membership plus the staging closure over ancestors.
    let mut members: BTreeSet<EventId> = BTreeSet::new();
    let mut worklist: VecDeque<EventId> = VecDeque::new();
    for id in batch {
        if staging.contains(id) && members.insert(id.clone()) {
            worklist.push_back(id.clone());
        }
    }
    let batch_members: BTreeSet<EventId> = members.clone();
    while let Some(id) = worklist.pop_front() {
        let Some(event) = staging.get(&id) else { continue };
        for parent in event.parent.as_slice() {
            // Only staged ancestors join the closure: a parent that is
            // committed or in the head needs no scheduling, and a parent that
            // is simply absent is discovered (and typed NeedsEvents) at
            // execution time, where fetchability is decidable.
            if staging.contains(parent) && members.insert(parent.clone()) {
                worklist.push_back(parent.clone());
            }
        }
    }

    // Plan-time skip: an event the head already contains is a redelivery;
    // skipping it (success, not error) is the idempotency half of the
    // ack/retry guarantee (268-A). The test is head containment, NOT
    // event_stored: committed does not imply incorporated. A crash between
    // commit_event and save_state leaves an event durably committed while
    // the rehydrated entity has never applied it (the gap-jump class's
    // crash-window face); a redelivery must SCHEDULE that event so the
    // apply repairs the window. For committed events the head does cover,
    // apply_event's own comparison no-ops them (Equal/StrictAscends), so
    // scheduling is harmless and the outcome reports AlreadyIntegrated.
    // D2's applied-set later restores an O(1) skip that tests
    // incorporation, not mere storage.
    //
    // The converse hole matters too: head containment only implies
    // committedness while the commit-before-state discipline held. A
    // commit_event failure after its apply leaves a head-contained,
    // staged, UNSTORED event; skipping that would strand it forever
    // (nothing else re-commits it), so it schedules, and the executor's
    // integrated-but-unstored backfill commits it. The extra event_stored
    // read runs only for head-contained redeliveries of still-staged
    // events, a shape that requires a prior local failure.
    let mut preresolved: Vec<(EventId, IngestOutcome)> = Vec::new();
    let mut scheduled: BTreeSet<EventId> = BTreeSet::new();
    for id in &members {
        if head.contains(id) {
            if staging.contains(id) && !getter.event_stored(id).await? {
                scheduled.insert(id.clone());
            } else {
                preresolved.push((id.clone(), IngestOutcome::Skipped(SkipReason::AlreadyCommitted)));
            }
        } else {
            scheduled.insert(id.clone());
        }
    }

    // 3: descendant re-drive to fixpoint. A staged event from a previous
    // delivery is schedulable once every parent is scheduled here, in the
    // head, or committed. Candidates come from the reverse index, seeded by
    // everything scheduled so far PLUS the current head: a buffered child
    // whose parent arrived through a state adoption has no scheduled seed
    // (the parent was never an event in any plan), so the head is its only
    // anchor. This is what makes the empty-batch re-plan after a state
    // apply drain waiting orphans (the 2.4 post-recovery semantics).
    // Processed in id order for determinism.
    let mut candidates: VecDeque<EventId> =
        scheduled.iter().chain(head.as_slice().iter()).flat_map(|id| staging.staged_children_of(id)).collect();
    while let Some(candidate) = candidates.pop_front() {
        if scheduled.contains(&candidate) || !staging.contains(&candidate) {
            continue;
        }
        let Some(event) = staging.get(&candidate) else { continue };
        let mut satisfied = true;
        for parent in event.parent.as_slice() {
            if scheduled.contains(parent) || head.contains(parent) {
                continue;
            }
            if getter.event_stored(parent).await? {
                continue;
            }
            satisfied = false;
            break;
        }
        if satisfied {
            scheduled.insert(candidate.clone());
            candidates.extend(staging.staged_children_of(&candidate));
        }
        // An unsatisfied candidate stays buffered; its own delivery already
        // reported its outcome, and a later arrival re-drives it.
    }

    // Parents-first order over the scheduled set. Edges outside the set are
    // committed, in the head, or execution-time concerns; for staged parents
    // that is guaranteed by the closure above, restoring by construction the
    // assumption topo_sort_events documents.
    let nodes: Vec<(EventId, Vec<EventId>)> =
        scheduled.iter().filter_map(|id| staging.get(id).map(|e| (id.clone(), e.parent.as_slice().to_vec()))).collect();
    let order = topo_sort_ids(nodes)?;

    // Availability walk over the sorted order: an event is appliable when
    // every parent is available, meaning scheduled earlier in this plan, in
    // the head, or committed. Creation events ground themselves. For an
    // unappliable event the outcome depends on why:
    //
    // - The entity has no local existence at that point in the plan (empty
    //   head, no creation available): NeedsState. The empty-head guard in
    //   apply_event would reject it anyway; typing it here keeps the doomed
    //   apply from running and tells the feeder the right recovery (fetch a
    //   snapshot).
    // - The entity exists but a parent is missing: on definitive storage
    //   (durable node, no remote fallback) that parent is provably absent,
    //   so the outcome is NeedsEvents now. On non-definitive storage the
    //   event schedules anyway: execution's comparison can fetch the parent
    //   remotely, and maps to NeedsEvents only if that fails.
    //
    // Both outcome classes leave the event staged (268-B).
    let mut available: BTreeSet<EventId> = BTreeSet::new();
    let mut entity_exists = !head.is_empty();
    let mut schedule: Vec<EventId> = Vec::with_capacity(order.len());
    for id in order {
        let Some(event) = staging.get(&id) else { continue };
        if event.is_entity_create() {
            entity_exists = true;
            available.insert(id.clone());
            schedule.push(id);
            continue;
        }
        let mut missing: Vec<EventId> = Vec::new();
        for parent in event.parent.as_slice() {
            if available.contains(parent) || head.contains(parent) {
                continue;
            }
            if getter.event_stored(parent).await? {
                continue;
            }
            missing.push(parent.clone());
        }
        if missing.is_empty() {
            entity_exists = true;
            available.insert(id.clone());
            schedule.push(id);
        } else if !entity_exists {
            preresolved.push((id.clone(), IngestOutcome::NeedsState { entity: event.entity_id }));
        } else if getter.storage_is_definitive() {
            preresolved.push((id.clone(), IngestOutcome::NeedsEvents { missing }));
        } else {
            // Speculative: counts as available for planning descendants; if
            // the remote fetch fails at execution, containment stops the
            // dependent chain there and types it.
            available.insert(id.clone());
            schedule.push(id);
        }
    }

    Ok(IngestPlan { schedule, preresolved, batch: batch_members })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RetrievalError;
    use ankurah_proto::{Attested, EntityId, Event, OperationSet};
    use async_trait::async_trait;
    use std::collections::BTreeMap;

    struct FakeStore {
        committed: BTreeSet<EventId>,
        definitive: bool,
    }

    #[async_trait]
    impl GetEvents for FakeStore {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
            Err(RetrievalError::EventNotFound(event_id.clone()))
        }
        async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { Ok(self.committed.contains(event_id)) }
        fn storage_is_definitive(&self) -> bool { self.definitive }
    }

    fn store(committed: &[EventId]) -> FakeStore { FakeStore { committed: committed.iter().cloned().collect(), definitive: true } }

    fn ephemeral_store(committed: &[EventId]) -> FakeStore {
        FakeStore { committed: committed.iter().cloned().collect(), definitive: false }
    }

    fn event(entity_id: EntityId, parents: &[&Attested<Event>]) -> Attested<Event> {
        Attested::opt(
            Event {
                entity_id,
                collection: "test".into(),
                operations: OperationSet(BTreeMap::new()),
                parent: ankurah_proto::Clock::from(parents.iter().map(|p| p.payload.id()).collect::<Vec<_>>()),
                generation: Event::generation_from_parents(parents.iter().map(|p| p.payload.generation)),
            },
            None,
        )
    }

    /// genesis <- e1 <- e2 for one entity, returned with their ids.
    fn chain(entity: EntityId) -> ([Attested<Event>; 3], [EventId; 3]) {
        let genesis = event(entity, &[]);
        let g = genesis.payload.id();
        let e1 = event(entity, &[&genesis]);
        let i1 = e1.payload.id();
        let e2 = event(entity, &[&e1]);
        let i2 = e2.payload.id();
        ([genesis, e1, e2], [g, i1, i2])
    }

    #[tokio::test]
    async fn batch_orders_parents_first_regardless_of_arrival() {
        let entity = EntityId::new();
        let ([genesis, e1, e2], [g, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        // Adversarial arrival order: child first.
        staging.stage(e2);
        staging.stage(e1);
        staging.stage(genesis);

        let plan = plan_entity(&Clock::from(Vec::new()), &[i2.clone(), i1.clone(), g.clone()], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![g, i1, i2]);
        assert!(plan.preresolved.is_empty());
    }

    #[tokio::test]
    async fn b3_residual_ancestor_from_previous_delivery_joins_the_closure() {
        let entity = EntityId::new();
        let ([genesis, e1, e2], [g, i1, i2]) = chain(entity);
        // Previous delivery staged e1 but it never applied and never
        // committed. Genesis is applied history (in the head).
        let staging = StagingArea::with_default_cap();
        staging.stage(e1);
        staging.stage(e2);
        drop(genesis);

        let head = Clock::from(vec![g]);
        let plan = plan_entity(&head, &[i2.clone()], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![i1, i2], "staged ancestor is scheduled ahead of the batch event");
    }

    #[tokio::test]
    async fn redrive_schedules_buffered_descendants_transitively() {
        let entity = EntityId::new();
        let ([genesis, e1, e2], [g, i1, i2]) = chain(entity);
        // e1 and e2 arrived earlier and buffered awaiting their ancestry;
        // the batch finally delivers genesis.
        let staging = StagingArea::with_default_cap();
        staging.stage(e1);
        staging.stage(e2);
        staging.stage(genesis);

        let plan = plan_entity(&Clock::from(Vec::new()), &[g.clone()], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![g, i1, i2], "reverse index re-drives the buffered chain");
    }

    #[tokio::test]
    async fn redrive_accepts_parents_satisfied_by_head_or_storage() {
        let entity = EntityId::new();
        let ([_, e1, e2], [g, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e2);
        staging.stage(e1);

        // Genesis is committed (in storage) rather than staged; e1 arrives in
        // the batch; buffered e2 must re-drive off e1 with its other ancestry
        // grounded in storage.
        let plan = plan_entity(&Clock::from(Vec::new()), &[i1.clone()], &staging, &store(&[g.clone()])).await.unwrap();

        // Head is empty and genesis is not scheduled here, but the entity
        // exists in storage terms; the projected-creation walk must not
        // misclassify. e1's parent (genesis) is committed, so e1 schedules.
        assert_eq!(plan.schedule, vec![i1, i2]);
        assert!(plan.preresolved.is_empty());
    }

    #[tokio::test]
    async fn unsatisfied_buffered_child_stays_buffered() {
        let entity = EntityId::new();
        let ([_, e1, e2], [_, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e2);

        // Batch delivers nothing related to e2's missing parent e1.
        let unrelated = event(EntityId::new(), &[]);
        let u = unrelated.payload.id();
        staging.stage(unrelated);
        drop(e1);

        let plan = plan_entity(&Clock::from(Vec::new()), &[u.clone()], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![u]);
        assert!(staging.contains(&i2), "unsatisfied orphan remains staged");
        assert!(!plan.schedule.contains(&i2));
        let _ = i1;
    }

    #[tokio::test]
    async fn committed_redelivery_is_skipped_not_rescheduled() {
        let entity = EntityId::new();
        let ([genesis, _, _], [g, _, _]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(genesis);

        let plan = plan_entity(&Clock::from(vec![g.clone()]), &[g.clone()], &staging, &store(&[g.clone()])).await.unwrap();

        assert!(plan.schedule.is_empty());
        assert_eq!(plan.preresolved, vec![(g, IngestOutcome::Skipped(SkipReason::AlreadyCommitted))]);
    }

    #[tokio::test]
    async fn orphan_noncreation_over_empty_head_needs_state() {
        let entity = EntityId::new();
        let ([_, e1, _], [g, i1, _]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e1);

        // Nothing creates the entity locally: genesis is neither staged nor
        // committed nor in the head.
        let plan = plan_entity(&Clock::from(Vec::new()), &[i1.clone()], &staging, &store(&[])).await.unwrap();

        assert!(plan.schedule.is_empty());
        assert_eq!(plan.preresolved, vec![(i1.clone(), IngestOutcome::NeedsState { entity })]);
        assert!(staging.contains(&i1), "needs-state event remains staged for recovery");
        let _ = g;
    }

    #[tokio::test]
    async fn missing_midchain_parent_on_definitive_storage_needs_events() {
        let entity = EntityId::new();
        let ([_, e1, e2], [g, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e2);
        drop(e1);

        // Entity exists (head at genesis) but e1 is provably absent on a
        // durable node: typed at plan time, retained for re-drive.
        let head = Clock::from(vec![g]);
        let plan = plan_entity(&head, &[i2.clone()], &staging, &store(&[])).await.unwrap();

        assert!(plan.schedule.is_empty());
        assert_eq!(plan.preresolved, vec![(i2.clone(), IngestOutcome::NeedsEvents { missing: vec![i1] })]);
        assert!(staging.contains(&i2), "needs-events event remains staged");
    }

    #[tokio::test]
    async fn missing_midchain_parent_on_ephemeral_storage_schedules_speculatively() {
        let entity = EntityId::new();
        let ([_, e1, e2], [g, _, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e2);
        drop(e1);

        // Same shape on an ephemeral node: absence is not provable, and the
        // execution-time comparison can fetch the parent from a peer.
        let head = Clock::from(vec![g]);
        let plan = plan_entity(&head, &[i2.clone()], &staging, &ephemeral_store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![i2]);
        assert!(plan.preresolved.is_empty());
    }

    #[tokio::test]
    async fn empty_batch_redrive_schedules_buffered_children_of_the_head() {
        let entity = EntityId::new();
        let ([_, e1, e2], [g, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e1);
        staging.stage(e2);

        // The head reached genesis through a state adoption; the batch is
        // empty, so the head is the only re-drive anchor for the buffered
        // chain.
        let head = Clock::from(vec![g]);
        let plan = plan_entity(&head, &[], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![i1, i2], "head-seeded re-drive drains the buffered chain");
        assert!(plan.preresolved.is_empty());
    }

    /// Head containment normally implies durably committed (the crash
    /// invariant), but a commit_event failure after an apply leaves a
    /// head-contained, staged, UNSTORED event. Skipping it would strand the
    /// backfill forever; it must schedule.
    #[tokio::test]
    async fn head_contained_but_unstored_member_schedules_for_backfill() {
        let entity = EntityId::new();
        let ([_, e1, _], [g, i1, _]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(e1);

        // e1 is in the head (applied) but the store has no record of it;
        // its parent (genesis) is committed, as in the real failure shape.
        let head = Clock::from(vec![i1.clone()]);
        let plan = plan_entity(&head, &[i1.clone()], &staging, &store(&[g.clone()])).await.unwrap();

        assert_eq!(plan.schedule, vec![i1], "head-contained but unstored must schedule so the executor backfills the log");
        assert!(plan.preresolved.is_empty());
    }

    #[tokio::test]
    async fn batch_carrying_its_own_genesis_schedules_cleanly() {
        let entity = EntityId::new();
        let ([genesis, e1, e2], [g, i1, i2]) = chain(entity);
        let staging = StagingArea::with_default_cap();
        staging.stage(genesis);
        staging.stage(e1);
        staging.stage(e2);

        // Unknown entity, full history in one batch (the V6-adjacent shape).
        let plan = plan_entity(&Clock::from(Vec::new()), &[i1.clone(), g.clone(), i2.clone()], &staging, &store(&[])).await.unwrap();

        assert_eq!(plan.schedule, vec![g, i1, i2]);
        assert!(plan.preresolved.is_empty());
    }
}
