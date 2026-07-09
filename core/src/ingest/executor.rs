//! The executor: the application sequence for every pipeline feed. The
//! PerItem arms and the remote commit lane execute through here; the LOCAL
//! commit lane shares only the planned, policy-checked phase one and keeps
//! its own phase-two macro-order (commit, advance the transaction forks,
//! relay to required peers, only then materialize and persist), because an
//! ephemeral node must not expose state its durable peer has not accepted.
//! That lane's ordering lives in context.rs; D4 (transactional visibility)
//! inherits both seams.
//!
//! Canonical order (the T8 pinning from the D1 plan): per event, apply to
//! the resident entity (TOCTOU compare-and-retry stays inside apply_event,
//! unchanged) and then commit to the event log, gated on the apply having
//! advanced the entity; per entity, persist the state buffer after its
//! events, uniformly on every arm; per batch, the feeder notifies the
//! reactor once with whatever actually advanced. A crash may orphan
//! committed events (harmless, content-addressed, idempotent) but never
//! yields persisted state referencing uncommitted events.
//!
//! Containment: a hard failure stops the remaining schedule (later events
//! likely descend from the failed one), but the applied prefix is real
//! progress: it is persisted, reported, and notified. That is the C4-11
//! semantic the EventOnly arm had and the EventBridge arm lacked; the
//! executor gives it to every feed. Phantom eviction is likewise uniform
//! here (C4-12): a speculatively materialized empty-head resident never
//! survives an item that failed to give it state, whether by hard failure
//! or by an unresolved NeedsState/NeedsEvents outcome.
//!
//! Recoverable non-failures (missing parents) surface as typed outcomes,
//! not errors: the event stays staged for descendant re-drive (268-B).
//! Verdict-driven gap replay does NOT live here: the maintainer amendment
//! (2026-07-06, recorded at the StrictDescends arm in entity.rs) placed it
//! inside apply_event, which this executor calls opaquely. The planner's
//! parents-first ordering covers the staged-ancestor cases; the
//! committed-but-unincorporated crash window is repaired inside the apply.

use ankurah_proto::{Attested, Event, EventId};

use super::outcome::{IngestOutcome, SkipReason};
use super::plan::IngestPlan;
use super::staging::StagingArea;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::{MutationError, RetrievalError};
use crate::retrieval::SuspenseEvents;
use async_trait::async_trait;

/// State persistence, supplied by the feeder because attestation needs the
/// node's PolicyAgent and the executor sits below the node layer. The
/// executor's other upward-flavored reach, phantom eviction, deliberately
/// stays a plain `&WeakEntitySet` parameter rather than a second capability
/// trait: eviction is entity-set surgery with no policy in D1; if D3 gates
/// eviction behind lifecycle policy, that is where it grows a seam.
#[async_trait]
pub(crate) trait PersistState: Send + Sync {
    async fn persist(&self, entity: &Entity) -> Result<(), MutationError>;
}

/// What one executed plan did.
pub(crate) struct ExecutionOutcome {
    /// Per-event dispositions, plan preresolutions included, schedule order.
    pub outcomes: Vec<(EventId, IngestOutcome)>,
    /// Events that applied and committed, in application order. The feeder
    /// shapes its EntityChange from these (streaming arms carry them on the
    /// change; delta arms carry an empty list), preserving today's wire of
    /// each arm.
    pub applied: Vec<Attested<Event>>,
    /// The first hard failure, after which the schedule stopped. The applied
    /// prefix above was still persisted; the feeder decides how the failure
    /// maps onto its error surface.
    pub failure: Option<MutationError>,
}

impl ExecutionOutcome {
    /// True when the entity advanced: the advance-only notification rule
    /// (the no-op suppression from PR #299, uniform across arms).
    pub fn advanced(&self) -> bool { !self.applied.is_empty() }

    /// The typed per-item error for a NeedsState outcome, if one occurred:
    /// the single edit point for the M5 surface every PerItem feeder maps
    /// that outcome onto. NeedsEvents is deliberately absent here; it is a
    /// buffered NON-error since the M8 retention flip. The Atomic commit
    /// lanes do not use this: they typed-fail both outcomes at plan time.
    pub fn needs_state_error(&self) -> Option<MutationError> {
        self.outcomes
            .iter()
            .any(|(_, o)| matches!(o, IngestOutcome::NeedsState { .. }))
            .then(|| crate::error::IngestError::Lineage(crate::error::LineageRejection::NonCreationOverEmptyHead).into())
    }
}

/// Execute a plan against one entity. PerItem containment semantics; the
/// Atomic (transaction) mode arrives with the commit-lane cutover (M6/M7).
pub(crate) async fn execute_plan<G: SuspenseEvents + Send + Sync>(
    plan: IngestPlan,
    entity: &Entity,
    entities: &WeakEntitySet,
    staging: &StagingArea,
    getter: &G,
    persist: &dyn PersistState,
) -> ExecutionOutcome {
    // Plan membership, captured for the retention sweep at the end: with
    // node-held staging, whatever this plan resolves must leave the area,
    // and whatever it does not resolve is judged by provenance (batch vs
    // carryover).
    let IngestPlan { schedule, preresolved, batch } = plan;
    let members: Vec<EventId> = preresolved.iter().map(|(id, _)| id.clone()).chain(schedule.iter().cloned()).collect();

    let mut outcomes: Vec<(EventId, IngestOutcome)> = preresolved;
    let mut applied: Vec<Attested<Event>> = Vec::new();
    let mut failure: Option<MutationError> = None;
    // Set when commit_event failed for an event whose effect the resident
    // already carries: persisting now would write a state whose head names
    // an event the log lacks, and the staged body is the only material the
    // backfill can later commit from.
    let mut uncommitted_in_head: Option<EventId> = None;

    for id in schedule {
        let Some(attested) = staging.get_attested(&id) else {
            outcomes.push((id, IngestOutcome::Skipped(SkipReason::NotStaged)));
            continue;
        };

        match entity.apply_event(getter, &attested.payload).await {
            Ok(true) => {
                if let Err(e) = getter.commit_event(&attested).await {
                    // Applied to the resident but not durable: the resident
                    // may run ahead of the log in memory (the safe
                    // direction), but nothing of this shape may persist,
                    // and the body must stay staged so a redelivery's
                    // backfill (the planner schedules head-contained
                    // unstored members) can commit it.
                    tracing::warn!(event_id = %id, "commit_event failed after the apply advanced the resident; suppressing persist until the log catches up: {e}");
                    uncommitted_in_head = Some(id);
                    failure = Some(e);
                    break;
                }
                outcomes.push((id.clone(), IngestOutcome::Applied));
                applied.push(attested);
            }
            Ok(false) => {
                // Equal or StrictAscends: the head already incorporates this
                // event's effect. If the log lacks it, commit it anyway: a
                // snapshot-adopted head references events that were never
                // individually stored, and bridges legitimately back-fill
                // them (log completeness is what lets this node serve
                // GetEvents and build bridges later). Log purity holds: only
                // integrated events commit.
                match getter.event_stored(&id).await {
                    Ok(false) => {
                        if let Err(e) = getter.commit_event(&attested).await {
                            // Same shape as above: the head carries it, the
                            // log does not; hold the body for the backfill.
                            uncommitted_in_head = Some(id);
                            failure = Some(e);
                            break;
                        }
                    }
                    Ok(true) => {
                        staging.remove(&id);
                    }
                    Err(e) => {
                        failure = Some(e.into());
                        break;
                    }
                }
                outcomes.push((id, IngestOutcome::Skipped(SkipReason::AlreadyIntegrated)));
            }
            Err(MutationError::RetrievalError(RetrievalError::EventNotFound(missing))) => {
                // A parent is not applied, not staged, and not fetchable:
                // typed, retained, integrable later by descendant re-drive
                // (268-B). Later schedule entries likely descend from this
                // one; stop rather than churn through their failures.
                outcomes.push((id, IngestOutcome::NeedsEvents { missing: vec![missing] }));
                break;
            }
            Err(e) => {
                failure = Some(super::type_comparison_error(e));
                break;
            }
        }
    }

    // Persist on every plan, advance or not (uniform state persistence,
    // REV 3). The applied prefix is real progress even when a failure
    // follows, and a no-op plan is not proof the buffer is current: a
    // sibling lane may have integrated these events with its own persist
    // still in flight, and the delta lanes re-read local storage to build
    // result sets when they return (read-your-application). Persisting the
    // resident's current state is always monotone-safe; the M2 no-op
    // elision raced exactly that window. A storage-backed currency check
    // (D2's applied-set) is the sound way to skip this write later. Two
    // exceptions: an empty-head entity has nothing true to persist (a
    // phantom's empty state must not land in storage), and a resident
    // whose head names an event a commit failure kept out of the log must
    // NOT persist, or the buffer would durably reference an uncommitted
    // event, the one ordering the crash invariant forbids. In that case
    // the buffer lags the log's coverage instead, the safe direction the
    // gap-replay machinery already repairs.
    if !entity.head().is_empty() && uncommitted_in_head.is_none() {
        if let Err(e) = persist.persist(entity).await {
            failure.get_or_insert(e);
        }
    }

    // A speculative empty-head resident from get_retrieve_or_create must not
    // outlive an item that failed to give it state (C4-12), uniformly on
    // every arm. Hard failures and unresolved outcomes (NeedsState,
    // NeedsEvents) both leave a phantom stateless; the staged events survive
    // in the staging area, and remove_if_phantom is a no-op for an entity
    // with real state.
    let unresolved = outcomes.iter().any(|(_, o)| matches!(o, IngestOutcome::NeedsState { .. } | IngestOutcome::NeedsEvents { .. }));
    if failure.is_some() || unresolved {
        entities.remove_if_phantom(&entity.id());
    }

    // Retention sweep (the 2.4 rule, live at M8): only events WAITING on
    // something stay staged. Applied and back-filled events were promoted
    // by commit_event; members with a resolving outcome (skips) leave the
    // area now that staging outlives the call: plan-time skips are
    // redeliveries, and rejection is not buffering. Members the plan never
    // resolved (the event a hard failure broke at, and the unattempted
    // suffix) are judged by provenance: a BATCH member leaves, because its
    // item error drives the sender's retry and idempotency makes the
    // redelivery safe; a CARRYOVER member (staged closure or re-drive, from
    // an earlier delivery) stays, because its delivery was acked as
    // buffered back then and nobody will redeliver it. Sweeping an
    // unresolved carryover would permanently orphan an acked event's
    // operations, the exact loss class buffering exists to prevent. Events
    // staged by OTHER deliveries and outside this plan's membership are
    // untouched either way.
    // One more retention override: an event the resident carries but the
    // log lacks (its commit failed) keeps its body staged regardless of
    // provenance; that body is the only material the redelivery backfill
    // can commit from.
    let disposition: std::collections::BTreeMap<&EventId, bool> =
        outcomes.iter().map(|(id, o)| (id, matches!(o, IngestOutcome::NeedsState { .. } | IngestOutcome::NeedsEvents { .. }))).collect();
    for id in &members {
        if uncommitted_in_head.as_ref() == Some(id) {
            continue;
        }
        match disposition.get(id) {
            Some(true) => {}
            Some(false) => {
                staging.remove(id);
            }
            None => {
                if batch.contains(id) {
                    staging.remove(id);
                }
            }
        }
    }

    ExecutionOutcome { outcomes, applied, failure }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::error::RetrievalError;
    use crate::ingest::plan_entity;
    use ankurah_proto::{EntityId, OperationSet};
    use async_trait::async_trait;
    use std::collections::BTreeMap;

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

    /// Getter whose `commit_event` fails for one designated event id (the
    /// deterministic stand-in for a transient storage fault) and records
    /// successful commits so `event_stored` answers like real storage.
    struct FailingCommitStore {
        staging: std::sync::Arc<StagingArea>,
        committed: std::sync::RwLock<std::collections::BTreeSet<EventId>>,
        fail_commit_of: EventId,
    }

    impl FailingCommitStore {
        fn new(staging: std::sync::Arc<StagingArea>, fail_commit_of: EventId) -> Self {
            Self { staging, committed: std::sync::RwLock::new(std::collections::BTreeSet::new()), fail_commit_of }
        }
    }

    #[async_trait]
    impl crate::retrieval::GetEvents for FailingCommitStore {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
            self.staging.get(event_id).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
        }
        async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
            Ok(self.committed.read().unwrap().contains(event_id))
        }
        fn storage_is_definitive(&self) -> bool { true }
    }

    #[async_trait]
    impl SuspenseEvents for FailingCommitStore {
        fn stage_event(&self, event: Event) { self.staging.stage(Attested::opt(event, None)); }
        async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
            if attested.payload.id() == self.fail_commit_of {
                return Err(MutationError::General("injected transient commit failure".into()));
            }
            self.committed.write().unwrap().insert(attested.payload.id());
            self.staging.remove(&attested.payload.id());
            Ok(())
        }
    }

    struct NoopPersist;
    #[async_trait]
    impl PersistState for NoopPersist {
        async fn persist(&self, _entity: &Entity) -> Result<(), MutationError> { Ok(()) }
    }

    struct RecordingPersist(std::sync::atomic::AtomicBool);
    impl RecordingPersist {
        fn new() -> Self { Self(std::sync::atomic::AtomicBool::new(false)) }
        fn called(&self) -> bool { self.0.load(std::sync::atomic::Ordering::SeqCst) }
    }
    #[async_trait]
    impl PersistState for RecordingPersist {
        async fn persist(&self, _entity: &Entity) -> Result<(), MutationError> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// The retention sweep must not delete a carryover orphan (buffered by an
    /// EARLIER delivery, acked as success then) when a transient hard failure
    /// stops the schedule at or before it: nobody will redeliver that event,
    /// so sweeping it permanently orphans its operations. Batch members are
    /// different: their item errors and the sender's retry re-delivers them.
    #[tokio::test]
    async fn transient_failure_keeps_carryover_orphans_staged() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let parent = event(entity_id, &[&genesis]);
        let p = parent.payload.id();
        let orphan = event(entity_id, &[&parent]);
        let b = orphan.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), p.clone());

        // The resident holds genesis; the orphan B buffered from an earlier
        // delivery; THIS delivery carries only the parent P.
        let entity = Entity::create(entity_id, "test".into());
        entity.apply_event(&getter, &genesis.payload).await.expect("genesis applies");
        getter.commit_event(&genesis).await.expect("genesis commits");
        staging.stage(orphan);
        staging.stage(parent);

        let plan = plan_entity(&entity.head(), &[p.clone()], &staging, &getter).await.expect("plan builds");
        assert_eq!(plan.schedule, vec![p.clone(), b.clone()], "re-drive schedules the buffered orphan behind its parent");

        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;

        assert!(outcome.failure.is_some(), "the injected commit failure surfaces");
        assert!(staging.contains(&b), "a carryover orphan the plan never resolved must stay buffered; sweeping it loses an acked event");
        // The commit-failed batch member is the one batch exception: its
        // effect is in the resident's head, so its body stays staged as the
        // backfill source (the redelivery pin below exercises the recovery).
        assert!(staging.contains(&p), "an applied-but-uncommitted event keeps its body staged for the backfill");
    }

    /// A commit_event failure AFTER the apply advanced the resident leaves
    /// the head naming an event the log lacks. Persisting that state would
    /// durably violate the commit-before-state invariant (the one crash
    /// discipline this module pins), and the planner's head-containment
    /// skip would make redelivery unable to repair it. The executor must
    /// suppress this plan's persist and keep the event's body staged so
    /// the backfill can commit it later.
    #[tokio::test]
    async fn commit_failure_never_persists_a_head_the_log_lacks() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let x = event(entity_id, &[&genesis]);
        let x_id = x.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), x_id.clone());

        let entity = Entity::create(entity_id, "test".into());
        entity.apply_event(&getter, &genesis.payload).await.expect("genesis applies");
        getter.commit_event(&genesis).await.expect("genesis commits");
        staging.stage(x);

        let plan = plan_entity(&entity.head(), &[x_id.clone()], &staging, &getter).await.expect("plan builds");
        let persist = RecordingPersist::new();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &persist).await;

        assert!(outcome.failure.is_some(), "the injected commit failure surfaces");
        assert!(entity.head().contains(&x_id), "the resident advanced in memory (safe: buffer may lag the log)");
        assert!(!persist.called(), "state whose head names an uncommitted event must never persist");
        assert!(staging.contains(&x_id), "the unstored event's body stays staged; it is the backfill source");
    }

    /// The recovery half: redelivering the same batch after the failed
    /// commit re-plans the head-contained-but-unstored event (the planner
    /// schedules it instead of skipping), and the executor's
    /// integrated-but-unstored backfill commits it with a now-working
    /// store. Persist resumes.
    #[tokio::test]
    async fn redelivery_backfills_a_head_contained_unstored_event() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let x = event(entity_id, &[&genesis]);
        let x_id = x.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let failing = FailingCommitStore::new(staging.clone(), x_id.clone());

        let entity = Entity::create(entity_id, "test".into());
        entity.apply_event(&failing, &genesis.payload).await.expect("genesis applies");
        failing.commit_event(&genesis).await.expect("genesis commits");
        staging.stage(x.clone());

        let plan = plan_entity(&entity.head(), &[x_id.clone()], &staging, &failing).await.expect("plan builds");
        let _ = execute_plan(plan, &entity, &entities, &staging, &failing, &NoopPersist).await;
        assert!(staging.contains(&x_id), "precondition: body retained after the failed commit");

        // The store heals (a different designated failure id); the sender
        // redelivers the same batch.
        let healed = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        healed.commit_event(&genesis).await.expect("the healed store still holds genesis");
        let plan = plan_entity(&entity.head(), &[x_id.clone()], &staging, &healed).await.expect("re-plan builds");
        assert!(plan.schedule.contains(&x_id), "a head-contained member that is staged but unstored must schedule for backfill");

        let persist = RecordingPersist::new();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &healed, &persist).await;
        assert!(outcome.failure.is_none(), "backfill commit succeeds: {:?}", outcome.failure);
        assert!(!staging.contains(&x_id), "promotion removes the backfilled event from staging");
        assert!(persist.called(), "persist resumes once the log caught up");
    }
}
