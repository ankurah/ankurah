//! The executor: the application sequence for every pipeline feed. The
//! PerItem arms and the remote commit lane execute through here; the LOCAL
//! commit lane shares the planned, policy-checked phase one and the shared
//! NodePersist persist funnel (the D2-6 one-home rule; context.rs routes
//! its materialize-and-persist step through it) while keeping its own
//! phase-two macro-order (commit, advance the transaction forks, relay to
//! required peers, only then materialize and persist), because an
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
//! progress: it is reported and notified, and it is persisted EXCEPT when
//! the failure was a commit failure that left an applied event out of the
//! log (uncommitted_in_head suppresses that persist so the buffer never
//! durably references an uncommitted event; the buffer lags the log's
//! coverage instead, the direction gap replay repairs). That is the C4-11
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
    /// prefix above was still persisted, UNLESS the failure was a commit
    /// failure that left an applied event out of the log (the
    /// uncommitted_in_head persist suppression: the buffer must never
    /// durably reference an uncommitted event). The feeder decides how the
    /// failure maps onto its error surface.
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
/// Every event this plan admits without the generation equation check
/// records its id in the node's admitted-unverified set (D2-3), carried by
/// `entities` (WeakEntitySet::unverified), which is what makes it
/// ineligible for the M5 accelerations.
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
    let IngestPlan { schedule, preresolved, batch, backfill, preverified } = plan;
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

        // Admission verification (D2-3), ONCE per admission, BEFORE the
        // apply so a rejected event never advances the resident. Three
        // dispositions:
        // - backfill members (the integrated-but-unstored lane: the head
        //   already carries the event, only its log row is missing) are
        //   NEVER checked: rejection could not un-apply them and would
        //   wedge the entity's log repair; they admit as adopted history
        //   and record unverified below.
        // - preverified plans (commit-lane phase one already ran the check
        //   over this schedule) do not re-verify.
        // - everything else checks the equation, resolving parents from the
        //   resident's materialized head generations first (REV 5 K: read
        //   FRESH each iteration, so an in-order bridge step verifies
        //   against the entries the previous apply just pinned; no read at
        //   all for covered parents) and locally held payloads otherwise
        //   (staging, then local storage; never a peer fetch): a mismatch
        //   is a typed hard failure with the same containment as any
        //   malformed event, and unresolvable parents admit unverified
        //   (the adopted-horizon extension shape on ephemeral nodes).
        // Recording happens only at durable admission (a successful
        // commit_event below): an event that fails to commit is not yet
        // admitted, and its redelivery re-decides.
        let unverified_admission = if backfill.contains(&id) {
            true
        } else if preverified {
            false
        } else {
            let materialized = entity.head_generations();
            match super::check_generation(getter, Some(&materialized), &attested.payload).await {
                Ok(super::GenerationCheck::Verified) => false,
                Ok(super::GenerationCheck::Unverifiable) => true,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        };

        match entity.apply_event(getter, &attested.payload, Some(entities.unverified())).await {
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
                if unverified_admission {
                    entities.unverified().insert(id.clone());
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
                        if unverified_admission {
                            entities.unverified().insert(id.clone());
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
    // resident's current state is always monotone-safe; the old no-op
    // elision raced exactly that window. The sound skip lives inside the
    // persist funnel (D2-6): the persist-currency marker elides only on
    // completed-persist testimony for the current head and epoch. Two
    // exceptions: an empty-head entity has nothing true to persist (a
    // phantom's empty state must not land in storage), and a resident
    // whose head names an event a commit failure kept out of the log must
    // NOT persist, or the buffer would durably reference an uncommitted
    // event, the one ordering the crash invariant forbids. In that case
    // the buffer lags the log's coverage instead, the safe direction the
    // gap-replay machinery already repairs.
    if !entity.head().is_empty() && uncommitted_in_head.is_none() {
        match persist.persist(entity).await {
            Ok(()) => {
                // The post-persist hook, insertion half (derivations
                // section 5; REV 5 section F): Ok from the funnel means a
                // set_state covering the entity's current head COMPLETED,
                // either just now or previously (the M4 marker elision:
                // elision fires only on marker currency, which is exactly
                // the completed-persist testimony these rows need). That
                // proves coverage for this plan's applied events and its
                // already-integrated skips (AlreadyCommitted
                // preresolutions are plan-time head members and
                // AlreadyIntegrated events are in the head's ancestry;
                // both stay covered by every later persisted head, since
                // heads never regress). Inserting the no-op skip outcomes
                // under an ELIDED persist is derivations 5's D2-3.d
                // enablement at this hook: the insert is gated on the
                // marker being current. The suppress-persist path above
                // inserts NOTHING (R-D2-3b), and neither does a failed
                // persist: a row must never testify to coverage the
                // persisted buffer lacks.
                entity.mark_applied(outcomes.iter().filter_map(|(id, o)| match o {
                    IngestOutcome::Applied
                    | IngestOutcome::Skipped(SkipReason::AlreadyIntegrated)
                    | IngestOutcome::Skipped(SkipReason::AlreadyCommitted) => Some(id.clone()),
                    _ => None,
                }));
            }
            Err(e) => {
                failure.get_or_insert(e);
            }
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
    use crate::ingest::plan_entity;
    use crate::ingest::testkit::{event, event_with_title, FailingCommitStore, NoState, NoopPersist, RecordingPersist};
    use crate::retrieval::GetEvents;
    use ankurah_proto::EntityId;
    use std::collections::BTreeMap;

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
        entity.apply_event(&getter, &genesis.payload, None).await.expect("genesis applies");
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
        entity.apply_event(&getter, &genesis.payload, None).await.expect("genesis applies");
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
        entity.apply_event(&failing, &genesis.payload, None).await.expect("genesis applies");
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

    /// R-D2-2c (plan REV 4 section 3 M2): the adopted-history backfill lane
    /// admits an event whose parents are NOT locally resolvable WITHOUT
    /// generation verification, and records its id in the bounded in-memory
    /// unverified set, which is what marks it INELIGIBLE for the M5
    /// generation accelerations (D2-4: eligible requires not-in-the-set).
    ///
    /// Shape: a resident adopts a state snapshot at head {h3}; h3's own body
    /// arrives later (a bridge redelivery), but its parent h2 exists nowhere
    /// on this node (below the adopted horizon). The planner schedules h3
    /// through the integrated-but-unstored backfill branch, the apply no-ops
    /// (Equal: the head already carries it), and the executor commits it to
    /// the log: an admission that never checked the generation equation.
    #[tokio::test]
    async fn r_d2_2c_adopted_backfill_admits_unverifiable_event_into_unverified_set() {
        let entity_id = EntityId::new();
        // True lineage h1 <- h2 <- h3, honestly stamped; h1 and h2 are NEVER
        // given to this node in any form.
        let h1 = event(entity_id, &[]);
        let h2 = event(entity_id, &[&h1]);
        let h3 = event(entity_id, &[&h2]);
        let h3_id = h3.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // Ephemeral store (the adopted-history shape lives on ephemeral
        // nodes: a definitive planner would type the absent parent
        // NeedsEvents instead); no injected failure, the designated fail id
        // matches nothing.
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xEE; 32]));

        // Adopt the state snapshot: resident materializes at head {h3} with
        // no event bodies anywhere local. The snapshot carries the honest
        // per-tip generation annotation, as any real sender would.
        let adopted = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![h3_id.clone()]),
            head_generations: ankurah_proto::GClock::from((h3.payload.generation, h3_id.clone())),
        };
        let (_, entity) = entities
            .with_state(&NoState, &getter, entity_id, "test".into(), adopted)
            .await
            .expect("fresh snapshot adoption materializes the resident");
        assert_eq!(entity.head(), ankurah_proto::Clock::from(vec![h3_id.clone()]), "precondition: adopted head");

        // The bridge redelivers h3's body; h2 is still absent everywhere.
        staging.stage(h3.clone());
        let plan = plan_entity(&entity.head(), &[h3_id.clone()], &staging, &getter).await.expect("plan builds");
        assert_eq!(plan.schedule, vec![h3_id.clone()], "head-contained but unstored member schedules for backfill");
        assert!(plan.backfill.contains(&h3_id), "the planner classifies it as the adopted-history backfill lane");

        let unverified = entities.unverified();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;

        // The lane ADMITS the event (no verification, no rejection)...
        assert!(outcome.failure.is_none(), "the backfill admission must not fail: {:?}", outcome.failure);
        assert!(getter.event_stored(&h3_id).await.unwrap(), "the backfill commits the event to the log");
        // ...and records it as admitted-unverified, hence ineligible (D2-4).
        assert!(
            unverified.contains(&h3_id),
            "an event admitted through the adopted-history backfill must land in the unverified set (it is ineligible for accelerations)"
        );
    }

    /// The other adopted-horizon shape: a FRESH event EXTENDING an adopted
    /// head whose body is not locally in hand. UPDATED AT M4 (plan REV 5
    /// sections B and K): head-parented arrivals are COVERED by the
    /// resident's materialized head generations, so the extension now
    /// VERIFIES against the materialization (whose adopted entries carry
    /// the state's trust envelope) instead of admitting unverified as M2
    /// built it, and it costs ZERO payload reads. Its own child then
    /// extends the induction the same way. Eligibility consequence (the K
    /// bookkeeping default): neither id lands in the unverified set, so
    /// both stay acceleration-eligible; an inherited wrong value could only
    /// cost performance (5b-ii: verdicts are immune to any generation
    /// assignment) and self-defeats at the next durable admission. The
    /// UNVERIFIABLE admission class still exists and is pinned by
    /// R-D2-2c (the backfill lane, whose members are never checked).
    #[tokio::test]
    async fn extending_an_adopted_head_verifies_against_the_materialization() {
        let entity_id = EntityId::new();
        let h1 = event(entity_id, &[]);
        let h2 = event(entity_id, &[&h1]);
        let extension = event(entity_id, &[&h2]); // honestly stamped child of the adopted head

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xEE; 32]));

        // Adopt at head {h2}; neither h1 nor h2 is local in any form.
        let adopted = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![h2.payload.id()]),
            head_generations: ankurah_proto::GClock::from((h2.payload.generation, h2.payload.id())),
        };
        let (_, entity) = entities
            .with_state(&NoState, &getter, entity_id, "test".into(), adopted)
            .await
            .expect("fresh snapshot adoption materializes the resident");

        // Deliver the extension: its parent is the materialized head tip,
        // so it verifies against the materialization, read-free, and stays
        // OUT of the unverified set.
        staging.stage(extension.clone());
        let ext_id = extension.payload.id();
        let plan = plan_entity(&entity.head(), &[ext_id.clone()], &staging, &getter).await.expect("plan builds");
        assert!(!plan.backfill.contains(&ext_id), "an extension is not the backfill lane; it goes through verification");
        let unverified = entities.unverified();
        let local_reads_before = getter.get_local_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "the extension applies: {:?}", outcome.failure);
        assert!(
            !unverified.contains(&ext_id),
            "a head-parented extension verifies against the materialization (REV 5 B); it must NOT record unverified"
        );
        assert_eq!(getter.get_local_event_calls() - local_reads_before, 0, "the covered verification reads no payloads");

        // A child of the extension is covered by the ADVANCED
        // materialization (the extension's own admission-verified entry):
        // the induction extends.
        let child = event(entity_id, &[&extension]);
        let child_id = child.payload.id();
        staging.stage(child.clone());
        let plan = plan_entity(&entity.head(), &[child_id.clone()], &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "the verified child applies: {:?}", outcome.failure);
        assert!(!unverified.contains(&child_id), "a verified admission must NOT be recorded unverified");
    }

    /// R-D2-3a (plan D2-5 as amended by REV 5 section C): a redelivered
    /// event this resident applied AND persisted is served by the
    /// applied-set's O(1) skip WITHOUT fetching events: zero `get_event`
    /// calls across the redelivery. The event sits BELOW the head (the head
    /// moved past it), so the planner schedules it and only apply_event can
    /// no-op it; before the skip exists that no-op costs a full comparison
    /// walk. The planner's `event_stored` probe and the admission
    /// verifier's `get_local_event` are cheap point reads counted
    /// separately (obligation (d)'s storedness conjunct legitimately
    /// remains on a served fast path).
    #[tokio::test]
    async fn r_d2_3a_redelivered_persisted_event_is_served_o1_without_fetching() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e2 = event(entity_id, &[&e1]);
        let (g_id, e1_id, e2_id) = (genesis.payload.id(), e1.payload.id(), e2.payload.id());

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // No injected failure: the designated fail id matches nothing.
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();

        // Deliver the chain; the persist completes.
        let entity = Entity::create(entity_id, "test".into());
        staging.stage(genesis.clone());
        staging.stage(e1.clone());
        staging.stage(e2.clone());
        let plan =
            plan_entity(&entity.head(), &[g_id.clone(), e1_id.clone(), e2_id.clone()], &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "clean delivery: {:?}", outcome.failure);
        assert!(entity.head().contains(&e2_id), "precondition: the head advanced to the tip");

        // Redeliver e1 (below the head): the planner schedules it, the
        // applied-set must serve it O(1).
        staging.stage(e1.clone());
        let plan = plan_entity(&entity.head(), &[e1_id.clone()], &staging, &getter).await.expect("re-plan builds");
        assert_eq!(plan.schedule, vec![e1_id.clone()], "precondition: a below-head redelivery schedules");
        let fetches_before = getter.get_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "the redelivery must not fail: {:?}", outcome.failure);
        assert_eq!(
            outcome.outcomes,
            vec![(e1_id.clone(), IngestOutcome::Skipped(SkipReason::AlreadyIntegrated))],
            "the redelivery no-ops as already integrated"
        );
        assert_eq!(
            getter.get_event_calls() - fetches_before,
            0,
            "R-D2-3a: an applied-and-persisted redelivery must be served by the O(1) skip without fetching events (a nonzero count is the comparison walk)"
        );
    }

    /// R-D2-3b (derivations section 5; the load-bearing regression arm):
    /// applied-set rows are written ONLY after a completed persist. The
    /// commit-failure crash window (e68288df: a commit failure after an
    /// apply suppresses the plan's persist) therefore inserts NOTHING,
    /// including for the sibling event whose own commit SUCCEEDED in the
    /// same plan: that event is durably committed while its persist was
    /// suppressed, the persisted buffer does not cover it, and a row
    /// claiming otherwise is the permanent-loss class the boundary rule
    /// exists to prevent. Recovery: once the redelivered backfill commits
    /// the failed event and persist resumes, the covered id becomes a
    /// member.
    #[tokio::test]
    async fn r_d2_3b_suppressed_persist_records_nothing_in_the_applied_set() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e2 = event(entity_id, &[&e1]);
        let (g_id, e1_id, e2_id) = (genesis.payload.id(), e1.payload.id(), e2.payload.id());

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // e2's commit fails; everything else works.
        let getter = FailingCommitStore::new(staging.clone(), e2_id.clone());
        let unverified = entities.unverified();

        // Control: a clean delivery whose persist completes records its ids.
        let entity = Entity::create(entity_id, "test".into());
        staging.stage(genesis.clone());
        let plan = plan_entity(&entity.head(), &[g_id.clone()], &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "clean genesis delivery: {:?}", outcome.failure);
        assert!(entity.applied_contains(&g_id), "control: a completed persist must record the applied event (the post-persist hook)");

        // The crash window: e1 applies AND commits, e2 applies and its
        // commit fails; the executor suppresses the persist, so NOTHING
        // from this plan may enter the applied-set.
        staging.stage(e1.clone());
        staging.stage(e2.clone());
        let plan = plan_entity(&entity.head(), &[e1_id.clone(), e2_id.clone()], &staging, &getter).await.expect("plan builds");
        let persist = RecordingPersist::new();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &persist).await;
        assert!(outcome.failure.is_some(), "the injected commit failure surfaces");
        assert!(!persist.called(), "precondition (e68288df): the plan's persist is suppressed");
        assert!(getter.event_stored(&e1_id).await.unwrap(), "precondition: e1 is durably committed");
        assert!(entity.head().contains(&e2_id), "precondition: the resident ran ahead of the log in memory");
        assert!(
            !entity.applied_contains(&e1_id),
            "R-D2-3b: an event durably committed while its plan's persist was suppressed must NOT be a member; the persisted buffer does not cover it"
        );
        assert!(!entity.applied_contains(&e2_id), "R-D2-3b: the uncommitted head event must not be a member either");

        // Recovery: the store heals (same durable log), the sender
        // redelivers e2; the planner's backfill escape schedules it
        // (head-contained, staged, unstored), the executor commits it, and
        // the now-completed persist records the coverage.
        let healed = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        healed.commit_event(&genesis).await.expect("the healed log holds genesis");
        healed.commit_event(&e1).await.expect("the healed log holds e1");
        assert!(staging.contains(&e2_id), "precondition: the failed event's body stayed staged as the backfill source");
        let plan = plan_entity(&entity.head(), &[e2_id.clone()], &staging, &healed).await.expect("re-plan builds");
        assert!(plan.backfill.contains(&e2_id), "precondition: the redelivery takes the backfill lane");
        let persist = RecordingPersist::new();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &healed, &persist).await;
        assert!(outcome.failure.is_none(), "the backfill commit succeeds: {:?}", outcome.failure);
        assert!(persist.called(), "persist resumes once the log caught up");
        assert!(entity.applied_contains(&e2_id), "after the log caught up and THIS persist completed, the covered id is recorded");
    }

    /// R-D2-3c (D2-5; the delta red-team MAJOR 7 eviction rule): evicting an
    /// id under cap pressure changes COST, never OUTCOME. The evicted id's
    /// redelivery pays the comparison walk to exactly the same no-op the
    /// skip would have served; a retained id is served O(1); the resident's
    /// state is identical either way.
    #[tokio::test]
    async fn r_d2_3c_eviction_changes_cost_never_outcome() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e2 = event(entity_id, &[&e1]);
        let e3 = event(entity_id, &[&e2]);
        let e4 = event(entity_id, &[&e3]);
        let e5 = event(entity_id, &[&e4]);
        let chain = [&genesis, &e1, &e2, &e3, &e4, &e5];
        let ids: Vec<EventId> = chain.iter().map(|e| e.payload.id()).collect();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();

        // Cap 4: the six applied ids overflow it, evicting the two oldest
        // (genesis, e1) FIFO.
        let entity = Entity::create_with_applied_cap(entity_id, "test".into(), 4);
        for ev in chain {
            staging.stage(ev.clone());
        }
        let plan = plan_entity(&entity.head(), &ids, &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "clean delivery: {:?}", outcome.failure);
        assert!(
            entity.applied_contains(&ids[3]) && entity.applied_contains(&ids[4]),
            "R-D2-3c precondition: recent ids are retained under the cap"
        );
        assert!(
            !entity.applied_contains(&ids[0]) && !entity.applied_contains(&ids[1]),
            "R-D2-3c precondition: the oldest ids were evicted under cap pressure"
        );

        let head_before = entity.head();

        // Retained member (e3): served O(1), zero event fetches.
        staging.stage(e3.clone());
        let plan = plan_entity(&entity.head(), &[ids[3].clone()], &staging, &getter).await.expect("plan builds");
        let before = getter.get_event_calls();
        let retained = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        let retained_fetches = getter.get_event_calls() - before;

        // Evicted id (e1): walks to the same conclusion.
        staging.stage(e1.clone());
        let plan = plan_entity(&entity.head(), &[ids[1].clone()], &staging, &getter).await.expect("plan builds");
        let before = getter.get_event_calls();
        let evicted = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        let evicted_fetches = getter.get_event_calls() - before;

        assert!(retained.failure.is_none() && evicted.failure.is_none(), "neither redelivery fails");
        assert_eq!(
            retained.outcomes.iter().map(|(_, o)| o.clone()).collect::<Vec<_>>(),
            evicted.outcomes.iter().map(|(_, o)| o.clone()).collect::<Vec<_>>(),
            "R-D2-3c: eviction must never change outcomes"
        );
        assert_eq!(entity.head(), head_before, "neither redelivery moves the head");
        assert_eq!(retained_fetches, 0, "R-D2-3c: the retained member is served O(1), zero event fetches");
        assert!(evicted_fetches > 0, "R-D2-3c: the evicted id pays the comparison walk (cost, not outcome)");
    }

    /// GClock pin (ii) (plan REV 5 section K, the inductive verification
    /// rule): an arrival whose parents are ALL covered by the resident's
    /// materialized head tips verifies against the MATERIALIZATION, so a
    /// covered-parent mis-stamp is REJECTED with ZERO payload reads: no
    /// `get_local_event` (the M2 verifier's parent read) and no `get_event`
    /// (a walk never starts; the rejection precedes the apply). Every
    /// materialized entry originates from an admission-verified stamp, so
    /// the covered check extends the induction; payload reads remain the
    /// fallback for arrivals whose parents the tips do not cover.
    #[tokio::test]
    async fn covered_parent_mis_stamp_rejected_against_materialization_without_payload_reads() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e1_id = e1.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();

        // Establish the resident at head {e1} through the pipeline: both
        // admissions verify, so the materialized entries are
        // admission-verified by construction (the induction's base).
        let entity = Entity::create(entity_id, "test".into());
        staging.stage(genesis.clone());
        staging.stage(e1.clone());
        let plan = plan_entity(&entity.head(), &[genesis.payload.id(), e1_id.clone()], &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "clean delivery: {:?}", outcome.failure);
        assert!(entity.head().contains(&e1_id), "precondition: resident head at e1");

        // The forgery: a child of e1 (a head tip, so its parent is covered
        // by the materialization) claiming 4 where the one correct stamp is
        // 3 (e1 carries generation 2).
        let forged = Event {
            entity_id,
            collection: "test".into(),
            operations: ankurah_proto::OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(vec![e1_id.clone()]),
            generation: 4,
        };
        let forged_id = forged.id();
        staging.stage(Attested::opt(forged, None));

        let plan = plan_entity(&entity.head(), &[forged_id.clone()], &staging, &getter).await.expect("plan builds");
        let local_reads_before = getter.get_local_event_calls();
        let walk_reads_before = getter.get_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;

        match outcome.failure {
            Some(MutationError::Ingest(crate::error::IngestError::Lineage(crate::error::LineageRejection::GenerationMismatch {
                claimed,
                expected,
                ..
            }))) => {
                assert_eq!((claimed, expected), (4, 3), "the typed rejection carries the claim and the one correct value");
            }
            other => panic!("expected a typed GenerationMismatch rejection, got {other:?}"),
        }
        assert!(!entity.head().contains(&forged_id), "a rejected event never advances the resident");
        assert_eq!(
            getter.get_local_event_calls() - local_reads_before,
            0,
            "GClock pin (ii): a covered-parent mis-stamp must be rejected against the materialization, not against payload reads"
        );
        assert_eq!(getter.get_event_calls() - walk_reads_before, 0, "no walk starts for an admission-rejected arrival");
    }

    /// The UNCOVERED-parent fallback (M4 remediation item 7, test-adequacy
    /// panel MAJOR 1): a parent that is locally HELD but BELOW the head
    /// (the concurrent-edit shape, the most common real divergence) is not
    /// covered by the materialized tips, so verification must fall back to
    /// LOCAL PAYLOAD READS, reject a mis-stamp typed, and classify the
    /// honest twin Verified. Before this pin, deleting the whole fallback
    /// arm (verify.rs, the get_local_event loop) passed every suite: all
    /// existing mis-stamp rejections travel the covered path. The payload
    /// read itself is bound by the counter, so an always-Unverifiable stub
    /// cannot pass.
    #[tokio::test]
    async fn uncovered_parent_mis_stamp_rejected_via_local_payload_reads() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]); // generation 2
        let e2 = event(entity_id, &[&e1]); // generation 3
        let e1_id = e1.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();

        // Head {e2}: e1 is committed BELOW the head, locally held, NOT a
        // materialized tip.
        let entity = Entity::create(entity_id, "test".into());
        staging.stage(genesis.clone());
        staging.stage(e1.clone());
        staging.stage(e2.clone());
        let plan = plan_entity(&entity.head(), &[genesis.payload.id(), e1_id.clone(), e2.payload.id()], &staging, &getter)
            .await
            .expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "clean delivery: {:?}", outcome.failure);
        assert!(entity.head().contains(&e2.payload.id()) && !entity.head().contains(&e1_id), "precondition: e1 sits below the head");

        // The forgery: a concurrent child of e1 claiming 4 (e1's payload
        // carries 2, so the one correct stamp is 3).
        let forged = Event {
            entity_id,
            collection: "test".into(),
            operations: ankurah_proto::OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(vec![e1_id.clone()]),
            generation: 4,
        };
        let forged_id = forged.id();
        staging.stage(Attested::opt(forged, None));

        let plan = plan_entity(&entity.head(), &[forged_id.clone()], &staging, &getter).await.expect("plan builds");
        let local_reads_before = getter.get_local_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;

        match outcome.failure {
            Some(MutationError::Ingest(crate::error::IngestError::Lineage(crate::error::LineageRejection::GenerationMismatch {
                claimed,
                expected,
                ..
            }))) => {
                assert_eq!((claimed, expected), (4, 3), "the typed rejection carries the claim and the payload-derived value");
            }
            other => panic!("an uncovered-parent mis-stamp must be rejected via local payload reads, got {other:?}"),
        }
        assert!(
            getter.get_local_event_calls() > local_reads_before,
            "the rejection must come from a LOCAL PAYLOAD READ of the below-head parent (binding the fallback arm itself)"
        );

        // The honest twin (claiming 3) travels the same fallback, applies,
        // and is classified VERIFIED: it must stay OUT of the unverified
        // set (an always-Unverifiable stub admits it but records it
        // acceleration-ineligible, which this half catches).
        let honest = event_with_title(entity_id, "honest-concurrent-twin", &[&e1]);
        let honest_id = honest.payload.id();
        staging.stage(honest.clone());
        let plan = plan_entity(&entity.head(), &[honest_id.clone()], &staging, &getter).await.expect("plan builds");
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "the honest concurrent twin applies: {:?}", outcome.failure);
        assert!(entity.head().contains(&honest_id), "the honest twin joined the head");
        assert!(
            !unverified.contains(&honest_id),
            "an honest below-head-parented event verified via payload reads is NOT recorded unverified (eligibility preserved)"
        );
    }

    /// Amendment K's THIRD covered class plus per-tip attribution under
    /// widening (M4 remediation item 8, test-adequacy panel MAJOR 2): a
    /// multi-tip head whose tips carry UNEQUAL generations must materialize
    /// each tip's OWN stamp (the layer path's widening attribution), and a
    /// SUBSET-parented arrival on that head must verify against the
    /// materialization read-free, with the max-confusion forgery (claiming
    /// 1 + the OTHER tip's generation) rejected typed. These are exactly
    /// the properties that distinguish the per-tip GClock from a scalar
    /// max; every pre-existing multi-tip test carried EQUAL tips, so a
    /// value misattribution at the widening site passed the whole suite
    /// (the debug_assert there checks id sets, not values).
    #[tokio::test]
    async fn subset_parented_arrival_on_an_unequal_multi_tip_head() {
        let entity_id = EntityId::new();
        let genesis = event(entity_id, &[]); // generation 1
        let a = event(entity_id, &[&genesis]); // generation 2
        let b = event_with_title(entity_id, "b", &[&genesis]); // generation 2, a DISTINCT concurrent sibling of a
        let c = event(entity_id, &[&b]); // generation 3

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();
        let entity = Entity::create(entity_id, "test".into());

        // Two deliveries: [genesis, a] lands head {a}; [b, c] widens through
        // the layer path to the unequal antichain {a, c}.
        for batch in [vec![genesis.clone(), a.clone()], vec![b.clone(), c.clone()]] {
            let ids: Vec<EventId> = batch.iter().map(|e| e.payload.id()).collect();
            for e in batch {
                staging.stage(e);
            }
            let plan = plan_entity(&entity.head(), &ids, &staging, &getter).await.expect("plan builds");
            let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
            assert!(outcome.failure.is_none(), "honest batch applies: {:?}", outcome.failure);
        }
        let head = entity.head();
        assert!(
            head.contains(&a.payload.id()) && head.contains(&c.payload.id()) && head.as_slice().len() == 2,
            "precondition: the head is the two-tip antichain {{a, c}}, got {head:?}"
        );

        // Per-tip attribution: each tip carries ITS OWN stamp, not a shared
        // max or a swap.
        let materialized = entity.head_generations();
        assert_eq!(materialized.generation_of(&a.payload.id()), Some(2), "tip a keeps its own stamp across the widening");
        assert_eq!(materialized.generation_of(&c.payload.id()), Some(3), "tip c pins its own stamp at the widening");

        // The max-confusion forgery FIRST, while tip a is still a
        // materialized head tip: claiming 1 + the OTHER tip's generation
        // (1 + 3 = 4) over parent {a}. The per-tip consult must reject
        // with a's OWN value (expected 3), READ-FREE (the covered path;
        // an honest subset child would supersede a and reroute this
        // through the payload fallback, which is a different pin).
        let forged = Event {
            entity_id,
            collection: "test".into(),
            operations: ankurah_proto::OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(vec![a.payload.id()]),
            generation: 4,
        };
        let forged_id = forged.id();
        staging.stage(Attested::opt(forged, None));
        let plan = plan_entity(&entity.head(), &[forged_id.clone()], &staging, &getter).await.expect("plan builds");
        let local_reads_before = getter.get_local_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        match outcome.failure {
            Some(MutationError::Ingest(crate::error::IngestError::Lineage(crate::error::LineageRejection::GenerationMismatch {
                claimed,
                expected,
                ..
            }))) => {
                assert_eq!(
                    (claimed, expected),
                    (4, 3),
                    "the rejection reads the PARENT TIP's own materialized value, never the head-wide max"
                );
            }
            other => panic!("the max-confusion forgery over a subset parent must be rejected typed, got {other:?}"),
        }
        assert_eq!(
            getter.get_local_event_calls() - local_reads_before,
            0,
            "the subset-parented rejection is served by the per-tip consult, zero payload reads"
        );

        // The third covered class, honest arm: an arrival parenting a
        // STRICT SUBSET of the multi-tip head ({a} alone) verifies against
        // the materialization, read-free, applies, and stays eligible.
        let honest = event_with_title(entity_id, "honest-subset-child", &[&a]); // generation 3
        let honest_id = honest.payload.id();
        staging.stage(honest.clone());
        let plan = plan_entity(&entity.head(), &[honest_id.clone()], &staging, &getter).await.expect("plan builds");
        let local_reads_before = getter.get_local_event_calls();
        let outcome = execute_plan(plan, &entity, &entities, &staging, &getter, &NoopPersist).await;
        assert!(outcome.failure.is_none(), "the honest subset-parented arrival applies: {:?}", outcome.failure);
        assert_eq!(
            getter.get_local_event_calls() - local_reads_before,
            0,
            "a subset-parented arrival on a multi-tip head verifies from the materialization: zero payload reads"
        );
        assert!(!unverified.contains(&honest_id), "verified subset-parented arrivals stay acceleration-eligible");
    }
}
