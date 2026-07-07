//! The executor: one owner for the application sequence every arm used to
//! hand-roll.
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
//! Verdict-driven gap replay hooks in here at M8, when the entity exposes
//! its comparison chain; until then the planner's ordering guarantees cover
//! the staged-ancestor cases.

use ankurah_proto::{Attested, Event, EventId};

use super::outcome::{IngestOutcome, SkipReason};
use super::plan::IngestPlan;
use super::staging::StagingArea;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::{MutationError, RetrievalError};
use crate::retrieval::SuspenseEvents;
use async_trait::async_trait;

/// State persistence, supplied by the feeder because attestation needs the
/// node's PolicyAgent and the executor sits below the node layer.
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
    // node-held staging, whatever this plan does not explicitly retain must
    // leave the area.
    let members: Vec<EventId> = plan.preresolved.iter().map(|(id, _)| id.clone()).chain(plan.schedule.iter().cloned()).collect();

    let mut outcomes: Vec<(EventId, IngestOutcome)> = plan.preresolved;
    let mut applied: Vec<Attested<Event>> = Vec::new();
    let mut failure: Option<MutationError> = None;

    for id in plan.schedule {
        let Some(attested) = staging.get_attested(&id) else {
            outcomes.push((id, IngestOutcome::Skipped(SkipReason::NotStaged)));
            continue;
        };

        match entity.apply_event(getter, &attested.payload).await {
            Ok(true) => {
                if let Err(e) = getter.commit_event(&attested).await {
                    // Applied to the resident but not durable: the same
                    // window today's arms have on a commit failure. The
                    // failure stops the item; redelivery re-applies
                    // idempotently.
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
    // (D2's applied-set) is the sound way to skip this write later. The
    // one exception: an empty-head entity has nothing true to persist (a
    // phantom's empty state must not land in storage).
    if !entity.head().is_empty() {
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
    // by commit_event; everything else this plan touched leaves the area
    // now that staging outlives the call: plan-time skips are redeliveries,
    // and a hard failure plus its unattempted suffix are the sender's retry
    // to re-deliver (rejection is not buffering). Events staged by OTHER
    // deliveries and not in this plan's membership are untouched.
    let keep: std::collections::BTreeSet<&EventId> = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, IngestOutcome::NeedsState { .. } | IngestOutcome::NeedsEvents { .. }))
        .map(|(id, _)| id)
        .collect();
    for id in &members {
        if !keep.contains(id) {
            staging.remove(id);
        }
    }

    ExecutionOutcome { outcomes, applied, failure }
}
