//! The shared state-apply: one function for every state-bearing feed
//! (StateSnapshot deltas, the StateAndEvent fast path, Get responses).
//!
//! with_state mediates through the resident entity, so the comparison
//! machinery decides what the incoming snapshot may do: fresh adoption and
//! strict descent advance, while equal, older, and divergent states are
//! no-ops (a stale fetch can never regress a newer resident, and a
//! divergent snapshot waits for its events). Events that arrived with the
//! state commit BEFORE the buffer persists: a crash must never yield
//! persisted state referencing uncommitted events. Persistence runs on
//! every apply with a non-empty head, advance or not: a no-op verdict is
//! not proof the buffer is current (the comment at the persist call
//! carries the race that killed the earlier elision). NOTIFICATION stays
//! advance-only. The caller builds the change; validation is NOT here,
//! because each arm keeps its own admission gate (unifying WHICH
//! validation runs is #274's jurisdiction).

use ankurah_proto::{Attested, CollectionId, EntityId, Event, State};

use super::executor::PersistState;
use super::staging::StagingArea;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::MutationError;
use crate::retrieval::{GetState, SuspenseEvents};

/// What a state feed did to one entity.
pub(crate) struct StateApplied {
    pub entity: Entity,
    /// True when the entity advanced (fresh adoption or strict descent);
    /// false when the incoming state was equal, older, or divergent. The
    /// advance-only notification rule rides this, as does persistence.
    pub advanced: bool,
}

/// Apply one state (plus any events that traveled with it) to one entity.
pub(crate) async fn apply_state_feed<S, E>(
    entities: &WeakEntitySet,
    state_getter: &S,
    event_getter: &E,
    staging: &StagingArea,
    entity_id: EntityId,
    collection_id: CollectionId,
    state: State,
    events_to_commit: &[Attested<Event>],
    persist: &dyn PersistState,
    unverified: &super::UnverifiedEvents,
) -> Result<StateApplied, MutationError>
where
    S: GetState + Send + Sync,
    E: SuspenseEvents + Send + Sync,
{
    let (changed, entity) = entities
        .with_state(state_getter, event_getter, entity_id, collection_id, state)
        .await
        .map_err(|e| super::type_comparison_error(e.into()))?;
    let advanced = !matches!(changed, Some(false));
    if advanced {
        for event in events_to_commit {
            // Adoption cargo is the adopted-history admission (D2-3): the
            // SNAPSHOT, not the generation equation, vouches for these
            // events, so a failed or impossible check demotes eligibility
            // (recorded in the unverified set) and warns; it never rejects
            // events whose effect the adopted state already carries
            // (retroactive rejection of committed history is D3's
            // jurisdiction). Verified cargo stays eligible for free; storage
            // errors surface at the commit_event right below either way.
            match super::check_generation(event_getter, &event.payload).await {
                Ok(super::GenerationCheck::Verified) => {}
                Ok(super::GenerationCheck::Unverifiable) => {
                    unverified.insert(event.payload.id());
                }
                Err(e) => {
                    tracing::warn!(event = %event.payload.id(), "adopted event's generation could not be verified or is inconsistent; demoting eligibility: {e}");
                    unverified.insert(event.payload.id());
                }
            }
            event_getter.commit_event(event).await?;
        }
    }
    // Persist on every apply, advance or not. A no-op apply is not proof
    // the buffer is current: a sibling lane may have materialized this
    // resident an instant ago with its own persist still in flight, and the
    // delta lanes re-read local storage to build result sets when they
    // return (read-your-application). Persisting the resident's current
    // state is always monotone-safe; eliding it raced exactly that window.
    // The sound elision is M4's persist-currency marker, not the
    // applied-set. Notification stays advance-only. Empty-head guard for
    // symmetry with the executor: a phantom's empty state must not land in
    // storage.
    let covered_head = entity.head();
    if !covered_head.is_empty() {
        persist.persist(&entity).await?;
        // The post-persist hook, insertion half (derivations section 5;
        // REV 5 section F): a state-adoption persist proves coverage for
        // the adopted head's own ids (and, on a no-op apply, the
        // resident's current head ids, which the just-persisted buffer
        // equally covers). Captured BEFORE the persist: any id covered by
        // the resident head when the persist began is covered by every
        // later persisted head, so a concurrent advance cannot make these
        // rows lie. Events BELOW the adopted horizon stay out (their
        // coverage is not enumerable here); their redelivery walks, which
        // is cost, not correctness. A failed persist inserts nothing (the
        // ? above returns first).
        entity.mark_applied(covered_head.iter().cloned());
    }

    // A state adoption can be exactly the thing a buffered orphan was
    // waiting for (268-B liveness; 2.4's post-recovery semantics are a
    // re-plan against the staging area with an empty batch). Head-seeded
    // re-drive schedules any staged event the new head satisfies; the
    // common case schedules nothing and costs one reverse-index lookup per
    // head id. Nothing here may fail the feed: the state already applied
    // and persisted, and failing now would swallow its notification.
    // Buffered events keep their own outcome surface from their original
    // delivery, so re-drive trouble is logged and the events follow the
    // retention rule.
    if advanced {
        match super::plan_entity(&entity.head(), &[], staging, event_getter).await {
            Ok(plan) if !plan.schedule.is_empty() => {
                let outcome = super::execute_plan(plan, &entity, entities, staging, event_getter, persist, unverified).await;
                if let Some(failure) = outcome.failure {
                    tracing::warn!(entity_id = %entity.id(), "buffered-event re-drive after state adoption failed: {failure}");
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(entity_id = %entity.id(), "re-drive planning after state adoption failed: {e}");
            }
        }
    }

    Ok(StateApplied { entity, advanced })
}
