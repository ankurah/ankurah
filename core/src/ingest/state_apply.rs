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
//! advance-only. The caller builds the change; policy validation is NOT
//! here, because each arm keeps its own admission gate (unifying WHICH
//! validation runs is #274's jurisdiction). Cargo GENERATIONS are checked
//! here, BEFORE the state applies (plan REV 5 section L): a stamp that
//! provably contradicts locally held parents aborts the whole item;
//! unverifiable cargo stores and records acceleration-ineligible.

use ankurah_proto::{Attested, CollectionId, EntityId, Event, EventId, State};

use super::executor::PersistState;
use super::staging::StagingArea;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::MutationError;
use crate::retrieval::{GetState, SuspenseEvents};

/// What a state feed did to one entity.
#[derive(Debug)]
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
    // The incoming state's OWN head-generation annotation is validated
    // first, BEFORE anything adopts (plan REV 5 section K, the validation
    // invariant): structurally on every node flavor (the annotation must
    // cover exactly the head's tips), and against locally held event
    // payloads on durable nodes (a durable node never adopts a
    // wire-carried value that contradicts an event it holds). Either
    // failure aborts the ENTIRE item typed, same containment as the cargo
    // check below. Ephemeral nodes adopt the values inside the state's own
    // trust envelope, so only the structural layer runs there.
    super::verify_state_head_generations(event_getter, &state).await?;

    // Cargo generations are checked BEFORE the state applies (plan REV 5
    // section L, reversing the M2 warn-and-store arm): a claim that
    // PROVABLY contradicts resolvable parents aborts the ENTIRE item
    // with the typed lineage error, so nothing from the item is adopted
    // or stored (the feeders unstage on error). The grievance is with the
    // state that vouched for the malformed history: valid non-advancing
    // input drops silently, verifiably INVALID input errors loudly,
    // uniform with the streaming lane's typed rejection. This is
    // admission rejection of malformed input, not a generation routing a
    // verdict (the suppress-only discipline is untouched). Parents resolve
    // from the EXISTING resident's materialized head generations first
    // (REV 5 K: update-shaped cargo parented on the current head verifies
    // read-free) and local payloads otherwise. UNVERIFIABLE
    // cargo (parents not resolvable) keeps the adopted-history
    // admission: the SNAPSHOT, not the equation, vouches for it, so it
    // stores and records acceleration-ineligible below; fresh adoption
    // therefore never trips the abort (no local parents, nothing
    // provable). A storage failure from the local parent read aborts the
    // item just as loudly (it would have failed the commit right below
    // anyway).
    let resident_materialization = entities.get(&entity_id).map(|resident| resident.head_generations());
    let mut unverifiable_cargo: Vec<EventId> = Vec::new();
    for event in events_to_commit {
        match super::check_generation(event_getter, resident_materialization.as_ref(), &event.payload).await {
            Ok(super::GenerationCheck::Verified) => {}
            Ok(super::GenerationCheck::Unverifiable) => unverifiable_cargo.push(event.payload.id()),
            Err(e) => return Err(e),
        }
    }

    let (changed, entity) = entities
        .with_state(state_getter, event_getter, entity_id, collection_id, state)
        .await
        .map_err(|e| super::type_comparison_error(e.into()))?;
    let advanced = !matches!(changed, Some(false));
    if advanced {
        for event in events_to_commit {
            event_getter.commit_event(event).await?;
            // Recording only at durable admission (the M2 discipline): an
            // unverifiable id becomes acceleration-ineligible once its
            // commit succeeds.
            if unverifiable_cargo.contains(&event.payload.id()) {
                unverified.insert(event.payload.id());
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{IngestError, LineageRejection};
    use crate::ingest::testkit::{event, FailingCommitStore, NoState, NoopPersist};
    use crate::ingest::{StagingArea, UnverifiedEvents};
    use ankurah_proto::EventId;
    use std::collections::BTreeMap;

    /// GClock pin (v) (plan REV 5 section K, the validation invariant): a
    /// DURABLE node never adopts a wire-carried head generation that
    /// contradicts an event payload it holds. The store holds the honest
    /// chain g (generation 1) then e1 (generation 2); the incoming state
    /// heads at e1 but annotates it with generation 9. Validation at
    /// application rejects the item with the typed lineage error and
    /// nothing is adopted: no resident materializes carrying the lie.
    /// (Payloads are the sole authoritative source; a lie adopted here
    /// would poison every commit stamped from this resident.)
    #[tokio::test]
    async fn durable_node_rejects_wire_head_generation_contradicting_held_events() {
        let entity_id = ankurah_proto::EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e1_id = e1.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // DEFINITIVE store (durable node) holding the honest payloads.
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xEE; 32]));
        getter.commit_event(&genesis).await.expect("genesis commits");
        getter.commit_event(&e1).await.expect("e1 commits");

        // The wire state heads at e1 with a lying annotation (claims 9; the
        // held payload carries 2).
        let lying = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![e1_id.clone()]),
            head_generations: ankurah_proto::GClock::from((9, e1_id.clone())),
        };

        let unverified = UnverifiedEvents::default();
        let result =
            apply_state_feed(&entities, &NoState, &getter, &staging, entity_id, "test".into(), lying, &[], &NoopPersist, &unverified).await;

        match result {
            Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::GenerationMismatch { claimed, expected, .. }))) => {
                assert_eq!((claimed, expected), (9, 2), "the typed rejection carries the wire claim and the held payload's generation");
            }
            other => panic!("GClock pin (v): a durable node must reject a wire annotation contradicting held events, got {other:?}"),
        }
        assert!(
            entities.get(&entity_id).is_none(),
            "GClock pin (v): nothing from the rejected item may be adopted; no resident carries the lie"
        );
    }

    /// The companion structural rule: an annotation that does not cover
    /// exactly the state's head tips is malformed input, rejected at the
    /// ingress boundary on EVERY node flavor (an adopted mismatch could
    /// never stamp a commit). Fresh adoption of a CONSISTENT pair on an
    /// ephemeral node stays untouched (the trust envelope; pinned by the
    /// adoption arm of the M4 integration tests).
    #[tokio::test]
    async fn structurally_mismatched_head_annotation_is_rejected_as_malformed() {
        let entity_id = ankurah_proto::EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // Ephemeral flavor: the structural check is not a trust question.
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xEE; 32]));

        // Head names e1; the annotation names genesis. Malformed.
        let mismatched = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![e1.payload.id()]),
            head_generations: ankurah_proto::GClock::from((1, genesis.payload.id())),
        };

        let unverified = UnverifiedEvents::default();
        let result =
            apply_state_feed(&entities, &NoState, &getter, &staging, entity_id, "test".into(), mismatched, &[], &NoopPersist, &unverified)
                .await;

        match result {
            Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::HeadGenerationsMismatch))) => {}
            other => panic!("a structurally mismatched head annotation must be rejected as malformed, got {other:?}"),
        }
        assert!(entities.get(&entity_id).is_none(), "nothing from the malformed item may be adopted");
    }

    /// The trust-envelope arm (REV 5 section K): an EPHEMERAL node adopts a
    /// consistent wire annotation as carried, no payload inspection (it
    /// holds no payloads to inspect; the state itself is the trust
    /// envelope), and the resident materializes carrying exactly those
    /// values, ready to stamp a commit read-free.
    #[tokio::test]
    async fn ephemeral_node_adopts_carried_annotation_inside_the_trust_envelope() {
        let entity_id = ankurah_proto::EntityId::new();
        let genesis = event(entity_id, &[]);
        let e1 = event(entity_id, &[&genesis]);
        let e1_id = e1.payload.id();

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // Ephemeral store holding NOTHING: bodiless adoption.
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xEE; 32]));

        let carried = ankurah_proto::GClock::from((2, e1_id.clone()));
        let state = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![e1_id.clone()]),
            head_generations: carried.clone(),
        };

        let unverified = UnverifiedEvents::default();
        let applied =
            apply_state_feed(&entities, &NoState, &getter, &staging, entity_id, "test".into(), state, &[], &NoopPersist, &unverified)
                .await
                .expect("a consistent bodiless snapshot adopts cleanly on an ephemeral node");
        assert!(applied.advanced, "fresh adoption advances");
        assert_eq!(
            applied.entity.head_generations(),
            carried,
            "the resident materializes the carried annotation (the stamp operand for its next commit)"
        );
    }
}
