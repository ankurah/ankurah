//! The shared state-apply: one function for every state-bearing feed
//! (StateSnapshot deltas, the StateAndEvent fast path, Get responses),
//! with ONE deliberate exception: join_system's system-root adoption is a
//! wire-state ingress that does not route through here (documented at its
//! site in system.rs: annotation check inlined, the peer's attested bytes
//! persisted verbatim through the funnel-bypassing safe direction).
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

use ankurah_proto::{Attested, Clock, CollectionId, EntityId, Event, EventId, State};

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
    /// Causally ordered events represented by the resulting change: cargo
    /// covered by an adopted state followed by any buffered descendants the
    /// adoption re-drove. State-only feeds usually carry only the latter.
    #[cfg_attr(not(test), allow(dead_code))]
    pub events: Vec<Attested<Event>>,
    /// Notification validated while holding the entity mutation span.
    pub change: Option<crate::changes::EntityChange>,
}

struct PreviewPersist;

#[async_trait::async_trait]
impl PersistState for PreviewPersist {
    async fn persist(&self, _entity: &Entity) -> Result<(), MutationError> { Ok(()) }
}

/// Apply one state (plus any events that traveled with it) to one entity.
#[cfg(test)]
pub(crate) async fn apply_state_feed<S, E>(
    entities: &WeakEntitySet,
    state_getter: &S,
    event_getter: &E,
    staging: &StagingArea,
    entity_id: EntityId,
    model_id: EntityId,
    collection_id: CollectionId,
    state: State,
    events_to_commit: &[Attested<Event>],
    persist: &dyn PersistState,
) -> Result<StateApplied, MutationError>
where
    S: GetState + Send + Sync,
    E: SuspenseEvents + Send + Sync,
{
    let expected_epoch = entities.reset_epoch();
    apply_state_feed_at_epoch(
        entities,
        state_getter,
        event_getter,
        staging,
        entity_id,
        model_id,
        collection_id,
        state,
        events_to_commit,
        persist,
        expected_epoch,
    )
    .await
}

pub(crate) async fn apply_state_feed_at_epoch<S, E>(
    entities: &WeakEntitySet,
    state_getter: &S,
    event_getter: &E,
    staging: &StagingArea,
    entity_id: EntityId,
    model_id: EntityId,
    collection_id: CollectionId,
    state: State,
    events_to_commit: &[Attested<Event>],
    persist: &dyn PersistState,
    expected_epoch: u64,
) -> Result<StateApplied, MutationError>
where
    S: GetState + Send + Sync,
    E: SuspenseEvents + Send + Sync,
{
    let scoped_getter = crate::retrieval::ScopedEventGetter::new(event_getter, entity_id, model_id);
    // Wire order is untrusted. This order is used both for cargo durability
    // and for the eventual EntityChange containment check.
    let ordered_cargo = crate::event_dag::ordering::topo_sort_events(events_to_commit.to_vec())?;
    // Annotation and cargo validation can fetch missing lineage and cache it.
    // Keep those reads on the admitted side of reset as well; the fenced
    // getter routes any peer lookup through Node::request_fenced.
    {
        let _reset_fence = entities.reset_fence_read().await;
        if entities.reset_epoch() != expected_epoch {
            return Err(MutationError::InvalidUpdate("system reset during state validation"));
        }
        let fenced_getter = crate::retrieval::ScopedEventGetter::new_fenced(event_getter, entity_id, model_id);

        // Validate the snapshot annotation before it can become
        // commit-stamping input on the resident.
        super::verify_state_head_generations(&fenced_getter, &state).await?;

        for event in &ordered_cargo {
            let id = event.payload.id();
            if event.payload.entity_id != entity_id {
                return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::EntityMismatch {
                    event: id,
                    expected: entity_id,
                    received: event.payload.entity_id,
                })
                .into());
            }

            // A snapshot may vouch only for cargo in its own causal history.
            // The comparison is generation-independent; Equal or
            // StrictDescends proves that the snapshot contains this event.
            let comparison = crate::event_dag::comparison::compare(
                &fenced_getter,
                &state.head,
                &Clock::from(vec![id.clone()]),
                crate::event_dag::DEFAULT_BUDGET,
            )
            .await?;
            match comparison.relation {
                crate::event_dag::AbstractCausalRelation::Equal | crate::event_dag::AbstractCausalRelation::StrictDescends { .. } => {}
                crate::event_dag::AbstractCausalRelation::BudgetExceeded { subject, other } => {
                    return Err(crate::error::IngestError::Budget {
                        original_budget: crate::event_dag::DEFAULT_BUDGET,
                        subject_frontier: subject,
                        other_frontier: other,
                    }
                    .into());
                }
                _ => {
                    return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::EventOutsideState { event: id }).into());
                }
            }
        }
    }

    // The complete state-feed span is on one side of reset and serialized
    // against every other mutation/persist lane for this entity.
    let (entity, advanced) = {
        let _reset_fence = entities.reset_fence_read().await;
        if entities.reset_epoch() != expected_epoch {
            return Err(MutationError::InvalidUpdate("system reset during state ingest"));
        }
        let fenced_getter = crate::retrieval::ScopedEventGetter::new_fenced(event_getter, entity_id, model_id);
        let mutation_span = entities.mutation_span(entity_id);
        let _mutation_guard = mutation_span.lock().await;

        let current = entities
            .get_or_retrieve_fenced(state_getter, &fenced_getter, &collection_id, &entity_id)
            .await
            .map_err(|e| super::type_comparison_error(e.into()))?;

        // Determine whether adoption will advance without mutating first. The
        // mutation span keeps this verdict stable until cargo is durable and
        // the state is applied.
        let should_advance = if let Some(current) = &current {
            let result =
                crate::event_dag::comparison::compare(&fenced_getter, &state.head, &current.head(), crate::event_dag::DEFAULT_BUDGET)
                    .await?;
            match result.relation {
                crate::event_dag::AbstractCausalRelation::StrictDescends { .. } => true,
                crate::event_dag::AbstractCausalRelation::Equal
                | crate::event_dag::AbstractCausalRelation::StrictAscends
                | crate::event_dag::AbstractCausalRelation::DivergedSince { .. } => false,
                crate::event_dag::AbstractCausalRelation::Disjoint { .. } => {
                    return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::Disjoint).into());
                }
                crate::event_dag::AbstractCausalRelation::BudgetExceeded { subject, other } => {
                    return Err(crate::error::IngestError::Budget {
                        original_budget: crate::event_dag::DEFAULT_BUDGET,
                        subject_frontier: subject,
                        other_frontier: other,
                    }
                    .into());
                }
            }
        } else {
            true
        };

        let mut unverifiable_cargo: Vec<EventId> = Vec::new();
        if should_advance {
            let resident_materialization = current.as_ref().map(Entity::head_generations);
            for event in &ordered_cargo {
                match super::check_generation(&fenced_getter, resident_materialization.as_ref(), &event.payload).await {
                    Ok(super::GenerationCheck::Verified) => {}
                    Ok(super::GenerationCheck::Unverifiable) => unverifiable_cargo.push(event.payload.id()),
                    Err(e) => return Err(e),
                }
            }

            // Covered cargo becomes durable before the resident can adopt a
            // state that references it. A failure leaves the resident
            // untouched; the caller may safely unstage and rely on retry.
            for event in &ordered_cargo {
                fenced_getter.commit_event(event).await?;
                if unverifiable_cargo.contains(&event.payload.id()) {
                    entities.unverified().insert(event.payload.id());
                }
            }
        }

        let (entity, advanced) = if should_advance {
            let (changed, entity) = entities
                .with_state_fenced(state_getter, &fenced_getter, entity_id, collection_id.clone(), state.clone())
                .await
                .map_err(|e| super::type_comparison_error(e.into()))?;
            (entity, !matches!(changed, Some(false)))
        } else {
            (current.expect("a non-advancing classification requires a resident"), false)
        };

        let covered_head = entity.head();
        if !covered_head.is_empty() {
            persist.persist_fenced(&entity).await?;
            entity.mark_applied(covered_head.iter().cloned());
        }
        (entity, advanced)
    };

    // A state adoption can satisfy buffered descendants. Execute that
    // re-drive on a detached preview first: commits happen only after each
    // preview mutation succeeds, while a commit failure cannot advance the
    // canonical resident to an uncommitted ghost head. Once every previewed
    // event is durable, adopt the preview state onto the canonical entity and
    // persist it under the normal fence and mutation span.
    let mut change_events = if advanced { ordered_cargo } else { Vec::new() };
    if advanced {
        match super::plan_entity_for(entity.id(), &entity.head(), &[], staging, &scoped_getter).await {
            Ok(plan) if !plan.schedule.is_empty() => {
                use std::sync::{atomic::AtomicBool, Arc};
                let preview = entity.snapshot(Arc::new(AtomicBool::new(true)));
                let outcome =
                    super::execute_plan_at_epoch(plan, &preview, entities, staging, &scoped_getter, &PreviewPersist, expected_epoch).await;
                if let Some(failure) = outcome.failure {
                    tracing::warn!(entity_id = %entity.id(), "buffered-event preview after state adoption failed: {failure}");
                } else if !outcome.applied.is_empty() {
                    let preview_state = preview.to_state()?;
                    let _reset_fence = entities.reset_fence_read().await;
                    if entities.reset_epoch() != expected_epoch {
                        return Err(MutationError::InvalidUpdate("system reset during state re-drive"));
                    }
                    let mutation_span = entities.mutation_span(entity.id());
                    let _mutation_guard = mutation_span.lock().await;
                    let fenced_getter = crate::retrieval::ScopedEventGetter::new_fenced(event_getter, entity_id, model_id);
                    if matches!(
                        entity.apply_state(&fenced_getter, &preview_state, Some(entities.unverified())).await?,
                        crate::entity::StateApplyResult::Applied
                    ) {
                        persist.persist_fenced(&entity).await?;
                        entity.mark_applied(entity.head().iter().cloned());
                        change_events.extend(outcome.applied);
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(entity_id = %entity.id(), "re-drive planning after state adoption failed: {e}");
            }
        }
    }

    let change = if advanced {
        let _reset_fence = entities.reset_fence_read().await;
        if entities.reset_epoch() != expected_epoch {
            return Err(MutationError::InvalidUpdate("system reset before state notification"));
        }
        let mutation_span = entities.mutation_span(entity.id());
        let _mutation_guard = mutation_span.lock().await;
        Some(
            crate::changes::EntityChange::new(entity.clone(), change_events.clone())
                .or_else(|_| crate::changes::EntityChange::new(entity.clone(), Vec::new()))?,
        )
    } else {
        None
    };

    Ok(StateApplied { entity, advanced, events: change_events, change })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{IngestError, LineageRejection};
    use crate::ingest::testkit::{event, event_with_title, FailingCommitStore, NoState, NoopPersist};
    use crate::ingest::StagingArea;
    use crate::retrieval::GetEvents;
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

        let unverified = entities.unverified();
        let result = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            lying,
            &[],
            &NoopPersist,
        )
        .await;

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

        let unverified = entities.unverified();
        let result = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            mismatched,
            &[],
            &NoopPersist,
        )
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

        let applied = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            &[],
            &NoopPersist,
        )
        .await
        .expect("a consistent bodiless snapshot adopts cleanly on an ephemeral node");
        assert!(applied.advanced, "fresh adoption advances");
        assert_eq!(
            applied.entity.head_generations(),
            carried,
            "the resident materializes the carried annotation (the stamp operand for its next commit)"
        );
    }

    /// The materialization consult on a PRE-EXISTING resident (M4
    /// remediation item 7's sibling, test-adequacy panel MINOR 4): every
    /// earlier test in this module runs against a fresh WeakEntitySet, so
    /// resident_materialization was None in all of them and the consult
    /// could be deleted unnoticed. This pin adopts head {h2} bodiless
    /// first, then feeds the standard update shape (state heading {h3},
    /// cargo h3 parented on the current head {h2}): the cargo must verify
    /// READ-FREE from the resident's materialization (the K claim for the
    /// highest-traffic lane), stay OUT of the unverified set (M5
    /// eligibility), and a mis-stamped twin must be rejected typed with
    /// the expected value read from the materialization (h2 is BODILESS
    /// here, so no payload fallback could ever supply it).
    #[tokio::test]
    async fn update_shaped_cargo_on_a_pre_existing_resident_verifies_from_the_materialization() {
        let entity_id = ankurah_proto::EntityId::new();
        let h2_id = EventId::from_bytes([2; 32]);

        let entities = crate::entity::WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        // Ephemeral store holding NOTHING: the adopted parent stays bodiless.
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xEE; 32]));
        let unverified = entities.unverified();

        // Pre-existing resident: bodiless adoption of head {h2} at
        // generation 2 (the trust envelope). The strong handle keeps it
        // resident across the feeds below.
        let adopted = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![h2_id.clone()]),
            head_generations: ankurah_proto::GClock::from((2, h2_id.clone())),
        };
        let resident = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            adopted,
            &[],
            &NoopPersist,
        )
        .await
        .expect("bodiless adoption succeeds")
        .entity;
        assert!(entities.get(&entity_id).is_some(), "precondition: the resident pre-exists the update feed");

        // The mis-stamped twin FIRST, while its parent h2 is the resident's
        // materialized head: cargo parented {h2} claiming 4 where the
        // materialization says the one correct stamp is 3. The expected
        // value can only come from the MATERIALIZATION (h2 is bodiless on
        // this node), so the typed rejection binds the consult itself.
        let lying = ankurah_proto::Event {
            entity_id,
            model: EntityId::from_bytes([0xEE; 16]),
            operations: ankurah_proto::OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(vec![h2_id.clone()]),
            generation: 4,
        };
        let lying_id = lying.id();
        let lying = Attested::opt(lying, None);
        staging.stage(lying.clone());
        let lying_state = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![lying_id.clone()]),
            head_generations: ankurah_proto::GClock::from((4, lying_id.clone())),
        };
        let result = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            lying_state,
            std::slice::from_ref(&lying),
            &NoopPersist,
        )
        .await;
        match result {
            Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::GenerationMismatch { claimed, expected, .. }))) => {
                assert_eq!((claimed, expected), (4, 3), "the expected value is read from the resident's materialization");
            }
            other => panic!("mis-stamped cargo over a materialized parent must be rejected typed, got {other:?}"),
        }
        staging.remove(&lying_id);
        assert!(resident.head().contains(&h2_id), "the rejected item adopted nothing; the resident still heads {{h2}}");

        // The honest update shape: cargo h3 parented on the current head,
        // state heading {h3} annotated with h3's stamp.
        let h3 = ankurah_proto::Event {
            entity_id,
            model: EntityId::from_bytes([0xEE; 16]),
            operations: ankurah_proto::OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(vec![h2_id.clone()]),
            generation: 3,
        };
        let h3_id = h3.id();
        let h3 = Attested::opt(h3, None);
        staging.stage(h3.clone());
        let update = ankurah_proto::State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: ankurah_proto::Clock::from(vec![h3_id.clone()]),
            head_generations: ankurah_proto::GClock::from((3, h3_id.clone())),
        };

        let reads_before = getter.get_local_event_calls();
        let applied = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            update,
            std::slice::from_ref(&h3),
            &NoopPersist,
        )
        .await
        .expect("the update-shaped item applies");
        assert!(applied.advanced, "strict descent advances");
        assert_eq!(
            getter.get_local_event_calls() - reads_before,
            0,
            "update-shaped cargo parented on the current head verifies READ-FREE from the resident's materialization"
        );
        assert!(!unverified.contains(&h3_id), "verified cargo stays acceleration-eligible (not recorded unverified)");
        assert_eq!(resident.head_generations().generation_of(&h3_id), Some(3), "the resident heads {{h3}} at its own stamp");
    }

    #[tokio::test]
    async fn cargo_commit_failure_leaves_the_resident_unadvanced_and_body_staged() {
        let entity_id = EntityId::new();
        let cargo = event_with_title(entity_id, "state", &[]);
        let cargo_id = cargo.payload.id();
        let state = State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![cargo_id.clone()]),
            head_generations: ankurah_proto::GClock::from((1, cargo_id.clone())),
        };
        let entities = WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        staging.stage(cargo.clone());
        let getter = FailingCommitStore::new(staging.clone(), cargo_id.clone());

        let result = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            std::slice::from_ref(&cargo),
            &NoopPersist,
        )
        .await;

        assert!(result.is_err(), "the injected cargo commit failure surfaces");
        assert!(entities.get(&entity_id).is_none(), "state is not adopted before cargo durability");
        assert!(staging.contains(&cargo_id), "the failed body remains available for retry");
    }

    #[tokio::test]
    async fn unrelated_cargo_is_rejected_before_adoption_or_commit() {
        let entity_id = EntityId::new();
        let state_tip = event_with_title(entity_id, "state-line", &[]);
        let cargo = event_with_title(entity_id, "foreign-line", &[]);
        let state_tip_id = state_tip.payload.id();
        let cargo_id = cargo.payload.id();
        let state = State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![state_tip_id.clone()]),
            head_generations: ankurah_proto::GClock::from((1, state_tip_id)),
        };
        let entities = WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        staging.stage(state_tip);
        staging.stage(cargo.clone());
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xFF; 32]));

        let result = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            std::slice::from_ref(&cargo),
            &NoopPersist,
        )
        .await;

        assert!(matches!(
            result,
            Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::EventOutsideState { event }))) if event == cargo_id
        ));
        assert!(entities.get(&entity_id).is_none());
        assert!(!getter.event_stored(&cargo_id).await.unwrap(), "unrelated cargo never reaches the log");
    }

    #[tokio::test]
    async fn child_first_state_cargo_is_committed_and_reported_parents_first() {
        let entity_id = EntityId::new();
        let genesis = event_with_title(entity_id, "g", &[]);
        let parent = event_with_title(entity_id, "p", &[&genesis]);
        let child = event_with_title(entity_id, "c", &[&parent]);
        let child_id = child.payload.id();
        let state = State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![child_id.clone()]),
            head_generations: ankurah_proto::GClock::from((3, child_id)),
        };
        let entities = WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        staging.try_stage_batch([child.clone(), parent.clone(), genesis.clone()]).unwrap();
        let getter = FailingCommitStore::new(staging.clone(), EventId::from_bytes([0xFF; 32]));

        let applied = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            &[child.clone(), parent.clone(), genesis.clone()],
            &NoopPersist,
        )
        .await
        .expect("covered child-first cargo applies");

        let ids: Vec<_> = applied.events.iter().map(|event| event.payload.id()).collect();
        assert_eq!(ids, vec![genesis.payload.id(), parent.payload.id(), child.payload.id()]);
        crate::changes::EntityChange::new(applied.entity, applied.events).expect("ordered notification satisfies containment");
    }

    #[tokio::test]
    async fn failed_re_drive_commit_does_not_expose_a_ghost_head() {
        let entity_id = EntityId::new();
        let parent = event_with_title(entity_id, "parent", &[]);
        let child = event_with_title(entity_id, "child", &[&parent]);
        let parent_id = parent.payload.id();
        let child_id = child.payload.id();
        let state = State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![parent_id.clone()]),
            head_generations: ankurah_proto::GClock::from((1, parent_id.clone())),
        };
        let entities = WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        staging.try_stage_batch([parent.clone(), child.clone()]).unwrap();
        let getter = FailingCommitStore::new(staging.clone(), child_id.clone());

        let applied = apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            std::slice::from_ref(&parent),
            &NoopPersist,
        )
        .await
        .expect("the adopted parent remains a successful state feed");

        assert_eq!(applied.entity.head(), Clock::from(vec![parent_id.clone()]));
        assert!(getter.event_stored(&parent_id).await.unwrap());
        assert!(!getter.event_stored(&child_id).await.unwrap());
        assert!(staging.contains(&child_id), "the failed child body stays available for its retry lease");
        assert_eq!(applied.events.iter().map(|event| event.payload.id()).collect::<Vec<_>>(), vec![parent_id]);
        assert!(applied.change.is_some(), "the committed parent notification remains valid");
    }

    #[tokio::test]
    async fn adopted_staged_head_is_backfilled_even_with_an_empty_batch() {
        let entity_id = EntityId::new();
        let head = event_with_title(entity_id, "adopted", &[]);
        let head_id = head.payload.id();
        let state = State {
            state_buffers: ankurah_proto::StateBuffers(BTreeMap::new()),
            head: Clock::from(vec![head_id.clone()]),
            head_generations: ankurah_proto::GClock::from((1, head_id.clone())),
        };
        let entities = WeakEntitySet::default();
        let staging = std::sync::Arc::new(StagingArea::with_default_cap());
        staging.stage(head);
        let getter = FailingCommitStore::ephemeral(staging.clone(), EventId::from_bytes([0xFF; 32]));

        apply_state_feed(
            &entities,
            &NoState,
            &getter,
            &staging,
            entity_id,
            EntityId::from_bytes([0xEE; 16]),
            "test".into(),
            state,
            &[],
            &NoopPersist,
        )
        .await
        .expect("snapshot adopts and re-drives backfill");

        assert!(getter.event_stored(&head_id).await.unwrap(), "the staged adopted head is backfilled to the log");
        assert!(!staging.contains(&head_id));
    }
}
