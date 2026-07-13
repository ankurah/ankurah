//! Admission verification for event and state generation metadata.
//!
//! An event with resolvable parents must satisfy
//! `generation == 1 + max(parent generations)`; a genesis must claim 1.
//! Mismatches are typed lineage rejections. Parent generations resolve first
//! from the resident's verified head annotation and then from staged or stored
//! local payloads. Verification never fetches from a peer.
//!
//! If any parent is unavailable from both sources, the event is admitted as
//! unverifiable and recorded in the node's unverified set. Partial knowledge
//! never rejects the event. Verification runs once at admission; preview
//! applies, canonical re-applies, and integrated-but-unstored backfill do not
//! repeat or retroactively impose it.

use ankurah_proto::{Event, GClock, State};

use crate::error::{IngestError, LineageRejection, MutationError};
use crate::retrieval::SuspenseEvents;

/// Outcome of a non-rejecting admission check.
pub(crate) enum GenerationCheck {
    /// Every parent resolved (materialization or local payload) and the
    /// equation holds (or the event is a genesis claiming exactly 1).
    Verified,
    /// At least one parent is neither materialized nor locally in hand; the
    /// equation cannot be checked. The caller admits the event and records
    /// it unverified.
    Unverifiable,
}

/// Check one event's claimed generation against its resolvable parents:
/// the resident's materialized head generations first (`materialized`,
/// when the caller has a resident; every entry is admission-verified by
/// induction), then local payload reads. `Err` carries the typed rejection
/// (mismatch) or a storage failure from the local read; `Ok(Unverifiable)`
/// is not an error.
pub(crate) async fn check_generation<G: SuspenseEvents + Send + Sync>(
    getter: &G,
    materialized: Option<&GClock>,
    event: &Event,
) -> Result<GenerationCheck, MutationError> {
    if event.parent.is_empty() {
        if event.generation != 1 {
            return Err(IngestError::Lineage(LineageRejection::GenerationMismatch {
                event: event.id(),
                claimed: event.generation,
                expected: 1,
            })
            .into());
        }
        return Ok(GenerationCheck::Verified);
    }

    let mut parent_generations = Vec::with_capacity(event.parent.len());
    for parent_id in event.parent.iter() {
        if let Some(generation) = materialized.and_then(|m| m.generation_of(parent_id)) {
            parent_generations.push(generation);
            continue;
        }
        match getter.get_local_event(parent_id).await? {
            Some(parent) => parent_generations.push(parent.generation),
            None => return Ok(GenerationCheck::Unverifiable),
        }
    }
    let expected = Event::generation_from_parents(parent_generations);
    if event.generation != expected {
        return Err(
            IngestError::Lineage(LineageRejection::GenerationMismatch { event: event.id(), claimed: event.generation, expected }).into()
        );
    }
    Ok(GenerationCheck::Verified)
}

/// Validate an incoming state's head-generation annotation before adoption.
///
/// Two layers:
/// 1. Every node requires the annotation to cover exactly the head tips via
///    [`GClock::matches_head`].
/// 2. A node with definitive storage also requires every tip payload to be
///    locally resolvable and to carry the annotated generation. Missing or
///    contradictory payloads reject the state before mutation.
///
/// Ephemeral nodes accept the annotation within the received state's trust
/// envelope. Engine rehydration does not pass through this wire boundary;
/// persisted records restore the node's own prior materialization directly.
pub(crate) async fn verify_state_head_generations<G: SuspenseEvents + Send + Sync>(getter: &G, state: &State) -> Result<(), MutationError> {
    if !state.head_generations.matches_head(&state.head) {
        return Err(IngestError::Lineage(LineageRejection::HeadGenerationsMismatch).into());
    }
    if getter.storage_is_definitive() {
        for (claimed, id) in state.head_generations.iter() {
            match getter.get_local_event(id).await? {
                Some(local) => {
                    if local.generation != *claimed {
                        return Err(IngestError::Lineage(LineageRejection::GenerationMismatch {
                            event: id.clone(),
                            claimed: *claimed,
                            expected: local.generation,
                        })
                        .into());
                    }
                }
                None => {
                    return Err(IngestError::Lineage(LineageRejection::UnresolvableHeadGenerationTip { event: id.clone() }).into());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{IngestError, LineageRejection, MutationError};
    use crate::ingest::testkit;
    use crate::ingest::StagingArea;
    use ankurah_proto::{Clock, EntityId, EventId, GClock, State, StateBuffers};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn state_with(head: Vec<EventId>, annotation: Vec<(u32, EventId)>) -> State {
        State { state_buffers: StateBuffers(BTreeMap::new()), head: Clock::from(head), head_generations: GClock::new(annotation) }
    }

    /// M4 remediation item 4 (cross-model review, verified by the
    /// coordinator): on a DEFINITIVE-storage node, a carried annotation
    /// entry for a tip whose event is NOT locally resolvable must be
    /// rejected typed, never adopted uninspected. Amendment K's durable
    /// rule says a durable node never adopts a wire-carried generation
    /// uninspected; the silent fall-through adopts exactly such a value,
    /// which then serves as rejection ground truth against honest
    /// descendants (wrongful GenerationMismatch at the covered-parent
    /// check) and as commit-stamp input. Unreachable from production wire
    /// lanes today (durable nodes ingest no wire states at this commit),
    /// which is why this red is unit-level, driving the boundary directly.
    #[tokio::test]
    async fn definitive_storage_rejects_a_carried_generation_for_an_unheld_tip() {
        let staging = Arc::new(StagingArea::default());
        let getter = testkit::FailingCommitStore::new(staging, EventId::from_bytes([0xFF; 32]));

        let unheld_tip = EventId::from_bytes([7; 32]);
        let state = state_with(vec![unheld_tip.clone()], vec![(3, unheld_tip.clone())]);

        let result = verify_state_head_generations(&getter, &state).await;
        assert!(
            matches!(
                &result,
                Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::UnresolvableHeadGenerationTip { event })))
                    if *event == unheld_tip
            ),
            "a definitive-storage node must reject a carried head generation it cannot inspect (unheld tip), got {result:?}"
        );
    }

    /// Control: a locally held tip whose carried value matches its payload
    /// passes on a definitive-storage node (the inspection succeeds).
    #[tokio::test]
    async fn definitive_storage_accepts_a_carried_generation_matching_the_held_payload() {
        let staging = Arc::new(StagingArea::default());
        let getter = testkit::FailingCommitStore::new(staging, EventId::from_bytes([0xFF; 32]));

        let entity_id = EntityId::new();
        let genesis = testkit::event(entity_id, &[]);
        getter.commit_event(&genesis).await.expect("commit succeeds");

        let state = state_with(vec![genesis.payload.id()], vec![(1, genesis.payload.id())]);
        verify_state_head_generations(&getter, &state).await.expect("a held, payload-matching annotation validates");
    }

    /// Control: the payload-contradiction arm still fires for a HELD tip
    /// with a lying carried value (pre-existing behavior, unchanged).
    #[tokio::test]
    async fn definitive_storage_rejects_a_carried_generation_contradicting_the_held_payload() {
        let staging = Arc::new(StagingArea::default());
        let getter = testkit::FailingCommitStore::new(staging, EventId::from_bytes([0xFF; 32]));

        let entity_id = EntityId::new();
        let genesis = testkit::event(entity_id, &[]);
        getter.commit_event(&genesis).await.expect("commit succeeds");

        let state = state_with(vec![genesis.payload.id()], vec![(2, genesis.payload.id())]);
        let result = verify_state_head_generations(&getter, &state).await;
        assert!(
            matches!(
                &result,
                Err(MutationError::Ingest(IngestError::Lineage(LineageRejection::GenerationMismatch { claimed: 2, expected: 1, .. })))
            ),
            "a held tip with a contradicting carried value stays a typed GenerationMismatch, got {result:?}"
        );
    }

    /// Control: an ephemeral (non-definitive) node adopts an unheld tip's
    /// carried value inside the state's trust envelope (REV 5 section K),
    /// unchanged by the durable-side tightening.
    #[tokio::test]
    async fn ephemeral_adopts_a_carried_generation_for_an_unheld_tip() {
        let staging = Arc::new(StagingArea::default());
        let getter = testkit::FailingCommitStore::ephemeral(staging, EventId::from_bytes([0xFF; 32]));

        let unheld_tip = EventId::from_bytes([7; 32]);
        let state = state_with(vec![unheld_tip.clone()], vec![(3, unheld_tip.clone())]);
        verify_state_head_generations(&getter, &state).await.expect("the trust envelope adopts on ephemeral nodes");
    }
}
