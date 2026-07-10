//! Admission verification of payload generations (plan REV 4, D2-3; plan
//! REV 5 section K's inductive verification rule).
//!
//! Every lane that applies an event with resolvable parents checks the
//! stamp equation `generation == 1 + max(parent generations)` at its
//! admission boundary (the executor loop for the PerItem arms, commit-lane
//! phase one for the transaction lanes); genesis events (empty parent
//! clock) must claim exactly 1 and are always verifiable. A mismatch is a
//! typed lineage rejection, contained like any other malformed event: the
//! stamp is deterministic given the parents, so rejection only ever fires
//! on a buggy or malicious writer (derivations section 5b).
//!
//! PARENT RESOLUTION ORDER (REV 5 section K): a parent covered by the
//! resident's MATERIALIZED head generations resolves from the
//! materialization with no read at all. Every materialized entry originates
//! from an admission-verified stamp pinned when its tip joined the head, so
//! a covered check extends the induction; head-parented arrivals, in-order
//! bridge steps (each apply advances the materialization the next step
//! reads), and subset-parented arrivals on multi-tip heads all verify
//! read-free. Parents NOT covered by the tips fall back to LOCAL payload
//! reads exactly as M2 built (staging area, then local storage, via
//! [`SuspenseEvents::get_local_event`]): verification never fetches from a
//! peer, and events are read wherever they are the only sound source (a
//! self-consistent forger satisfies any equation over values it wrote, so
//! uncovered cases must check real payloads). Parents in neither place make
//! the admission UNVERIFIABLE, not invalid; the caller admits the event and
//! records its id in the unverified set (adopted-history admission;
//! eligibility is the M5 consumer). The check is all-or-nothing over the
//! parent set: partial knowledge never rejects (D2-3 verifies "where
//! parents are resolvable", and walk-time edge checks own the opportunistic
//! per-edge detection later).
//!
//! Verification runs ONCE per admission: fork previews and canonical
//! re-applies of an already-admitted event do not re-verify, and members of
//! the integrated-but-unstored backfill lane (an event the resident's head
//! already carries whose log row is missing) are never checked at all;
//! rejecting one could never un-apply it and would wedge that entity's log
//! repair forever (retroactive rejection of committed history is D3's
//! jurisdiction).

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

/// Validate an incoming state's head-generation annotation at the ingress
/// boundary, BEFORE adoption (plan REV 5 section K, the validation
/// invariant).
///
/// Two layers:
/// 1. STRUCTURAL, every node flavor: the annotation must cover exactly the
///    head's tips ([`GClock::matches_head`]). A mismatched pair is
///    malformed input (an adopted mismatch could never stamp a commit),
///    rejected with the typed lineage error like any other malformed
///    input.
/// 2. PAYLOAD CONTRADICTION, durable nodes only (`storage_is_definitive`):
///    a durable node never adopts a wire-carried generation uninspected.
///    Each entry whose tip event is locally held (staging, then local
///    storage) must match that payload's generation; a contradiction
///    aborts the item typed, nothing adopted. Entries for tips NOT locally
///    held keep the carried value (nothing provable; the same posture as
///    unverifiable cargo, and a wrong value self-defeats at the next
///    admission that can check it). Ephemeral nodes skip this layer: they
///    adopt inside the same trust envelope as the state itself, and an
///    inherited lie self-defeats when their next commit relays to a
///    durable peer holding the real payloads.
///
/// Engine rehydration does NOT pass through here: the persisted entity
/// record is the node's own prior materialization (that is what makes
/// restart read-free); this boundary exists for WIRE-carried states.
pub(crate) async fn verify_state_head_generations<G: SuspenseEvents + Send + Sync>(getter: &G, state: &State) -> Result<(), MutationError> {
    if !state.head_generations.matches_head(&state.head) {
        return Err(IngestError::Lineage(LineageRejection::HeadGenerationsMismatch).into());
    }
    if getter.storage_is_definitive() {
        for (claimed, id) in state.head_generations.iter() {
            if let Some(local) = getter.get_local_event(id).await? {
                if local.generation != *claimed {
                    return Err(IngestError::Lineage(LineageRejection::GenerationMismatch {
                        event: id.clone(),
                        claimed: *claimed,
                        expected: local.generation,
                    })
                    .into());
                }
            }
        }
    }
    Ok(())
}
