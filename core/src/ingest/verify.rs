//! Admission verification of payload generations (plan REV 4, D2-3).
//!
//! Every lane that applies an event with LOCALLY resolvable parents checks
//! the stamp equation `generation == 1 + max(parent generations)` at its
//! admission boundary (the executor loop for the PerItem arms, commit-lane
//! phase one for the transaction lanes); genesis events (empty parent
//! clock) must claim exactly 1 and are always verifiable. A mismatch is a
//! typed lineage rejection, contained like any other malformed event: the
//! stamp is deterministic given the parents, so rejection only ever fires
//! on a buggy or malicious writer (derivations section 5b).
//!
//! Resolution is LOCAL ONLY (staging area, then local storage, via
//! [`SuspenseEvents::get_local_event`]): verification never fetches from a
//! peer. Parents not in hand make the admission UNVERIFIABLE, not invalid;
//! the caller admits the event and records its id in the unverified set
//! (adopted-history admission; eligibility is the M5 consumer). The check
//! is all-or-nothing over the parent set: partial knowledge never rejects
//! (D2-3 verifies "where parents are local", and walk-time edge checks own
//! the opportunistic per-edge detection later).
//!
//! Verification runs ONCE per admission: fork previews and canonical
//! re-applies of an already-admitted event do not re-verify, and members of
//! the integrated-but-unstored backfill lane (an event the resident's head
//! already carries whose log row is missing) are never checked at all;
//! rejecting one could never un-apply it and would wedge that entity's log
//! repair forever (retroactive rejection of committed history is D3's
//! jurisdiction).

use ankurah_proto::Event;

use crate::error::{IngestError, LineageRejection, MutationError};
use crate::retrieval::SuspenseEvents;

/// Outcome of a non-rejecting admission check.
pub(crate) enum GenerationCheck {
    /// Every parent resolved locally and the equation holds (or the event is
    /// a genesis claiming exactly 1).
    Verified,
    /// At least one parent is not locally in hand; the equation cannot be
    /// checked. The caller admits the event and records it unverified.
    Unverifiable,
}

/// Check one event's claimed generation against its locally resolvable
/// parents. `Err` carries the typed rejection (mismatch) or a storage
/// failure from the local read; `Ok(Unverifiable)` is not an error.
pub(crate) async fn check_generation<G: SuspenseEvents + Send + Sync>(getter: &G, event: &Event) -> Result<GenerationCheck, MutationError> {
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
