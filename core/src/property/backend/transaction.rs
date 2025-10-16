use ankurah_proto::EventId;

use crate::causal_dag::forward_view::ReadySet;
use crate::error::MutationError;
use crate::retrieval::TEvent;

/// Ephemeral per-backend session for applying a ForwardView with atomic commit/rollback.
///
/// Each backend implements its own deterministic resolution strategy for concurrent updates.
/// Transactions accumulate diffs from the current state and apply them atomically on commit.
pub trait PropertyTransaction<E: TEvent<Id = EventId>>: Send {
    /// Apply a single ReadySet to this transaction.
    ///
    /// The transaction should:
    /// - Examine Primary events (already applied to entity) for comparison
    /// - Apply Concurrency events using backend-specific resolution
    /// - Accumulate diffs from the current state
    ///
    /// For LWW: use lineage + lexicographic tiebreak; Primary is comparison-only
    /// For Yrs: apply all ops; duplicates are idempotent
    fn apply_ready_set(&mut self, ready_set: &ReadySet<E>) -> Result<(), MutationError>;

    /// Commit accumulated changes to the backend.
    ///
    /// After commit, the backend state reflects all applied ReadySets.
    /// Returns Ok(()) on success.
    fn commit(&mut self) -> Result<(), MutationError>;

    /// Rollback accumulated changes, discarding all pending state.
    ///
    /// After rollback, the backend state is unchanged from when `begin()` was called.
    fn rollback(&mut self) -> Result<(), MutationError>;
}
