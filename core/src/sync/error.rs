//! What goes wrong with a digest tree itself, which of the crate's errors
//! mean a tree refused a change, and how an engine's own errors pass through
//! the crate and come back as themselves.

use std::fmt;

use ankurah_digest_tree::{Error as DigestTreeError, StoreError};

use super::TreeId;
use crate::error::{MutationError, RetrievalError};

/// What goes wrong with a digest tree itself, carried as the source of core's
/// [`MutationError`] or [`RetrievalError`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A creation found a tree under its id already.
    #[error("a tree {0} exists already")]
    Exists(TreeId),

    /// A drop or an opening found no tree under its id.
    #[error("there is no tree {0}")]
    NoSuchTree(TreeId),

    /// An opening found the tree marked invalid after a refused change. It
    /// must be rebuilt before it can be opened again.
    #[error("tree {0} is marked invalid")]
    Invalid(TreeId),

    /// Initial membership has not reached a complete canonical-state boundary.
    #[error("tree {0} is still being built")]
    Building(TreeId),

    /// A read found the tree dropped, created again or marked invalid since
    /// its handle was opened.
    #[error("tree {0} was dropped, created again or marked invalid since it was opened")]
    Retired(TreeId),

    /// One apply was given two changes for one tree: a storage transaction
    /// changes a tree at most once.
    #[error("tree {0} is changed twice in one apply")]
    ChangedTwice(TreeId),

    /// Maintenance received a tree from another engine instance.
    #[error("tree {0} belongs to a different storage engine")]
    ForeignTree(TreeId),

    /// A storage engine's answers to map reads don't fit the reads, as the
    /// storage contract says they do: another number of answers than reads,
    /// or an answer of another kind.
    #[error("a storage engine answered map reads with answers of another number or kind")]
    AnswerMisfit,

    /// Bytes the tree keeps in the engine's maps don't decode.
    #[error("tree {tree}'s {part} doesn't decode")]
    Damaged {
        /// The tree.
        tree: TreeId,

        /// What didn't decode.
        part: TreePart,

        /// Why, where the crate's decoding said.
        #[source]
        cause: Option<StoreError>,
    },
}

/// A part of what a tree keeps in the engine's maps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreePart {
    /// The tree's entry in the trees map.
    Entry,

    /// One of the tree's rows.
    Row,

    /// The key of one of the tree's rows.
    RowKey,
}

impl fmt::Display for TreePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Entry => "entry",
            Self::Row => "row",
            Self::RowKey => "row key",
        })
    }
}

impl Error {
    /// A tree's bytes that don't decode, as a store's failure the crate
    /// passes on and the storage adapter recognises as the tree's refusal.
    pub(super) fn damaged(tree: &TreeId, part: TreePart, cause: Option<StoreError>) -> StoreError {
        StoreError::permanent(Self::Damaged { tree: tree.clone(), part, cause })
    }
}

impl From<Error> for MutationError {
    fn from(error: Error) -> Self { MutationError::General(Box::new(error)) }
}

impl From<Error> for RetrievalError {
    fn from(error: Error) -> Self { RetrievalError::storage(error) }
}

/// The crate's error from applying a tree's changes, as the tree's refusal,
/// or as the failure of the whole call.
///
/// A tree refuses a change when the crate reports any error but its store's
/// failure, such as a member the tree doesn't hold in the version a change
/// names, or when the tree's own bytes don't decode. The storage adapter marks it
/// invalid because the requested transition could not be applied. Incorrect
/// or replayed input can also cause a refusal; refusal alone does not prove
/// the stored tree is corrupt. Any other store failure fails the whole call,
/// as [`change_failure`] makes it.
pub(super) fn refusal(error: DigestTreeError) -> Result<DigestTreeError, MutationError> {
    match &error {
        DigestTreeError::Store(failure) if !is_damage(failure) => Err(change_failure(error)),
        _ => Ok(error),
    }
}

/// Whether a store's failure is the storage adapter's own report of bytes that
/// don't decode.
fn is_damage(failure: &StoreError) -> bool {
    std::error::Error::source(failure)
        .and_then(|source| source.downcast_ref::<Error>())
        .is_some_and(|error| matches!(error, Error::Damaged { .. }))
}

/// A storage transaction's failure of a map call a tree made through it, as
/// the crate's store failure, with the engine's error as its cause, for
/// [`change_failure`] to take back: retryable where core retries it, at a
/// write conflict.
pub(super) fn transaction_store_failure(error: MutationError) -> StoreError {
    match matches!(error, MutationError::WriteConflict) {
        true => StoreError::retryable(error),
        false => StoreError::permanent(error),
    }
}

/// A snapshot's failure to begin, or to answer a read a tree made in it, as
/// the crate's store failure, with the engine's error as its cause:
/// retryable where the engine found the state it read moved, so that the
/// reader begins another snapshot. [`open_failure`] takes the engine's error
/// back where a tree is opened; a tree's other reads give their caller the
/// crate's error as it is.
pub(super) fn snapshot_store_failure(error: RetrievalError) -> StoreError {
    match &error {
        RetrievalError::MutationError(cause) if matches!(**cause, MutationError::WriteConflict) => StoreError::retryable(error),
        _ => StoreError::permanent(error),
    }
}

/// The crate's error from a change through the storage adapter, as core's: the engine's
/// own error where the crate's store failed with it, a write conflict core
/// retries among them, and otherwise the crate's error.
pub(super) fn change_failure(error: DigestTreeError) -> MutationError {
    engine_error(error).unwrap_or_else(|error| MutationError::General(Box::new(error)))
}

/// The crate's failure to open a tree, as core's: the engine's own error
/// where the crate's store failed with it, so that a retryable one stays
/// retryable, and otherwise the crate's error.
pub(super) fn open_failure(error: DigestTreeError) -> RetrievalError { engine_error(error).unwrap_or_else(RetrievalError::storage) }

/// The engine's error the crate's store failed with, where `error` is that
/// failure and its cause an `E`; otherwise `error`, as it was.
fn engine_error<E: std::error::Error + 'static>(error: DigestTreeError) -> Result<E, DigestTreeError> {
    match error {
        DigestTreeError::Store(failure) => failure.downcast().map_err(DigestTreeError::Store),
        error => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A storage transaction's error, given to the crate as its store's
    /// failure, comes back from the crate's error as itself: a write conflict
    /// as one, retryable, and any other as it was.
    #[test]
    fn a_transactions_error_comes_back_from_the_crates_as_itself() {
        let conflict = transaction_store_failure(MutationError::WriteConflict);
        assert!(conflict.is_retryable());
        assert!(matches!(change_failure(DigestTreeError::Store(conflict)), MutationError::WriteConflict));

        let failed = transaction_store_failure(MutationError::General("the engine failed".into()));
        assert!(!failed.is_retryable());
        let taken_back = change_failure(DigestTreeError::Store(failed));
        assert!(matches!(&taken_back, MutationError::General(error) if error.to_string() == "the engine failed"), "{taken_back:?}");
    }

    /// A snapshot's error comes back the same way: one that found its state
    /// moved as itself, retryable, and any other as it was.
    #[test]
    fn a_snapshots_error_comes_back_from_the_crates_as_itself() {
        let moved = snapshot_store_failure(MutationError::WriteConflict.into());
        assert!(moved.is_retryable());
        let taken_back = open_failure(DigestTreeError::Store(moved));
        assert!(
            matches!(&taken_back, RetrievalError::MutationError(cause) if matches!(**cause, MutationError::WriteConflict)),
            "{taken_back:?}"
        );

        let failed = snapshot_store_failure(RetrievalError::Other("the engine failed".to_owned()));
        assert!(!failed.is_retryable());
        let taken_back = open_failure(DigestTreeError::Store(failed));
        assert!(matches!(&taken_back, RetrievalError::Other(failure) if failure == "the engine failed"), "{taken_back:?}");
    }

    /// A store failure that isn't an engine's, and the crate's other errors,
    /// come back as the crate's error.
    #[test]
    fn the_crates_own_errors_come_back_as_the_crates() {
        let failed = change_failure(DigestTreeError::Store(StoreError::permanent("a store answered wrongly")));
        assert!(matches!(&failed, MutationError::General(error) if error.to_string().contains("a store answered wrongly")), "{failed:?}");

        let failed = open_failure(DigestTreeError::Inconsistent("the tree has no root node"));
        assert!(
            matches!(&failed, RetrievalError::StorageError(error) if error.to_string().contains("the tree has no root node")),
            "{failed:?}"
        );
    }
}
