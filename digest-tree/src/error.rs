//! What a tree's operations report when they cannot read or write.

use std::error::Error;

/// Why an operation on a tree could not read or write.
#[derive(Debug, thiserror::Error)]
pub enum TreeError {
    /// A conditional write found another value under its key than the writer
    /// expected, when the write was made or when its transaction committed:
    /// another transaction wrote there since the writer read. The whole
    /// transaction fails and nothing it wrote lands; the writer retries from
    /// fresh reads in a new transaction.
    #[error("a conditional write found another value than expected")]
    Conflict,
    /// A stored key or value is not in the layout this build reads, or
    /// carries bytes that are no element: the rows were written by another
    /// version or in another group, or damaged. A tree is derived from its
    /// leaves, so it is rebuilt from them.
    #[error("a stored {what} does not decode")]
    Unreadable {
        /// Which key or value, as the [`layout`](crate::layout) names them:
        /// "leaf value", "node key" and so on.
        what: &'static str,
        /// What was wrong with the bytes, when a decoder said.
        #[source]
        source: Option<Box<dyn Error + Send + Sync + 'static>>,
    },
    /// Summing a tree node's base row and its deltas took the count past the
    /// range of `i64`, which no set of leaves does: the rows are damaged.
    #[error("a tree node's count overflows")]
    CountOverflow(#[from] crate::digest::CountOverflow),
}

impl TreeError {
    /// A stored `what` that `source` refused.
    pub(crate) fn unreadable(what: &'static str, source: impl Error + Send + Sync + 'static) -> Self {
        Self::Unreadable { what, source: Some(Box::new(source)) }
    }
}
