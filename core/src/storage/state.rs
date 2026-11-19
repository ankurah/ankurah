//! Consistent canonical-state reads and storage-local continuation.

use std::num::NonZeroUsize;

use ankql::ast::{Resolved, Selection};
use ankurah_proto::{Attested, EntityId, EntityState};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RetrievalError;

/// An engine's opaque description of a canonical-state read boundary.
///
/// This token is local to one storage incarnation. It is neither a causal
/// clock nor a peer's known state. Its bytes may encode outstanding writes as
/// well as a boundary; comparing tokens or extracting a maximum is invalid.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateCursor(Vec<u8>);

impl StateCursor {
    /// Construct an engine-defined token. Engines must validate tokens on use,
    /// including those recovered from persistent storage.
    pub fn new(bytes: Vec<u8>) -> Self { Self(bytes) }

    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

/// Canonical entity states from one committed read boundary.
///
/// Sequential pages return each changed entity at most once, at its latest
/// version visible to this read. They coalesce intermediate versions. With no
/// starting cursor, they return every entity. Page order is engine-defined.
/// Point reads and limited selections use the same boundary, irrespective of
/// the starting cursor or how many pages have been consumed.
///
/// Engines may retain a native snapshot or validate that their read boundary
/// has not changed. In the latter case any operation, including `finish`, can
/// fail with a write conflict. The caller must discard the entire attempt;
/// partially consumed pages cannot establish recovery coverage.
#[async_trait]
pub trait StateSnapshot: Send {
    /// Read at most `limit` changed states. An empty page ends the scan.
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Read one result per identity, in input order, at this snapshot's boundary.
    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError>;

    /// Read the complete selected set, including unchanged replacement members
    /// needed when a limited selection loses a member. Requires a LIMIT clause.
    /// ORDER BY ties, and selections without ORDER BY, use ascending entity ID.
    async fn limited_states(&mut self, selection: &Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Validate and close the read, returning its continuation token.
    ///
    /// The token identifies the read boundary, not which rows the caller used.
    /// Before publishing it alongside derived data, the caller must reconcile
    /// every changed entity, or replace the entire selected set. A failed or
    /// cancelled finish establishes no coverage.
    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError>;
}
