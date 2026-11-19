//! Consistent canonical-state reads and storage-local continuation.

use std::num::NonZeroUsize;

use ankql::ast::{Resolved, Selection};
use ankurah_proto::{Attested, EntityId, EntityState};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::RetrievalError;
use crate::selection::ordering::OrderBoundary;

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

/// Which canonical states to read at one committed boundary.
#[derive(Clone, Copy, Debug)]
pub enum StateScan<'a> {
    /// Latest versions changed since the cursor, or every entity with no cursor.
    Changes(Option<&'a StateCursor>),
    /// The complete result of a limited query, using the engine's normal query
    /// semantics. The engine prepares its query before acquiring the boundary.
    Selection(&'a Selection<Resolved>),
    /// Changed states plus query operations at the same boundary. The engine
    /// prepares this limited selection before acquiring its snapshot; its
    /// predicate can then test changed identities and refill selected gaps.
    SelectionChanges { selection: &'a Selection<Resolved>, since: Option<&'a StateCursor> },
}

/// Canonical entity states from one committed read boundary.
///
/// Sequential pages return each requested entity at most once, at its latest
/// version visible to this read. Change scans coalesce intermediate versions;
/// selection scans return the complete limited result, including unchanged
/// replacement members. Point reads use the same boundary as every page.
///
/// Engines may retain a native snapshot or validate that their read boundary
/// has not changed. In the latter case any operation, including `finish`, can
/// fail with a write conflict. The caller must discard the entire attempt;
/// partially consumed pages cannot establish recovery coverage.
#[async_trait]
pub trait StateSnapshot: Send {
    /// Read at most `limit` requested states. An empty page ends the scan.
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Read one result per identity, in input order, at this snapshot's boundary.
    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError>;

    /// Identities matching the prepared Selection's predicate, ignoring its
    /// ORDER BY and LIMIT. Results are a subset of `ids`; order is unspecified.
    /// Requires a snapshot opened with `StateScan::SelectionChanges`.
    async fn matching_ids(&mut self, ids: &[EntityId]) -> Result<Vec<EntityId>, RetrievalError>;

    /// The next matching states strictly after `after`, in the prepared
    /// Selection's total order. `None` starts at the beginning. This operation
    /// uses `limit` rather than the Selection's original LIMIT, and does not
    /// consume or replace the change scan. It uses the same committed boundary
    /// as `next_page` and point reads.
    /// Requires `StateScan::SelectionChanges`; a mismatched boundary is an error.
    async fn select_after(
        &mut self,
        after: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Validate and close the read, returning its continuation token.
    ///
    /// The token identifies the read boundary, not which rows the caller used.
    /// Before publishing it alongside derived data, the caller must reconcile
    /// every changed entity, or replace the entire selected set. A failed or
    /// cancelled finish establishes no coverage.
    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError>;
}
