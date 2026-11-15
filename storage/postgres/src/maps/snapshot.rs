//! A read of the maps that sees one committed state: a `REPEATABLE READ`,
//! read-only transaction on a connection of its own.

use std::sync::Arc;

use ankurah_core::error::RetrievalError;
use ankurah_core::storage::{MapRead, MapRows, MapSnapshot};
use async_trait::async_trait;

use super::connection::{Lease, Pool};
use super::request::Request;
use super::MapError;

/// What opens a snapshot's transaction, which takes its snapshot at its first
/// read: every commit that completed before the snapshot began is in it.
const BEGIN_SNAPSHOT: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

/// A snapshot of the PostgreSQL engine's maps, begun by
/// [`StorageEngine::map_snapshot`](ankurah_core::storage::StorageEngine::map_snapshot).
/// Its first read begins its transaction, and dropping it ends the
/// transaction without waiting, giving its connection back at once.
pub struct PostgresMapSnapshot {
    lease: Lease,
}

impl PostgresMapSnapshot {
    /// A snapshot on a connection of `pool`'s.
    pub(crate) async fn begin(pool: &Arc<Pool>) -> Result<Self, MapError> { Ok(Self { lease: pool.lease_for_snapshot().await? }) }
}

#[async_trait]
impl MapSnapshot for PostgresMapSnapshot {
    /// The reads, in one request behind the snapshot's `BEGIN` if it is the
    /// first.
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
        let mut request = Request::on(self.lease.connection());
        if !self.lease.in_transaction() {
            request.then(self.lease.begin(BEGIN_SNAPSHOT));
        }

        request.read(reads);
        Ok(request.send().await.map_err(MapError::request)?)
    }
}

impl Drop for PostgresMapSnapshot {
    fn drop(&mut self) {
        if self.lease.in_transaction() {
            self.lease.end_unanswered("ROLLBACK");
        }
    }
}
