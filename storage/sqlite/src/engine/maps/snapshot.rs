//! A read of the maps that sees one committed state: a read transaction on a
//! connection of its own.

use ankurah_core::error::RetrievalError;
use ankurah_core::storage::{MapRead, MapRows, MapSnapshot};
use async_trait::async_trait;

use super::connections::Lease;
use super::{statements, Maps};
use crate::error::SqliteError;

/// A snapshot of the SQLite engine's maps, begun by
/// [`StorageEngine::map_snapshot`](ankurah_core::storage::StorageEngine::map_snapshot).
///
/// Its first read begins its transaction, which in the engine's
/// write-ahead-log journal mode reads the state the last commit before it
/// left, and goes on reading that state while later commits land.
pub struct SqliteMapSnapshot {
    lease: Lease,
}

impl SqliteMapSnapshot {
    /// A snapshot on a connection of its own.
    pub(crate) async fn begin(maps: &Maps) -> Result<Self, SqliteError> { Ok(Self { lease: maps.lease().await? }) }
}

#[async_trait]
impl MapSnapshot for SqliteMapSnapshot {
    /// The reads, in one call, behind the snapshot's `BEGIN` if it is the
    /// first.
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
        let rows = self
            .lease
            .run(move |connection| {
                if connection.is_autocommit() {
                    connection.execute_batch("BEGIN")?;
                }

                reads.iter().map(|read| statements::read(connection, read)).collect()
            })
            .await?;

        Ok(rows)
    }
}
