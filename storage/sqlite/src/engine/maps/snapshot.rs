//! A committed map view: a WAL read transaction on a file-backed database,
//! or short reads validated by revision on a private-memory database.

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
/// A private-memory database instead checks its map revision on every read
/// and returns a write conflict if a commit or reset changed that revision.
pub struct SqliteMapSnapshot {
    source: Source,
}

enum Source {
    File(Lease),
    Memory { connection: crate::connection::PooledConnection, revision: Option<u64> },
}

impl SqliteMapSnapshot {
    /// A snapshot on a connection of its own.
    pub(crate) async fn begin(maps: &Maps) -> Result<Self, SqliteError> {
        let source = if let Some(connection) = maps.memory_connection().await? {
            Source::Memory { connection, revision: None }
        } else {
            Source::File(maps.lease().await?)
        };
        Ok(Self { source })
    }
}

#[async_trait]
impl MapSnapshot for SqliteMapSnapshot {
    /// The reads, in one call, behind the snapshot's `BEGIN` if it is the
    /// first.
    async fn map_read(&mut self, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
        let lease = match &mut self.source {
            Source::Memory { connection, revision } => {
                let (current, rows) = super::memory::read(connection, *revision, reads).await?;
                *revision = Some(current);
                return Ok(rows);
            }
            Source::File(lease) => lease,
        };
        let rows = lease
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
