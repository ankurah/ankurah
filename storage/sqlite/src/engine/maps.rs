//! Ordered maps of bytes on the SQLite engine, beside its entity records,
//! and the locks its storage transactions take.
//!
//! Every map's entries live in one table, keyed by map and key, whose keys
//! order by their bytes as maps' keys do. SQLite lets one transaction at a
//! time write to a database, so a lock is the database's write lock, whatever
//! names a transaction asks for. A storage transaction's first map call takes
//! the engine's writer turn, then begins a native transaction with `BEGIN
//! IMMEDIATE`, which takes the write lock, on a connection the transaction
//! holds to its end. Its map calls run in that transaction, its writes as they
//! are made, and its commit writes its entity rows there and commits. Until it
//! ends, every other writer to the database waits: the engine's other map
//! transactions for the writer turn, and every other write, of this process or
//! another, for the write lock, up to the busy timeout. A snapshot is a read
//! transaction on a connection of its own, which in the engine's
//! write-ahead-log journal mode waits for no writer.
//!
//! A database in memory keeps no maps: each connection to it is a database of
//! its own.

pub(super) mod calls;
mod connections;
mod snapshot;
pub(super) mod statements;

use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};

use crate::connection::SqliteConnectionManager;
use crate::error::SqliteError;

use self::connections::Connections;
pub(super) use self::connections::Lease;
pub use self::snapshot::SqliteMapSnapshot;

/// What the engine's map calls run on: their connections, and the writer
/// turn its map transactions take.
pub(crate) struct Maps {
    connections: Arc<Connections>,

    /// The engine's writer turn, so that its map transactions queue for the
    /// write lock here, holding no connection, rather than in SQLite's busy
    /// wait, which gives up at its timeout.
    turn: Arc<Mutex<()>>,

    /// Set once the database is known to keep maps: on a file, with the
    /// maps' table created.
    table: OnceCell<()>,
}

impl Maps {
    /// The map calls of an engine whose connections `pool` opens.
    pub(super) fn new(pool: bb8::Pool<SqliteConnectionManager>) -> Self {
        Self { connections: Connections::new(pool), turn: Arc::new(Mutex::new(())), table: OnceCell::new() }
    }

    /// A connection for map calls, on a database that keeps maps.
    async fn lease(&self) -> Result<Lease, SqliteError> {
        self.table.get_or_try_init(|| self.create_table()).await?;
        self.connections.lease().await
    }

    /// Create the maps' table, if the database has none yet:
    /// [`SqliteError::MapsInMemory`] for a database in memory, which has no
    /// file.
    async fn create_table(&self) -> Result<(), SqliteError> {
        let mut lease = self.connections.lease().await?;

        lease
            .run(|connection| {
                let file: String =
                    connection.query_row("SELECT file FROM pragma_database_list WHERE name = 'main'", [], |row| row.get(0))?;
                if file.is_empty() {
                    return Err(SqliteError::MapsInMemory);
                }

                statements::create_table(connection)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use ankurah_core::storage::{LockName, StorageEngine, StorageTransaction};

    use crate::SqliteStorageEngine;

    /// A storage transaction holds the engine's writer turn from its first
    /// map call to its end, so that the engine's other map transactions wait
    /// for it at the turn, never at the database's write lock.
    #[tokio::test]
    async fn a_transaction_holds_the_writer_turn_from_its_first_map_call_to_its_end() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let engine = SqliteStorageEngine::open(directory.path().join("turn.sqlite")).await?;

        let mut trx = engine.transaction();
        trx.map_lock_and_read(&[LockName::new("l")], vec![]).await?;
        let held = engine.maps.turn.try_lock().is_err();

        trx.commit().await?.committed()?;
        let free = engine.maps.turn.try_lock().is_ok();

        assert!(held, "the transaction holds the turn once its first map call has returned");
        assert!(free, "the turn is free once the transaction has ended");
        Ok(())
    }
}
