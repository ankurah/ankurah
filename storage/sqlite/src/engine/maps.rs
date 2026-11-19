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
//! A private-memory database has one connection. Its map calls release that
//! connection between operations, stage writes, and validate a shared map
//! revision when they read and commit.

pub(super) mod calls;
mod connections;
pub(super) mod memory;
mod snapshot;
pub(super) mod statements;

use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;

use self::connections::Connections;
pub(super) use self::connections::Lease;
pub use self::snapshot::SqliteMapSnapshot;

/// What the engine's map calls run on: their connections, and the writer
/// turn its map transactions take.
pub(crate) struct Maps {
    connections: Arc<Connections>,
    pub(super) pool: bb8::Pool<SqliteConnectionManager>,
    pub(super) names: ankurah_core::storage::LockTable,
    /// Keep access to a private-memory database independent of pool checkout:
    /// an unpolled checkout future can otherwise retain its only pool lease.
    memory: OnceCell<Option<PooledConnection>>,

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
        Self {
            connections: Connections::new(pool.clone()),
            pool,
            names: Default::default(),
            memory: OnceCell::new(),
            turn: Arc::new(Mutex::new(())),
            table: OnceCell::new(),
        }
    }

    pub(super) async fn in_memory(&self) -> Result<bool, SqliteError> { Ok(self.memory_connection().await?.is_some()) }

    pub(super) async fn memory_connection(&self) -> Result<Option<PooledConnection>, SqliteError> {
        Ok(self
            .memory
            .get_or_try_init(|| async {
                let pooled = self.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
                let memory = pooled
                    .with_connection(|connection| {
                        let file: String =
                            connection.query_row("SELECT file FROM pragma_database_list WHERE name = 'main'", [], |row| row.get(0))?;
                        Ok(file.is_empty())
                    })
                    .await?;
                Ok::<_, SqliteError>(memory.then(|| (*pooled).clone()))
            })
            .await?
            .clone())
    }

    /// A connection for map calls, on a database that keeps maps.
    pub(super) async fn lease(&self) -> Result<Lease, SqliteError> {
        self.table.get_or_try_init(|| self.create_table()).await?;
        self.connections.lease().await
    }

    /// Create the file-backed database's maps on a dedicated connection.
    async fn create_table(&self) -> Result<(), SqliteError> {
        let mut lease = self.connections.lease().await?;

        lease.run(statements::create_table).await
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
