//! Connection manager for bb8 pool with rusqlite

use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::error::SqliteError;

/// Configuration for SQLite connections
#[derive(Clone, Debug)]
pub enum SqliteConfig {
    /// File-based database
    File(PathBuf),
    /// In-memory database (for testing)
    Memory,
}

/// A wrapper around a SQLite connection that can be used with bb8
///
/// Since rusqlite::Connection is not Send, we wrap it in a Mutex
/// and use spawn_blocking for all operations.
pub struct SqliteConnectionManager {
    config: SqliteConfig,
}

impl SqliteConnectionManager {
    pub fn new(config: SqliteConfig) -> Self { Self { config } }

    pub fn file(path: impl Into<PathBuf>) -> Self { Self::new(SqliteConfig::File(path.into())) }

    pub fn memory() -> Self { Self::new(SqliteConfig::Memory) }

    fn create_connection(&self) -> Result<Connection, SqliteError> {
        let conn = match &self.config {
            SqliteConfig::File(path) => Connection::open(path)?,
            SqliteConfig::Memory => Connection::open_in_memory()?,
        };

        // Performance optimizations
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;
             PRAGMA cache_size=-64000;
             PRAGMA mmap_size=268435456;
             PRAGMA temp_store=MEMORY;",
        )?;
        crate::id_bytes::register(&conn)?;

        Ok(conn)
    }
}

/// A pooled SQLite connection wrapper
///
/// Wraps the rusqlite Connection in an Arc<Mutex> for thread-safe access
/// since rusqlite connections are not Send.
pub struct PooledConnection {
    inner: Arc<Mutex<Connection>>,
}

impl PooledConnection {
    pub fn new(conn: Connection) -> Self { Self { inner: Arc::new(Mutex::new(conn)) } }

    /// Execute a function with the connection, as [`run`](Self::run) runs it.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        self.run(move |connection| f(connection)).await
    }

    /// Execute a function with mutable access to the connection, as
    /// [`run`](Self::run) runs it.
    pub async fn with_connection_mut<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        self.run(f).await
    }

    /// Run `f` on the connection, on a blocking thread, as every call into the
    /// connection runs: rusqlite's calls block. The connection's lock is taken
    /// before the thread, so that a closure whose caller is dropped once it
    /// holds the lock still runs before any later one on the connection, and
    /// so that no closure holds a thread while it waits for the lock: one that
    /// did could hold the last thread against the lock's holder, itself
    /// waiting for a thread.
    async fn run<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let mut guard = self.inner.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || f(&mut guard)).await.map_err(|e| SqliteError::TaskJoin(e.to_string()))?
    }
}

impl Clone for PooledConnection {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl bb8::ManageConnection for SqliteConnectionManager {
    type Connection = PooledConnection;
    type Error = SqliteError;

    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let config = self.config.clone();
        async move {
            let manager = SqliteConnectionManager::new(config);
            tokio::task::spawn_blocking(move || manager.create_connection().map(PooledConnection::new))
                .await
                .map_err(|e| SqliteError::TaskJoin(e.to_string()))?
        }
    }

    #[allow(refining_impl_trait)]
    fn is_valid<'a, 'b>(&'a self, conn: &'b mut Self::Connection) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let conn = conn.clone();
        async move { conn.with_connection(|connection| connection.execute_batch("SELECT 1").map_err(SqliteError::from)).await }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool { false }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::mpsc;

    use futures_util::FutureExt;
    use tokio::runtime::Builder;

    use super::*;

    /// A read made after a mutable call that still waits for a blocking
    /// thread sees that call's write: the call took the connection's lock
    /// before the thread, so the read waits behind it. Had the call taken the
    /// thread first, the read would have run ahead of the write.
    #[test]
    fn a_read_after_a_mutable_call_waiting_for_a_thread_sees_its_write() -> anyhow::Result<()> {
        let one_blocking_thread = || Builder::new_current_thread().max_blocking_threads(1).enable_all().build();
        let (occupied, free) = (one_blocking_thread()?, one_blocking_thread()?);

        free.block_on(async {
            let connection = PooledConnection::new(Connection::open_in_memory()?);
            connection
                .with_connection(|connection| Ok(connection.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1)")?))
                .await?;

            let (release, released) = mpsc::channel::<()>();
            occupied.spawn_blocking(move || released.recv());

            // Queued on `occupied` for its one thread, and holding the lock.
            let writer = connection.clone();
            let mut write = pin!(writer.with_connection_mut(|connection| Ok(connection.execute_batch("UPDATE t SET v = 2")?)));
            {
                let _occupied = occupied.enter();
                assert!(write.as_mut().now_or_never().is_none(), "the write waits for the thread");
            }

            let mut read =
                pin!(connection.with_connection(|connection| Ok(connection.query_row("SELECT v FROM t", [], |row| row.get::<_, i64>(0))?)));
            assert!(read.as_mut().now_or_never().is_none(), "the read waits for the lock the write holds");

            release.send(())?;
            write.await?;
            assert_eq!(read.await?, 2, "the read sees the write that took the lock before it");
            Ok(())
        })
    }
}
