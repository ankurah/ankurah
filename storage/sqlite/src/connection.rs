//! Connection manager for bb8 pool with rusqlite

use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tempfile::TempDir;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

use crate::error::SqliteError;

/// Opens connections to one database file for bb8.
#[derive(Clone)]
pub struct SqliteConnectionManager {
    path: PathBuf,
    /// Each connection also holds the directory, because it can outlive its manager.
    directory: Option<Arc<TempDir>>,
    /// Shared by native calls, exclusive to a reset across every connection.
    reset: Arc<RwLock<()>>,
}

impl SqliteConnectionManager {
    pub fn file(path: impl Into<PathBuf>) -> Self { Self { path: path.into(), directory: None, reset: Arc::default() } }

    /// A manager for a new database file in `directory`.
    pub(crate) fn temporary(directory: TempDir) -> Self {
        Self { path: directory.path().join("ankurah.sqlite"), directory: Some(Arc::new(directory)), reset: Arc::default() }
    }

    fn create_connection(&self) -> Result<PooledConnection, SqliteError> {
        let conn = Connection::open(&self.path)?;

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
        crate::order_keys::register(&conn)?;

        Ok(PooledConnection::new(conn, self.directory.clone(), self.reset.clone()))
    }
}

/// An SQLite connection and the temporary directory of its database, if it
/// has one. `connection` comes first so that it closes before the directory
/// can be deleted, and a call whose caller was cancelled holds both until the
/// call ends.
struct OpenConnection {
    connection: Connection,
    _directory: Option<Arc<TempDir>>,
}

/// A pooled SQLite connection wrapper
///
/// Serializes access to the connection; its blocking calls run on the
/// runtime's blocking threads.
pub struct PooledConnection {
    inner: Arc<Mutex<OpenConnection>>,
    reset: Arc<RwLock<()>>,
    /// A map writer keeps this between calls, including cancellation cleanup.
    reset_guard: Option<Arc<OwnedRwLockReadGuard<()>>>,
}

impl PooledConnection {
    /// Wrap `conn` and the temporary directory of its database, if it has one.
    fn new(conn: Connection, directory: Option<Arc<TempDir>>, reset: Arc<RwLock<()>>) -> Self {
        Self { inner: Arc::new(Mutex::new(OpenConnection { connection: conn, _directory: directory })), reset, reset_guard: None }
    }

    /// Keep reset protection across an entire native writer transaction.
    pub(crate) async fn hold_reset(&mut self) { self.reset_guard = Some(Arc::new(self.reset.clone().read_owned().await)); }

    pub(crate) fn release_reset(&mut self) { self.reset_guard = None; }

    /// Drain native calls on all connections, then run the reset exclusively.
    /// The blocking closure owns the guard even if its caller is cancelled.
    pub(crate) async fn reset<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let reset = self.reset.clone().write_owned().await;
        self.run_protected(move |connection| {
            let _reset = reset;
            f(connection)
        })
        .await
    }

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
        let reset = match &self.reset_guard {
            Some(held) => held.clone(),
            None => Arc::new(self.reset.clone().read_owned().await),
        };
        self.run_protected(move |connection| {
            let _reset = reset;
            f(connection)
        })
        .await
    }

    async fn run_protected<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let mut guard = self.inner.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || f(&mut guard.connection)).await.map_err(|e| SqliteError::TaskJoin(e.to_string()))?
    }
}

impl Clone for PooledConnection {
    fn clone(&self) -> Self { Self { inner: self.inner.clone(), reset: self.reset.clone(), reset_guard: self.reset_guard.clone() } }
}

impl bb8::ManageConnection for SqliteConnectionManager {
    type Connection = PooledConnection;
    type Error = SqliteError;

    fn connect(&self) -> impl std::future::Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let manager = self.clone();
        async move {
            let reset = manager.reset.clone().read_owned().await;
            tokio::task::spawn_blocking(move || {
                let _reset = reset;
                manager.create_connection()
            })
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
            let connection = PooledConnection::new(Connection::open_in_memory()?, None, Arc::default());
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

    /// A connection keeps the temporary directory of its database after its
    /// manager drops, as a pool's idle connections do: bb8 drops a pool's
    /// manager before them.
    #[tokio::test]
    async fn a_connection_keeps_its_temporary_directory_after_its_manager_drops() -> anyhow::Result<()> {
        use bb8::ManageConnection;

        let manager = SqliteConnectionManager::temporary(tempfile::tempdir()?);
        let directory = manager.path.parent().expect("the database file is in the directory").to_path_buf();
        let connection = manager.connect().await?;

        drop(manager);
        assert!(directory.exists(), "the connection holds the directory");
        connection.with_connection(|connection| Ok(connection.execute_batch("CREATE TABLE t (v INTEGER)")?)).await?;

        drop(connection);
        assert!(!directory.exists(), "the directory is deleted once its last connection closes");
        Ok(())
    }

    #[tokio::test]
    async fn a_cancelled_call_keeps_its_directory_until_the_connection_closes() -> anyhow::Result<()> {
        use std::time::Duration;

        use bb8::ManageConnection;

        let manager = SqliteConnectionManager::temporary(tempfile::tempdir()?);
        let directory = manager.path.parent().unwrap().to_path_buf();
        let connection = manager.connect().await?;
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, resume) = mpsc::channel();
        let (finished, completion) = tokio::sync::oneshot::channel();
        let call = tokio::spawn(async move {
            connection
                .with_connection(move |connection| {
                    let _ = started.send(());
                    resume.recv_timeout(Duration::from_secs(5)).expect("release the blocking call");
                    let result = connection.execute_batch("CREATE TABLE t (v INTEGER)");
                    let _ = finished.send(result);
                    Ok(())
                })
                .await
        });
        entered.await?;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        drop(manager);
        assert!(directory.exists(), "the blocking call still owns the connection and directory");

        release.send(())?;
        completion.await??;
        tokio::time::timeout(Duration::from_secs(5), async {
            while directory.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        Ok(())
    }
}
