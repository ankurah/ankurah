//! The connections map calls run on: opened as the engine's are, apart from
//! its pool, a few kept for the next, and none handed on while it holds a
//! transaction or a call may run on it.
//!
//! A storage transaction holds one from its first map call to its end, and a
//! snapshot one for as long as it lasts, many of them at once where reads
//! stay open across commits: so they don't come from the engine's pool,
//! whose size bounds its own reads and commits.
//!
//! Every call into SQLite blocks, so a lease runs each on a blocking thread,
//! as the engine runs its own, and learns from its end whether the
//! connection holds a transaction. A lease dropped while its connection
//! holds one, or while a call may still run on it, ends that transaction on
//! a blocking thread once the call has ended, and only then gives the
//! connection back, and the writer turn it holds.

use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::OwnedMutexGuard;

use crate::connection::{PooledConnection, SqliteConnectionManager};
use crate::error::SqliteError;

/// How many connections the engine keeps for its next map calls: enough for
/// a commit's transaction and the reads beside it. More that come back
/// close. Snapshots' connections have no cap: each open snapshot holds one.
const KEPT_CONNECTIONS: usize = 4;

/// How many statements each connection keeps prepared: room for the 22 that
/// map calls run, a range's for each of its nine kinds of bounds in both
/// directions among them, past rusqlite's default of 16, where they would
/// evict each other.
const PREPARED_STATEMENTS: usize = 32;

/// The connections the engine's map calls run on.
pub(super) struct Connections {
    /// The engine's pool, whose connection manager opens each connection, so
    /// that it opens the engine's database as the engine's do; the pool
    /// itself never sees them.
    pool: bb8::Pool<SqliteConnectionManager>,

    /// Connections holding no transaction, ready to lease: at most
    /// [`KEPT_CONNECTIONS`].
    kept: Mutex<Vec<PooledConnection>>,
}

/// A connection leased for one storage transaction's map calls or one
/// snapshot's reads. Dropping the lease gives the connection back once it
/// holds no transaction.
pub(in crate::engine) struct Lease {
    connection: Option<PooledConnection>,
    connections: Arc<Connections>,

    /// Whether the connection holds neither a transaction nor a call that
    /// may still run, as the lease last learned it.
    idle: bool,

    /// The engine's writer turn, which a map transaction's lease holds until
    /// its connection's transaction has ended.
    turn: Option<OwnedMutexGuard<()>>,
}

impl Connections {
    pub(super) fn new(pool: bb8::Pool<SqliteConnectionManager>) -> Arc<Self> { Arc::new(Self { pool, kept: Mutex::new(Vec::new()) }) }

    /// A connection kept from an earlier map call, or a new one.
    pub(super) async fn lease(self: &Arc<Self>) -> Result<Lease, SqliteError> {
        let kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner).pop();
        let connection = match kept {
            Some(connection) => connection,
            None => self.connect().await?,
        };

        Ok(Lease { connection: Some(connection), connections: Arc::clone(self), idle: true, turn: None })
    }

    /// A new connection to the engine's database, opened as the engine's are.
    async fn connect(&self) -> Result<PooledConnection, SqliteError> {
        let connection = self.pool.dedicated_connection().await?;
        connection
            .with_connection(|connection| {
                connection.set_prepared_statement_cache_capacity(PREPARED_STATEMENTS);
                Ok(())
            })
            .await?;

        Ok(connection)
    }

    /// Keep `connection`, which holds no transaction, for the next map call,
    /// unless as many are kept already, in which case it closes.
    fn put_back(&self, connection: PooledConnection) {
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        if kept.len() < KEPT_CONNECTIONS {
            kept.push(connection);
        }
    }

    /// End the transaction `connection` holds, once any call still running on
    /// it has ended, then keep the connection, or let it close if the
    /// transaction didn't end, which ends it.
    async fn end_transaction(self: Arc<Self>, connection: PooledConnection) {
        let ended = connection
            .with_connection(|connection| match connection.is_autocommit() {
                true => Ok(()),
                false => Ok(connection.execute_batch("ROLLBACK")?),
            })
            .await;

        match ended {
            Ok(()) => self.put_back(connection),
            Err(failure) => tracing::warn!("a dropped map transaction didn't roll back, so its connection closes: {failure}"),
        }
    }
}

impl Lease {
    /// Hold the engine's writer turn until the connection's transaction has
    /// ended.
    pub(super) fn hold(&mut self, turn: OwnedMutexGuard<()>) { self.turn = Some(turn); }

    /// The connection, for a call of the engine's own to run in its
    /// transaction.
    pub(in crate::engine) fn connection(&self) -> &PooledConnection {
        self.connection.as_ref().expect("a lease holds its connection until dropped")
    }

    /// Run `call` on the connection, on a blocking thread: what it returns.
    /// A call the lease doesn't see end, as when its caller is dropped while
    /// it runs, keeps the connection from going back until it has ended.
    pub(in crate::engine) async fn run<T: Send + 'static>(
        &mut self,
        call: impl FnOnce(&rusqlite::Connection) -> Result<T, SqliteError> + Send + 'static,
    ) -> Result<T, SqliteError> {
        self.idle = false;
        let (result, autocommit) =
            self.connection().with_connection(move |connection| Ok((call(connection), connection.is_autocommit()))).await?;

        self.idle = autocommit;
        result
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else { return };
        let turn = self.turn.take();

        if self.idle {
            self.connections.put_back(connection);
            return;
        }

        // With no runtime to end the transaction on, the connection goes:
        // SQLite rolls back the transaction it holds as it closes, once no
        // call runs on it.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };

        let connections = Arc::clone(&self.connections);
        runtime.spawn(async move {
            connections.end_transaction(connection).await;
            drop(turn);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::mpsc;

    use futures_util::FutureExt;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    use super::*;
    use crate::connection::SqliteConnectionManager;

    /// A call dropped before its blocking thread has started, held back here
    /// behind another on its runtime's one blocking thread, runs before its
    /// lease's cleanup ends its transaction and gives the connection back, and
    /// the writer turn with it: the next lease finds no transaction left open.
    #[test]
    fn a_call_dropped_before_its_thread_takes_the_connection_ends_before_the_connection_goes_back() -> anyhow::Result<()> {
        let one_blocking_thread = || Builder::new_current_thread().max_blocking_threads(1).enable_all().build();
        let (here, occupied) = (one_blocking_thread()?, one_blocking_thread()?);

        here.block_on(async {
            let directory = tempfile::tempdir()?;
            let pool = bb8::Pool::builder().build(SqliteConnectionManager::file(directory.path().join("lease.sqlite"))).await?;
            let connections = Connections::new(pool);
            let turn = Arc::new(tokio::sync::Mutex::new(()));

            let mut lease = connections.lease().await?;
            lease.hold(Arc::clone(&turn).lock_owned().await);

            let (release, released) = mpsc::channel::<()>();
            occupied.spawn_blocking(move || released.recv());

            let (ran, has_run) = oneshot::channel();
            {
                let _occupied = occupied.enter();
                let mut call = pin!(lease.run(move |connection| {
                    connection.execute_batch("BEGIN IMMEDIATE")?;
                    ran.send(()).expect("the test waits for the call");
                    Ok(())
                }));

                assert!(call.as_mut().now_or_never().is_none());
            }

            drop(lease);

            // The cleanup runs on `here`, whose one blocking thread runs this
            // after any blocking call the cleanup has made.
            tokio::spawn(async { tokio::task::spawn_blocking(|| ()).await }).await??;
            release.send(())?;
            has_run.await?;
            drop(turn.lock().await);

            let mut next = connections.lease().await?;
            assert!(next.run(|connection| Ok(connection.is_autocommit())).await?, "the next lease's connection holds no transaction");
            Ok(())
        })
    }
}
