//! Connection pooling for map transactions, snapshots and resets.
//!
//! A connection returns to the pool after its transaction ends, or after its
//! end statement is queued ahead of the next lease's requests. Dropping a lease
//! with unfinished transaction cleanup closes its connection and attempts to
//! cancel any pending statement, such as a lock request. PostgreSQL rolls the
//! transaction back unless its `COMMIT` already took effect, releasing its locks.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bb8_postgres::PostgresConnectionManager;
use futures_util::FutureExt;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::error::Severity;
use tokio_postgres::NoTls;

use super::schema::{self, Statements};
use super::Connection;
use crate::maps::MapError;

/// Connection limits and timeouts for map calls. PostgreSQL timeouts are
/// rounded up to whole milliseconds, with a minimum of one millisecond;
/// PostgreSQL would interpret zero as disabling the timeout.
#[derive(Clone, Copy, Debug)]
pub struct MapSettings {
    /// Maximum idle connections retained for reuse.
    pub max_idle_connections: usize,

    /// Maximum concurrent connections for map transactions, snapshots and
    /// resets. Callers wait for a pool permit once this limit is reached.
    pub max_connections: usize,

    /// Timeout for a transaction or reset to obtain a connection, including
    /// waiting for a pool permit. For snapshots, this limits opening the
    /// connection after obtaining a permit.
    pub connection_timeout: Duration,

    /// Timeout for a snapshot to obtain a pool permit. This bounds waiting
    /// behind open map transactions, independently of connection setup and
    /// query execution.
    pub snapshot_pool_wait_timeout: Duration,

    /// How long a map transaction or a reset waits for a lock before it
    /// fails, as PostgreSQL's `lock_timeout`.
    pub lock_timeout: Duration,

    /// How long a map transaction may sit idle before PostgreSQL ends its
    /// session, as `idle_in_transaction_session_timeout`: what frees the
    /// locks of a transaction whose owner stalled without ending it.
    /// Snapshots, which hold nothing others wait for, have no such limit.
    pub idle_in_transaction_timeout: Duration,
}

impl Default for MapSettings {
    fn default() -> Self {
        Self {
            max_idle_connections: 2,
            max_connections: 16,
            connection_timeout: Duration::from_secs(30),
            snapshot_pool_wait_timeout: Duration::from_millis(100),
            lock_timeout: Duration::from_secs(30),
            idle_in_transaction_timeout: Duration::from_secs(30),
        }
    }
}

/// The engine's connections for map calls.
pub(crate) struct Pool {
    /// The engine's pool, whose connection manager opens each of these
    /// connections, so that they connect as the engine's do; the pool itself
    /// never sees them.
    engine: bb8::Pool<PostgresConnectionManager<NoTls>>,

    settings: MapSettings,

    /// What opens a map transaction's native transaction: the `BEGIN`; the
    /// time its session may then sit idle before PostgreSQL ends it, which
    /// frees its locks from an owner that stalled; and its share of the
    /// reset lock. Only a map transaction waits on its owner while it holds
    /// what others wait for, so only it gets the limit.
    begin_map_transaction: String,

    /// Set once the table that holds the maps exists, which every
    /// connection's statements name: before the first connection prepares
    /// them.
    table: OnceCell<()>,

    /// Connections safe for reuse, up to `max_idle_connections`.
    idle: Mutex<Vec<Connection>>,

    /// One permit for each connection the engine may have leased at once.
    permits: Arc<Semaphore>,
}

/// A connection leased from a pool. On drop, return it to the idle pool if its
/// transaction has ended or its end statement is queued, and the pool has room;
/// otherwise close it.
pub(crate) struct Lease {
    connection: Option<Connection>,
    pool: Arc<Pool>,

    /// Whether transaction cleanup still prevents reuse. Cleared after the
    /// transaction ends or its end statement is queued ahead of later requests.
    in_transaction: AtomicBool,

    _permit: OwnedSemaphorePermit,
}

impl Pool {
    pub(crate) fn new(engine: bb8::Pool<PostgresConnectionManager<NoTls>>, settings: MapSettings) -> Arc<Self> {
        let begin_map_transaction = format!(
            "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL idle_in_transaction_session_timeout = {}; {}",
            setting_millis(settings.idle_in_transaction_timeout),
            schema::share_reset_lock(),
        );

        let permits = Arc::new(Semaphore::new(settings.max_connections));
        Arc::new(Self { engine, settings, begin_map_transaction, table: OnceCell::new(), idle: Mutex::new(Vec::new()), permits })
    }

    /// What opens a map transaction's native transaction: `READ COMMITTED`,
    /// so that each statement after a lock takes its snapshot once the lock
    /// is granted; limited in how long it may sit idle; and sharing the reset
    /// lock.
    pub(crate) fn begin_map_transaction(&self) -> &str { &self.begin_map_transaction }

    /// Lease an idle or new connection for a map transaction or reset,
    /// waiting for a permit if necessary. The connection timeout covers both.
    pub(crate) async fn lease(self: &Arc<Self>) -> Result<Lease, MapError> {
        let leasing = async {
            let permit = Arc::clone(&self.permits).acquire_owned().await.expect("a pool never closes its semaphore");
            self.lease_with(permit).await
        };

        let waited = self.settings.connection_timeout;
        tokio::time::timeout(waited, leasing).await.map_err(|_| MapError::ConnectionTimeout { waited })?
    }

    /// Lease a connection for a snapshot. Waiting for a permit uses the
    /// snapshot pool timeout; opening a connection uses the connection timeout.
    pub(crate) async fn lease_for_snapshot(self: &Arc<Self>) -> Result<Lease, MapError> {
        let most = self.settings.max_connections;
        let permit = tokio::time::timeout(self.settings.snapshot_pool_wait_timeout, Arc::clone(&self.permits).acquire_owned()).await;
        let permit = permit.map_err(|_| MapError::ConnectionsInUse { most })?.expect("a pool never closes its semaphore");

        let waited = self.settings.connection_timeout;
        tokio::time::timeout(waited, self.lease_with(permit)).await.map_err(|_| MapError::ConnectionTimeout { waited })?
    }

    /// A lease of an idle connection, or of a new one, under `permit`.
    async fn lease_with(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Result<Lease, MapError> {
        let connection = match self.take_idle() {
            Some(connection) => connection,
            None => self.connect().await?,
        };

        Ok(Lease { connection: Some(connection), pool: Arc::clone(self), in_transaction: AtomicBool::new(false), _permit: permit })
    }

    /// An idle connection that is still open, if the pool holds one.
    fn take_idle(&self) -> Option<Connection> {
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        while let Some(connection) = idle.pop() {
            if !connection.client.is_closed() {
                return Some(connection);
            }
        }

        None
    }

    /// A new connection, its session's lock timeout set and its statements
    /// prepared after creating the maps' table if this is the pool's first.
    async fn connect(&self) -> Result<Connection, MapError> {
        let mut client = self.engine.dedicated_connection().await.map_err(MapError::database)?;
        let lock_timeout = format!("SET lock_timeout = {}", setting_millis(self.settings.lock_timeout));
        client.batch_execute(&lock_timeout).await.map_err(MapError::database)?;
        self.table.get_or_try_init(|| schema::create_table(&mut client)).await.map_err(MapError::Table)?;
        let statements = Statements::prepare(&client).await.map_err(MapError::database)?;

        Ok(Connection { client, statements })
    }

    /// Return a connection safe for reuse to the idle pool if it has room.
    fn put_back(&self, connection: Connection) {
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        if idle.len() < self.settings.max_idle_connections {
            idle.push(connection);
        }
    }
}

impl Lease {
    pub(crate) fn connection(&self) -> &Connection { self.connection.as_ref().expect("a lease holds its connection until dropped") }

    /// Whether transaction cleanup still prevents this connection's reuse.
    pub(crate) fn in_transaction(&self) -> bool { self.in_transaction.load(Ordering::SeqCst) }

    /// Send `begin`, which opens a transaction. Until an end is confirmed or
    /// queued, dropping the lease closes the connection.
    pub(crate) fn begin<'l>(&'l self, begin: &'l str) -> impl Future<Output = Result<(), tokio_postgres::Error>> + 'l {
        self.in_transaction.store(true, Ordering::SeqCst);
        self.connection().client.batch_execute(begin)
    }

    /// Send `end`, a `COMMIT` or a `ROLLBACK`, and note the transaction ended
    /// once the database has answered it, even with a refusal that keeps the
    /// session, which rolls the transaction back.
    pub(crate) async fn end(&self, end: &str) -> Result<(), tokio_postgres::Error> {
        let ended = self.connection().client.batch_execute(end).await;
        let refused_in_session =
            |error: &tokio_postgres::Error| error.as_db_error().is_some_and(|refusal| refusal.parsed_severity() == Some(Severity::Error));

        if ended.as_ref().map_or_else(refused_in_session, |()| true) {
            self.in_transaction.store(false, Ordering::SeqCst);
        }

        ended
    }

    /// Send `end` without waiting for it to run, and note the transaction
    /// ended: any later request on the connection runs after it.
    pub(crate) fn end_unanswered(&self, end: &str) {
        if self.connection().client.batch_execute(end).now_or_never().is_none_or(|ended| ended.is_ok()) {
            self.in_transaction.store(false, Ordering::SeqCst);
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else { return };
        if connection.client.is_closed() {
            return;
        }

        if !*self.in_transaction.get_mut() {
            self.pool.put_back(connection);
            return;
        }

        // The session may be waiting on a statement, such as a request for a
        // lock, and the connection lives until its answer comes; cancel it,
        // so that the session ends now, giving up as a connection would.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let (cancel, deadline) = (connection.client.cancel_token(), self.pool.settings.connection_timeout);
            runtime.spawn(async move { tokio::time::timeout(deadline, cancel.cancel_query(NoTls)).await });
        }
    }
}

/// A duration as the whole milliseconds a PostgreSQL timeout takes, rounded
/// up, since PostgreSQL reads zero as no timeout, and no more than it takes.
fn setting_millis(duration: Duration) -> u128 { duration.as_nanos().div_ceil(1_000_000).clamp(1, i32::MAX as u128) }

#[cfg(test)]
mod tests;
