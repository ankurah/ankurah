//! What the storage adapter's tests on SQLite share: a database file of their
//! own, the nodes on it, and the test's own connection to it. The trees of
//! items they keep are the suite's, from its fixtures.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ankurah::core::error::MutationError;
use ankurah::core::sync::conformance::fixtures;
use ankurah_digest_tree::Ristretto255;
use ankurah_storage_sqlite::{SqliteConnectionManager, SqliteError, SqliteStorageEngine};
use anyhow::Context;
use bb8::{CustomizeConnection, ManageConnection};
use rusqlite::Connection;
use tempfile::TempDir;

/// A database file of the test's own, in a directory that goes with it.
/// SQLite lets several processes open one file, so each node is an engine of
/// its own on it, with connections of its own, as another process's would
/// be.
pub struct Database {
    directory: TempDir,
}

impl Database {
    pub fn start() -> anyhow::Result<Self> { Ok(Self { directory: tempfile::tempdir()? }) }

    /// A node's engine on the database, whose connections wait for the
    /// database's write lock as long as SQLite's default busy timeout.
    pub async fn node(&self) -> anyhow::Result<Arc<SqliteStorageEngine>> { self.node_opened(None).await }

    /// A node's engine on the database, whose connections wait for the
    /// database's write lock as `busy` says.
    pub async fn node_with(&self, busy: Busy) -> anyhow::Result<Arc<SqliteStorageEngine>> { self.node_opened(Some(busy)).await }

    async fn node_opened(&self, busy: Option<Busy>) -> anyhow::Result<Arc<SqliteStorageEngine>> {
        let mut pool = bb8::Pool::builder();
        if let Some(busy) = busy {
            pool = pool.connection_customizer(Box::new(busy));
        }

        let pool = pool.build(SqliteConnectionManager::file(self.path())).await.context("building a node's pool")?;
        Ok(Arc::new(SqliteStorageEngine::new(pool).await.context("opening a node's engine")?))
    }

    /// The test's own connection to the database, through which it reaches
    /// into what the nodes keep.
    pub fn inspector(&self) -> anyhow::Result<Connection> { Ok(Connection::open(self.path())?) }

    fn path(&self) -> PathBuf { self.directory.path().join("ankurah.sqlite") }
}

/// How a node's connections wait for the database's write lock while
/// another connection holds it.
#[derive(Debug, Clone, Copy)]
pub enum Busy {
    /// Try for the lock again and again, sleeping between tries, until this
    /// long has passed: SQLite's busy timeout, in whole milliseconds.
    Timeout(Duration),

    /// Run this before each try again, with how many tries came before, and
    /// give up once it says to: SQLite's busy handler.
    Handler(fn(i32) -> bool),
}

/// The connection the engine's pool opens.
type PooledConnection = <SqliteConnectionManager as ManageConnection>::Connection;

impl CustomizeConnection<PooledConnection, SqliteError> for Busy {
    fn on_acquire<'a>(
        &'a self,
        connection: &'a mut PooledConnection,
    ) -> Pin<Box<dyn Future<Output = Result<(), SqliteError>> + Send + 'a>> {
        let busy = *self;
        Box::pin(connection.with_connection(move |connection| {
            match busy {
                Busy::Timeout(timeout) => connection.busy_timeout(timeout)?,
                Busy::Handler(handler) => connection.busy_handler(Some(handler))?,
            }

            Ok(())
        }))
    }
}

/// A tree of fixture items on SQLite.
pub type Tree = fixtures::Tree<SqliteStorageEngine, Ristretto255>;

/// SQLite's own refusal that `error` carries, if it carries one.
pub fn refusal(error: &MutationError) -> Option<&rusqlite::Error> {
    match error {
        MutationError::General(error) => match error.downcast_ref() {
            Some(SqliteError::Rusqlite(refusal)) => Some(refusal),
            _ => None,
        },

        _ => None,
    }
}

/// Whether `error` is SQLite giving up on the database's write lock, held
/// by another connection, once its busy timeout passed.
pub fn gave_up_on_the_write_lock(error: &MutationError) -> bool {
    refusal(error).and_then(rusqlite::Error::sqlite_error_code) == Some(rusqlite::ErrorCode::DatabaseBusy)
}
