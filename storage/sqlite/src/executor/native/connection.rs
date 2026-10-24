//! Connection manager for bb8 pool with rusqlite, and the engine's pools of
//! its connections

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{config::DbConfig, Connection};
use tokio::sync::Mutex;

use crate::{error::SqliteError, executor::SESSION_PRAGMAS};

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
        conn.execute_batch(SESSION_PRAGMAS)?;

        // So that a stale or misspelt double-quoted column name fails with
        // "no such column" instead of reading as the string of its name,
        // double-quoted string literals are off, in queries and in DDL
        // alike, as on every Node.js database the engine accepts (see
        // nodejs/adoption.rs).
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;

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

    /// Execute a function with the connection
    ///
    /// This acquires the mutex lock and runs the provided closure with the connection.
    /// The closure is executed within spawn_blocking since rusqlite operations are synchronous.
    pub async fn with_connection<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn.blocking_lock();
            f(&guard)
        })
        .await
        .map_err(|e| SqliteError::TaskJoin(e.to_string()))?
    }

    /// Execute a function with mutable access to the connection
    pub async fn with_connection_mut<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.blocking_lock();
            f(&mut guard)
        })
        .await
        .map_err(|e| SqliteError::TaskJoin(e.to_string()))?
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
        let conn_inner = conn.inner.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                let guard = conn_inner.blocking_lock();
                guard.execute_batch("SELECT 1").map_err(SqliteError::from)
            })
            .await
            .map_err(|e| SqliteError::TaskJoin(e.to_string()))?
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool { false }
}

/// The engine's pool of rusqlite connections.
pub(crate) type Pool = bb8::Pool<SqliteConnectionManager>;

/// Default connection pool size
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// The pool behind `SqliteStorageEngine::open`.
pub(crate) async fn file_pool(path: &Path) -> anyhow::Result<Pool> {
    let manager = SqliteConnectionManager::file(path);
    Ok(bb8::Pool::builder().max_size(DEFAULT_POOL_SIZE).build(manager).await?)
}

/// The pool behind `SqliteStorageEngine::open_in_memory`.
pub(crate) async fn memory_pool() -> anyhow::Result<Pool> {
    let manager = SqliteConnectionManager::memory();
    // For in-memory, we use a single connection to keep the database alive
    Ok(bb8::Pool::builder().max_size(1).build(manager).await?)
}
