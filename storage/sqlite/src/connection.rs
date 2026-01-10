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
