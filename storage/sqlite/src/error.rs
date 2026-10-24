//! Error types for SQLite storage engine

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("SQLite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// An error of the node executor, on either Node driver.
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error("node executor error: {0}")]
    Node(String),

    /// SQLite refused a statement run through node:sqlite, or the Node.js
    /// executor refused SQL that holds no statement, on either driver, as
    /// SQLite refuses it natively (SQLITE_MISUSE). `code` is SQLite's
    /// extended result code, which rusqlite's `SqliteFailure` carries
    /// natively; [`SqliteError::sqlite_code`] reads it from either.
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error("node executor error: {message}")]
    NodeSqlite { code: i32, message: String },

    #[error("Connection pool error: {0}")]
    Pool(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Dump error: {0}")]
    Dump(String),

    #[error("DDL error: {0}")]
    DDL(String),

    #[error("corrupt durable record: {0}")]
    CorruptRecord(String),

    #[error("unexpected row shape: {0}")]
    Row(String),

    #[error("SQL generation error: {0}")]
    SqlGeneration(String),

    #[error("Task join error: {0}")]
    TaskJoin(String),

    #[error("incompatible store protocol version: found {found}, required {expected}; reset your development database (or migrate the store) before opening it with this binary")]
    ProtocolVersionMismatch { found: String, expected: u32 },

    #[error("store has existing ankurah tables but no recorded protocol version (pre-{expected} store); reset your development database (or migrate the store) before opening it with this binary")]
    UnversionedStore { expected: u32 },
}

impl SqliteError {
    /// SQLite's extended result code when SQLite itself refused a statement,
    /// such as 1555 for a duplicate primary key or 5 for a lock held past the
    /// busy timeout, read the same way whether rusqlite or node:sqlite ran the
    /// statement; `None` for every other error. better-sqlite3 hands the code
    /// over only by name, which is not read, so its refusals have `None` too,
    /// but for SQL that holds no statement, which the Node.js executor refuses
    /// before the driver sees it, with SQLITE_MISUSE, 21, on every driver.
    ///
    /// A call that passes more values than its statement takes parameters
    /// differs between drivers: rusqlite and better-sqlite3 refuse it
    /// themselves, before SQLite binds anything, so their code is `None`,
    /// where node:sqlite's refusal is SQLite's SQLITE_RANGE, `Some(25)`. A
    /// call that passes too few has `None` on every driver, refused before
    /// SQLite runs by rusqlite, by the Node.js executor on node:sqlite, and by
    /// better-sqlite3.
    pub fn sqlite_code(&self) -> Option<i32> {
        match self {
            // rusqlite reports a statement SQLite could not parse as an
            // SqlInputError, which carries the same code as a SqliteFailure.
            #[cfg(not(target_arch = "wasm32"))]
            SqliteError::Rusqlite(rusqlite::Error::SqliteFailure(error, _) | rusqlite::Error::SqlInputError { error, .. }) => {
                Some(error.extended_code)
            }
            #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
            SqliteError::NodeSqlite { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl From<SqliteError> for ankurah_core::error::RetrievalError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::RetrievalError::storage(err) }
}

impl From<SqliteError> for ankurah_core::error::MutationError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::MutationError::General(Box::new(err)) }
}

impl From<SqliteError> for ankurah_core::error::StateError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::StateError::DDLError(Box::new(err)) }
}
