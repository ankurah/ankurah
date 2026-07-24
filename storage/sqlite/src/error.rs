//! Error types for SQLite storage engine

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[error("SQLite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    #[error("Connection pool error: {0}")]
    Pool(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("DDL error: {0}")]
    DDL(String),

    #[error("corrupt durable record: {0}")]
    CorruptRecord(String),

    #[error("SQL generation error: {0}")]
    SqlGeneration(String),

    #[error("Task join error: {0}")]
    TaskJoin(String),

    #[error("incompatible store protocol version: found {found}, required {expected}; reset your development database (or migrate the store) before opening it with this binary")]
    ProtocolVersionMismatch { found: String, expected: u32 },

    #[error("store has existing ankurah tables but no recorded protocol version (pre-{expected} store); reset your development database (or migrate the store) before opening it with this binary")]
    UnversionedStore { expected: u32 },
}

impl From<SqliteError> for ankurah_core::error::RetrievalError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::RetrievalError::StorageError(Box::new(err)) }
}

impl From<SqliteError> for ankurah_core::error::MutationError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::MutationError::General(Box::new(err)) }
}

impl From<SqliteError> for ankurah_core::error::StateError {
    fn from(err: SqliteError) -> Self { ankurah_core::error::StateError::DDLError(Box::new(err)) }
}
