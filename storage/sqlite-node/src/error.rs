//! Error types for the SQLite engine over node:sqlite

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteNodeError {
    #[error("node:sqlite error: {0}")]
    Node(String),

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

    #[error("incompatible store protocol version: found {found}, required {expected}; reset your development database (or migrate the store) before opening it with this binary")]
    ProtocolVersionMismatch { found: String, expected: u32 },

    #[error("store has existing ankurah tables but no recorded protocol version (pre-{expected} store); reset your development database (or migrate the store) before opening it with this binary")]
    UnversionedStore { expected: u32 },
}

impl From<SqliteNodeError> for ankurah_core::error::RetrievalError {
    fn from(err: SqliteNodeError) -> Self { ankurah_core::error::RetrievalError::storage(err) }
}

impl From<SqliteNodeError> for ankurah_core::error::MutationError {
    fn from(err: SqliteNodeError) -> Self { ankurah_core::error::MutationError::General(Box::new(err)) }
}

impl From<SqliteNodeError> for ankurah_core::error::StateError {
    fn from(err: SqliteNodeError) -> Self { ankurah_core::error::StateError::DDLError(Box::new(err)) }
}
