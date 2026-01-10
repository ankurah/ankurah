//! SQLite storage engine for Ankurah
//!
//! Provides a lightweight embedded database storage option using SQLite.
//! Sits between Sled (pure KV) and Postgres (full SQL server), offering:
//!
//! - Single-file database (portable, easy backup)
//! - Full SQL query capabilities without external server
//! - Native support on all platforms including mobile (iOS, Android)
//!
//! # SQLite Version Requirements
//!
//! This implementation requires SQLite 3.45.0 or later for JSONB support.
//! The `rusqlite` crate with the "bundled" feature includes a compatible
//! SQLite version by default. JSONB support enables:
//!
//! - `jsonb()` function for type-aware JSON comparisons
//! - `->` and `->>` operators for JSON path traversal
//! - Efficient JSONB storage as BLOB
//!
//! See [SQLite JSON documentation](https://sqlite.org/json1.html) for details.
//!
//! # Example
//!
//! ```rust,ignore
//! use ankurah_storage_sqlite::SqliteStorageEngine;
//!
//! // Open a file-based database
//! // liaison id=storage-sqlite
//! let storage = SqliteStorageEngine::open("myapp.db").await?;
//! // liaison end
//!
//! // Or use an in-memory database for testing
//! let storage = SqliteStorageEngine::open_in_memory().await?;
//! ```

mod connection;
mod engine;
mod error;
pub mod sql_builder;
mod value;

pub use connection::SqliteConnectionManager;
pub use engine::{SqliteBucket, SqliteStorageEngine};
pub use error::SqliteError;
pub use value::SqliteValue;
