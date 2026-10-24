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
//! # Drivers
//!
//! The engine's SQL is written once, against [`executor::SqliteExecutor`],
//! so that one engine serves every SQLite driver. Natively it runs on
//! rusqlite connections from a bb8 pool. On wasm32, with the `nodejs`
//! feature, it serves an Ankurah node running inside Node.js, where
//! IndexedDB does not exist (a VS Code extension host, an Electron main
//! process, a JavaScript CLI): on Node.js's built-in `node:sqlite`, from
//! Node.js 22.20.0 on 22, 24.7.0 on 24, or any later release line, or on a
//! better-sqlite3 database the host hands to `from_database`. Every driver
//! reads and writes the same tables, so a database file opens under any of
//! them, except that the Node.js executor refuses one whose text encoding
//! is UTF-16.
//!
//! # Example
//!
//! ```rust,ignore
//! use ankurah_storage_sqlite::SqliteStorageEngine;
//!
//! // Open a file-based database
//! let storage = SqliteStorageEngine::open("myapp.db").await?;
//!
//! // Or use an in-memory database for testing
//! let storage = SqliteStorageEngine::open_in_memory().await?;
//!
//! // On wasm32 with the `nodejs` feature: adopt a database the host constructed
//! // (node:sqlite's DatabaseSync, or better-sqlite3's Database)
//! let storage = SqliteStorageEngine::from_database(database).await?;
//! ```

mod dump;
mod engine;
mod error;
pub mod executor;
pub mod sql_builder;
mod value;

pub use engine::{SqliteStorageEngine, SqliteTransaction};
pub use error::{DatabaseRefusal, Refusal, SqliteError, SqliteFailure};
#[cfg(not(target_arch = "wasm32"))]
pub use executor::native::connection::SqliteConnectionManager;
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
pub use executor::nodejs::{NodeJsConnection, NodeJsPool};
pub use value::{SqliteValue, SqliteValueRef};

// The unit tests build their engines on the SQLite driver the run names,
// through the file the integration tests use for it. That file names this
// crate by its name, as an integration test must, so under test the crate
// answers to its own name too.
#[cfg(test)]
extern crate self as ankurah_storage_sqlite;
#[cfg(test)]
#[path = "../tests/common/driver.rs"]
mod test_driver;
