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
//! process, a JavaScript CLI), on better-sqlite3: the host passes
//! better-sqlite3's `Database` class to `open` and `open_in_memory`, which
//! build each database with it. Both drivers read and write the same tables,
//! so a database file opens under either, except that the Node.js executor
//! refuses one whose text encoding is UTF-16.
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
//! // On wasm32 with the `nodejs` feature, each also takes better-sqlite3's
//! // `Database` class, the export of `require('better-sqlite3')`, from the host
//! let storage = SqliteStorageEngine::open("myapp.db", &database_class).await?;
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

// The unit tests build their engines as the integration tests do, through
// the file the integration tests use for it. That file names this crate by
// its name, as an integration test must, so under test the crate answers to
// its own name too.
#[cfg(test)]
extern crate self as ankurah_storage_sqlite;
#[cfg(test)]
#[path = "../tests/common/driver.rs"]
mod test_driver;
