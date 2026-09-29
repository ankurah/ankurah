//! SQLite storage engine for Ankurah nodes running in Node.js
//!
//! The WASM build of an Ankurah node has no IndexedDB inside Node.js and no
//! native library either, but Node.js 22.5 and later ship SQLite as the
//! built-in `node:sqlite` module. This engine drives it from WASM through a
//! `DatabaseSync`-shaped object (`prepare`, `exec`, statements with `run` and
//! `all`); a `better-sqlite3` database has the same shape and works as well.
//!
//! The tables, the model materializations, the pushdown query and the
//! exact-head commit mirror the native `ankurah-storage-sqlite` engine
//! statement for statement, so a database file is interchangeable between the
//! two. The engine's SQL is written against a small synchronous executor
//! (`exec.rs`); `node.rs` is the executor that reaches JavaScript.
//!
//! # Example
//!
//! ```rust,ignore
//! use ankurah_storage_sqlite_node::SqliteNodeStorageEngine;
//!
//! // Open a file-based database through node:sqlite
//! let storage = SqliteNodeStorageEngine::open("myapp.db").await?;
//!
//! // Or an in-memory one, for tests and ephemeral nodes
//! let storage = SqliteNodeStorageEngine::open_in_memory().await?;
//!
//! // Or adopt a database the host constructed (node:sqlite's DatabaseSync, or better-sqlite3)
//! let storage = SqliteNodeStorageEngine::from_database(database).await?;
//! ```

mod dump;
mod engine;
mod error;
pub mod exec;
mod node;
pub mod sql_builder;
mod value;

pub use engine::{SqliteNodeStorageEngine, SqliteNodeTransaction};
pub use error::SqliteNodeError;
pub use exec::{Behavior, Row, SqliteExecutor, Transaction};
pub use node::NodeConnection;
pub use value::SqliteValue;
