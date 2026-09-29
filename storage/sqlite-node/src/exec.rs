//! The executor boundary of the engine.
//!
//! Everything above this boundary decides *which* SQL to run: the shared
//! tables, the per-model materializations and their column maps, the pushdown
//! predicate, the exact-head commit protocol. Everything below it runs SQL on
//! one SQLite connection through JavaScript: `node.rs`, over Node.js's
//! built-in `node:sqlite` module. Keeping the JavaScript calls behind
//! [`SqliteExecutor`] keeps them out of the engine's SQL logic, and lets a
//! second JavaScript SQLite (better-sqlite3 today by shape, a browser SQLite
//! build later) plug in without touching that logic.

use crate::{error::SqliteNodeError, value::SqliteValue};

/// One SQLite connection, driven synchronously and in order.
///
/// Parameters are positional `?` placeholders. A [`SqliteValue::Jsonb`]
/// parameter is bound as its JSON text, because the SQL that carries one
/// always wraps its placeholder in `jsonb(?)`.
pub trait SqliteExecutor {
    /// Run one statement that yields no rows; returns the number of rows it changed.
    fn execute(&self, sql: &str, params: &[SqliteValue]) -> Result<usize, SqliteNodeError>;

    /// Run one statement and collect every row it yields, in order.
    fn query(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>, SqliteNodeError>;
}

impl dyn SqliteExecutor + '_ {
    /// Run a statement and return its first row, if it yields one.
    pub fn query_row(&self, sql: &str, params: &[SqliteValue]) -> Result<Option<Row>, SqliteNodeError> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// Open a transaction on this connection. It rolls back when dropped
    /// without [`Transaction::commit`], so an early `?` leaves nothing behind.
    pub fn begin(&self, behavior: Behavior) -> Result<Transaction<'_>, SqliteNodeError> {
        self.execute(
            match behavior {
                Behavior::Deferred => "BEGIN",
                Behavior::Immediate => "BEGIN IMMEDIATE",
            },
            &[],
        )?;
        Ok(Transaction { executor: self, open: true })
    }
}

/// How a transaction takes its lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behavior {
    /// `BEGIN`: a read snapshot that upgrades to a write lock on the first write.
    Deferred,
    /// `BEGIN IMMEDIATE`: the write lock is taken up front, so writers serialize
    /// before any expectation is read.
    Immediate,
}

/// An open transaction on an executor. Statements run through it execute
/// inside the transaction; dropping it uncommitted rolls back.
pub struct Transaction<'e> {
    executor: &'e dyn SqliteExecutor,
    open: bool,
}

impl<'e> Transaction<'e> {
    pub fn commit(mut self) -> Result<(), SqliteNodeError> {
        self.executor.execute("COMMIT", &[])?;
        self.open = false;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), SqliteNodeError> {
        self.executor.execute("ROLLBACK", &[])?;
        self.open = false;
        Ok(())
    }
}

/// Statements run through the transaction execute inside it.
impl SqliteExecutor for Transaction<'_> {
    fn execute(&self, sql: &str, params: &[SqliteValue]) -> Result<usize, SqliteNodeError> { self.executor.execute(sql, params) }

    fn query(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>, SqliteNodeError> { self.executor.query(sql, params) }
}

impl<'e> std::ops::Deref for Transaction<'e> {
    type Target = dyn SqliteExecutor + 'e;
    fn deref(&self) -> &Self::Target { self.executor }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.open {
            // The statement that failed already reported its error; a rollback
            // failure here has nothing left to report to.
            let _ = self.executor.execute("ROLLBACK", &[]);
        }
    }
}

/// One row read through the executor boundary, addressed by column position.
#[derive(Debug, Clone, PartialEq)]
pub struct Row(Vec<SqliteValue>);

impl Row {
    pub fn new(values: Vec<SqliteValue>) -> Self { Self(values) }

    pub fn values(&self) -> &[SqliteValue] { &self.0 }

    pub fn into_values(self) -> Vec<SqliteValue> { self.0 }

    pub fn get(&self, index: usize) -> Result<&SqliteValue, SqliteNodeError> {
        self.0.get(index).ok_or_else(|| SqliteNodeError::Row(format!("column {index} is missing from a row of {} columns", self.0.len())))
    }

    /// A TEXT column.
    pub fn text(&self, index: usize) -> Result<String, SqliteNodeError> {
        match self.get(index)? {
            SqliteValue::Text(text) => Ok(text.clone()),
            other => Err(SqliteNodeError::Row(format!("column {index} holds {} where TEXT was expected", other.sqlite_type()))),
        }
    }

    /// A TEXT column that may be NULL.
    pub fn opt_text(&self, index: usize) -> Result<Option<String>, SqliteNodeError> {
        match self.get(index)? {
            SqliteValue::Null => Ok(None),
            _ => self.text(index).map(Some),
        }
    }

    /// A BLOB column.
    pub fn blob(&self, index: usize) -> Result<Vec<u8>, SqliteNodeError> {
        match self.get(index)? {
            SqliteValue::Blob(bytes) => Ok(bytes.clone()),
            other => Err(SqliteNodeError::Row(format!("column {index} holds {} where BLOB was expected", other.sqlite_type()))),
        }
    }

    /// An INTEGER column.
    pub fn integer(&self, index: usize) -> Result<i64, SqliteNodeError> {
        match self.get(index)? {
            SqliteValue::Integer(value) => Ok(*value),
            other => Err(SqliteNodeError::Row(format!("column {index} holds {} where INTEGER was expected", other.sqlite_type()))),
        }
    }
}
