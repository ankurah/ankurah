//! The executor boundary of the engine.
//!
//! The boundary exists so that the engine's SQL is written once for every
//! SQLite driver. Everything above it decides *which* SQL to run: the shared
//! tables, the per-model materializations and their column maps, the pushdown
//! predicate, the exact-head commit. Everything below it runs that SQL on one
//! SQLite connection: `native.rs` over rusqlite, and on wasm32 `node.rs` over
//! Node.js's built-in `node:sqlite` module.

use crate::{
    error::SqliteError,
    value::{SqliteValue, SqliteValueRef},
    SqliteStorageEngine,
};

/// One SQLite connection, driven synchronously and in order.
///
/// Parameters are positional `?` placeholders, bound by reference so that a
/// caller's TEXT and BLOB are not copied to be bound. A
/// [`SqliteValueRef::Jsonb`] parameter is bound as its JSON text, because the
/// SQL that carries one always wraps its placeholder in `jsonb(?)`.
///
/// Each call runs one statement: SQL that holds a second one is refused
/// before either runs. Whitespace, semicolons and comments after the
/// statement are no second statement.
///
/// # Where the node executor differs from the native one
///
/// - On Node a compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the
///   top level of a query is refused before it runs. Node reads a query's
///   rows through a typed reader that nests the query in a WITH clause, where
///   a compound SELECT takes the affinity of its first SELECT's columns,
///   which can change a value from a later SELECT: an INTEGER past 2^53 read
///   through a REAL column would read as the nearest REAL. Selected as `WITH
///   x AS MATERIALIZED (…) SELECT * FROM x`, a compound SELECT reads the same
///   on every driver. SQLite's query plan decides what is at the top level,
///   so a compound in a subquery that SQLite flattens into the query is
///   refused too. No statement of the engine's own is a compound SELECT.
/// - Named parameters (`:a`, `@a`, `$a`) are not supported on Node:
///   node:sqlite binds no positional value to one, leaving it NULL, and
///   better-sqlite3 refuses the call. rusqlite binds them by position.
/// - better-sqlite3 binds no positional value to a numbered parameter such as
///   `?1`, and refuses the call; rusqlite and node:sqlite bind the value at
///   that position.
/// - On Node a query that SQLite cannot nest in a WITH clause is refused,
///   before it runs, unless it is a PRAGMA or an EXPLAIN, and TEXT in the
///   rows of a PRAGMA or an EXPLAIN is refused.
/// - better-sqlite3 refuses a query whose SQL holds more than whitespace
///   after the semicolon that ends its statement, as `SELECT 1; -- note`
///   does: the typed reader nests the SQL better-sqlite3 prepared, and
///   better-sqlite3 keeps what follows the semicolon in it.
pub trait SqliteExecutor {
    /// Run one statement that yields no rows; returns the number of rows it changed.
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError>;

    /// Run one statement and collect every row it yields, in order.
    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError>;
}

impl dyn SqliteExecutor + '_ {
    /// Run a statement and return its first row, if it yields one.
    pub fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// Open a transaction on this connection. It rolls back when dropped
    /// without [`Transaction::commit`], so an early `?` leaves nothing behind.
    pub fn begin(&self, behavior: Behavior) -> Result<Transaction<'_>, SqliteError> {
        self.execute(
            match behavior {
                Behavior::Deferred => "BEGIN DEFERRED",
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
    /// `BEGIN DEFERRED`: a read snapshot that upgrades to a write lock on the first write.
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

impl Transaction<'_> {
    pub fn commit(mut self) -> Result<(), SqliteError> {
        self.executor.execute("COMMIT", &[])?;
        self.open = false;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), SqliteError> {
        self.executor.execute("ROLLBACK", &[])?;
        self.open = false;
        Ok(())
    }
}

/// Statements run through the transaction execute inside it.
impl SqliteExecutor for Transaction<'_> {
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> { self.executor.execute(sql, params) }

    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> { self.executor.query(sql, params) }
}

impl<'e> std::ops::Deref for Transaction<'e> {
    type Target = dyn SqliteExecutor + 'e;
    fn deref(&self) -> &Self::Target { self.executor }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if self.open {
            // The statement that failed already reported its error; a rollback
            // failure here has nothing left to report to. rusqlite's own
            // transaction guard drops the same way.
            let _ = self.executor.execute("ROLLBACK", &[]);
        }
    }
}

/// One row read through the executor boundary, addressed by column position.
///
/// The getters are strict: each accepts only its own SQLite storage class and
/// refuses any other, as rusqlite's `String` and `Vec<u8>` reads do, so a
/// value of the wrong type is an error rather than a conversion. TEXT and
/// BLOB are lent from the row, which holds the one copy the driver made of
/// them (TEXT already checked to be UTF-8); a caller that keeps one takes
/// its own.
#[derive(Debug, Clone)]
pub struct Row(Vec<SqliteValue>);

impl Row {
    pub fn new(values: Vec<SqliteValue>) -> Self { Self(values) }

    pub fn values(&self) -> &[SqliteValue] { &self.0 }

    pub fn into_values(self) -> Vec<SqliteValue> { self.0 }

    pub fn get(&self, index: usize) -> Result<&SqliteValue, SqliteError> {
        self.0.get(index).ok_or_else(|| SqliteError::Row(format!("column {index} is missing from a row of {} columns", self.0.len())))
    }

    /// A TEXT column.
    pub fn text(&self, index: usize) -> Result<&str, SqliteError> {
        match self.get(index)? {
            SqliteValue::Text(text) => Ok(text),
            other => Err(mismatch(index, other, "TEXT")),
        }
    }

    /// A TEXT column that may be NULL.
    pub fn opt_text(&self, index: usize) -> Result<Option<&str>, SqliteError> {
        match self.get(index)? {
            SqliteValue::Null => Ok(None),
            _ => self.text(index).map(Some),
        }
    }

    /// A BLOB column.
    pub fn blob(&self, index: usize) -> Result<&[u8], SqliteError> {
        match self.get(index)? {
            SqliteValue::Blob(bytes) => Ok(bytes),
            other => Err(mismatch(index, other, "BLOB")),
        }
    }

    /// An INTEGER column.
    pub fn integer(&self, index: usize) -> Result<i64, SqliteError> {
        match self.get(index)? {
            SqliteValue::Integer(value) => Ok(*value),
            other => Err(mismatch(index, other, "INTEGER")),
        }
    }
}

/// The refusal of a value of another storage class, the same on both
/// drivers. Changed from main, where rusqlite's typed getters refused it as
/// `InvalidColumnType`, naming the column; this message names it by position.
fn mismatch(index: usize, found: &SqliteValue, expected: &str) -> SqliteError {
    let found = if matches!(found, SqliteValue::Null) { "NULL" } else { found.sqlite_type() };
    SqliteError::Row(format!("column {index} holds {found} where {expected} was expected"))
}

impl SqliteStorageEngine {
    /// Run a closure on one of the engine's connections through the executor
    /// boundary: raw SQL access for embedders and tests, the same on every driver.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.pool().get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        conn.with_executor(f).await
    }
}
