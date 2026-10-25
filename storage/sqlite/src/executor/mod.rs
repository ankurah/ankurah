//! The executor boundary of the engine.
//!
//! The boundary exists so that the engine's SQL is written once for every
//! SQLite driver. Everything above it decides *which* SQL to run: the shared
//! tables, the per-model materializations and their column maps, the pushdown
//! predicate, the exact-head commit. Everything below it runs that SQL on one
//! SQLite connection: `native` over rusqlite, and on wasm32 `nodejs` over
//! better-sqlite3, whose `Database` class the host passes to
//! `SqliteStorageEngine::open` and `open_in_memory`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
pub(crate) mod nodejs;
mod statement_scan;

// The executor this target compiles: `native` over rusqlite, or on wasm32
// `nodejs`, over better-sqlite3. The engine reaches it only through two
// items, which each executor provides:
// - `Pool`, the engine's pool, whose `get()` hands out a pooled connection;
// - `PooledConnection`, the connection it hands out, whose `with_executor`
//   runs a closure on it through `SqliteExecutor`.
// Natively it also opens its pools through `file_pool(path)` and
// `memory_pool()`, for `SqliteStorageEngine::open` and `open_in_memory`. On
// Node.js those two constructors also take the class the host passes in, so
// the Node.js executor defines them.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::connection::{file_pool, memory_pool, Pool, PooledConnection};
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
pub(crate) use nodejs::{Pool, PooledConnection};
#[cfg(all(target_arch = "wasm32", not(feature = "nodejs")))]
compile_error!("on wasm32, ankurah-storage-sqlite runs inside Node.js, on better-sqlite3: enable the `nodejs` feature");

use crate::{
    error::SqliteError,
    value::{SqliteValue, SqliteValueRef},
};

/// The session pragmas every connection the engine opens runs, natively and
/// on Node.js alike, so that the engine runs with the same session settings
/// on every driver. The busy timeout is not among them: it must come before
/// any statement that reads the file, so each executor has its driver set
/// it as it opens a connection.
pub(crate) const SESSION_PRAGMAS: &str = "PRAGMA journal_mode=WAL;
     PRAGMA synchronous=NORMAL;
     PRAGMA foreign_keys=ON;
     PRAGMA cache_size=-64000;
     PRAGMA mmap_size=268435456;
     PRAGMA temp_store=MEMORY;";

/// What the engine runs its SQL through, so that one engine serves every
/// SQLite driver: one SQLite connection, driven synchronously and in order.
///
/// Parameters are positional `?` placeholders, bound by reference, so that a
/// caller's TEXT and BLOB are not first copied into owned values. A
/// [`SqliteValueRef::Jsonb`] parameter is bound as its JSON text, TEXT, on
/// every driver, which SQL that stores it as JSONB wraps in `jsonb(?)`.
///
/// Each call runs one statement. SQL that holds a second one is refused
/// before SQLite prepares either, since SQLite applies some PRAGMAs while it
/// prepares them. Whitespace, semicolons and comments after the statement
/// are no second statement, but anything else is, even one byte SQLite
/// cannot read. A CREATE TRIGGER statement runs on to the END of its body.
/// SQL that holds only whitespace, semicolons and comments is refused as
/// SQLite refuses to run it, with SQLITE_MISUSE.
///
/// # Where the Node.js executor differs from the native one
///
/// better-sqlite3 hands TEXT over only as a JavaScript string, which puts
/// U+FFFD in place of each byte that is not UTF-8. So the Node.js executor
/// reads a query's rows through a typed reader, which nests the query in a
/// materialized WITH clause and reads each column as two: its storage class,
/// and its value, TEXT as the bytes of a BLOB. Most of the differences come
/// from that reader.
///
/// - A compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the top
///   level of a query, or in a scalar subquery there, is refused before it
///   runs ([`Refusal::CompoundSelect`]), since the typed reader could read
///   one of its INTEGERs as a REAL. So is a VALUES of more than one row, a
///   compound of its rows, there and, unless SQLite runs it as a compound
///   natively too, wherever else it stands, an IN included. Selected as
///   `WITH x AS MATERIALIZED (…) SELECT * FROM x`, either reads the same on
///   every driver, and no statement of the engine's own holds one. The
///   Node.js executor's `refuse_a_compound_select` says which other
///   compounds it lets through, and which it refuses though they read the
///   same everywhere.
/// - A statement with a named parameter (`:a`, `@a`, `#a` or `$a`) is
///   refused before it runs ([`Refusal::NamedParameter`]). rusqlite binds a
///   call's values to named and positional parameters alike, in order, but
///   better-sqlite3 binds a named one only by name. Write a positional `?`
///   for each parameter.
/// - better-sqlite3 binds a numbered parameter such as `?1` only by name, as
///   it binds a named one, so it refuses, in its own words, a call that
///   gives one a positional value; rusqlite binds the value at that
///   position.
/// - A statement that yields rows and that SQLite cannot nest in the typed
///   reader, such as one with a RETURNING clause, is refused before it can
///   write ([`Refusal::NotNestable`]), unless it is a PRAGMA or an EXPLAIN.
///   Their TEXT is read as the driver's string, and a string holding U+FFFD
///   is refused ([`Refusal::ReplacementCharacter`]).
/// - So the engine refuses a model's table with a column whose name holds
///   U+FFFD, since it reads the names through `PRAGMA table_info`, and it
///   refuses a name that is not UTF-8 as one holding U+FFFD, where natively
///   it refuses it as TEXT that is not UTF-8.
/// - A query that yields more than half of SQLite's column limit, 1000 of
///   the default 2000 columns, is refused before it runs
///   ([`Refusal::TooManyColumns`]), since the typed reader reads each column
///   as two; natively it reads up to the limit.
/// - [`query_row`](SqliteExecutor::query_row) decodes only the first row,
///   but SQLite computes every row of the typed reader's WITH clause before
///   the first comes back. So an error SQLite raises while computing a later
///   row, such as an integer overflow, fails the call, where natively SQLite
///   computes only the first row.
/// - A virtual table's column declared REAL that returns an INTEGER reads as
///   the nearest REAL, since the typed reader's WITH clause gives the column
///   REAL affinity, where natively it reads as the INTEGER. Only a loaded
///   extension reaches this: better-sqlite3's JavaScript virtual tables give
///   their columns no type, and SQLite's own R*Tree returns REALs from its
///   REAL columns.
/// - Errors take each target's own variants of [`SqliteError`]. Natively
///   every error SQLite raises, and every refusal of rusqlite's, is a
///   `SqliteError::Rusqlite`. On Node.js an error SQLite raised is a
///   `SqliteError::Sqlite`, a refusal of the executor's a
///   `SqliteError::Refused`, and any other JavaScript exception,
///   better-sqlite3's own refusal among them, a `SqliteError::JavaScript`.
///   [`SqliteError::refusal`] and [`SqliteError::sqlite_code`] answer the
///   same on every driver, except in the cases their docs list.
///
/// [`Refusal::CompoundSelect`]: crate::Refusal::CompoundSelect
/// [`Refusal::NamedParameter`]: crate::Refusal::NamedParameter
/// [`Refusal::NotNestable`]: crate::Refusal::NotNestable
/// [`Refusal::ReplacementCharacter`]: crate::Refusal::ReplacementCharacter
/// [`Refusal::TooManyColumns`]: crate::Refusal::TooManyColumns
pub trait SqliteExecutor {
    /// Run one statement for its effect, and return SQLite's changes(), as
    /// rusqlite does: the number of rows the statement changed when it is an
    /// INSERT, UPDATE or DELETE, and after any other statement, such as DDL
    /// or BEGIN, still the number the last of those changed. A statement
    /// whose first step yields a row is refused
    /// ([`Refusal::ExecuteReturnedRows`]), since its rows belong to
    /// [`query`](SqliteExecutor::query).
    ///
    /// [`Refusal::ExecuteReturnedRows`]: crate::Refusal::ExecuteReturnedRows
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError>;

    /// Run one statement and return every row it yields, in order. A
    /// statement that yields no rows, such as an INSERT, runs for its effect
    /// and returns none.
    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError>;

    /// Run one statement and return its first row, if it yields one. No row
    /// past the first is decoded, as rusqlite's `query_row` decodes none, so
    /// a later row that cannot be read does not fail the call; on Node.js
    /// SQLite still computes every row first (see the Node.js differences
    /// above).
    fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError>;
}

impl dyn SqliteExecutor + '_ {
    /// Open a transaction on this connection. It rolls back when dropped
    /// without [`Transaction::commit`], so an early `?` leaves nothing behind.
    pub fn begin(&self, behavior: TransactionBehavior) -> Result<Transaction<'_>, SqliteError> {
        self.execute(
            match behavior {
                TransactionBehavior::Deferred => "BEGIN DEFERRED",
                TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            },
            &[],
        )?;
        Ok(Transaction { executor: self, open: true })
    }

    /// Open the savepoint `name` on this connection, so that the statements
    /// run through it read one snapshot and keep or undo their work as one,
    /// whether or not a transaction is open: it begins a transaction where
    /// none is, and nests in the open one, where BEGIN would fail. `name`
    /// goes into the SQL as it is, so it must be an identifier. Dropped
    /// without [`Savepoint::release`], it rolls back to the savepoint and
    /// then releases it, as rusqlite's `Savepoint` does, so that an early `?`
    /// leaves nothing behind; a savepoint whose ROLLBACK TO fails is not
    /// released, which would keep the work it was to undo. The errors of
    /// that ROLLBACK TO and RELEASE are dropped, so that the `?` reports the
    /// error of the statement that failed.
    pub(crate) fn savepoint(&self, name: &'static str) -> Result<Savepoint<'_>, SqliteError> {
        self.execute(&format!("SAVEPOINT {name}"), &[])?;
        Ok(Savepoint { executor: self, name, open: true })
    }
}

/// How a transaction takes its lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBehavior {
    /// `BEGIN DEFERRED`: a read snapshot that upgrades to a write lock on the first write.
    Deferred,
    /// `BEGIN IMMEDIATE`: the write lock is taken up front, so that writers
    /// serialize before any of them reads the head its commit expects.
    Immediate,
}

/// A SQL transaction on one connection, BEGIN to COMMIT, which rolls back
/// when dropped uncommitted. Statements run through it execute inside it.
/// [`SqliteTransaction`](crate::SqliteTransaction) is another thing: it
/// buffers the storage writes of Ankurah's `StorageTransaction`, and commits
/// them in one SQL transaction of this kind.
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

    fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
        self.executor.query_row(sql, params)
    }
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

/// An open savepoint on an executor (see `savepoint`). Statements run
/// through it execute inside it; dropping it unreleased rolls back to it and
/// releases it.
pub(crate) struct Savepoint<'e> {
    executor: &'e dyn SqliteExecutor,
    name: &'static str,
    open: bool,
}

impl Savepoint<'_> {
    /// Release the savepoint, keeping what ran inside it.
    pub(crate) fn release(mut self) -> Result<(), SqliteError> {
        self.executor.execute(&format!("RELEASE {}", self.name), &[])?;
        self.open = false;
        Ok(())
    }
}

/// Statements run through the savepoint execute inside it.
impl SqliteExecutor for Savepoint<'_> {
    fn execute(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> { self.executor.execute(sql, params) }

    fn query(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> { self.executor.query(sql, params) }

    fn query_row(&self, sql: &str, params: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
        self.executor.query_row(sql, params)
    }
}

impl Drop for Savepoint<'_> {
    fn drop(&mut self) {
        if self.open {
            // The statement that failed, inside the savepoint or the RELEASE
            // that would have ended it, already reported its error, which is
            // the one to report: these fail, typically, only because SQLite
            // rolled the whole transaction back for that error, so that the
            // savepoint is gone. RELEASE runs only once ROLLBACK TO has
            // undone the savepoint's work, since releasing a savepoint keeps
            // its work.
            if self.executor.execute(&format!("ROLLBACK TO {}", self.name), &[]).is_ok() {
                let _ = self.executor.execute(&format!("RELEASE {}", self.name), &[]);
            }
        }
    }
}

/// One row read through the executor boundary, addressed by column position.
///
/// The getters are strict: each accepts only its own SQLite storage class and
/// refuses any other, as rusqlite's `String` and `Vec<u8>` reads do, so a
/// value of the wrong type is an error rather than a conversion. TEXT and
/// BLOB are lent from the row, which owns them (TEXT already checked to be
/// UTF-8); a caller that keeps one takes its own copy.
#[derive(Debug, Clone)]
pub struct Row(Vec<SqliteValue>);

impl Row {
    pub fn new(values: Vec<SqliteValue>) -> Self { Self(values) }

    pub fn values(&self) -> &[SqliteValue] { &self.0 }

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

/// The refusal of a value of another storage class than a getter reads, the
/// same on every driver. It names the column by its position, since a `Row`
/// holds no column names.
fn mismatch(index: usize, found: &SqliteValue, expected: &str) -> SqliteError {
    let found = if matches!(found, SqliteValue::Null) { "NULL" } else { found.sqlite_type() };
    SqliteError::Row(format!("column {index} holds {found} where {expected} was expected"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::cell::RefCell;

    use rusqlite::ffi;

    use super::*;

    /// A savepoint dropped unreleased rolls back to itself and then releases
    /// itself, and stays unreleased when SQLite fails its ROLLBACK TO, here
    /// with an I/O error, rather than keep the work it was to undo.
    #[test]
    fn a_savepoint_dropped_unreleased_releases_itself_only_once_rolled_back() {
        /// An executor that records each statement it runs, and fails each
        /// ROLLBACK TO when `fails` is set.
        struct Recording {
            statements: RefCell<Vec<String>>,
            fails: bool,
        }

        impl SqliteExecutor for Recording {
            fn execute(&self, sql: &str, _: &[SqliteValueRef<'_>]) -> Result<usize, SqliteError> {
                self.statements.borrow_mut().push(sql.to_owned());
                if self.fails && sql.starts_with("ROLLBACK TO") {
                    return Err(rusqlite::Error::SqliteFailure(ffi::Error::new(ffi::SQLITE_IOERR), None).into());
                }
                Ok(0)
            }

            fn query(&self, _: &str, _: &[SqliteValueRef<'_>]) -> Result<Vec<Row>, SqliteError> {
                unreachable!("a savepoint only executes")
            }

            fn query_row(&self, _: &str, _: &[SqliteValueRef<'_>]) -> Result<Option<Row>, SqliteError> {
                unreachable!("a savepoint only executes")
            }
        }

        for (fails, statements) in
            [(false, &["SAVEPOINT s", "ROLLBACK TO s", "RELEASE s"][..]), (true, &["SAVEPOINT s", "ROLLBACK TO s"][..])]
        {
            let executor = Recording { statements: RefCell::default(), fails };
            drop((&executor as &dyn SqliteExecutor).savepoint("s").unwrap());
            assert_eq!(*executor.statements.borrow(), statements, "when ROLLBACK TO fails: {fails}");
        }
    }
}
