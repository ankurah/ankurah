//! Error types for SQLite storage engine

use std::str::Utf8Error;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("SQLite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// An error SQLite itself raised, such as a constraint's failure or a
    /// syntax error, through better-sqlite3: what rusqlite reports natively
    /// as its `SqliteFailure`.
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error("SQLite error: {0}")]
    Sqlite(SqliteFailure),

    /// A call, a driver or a database that the Node.js executor refuses (see
    /// [`Refusal`]).
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error(transparent)]
    Refused(#[from] Refusal),

    /// A JavaScript exception that is no error SQLite raised, with its
    /// message: better-sqlite3's own refusal, such as of a call that passes
    /// too few values, a host function's exception, or a TypeError
    /// better-sqlite3 throws.
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error("JavaScript exception: {0}")]
    JavaScript(String),

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

    /// A value that a [`Row`](crate::executor::Row) getter refuses, the same
    /// on every driver: one of another storage class than the getter reads,
    /// or a column the row does not have.
    #[error("unexpected value: {0}")]
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
    /// busy timeout, the same whichever driver ran the statement: rusqlite
    /// hands the code over as its number, and better-sqlite3 by its name,
    /// which is read as the number sqlite3.h gives it. On better-sqlite3 a
    /// name that no SQLite release up to the crate's table of names defines,
    /// as a later release could add, has `None`. On Node.js the executor's
    /// refusal of a statement SQLite cannot nest answers the code of SQLite's
    /// error. `None` for every other error, a call that passes more or fewer
    /// values than its statement takes parameters among them: rusqlite and
    /// better-sqlite3 refuse such a call themselves, before SQLite binds
    /// anything.
    ///
    /// SQL that holds no statement differs between drivers: natively SQLite
    /// refuses to run it, with SQLITE_MISUSE, `Some(21)`, where better-sqlite3
    /// refuses to prepare it itself, as a [`SqliteError::JavaScript`], with
    /// `None`.
    pub fn sqlite_code(&self) -> Option<i32> {
        match self {
            // rusqlite reports a statement SQLite could not parse as an
            // SqlInputError, which carries the same code as a SqliteFailure.
            #[cfg(not(target_arch = "wasm32"))]
            SqliteError::Rusqlite(rusqlite::Error::SqliteFailure(error, _) | rusqlite::Error::SqlInputError { error, .. }) => {
                Some(error.extended_code)
            }
            #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
            SqliteError::Sqlite(failure) | SqliteError::Refused(Refusal::NotNestable(failure)) => failure.code,
            _ => None,
        }
    }

    /// The refusal this error is, when the executor refused a call, or on
    /// Node.js a driver or a database, rather than run or read it otherwise
    /// than rusqlite does: one question for every driver, as
    /// [`SqliteError::sqlite_code`] is for SQLite's result codes. Natively it
    /// answers rusqlite's own refusals as the refusals of their kinds:
    /// `MultipleStatement`, `ExecuteReturnedResults`, `InvalidParameterCount`
    /// for a call that passes fewer values than its statement takes, and
    /// `FromSqlConversionFailure` for TEXT that is not UTF-8. On Node.js it
    /// answers the refusal the executor raised.
    ///
    /// `None` for every other error, a call that passes too many values among
    /// them (see `sqlite_code`).
    ///
    /// It answers the same on every driver except in two cases:
    /// - a call that passes too few values: better-sqlite3 refuses it itself,
    ///   as a JavaScript exception, so `refusal` is `None` there;
    /// - TEXT that is not UTF-8 in a row of a PRAGMA or an EXPLAIN:
    ///   [`Refusal::TextNotUtf8`] natively, and
    ///   [`Refusal::ReplacementCharacter`] on Node.js, where better-sqlite3
    ///   hands such TEXT over with U+FFFD in place of each byte that is not
    ///   UTF-8.
    pub fn refusal(&self) -> Option<Refusal> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            SqliteError::Rusqlite(error) => match error {
                rusqlite::Error::MultipleStatement => Some(Refusal::MultipleStatements),
                rusqlite::Error::ExecuteReturnedResults => Some(Refusal::ExecuteReturnedRows),
                rusqlite::Error::InvalidParameterCount(passed, needed) if passed < needed => {
                    Some(Refusal::TooFewValues { passed: *passed })
                }
                rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, error) => {
                    error.downcast_ref::<Utf8Error>().map(|error| Refusal::TextNotUtf8 { column: *column, error: *error })
                }
                _ => None,
            },
            #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
            SqliteError::Refused(refusal) => Some(refusal.clone()),
            _ => None,
        }
    }
}

/// An error SQLite raised, as better-sqlite3 hands it over.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct SqliteFailure {
    /// SQLite's extended result code, such as 1555 for
    /// SQLITE_CONSTRAINT_PRIMARYKEY: better-sqlite3 hands it over by its
    /// name, which is read as the number sqlite3.h gives it. `None` for a
    /// name better-sqlite3 gives that no SQLite release up to the crate's
    /// table of names defines.
    pub code: Option<i32>,
    /// SQLite's message, such as "UNIQUE constraint failed: things.k".
    pub message: String,
}

/// A call, or on Node.js a driver or a database, that the engine's executor
/// refuses rather than run or read it otherwise than rusqlite does: what
/// [`SqliteError::refusal`] answers, on every driver.
///
/// The first four are rusqlite's own refusals, which `refusal` answers
/// natively; natively, `SqliteError::Rusqlite` displays each after "SQLite
/// error: ". The Node.js executor makes three of them as rusqlite does, in
/// rusqlite's words; the third, `TooFewValues`, better-sqlite3 makes itself,
/// in its own words. Only the Node.js executor makes the others.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Refusal {
    /// SQL that holds a second statement, refused before SQLite prepares
    /// either, since SQLite applies some PRAGMAs while it prepares them:
    /// natively rusqlite's `MultipleStatement`.
    #[error("Multiple statements provided")]
    MultipleStatements,

    /// `execute` given a statement whose first step yields a row, which
    /// belongs to `query`: natively rusqlite's `ExecuteReturnedResults`.
    #[error("Execute returned results - did you mean to call query?")]
    ExecuteReturnedRows,

    /// A call that passes `passed` values, fewer than its statement takes
    /// parameters, refused before it runs: natively rusqlite's
    /// `InvalidParameterCount`, which also counts the parameters the
    /// statement takes. On Node.js better-sqlite3 refuses such a call itself,
    /// as a [`SqliteError::JavaScript`].
    #[error("Wrong number of parameters passed to query. Got {passed}, needed more")]
    TooFewValues { passed: usize },

    /// TEXT whose bytes are not UTF-8, in column `column` of a row, refused
    /// rather than read with a replacement for each byte that is not:
    /// natively rusqlite's `FromSqlConversionFailure`.
    #[error("Conversion error from type Text at index: {column}, {error}")]
    TextNotUtf8 {
        column: usize,
        #[source]
        error: Utf8Error,
    },

    /// A compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the top
    /// level of a query, or in a scalar subquery there, or a VALUES of more
    /// than one row there or, unless SQLite runs it as a compound natively
    /// too, anywhere in the query, refused before it runs, since the Node.js
    /// executor's typed reader could change one of its values (see
    /// `refuse_a_compound_select`).
    #[error(
        "a compound SELECT at the top level of a query, or in a scalar subquery there, or a VALUES of more than one row, is \
         refused on Node.js, since one of its INTEGERs could read back as a REAL; select from it as `WITH x AS MATERIALIZED \
         (…) SELECT * FROM x`, which reads the same on every driver"
    )]
    CompoundSelect,

    /// A query of `width` columns, more than half of SQLite's column limit,
    /// refused before it runs: the Node.js executor's typed reader reads each
    /// column as two, its storage class and its value.
    #[error(
        "a query of {width} columns is refused on Node.js, since the Node.js executor reads each column as two, its storage \
         class and its value, within SQLite's column limit, 2000 by default; select at most half that limit, 1000 by \
         default, in one query"
    )]
    TooManyColumns { width: usize },

    /// A statement that yields rows, other than a PRAGMA or an EXPLAIN, whose
    /// typed reader SQLite refused to prepare, with SQLite's error: SQLite
    /// nests only a query in a WITH clause, so that a statement with a
    /// RETURNING clause is refused here, before it can write.
    #[error(
        "a statement that yields rows and that SQLite cannot nest in a WITH clause ({0}), such as one with a RETURNING \
         clause, is refused on Node.js before it runs, since the Node.js executor reads rows exactly only through such a \
         clause; write with `execute`, and read with a query"
    )]
    NotNestable(#[source] SqliteFailure),

    /// TEXT holding U+FFFD in column `column` of a PRAGMA's or an EXPLAIN's
    /// row, which SQLite would not nest in a typed reader, so that the driver
    /// hands its TEXT over as a string, in which U+FFFD may stand for a byte
    /// that is not UTF-8.
    #[error(
        "TEXT holding U+FFFD in column {column} of a PRAGMA or an EXPLAIN is refused on Node.js, since better-sqlite3 hands \
         such TEXT over as a string, in which U+FFFD may stand for a byte that is not UTF-8; select it through a query, \
         such as one of a table-valued pragma function like pragma_table_info, which reads TEXT as its bytes"
    )]
    ReplacementCharacter { column: usize },

    /// A driver or a database the Node.js executor cannot run on (see
    /// [`DatabaseRefusal`]).
    #[error(transparent)]
    Database(#[from] DatabaseRefusal),
}

/// A database the Node.js executor refuses, as a [`Refusal::Database`].
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
impl From<DatabaseRefusal> for SqliteError {
    fn from(refusal: DatabaseRefusal) -> Self { SqliteError::Refused(Refusal::Database(refusal)) }
}

/// A driver or a database the Node.js executor cannot run on, refused as the
/// engine opens the database: a driver that is not better-sqlite3's
/// `Database` class, or a database the executor cannot read exactly. A value
/// the database hands over later that the executor cannot read is refused
/// the same way, as it is read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DatabaseRefusal {
    /// A value passed as the driver that is not a class, a constructor with
    /// a prototype; `found` is how the value reads.
    #[error(
        "{found} is refused as the driver, since it is not a class; pass better-sqlite3's `Database` class, the export of \
         `require('better-sqlite3')`"
    )]
    NotADatabaseClass { found: String },

    /// A driver whose databases lack the method `name`.
    #[error(
        "a driver whose databases have no `{name}` method is refused, since the Node.js executor calls it; better-sqlite3's \
         `Database` has every method it calls"
    )]
    NoDatabaseMethod { name: &'static str },

    /// A database whose statements lack the method `name`.
    #[error(
        "a database whose statements have no `{name}` method is refused, since the Node.js executor calls it; \
         better-sqlite3's statements have every method it calls"
    )]
    NoStatementMethod { name: &'static str },

    /// A database whose statements hand an INTEGER over as a number though
    /// asked for BigInts.
    #[error(
        "a database whose statements hand INTEGERs over as JavaScript numbers is refused, since a number cannot hold every \
         INTEGER exactly, nor be told from a REAL; the Node.js executor asks for BigInts through better-sqlite3's \
         safeIntegers"
    )]
    NoExactIntegers,

    /// A database whose text encoding is not UTF-8.
    #[error("a database whose text encoding is not UTF-8 is refused, since the Node.js executor reads TEXT as UTF-8 bytes")]
    NotUtf8,

    /// A database whose SQLite reads a double-quoted name that matches no
    /// column as a string literal, where every connection the engine opens
    /// natively refuses such a name.
    #[error(
        "a database whose SQLite reads a double-quoted name that matches no column as a string literal is refused, since the \
         engine refuses such a name natively; use a better-sqlite3 whose SQLite has double-quoted string literals off, as \
         its own build does"
    )]
    DoubleQuotedStringLiterals,

    /// A value, a row or a result the database handed over that the executor
    /// cannot read as SQLite's; the text says what arrived, and where.
    #[error("the database handed over {0}, which the Node.js executor cannot read")]
    Unreadable(String),
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    /// The refusals the Node.js executor makes as rusqlite does read in
    /// rusqlite's words, and `refusal` answers each of rusqlite's own with
    /// the same refusal. The count of parameters a call needs, which rusqlite
    /// names, the Node.js executor finds only to be more than it passed; and
    /// a call that passes too many values is no refusal.
    #[test]
    fn the_refusals_rusqlite_makes_read_in_its_words() {
        let not_utf8 = String::from_utf8(vec![0xff, b'A']).unwrap_err().utf8_error();
        let refusals = [
            (rusqlite::Error::MultipleStatement, Refusal::MultipleStatements),
            (rusqlite::Error::ExecuteReturnedResults, Refusal::ExecuteReturnedRows),
            (
                rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(not_utf8)),
                Refusal::TextNotUtf8 { column: 1, error: not_utf8 },
            ),
        ];
        for (native, refusal) in refusals {
            assert_eq!(native.to_string(), refusal.to_string());
            assert_eq!(SqliteError::from(native).refusal(), Some(refusal));
        }
        let too_few = rusqlite::Error::InvalidParameterCount(1, 2);
        let words = "Wrong number of parameters passed to query. Got 1, needed ";
        assert!(too_few.to_string().starts_with(words), "{too_few}");
        assert!(Refusal::TooFewValues { passed: 1 }.to_string().starts_with(words));
        assert_eq!(SqliteError::from(too_few).refusal(), Some(Refusal::TooFewValues { passed: 1 }));
        assert_eq!(SqliteError::from(rusqlite::Error::InvalidParameterCount(3, 2)).refusal(), None);
    }
}
