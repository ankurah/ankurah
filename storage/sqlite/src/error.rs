//! Error types for SQLite storage engine

use std::str::Utf8Error;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("SQLite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// An error SQLite itself raised, such as a constraint's failure or a
    /// syntax error, through either Node.js driver: what rusqlite reports
    /// natively as its `SqliteFailure`.
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error("SQLite error: {0}")]
    Sqlite(SqliteFailure),

    /// A call, or a database, that the Node.js executor refuses (see [`Refusal`]).
    #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
    #[error(transparent)]
    Refused(#[from] Refusal),

    /// A JavaScript exception that is no error SQLite raised, with its
    /// message: a driver's own refusal, such as better-sqlite3's of a call
    /// that passes too few values, a host function's exception, or a
    /// TypeError a driver throws.
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

    #[error("unexpected row shape: {0}")]
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

/// SQLite's result code for a misuse of its API, which it returns natively
/// for a step of SQL that holds no statement.
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
const SQLITE_MISUSE: i32 = 21;

impl SqliteError {
    /// SQLite's extended result code when SQLite itself refused a statement,
    /// such as 1555 for a duplicate primary key or 5 for a lock held past the
    /// busy timeout, the same whichever driver ran the statement: rusqlite
    /// and node:sqlite hand the code over as its number, and better-sqlite3
    /// by its name, which is read as the number sqlite3.h gives it. On
    /// better-sqlite3 a name that no SQLite release up to the crate's table
    /// of names defines, as a later release could add, has `None`. On Node.js
    /// the executor's refusal of SQL that holds no statement answers 21,
    /// SQLITE_MISUSE, the code SQLite refuses it with natively, and its
    /// refusal of a statement SQLite cannot nest answers the code of SQLite's
    /// error. `None` for every other error.
    ///
    /// A call that passes more values than its statement takes parameters
    /// differs between drivers: rusqlite and better-sqlite3 refuse it
    /// themselves, before SQLite binds anything, so their code is `None`,
    /// where node:sqlite's refusal is SQLite's SQLITE_RANGE, `Some(25)`. A
    /// call that passes too few has `None` on every driver, refused before
    /// SQLite runs by rusqlite, by the Node.js executor on node:sqlite, and by
    /// better-sqlite3.
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
            #[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
            SqliteError::Refused(Refusal::NoStatement) => Some(SQLITE_MISUSE),
            _ => None,
        }
    }

    /// The refusal this error is, when the executor refused a call, or on
    /// Node.js a database, rather than run or read it otherwise than rusqlite
    /// does: one question for every driver, as [`SqliteError::sqlite_code`]
    /// is for SQLite's result codes. Natively it answers rusqlite's own
    /// refusals of the kinds the Node.js executor makes as rusqlite does:
    /// `MultipleStatement`, `ExecuteReturnedResults`, `InvalidParameterCount`
    /// for a call that passes fewer values than its statement takes, and
    /// `FromSqlConversionFailure` for TEXT that is not UTF-8. On Node.js it
    /// answers the refusal the executor raised.
    ///
    /// `None` for every other error. That includes a call that passes too
    /// many values (see `sqlite_code`), on better-sqlite3 a call that passes
    /// too few, which better-sqlite3 refuses itself as a
    /// [`SqliteError::JavaScript`], and natively SQL that holds no statement,
    /// which SQLite refuses itself (see `sqlite_code`).
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

/// An error SQLite raised, as a Node.js driver hands it over.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct SqliteFailure {
    /// SQLite's extended result code, such as 1555 for
    /// SQLITE_CONSTRAINT_PRIMARYKEY: node:sqlite hands it over as its number,
    /// and better-sqlite3 by its name, which is read as the number sqlite3.h
    /// gives it. `None` for a name better-sqlite3 gives that no SQLite release
    /// up to the crate's table of names defines.
    pub code: Option<i32>,
    /// SQLite's message, such as "UNIQUE constraint failed: things.k".
    pub message: String,
}

/// The Node.js releases whose node:sqlite the Node.js executor runs on.
const NODE_JS_RELEASES: &str = "Node.js 22.20.0 or later on 22, 24.7.0 or later on 24, or a later release line";

/// A call, or on Node.js a database, that the engine's executor refuses
/// rather than run or read it otherwise than rusqlite does: what
/// [`SqliteError::refusal`] answers, on every driver.
///
/// The first four are rusqlite's own refusals, which the Node.js executor
/// makes as rusqlite does and in rusqlite's words, so that each reads the
/// same on every driver. Only the Node.js executor makes the others.
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
    /// statement takes, where the Node.js executor finds only that it takes
    /// more.
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

    /// SQL that holds no statement, only whitespace, semicolons and
    /// comments, refused before a driver prepares it: SQLite refuses to run
    /// such SQL, natively with SQLITE_MISUSE, which
    /// [`SqliteError::sqlite_code`] answers for this refusal too, and
    /// node:sqlite can crash the process once it collects a statement
    /// prepared from such SQL while its database is open.
    #[error(
        "SQL that holds no statement, only whitespace, semicolons and comments, is refused on Node.js, as SQLite refuses to \
         run it natively (SQLITE_MISUSE)"
    )]
    NoStatement,

    /// A compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the top
    /// level of a query, or in a scalar subquery there, refused before it
    /// runs, since the Node.js executor's typed reader could change one of
    /// its values (see `refuse_a_compound_select`).
    #[error(
        "a compound SELECT at the top level of a query, or in a scalar subquery there, is refused on Node.js, since one of \
         its INTEGERs could read back as a REAL; select from it as `WITH x AS MATERIALIZED (…) SELECT * FROM x`, which reads \
         the same on every driver"
    )]
    CompoundSelect,

    /// A statement with a named parameter (`:a`, `@a`, `#a` or `$a`), the
    /// first of which is `name`, refused before it runs: node:sqlite binds
    /// no positional value to one, leaving it NULL, and better-sqlite3 binds
    /// one only by name, where rusqlite binds a call's values to every
    /// parameter in turn.
    #[error(
        "a statement with the named parameter {name} is refused on Node.js, since neither Node.js driver binds a call's \
         values to it in turn, as rusqlite does; write a positional `?` for each parameter"
    )]
    NamedParameter { name: String },

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

    /// SQL that goes on past the statement SQLite prepared from it, though
    /// the scan before preparing found nothing past that statement but what
    /// SQLite skips: the driver would run that statement alone.
    #[error(
        "SQL that goes on past the statement SQLite prepared from it is refused on Node.js, since the driver would run \
         that statement alone; run each statement in a call of its own"
    )]
    SqlPastItsStatement,

    /// TEXT holding U+FFFD in column `column` of a PRAGMA's or an EXPLAIN's
    /// row, which SQLite would not nest in a typed reader, so that the driver
    /// hands its TEXT over as a string, in which U+FFFD may stand for a byte
    /// that is not UTF-8.
    #[error(
        "TEXT holding U+FFFD in column {column} of a PRAGMA or an EXPLAIN is refused on Node.js, since both Node.js drivers \
         hand such TEXT over as a string, in which U+FFFD may stand for a byte that is not UTF-8; select it through a \
         query, such as one of a table-valued pragma function like pragma_table_info, which reads TEXT as its bytes"
    )]
    ReplacementCharacter { column: usize },

    /// A database the Node.js executor cannot run on, or a runtime it cannot
    /// open one in.
    #[error(transparent)]
    Database(#[from] DatabaseRefusal),
}

/// A database the Node.js executor refuses, as a [`Refusal::Database`].
#[cfg(all(target_arch = "wasm32", feature = "nodejs"))]
impl From<DatabaseRefusal> for SqliteError {
    fn from(refusal: DatabaseRefusal) -> Self { SqliteError::Refused(Refusal::Database(refusal)) }
}

/// A database the Node.js executor cannot run on, refused as the engine
/// adopts it or opens it, or a runtime it cannot open one in. A value the
/// database hands over later that the executor cannot read is refused the
/// same way, as it is read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DatabaseRefusal {
    /// A runtime without what opening node:sqlite's `DatabaseSync` takes,
    /// `missing` naming what it lacks: the `process` global,
    /// `process.getBuiltinModule`, the `node:sqlite` module, or its
    /// `DatabaseSync` constructor.
    #[error(
        "a runtime without {missing} is refused, since it offers no node:sqlite DatabaseSync to open; run inside {}",
        NODE_JS_RELEASES
    )]
    NoNodeSqlite { missing: &'static str },

    /// A value handed over as a database that is not an object; `found` is
    /// how the value reads.
    #[error(
        "{found} is refused as a database, since it is not an object, such as node:sqlite's DatabaseSync, with `prepare` and \
         `exec` methods"
    )]
    NotAnObject { found: String },

    /// A database object without the method `name`.
    #[error("a database without a `{name}` method is refused, since the Node.js executor calls it")]
    NoDatabaseMethod { name: &'static str },

    /// A database whose statements lack the method `name`.
    #[error(
        "a database whose statements have no `{name}` method is refused, since the Node.js executor calls it; node:sqlite's \
         and better-sqlite3's statements have every method it calls"
    )]
    NoStatementMethod { name: &'static str },

    /// A database whose statements cannot hand rows over as arrays, which
    /// keep every column at its position in the statement.
    #[error(
        "a database whose statements cannot hand rows over as arrays is refused, since rows keyed by column name do not \
         keep every column at its position; its statements need setReturnArrays (node:sqlite's) or raw (better-sqlite3's)"
    )]
    NoPositionalRows,

    /// A database whose statements cannot hand INTEGERs over as BigInts, or
    /// hand one over as a number though asked for BigInts.
    #[error(
        "a database whose statements hand INTEGERs over as JavaScript numbers is refused, since a number cannot hold every \
         INTEGER exactly, nor be told from a REAL; its statements need setReadBigInts (node:sqlite's) or safeIntegers \
         (better-sqlite3's)"
    )]
    NoExactIntegers,

    /// A database whose statements report no SQL, which the executor reads
    /// to check where SQLite ended each statement.
    #[error(
        "a database whose statements report no SQL is refused, since the Node.js executor checks where SQLite ended each \
         statement; its statements need sourceSQL (node:sqlite's) or source (better-sqlite3's)"
    )]
    NoStatementSource,

    /// A database that skips a numbered parameter such as `?2` as it binds a
    /// call's values, as node:sqlite did before Node.js 22.20.0 on 22 and
    /// 24.7.0 on 24.
    #[error(
        "a database that skips a numbered parameter such as `?2` as it binds a call's values is refused, since that leaves \
         the parameter NULL where rusqlite binds the value at its position, as node:sqlite did before Node.js 22.20.0 on 22 \
         and 24.7.0 on 24; run inside {}",
        NODE_JS_RELEASES
    )]
    SkipsNumberedParameters,

    /// A database whose text encoding is not UTF-8.
    #[error("a database whose text encoding is not UTF-8 is refused, since the Node.js executor reads TEXT as UTF-8 bytes")]
    NotUtf8,

    /// A database that reads a double-quoted name that matches no column as
    /// a string literal, where every connection the engine opens natively
    /// refuses such a name.
    #[error(
        "a database that reads a double-quoted name that matches no column as a string literal is refused, since the engine \
         refuses such a name natively; turn double-quoted string literals off, as node:sqlite's DatabaseSync has them \
         unless it is constructed with `enableDoubleQuotedStringLiterals: true`"
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
