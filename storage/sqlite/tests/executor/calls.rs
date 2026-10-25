//! What `execute`, `query` and `query_row` run and return, on every executor.

use ankurah_storage_sqlite::{executor::SqliteExecutor, Refusal, SqliteError, SqliteValueRef};
use anyhow::Result;

use crate::common::{self, engine_test, TempDb};

/// `query_row` decodes a statement's first row and no row past it, on every
/// driver, as rusqlite's does: a later row whose TEXT is not UTF-8 fails
/// `query`, and not `query_row`.
#[engine_test]
async fn query_row_reads_no_row_past_the_first() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (first, every) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE texts (id INTEGER PRIMARY KEY, v TEXT)", &[])?;
            executor.execute("INSERT INTO texts VALUES (1, 'valid'), (2, CAST(x'ff41' AS TEXT))", &[])?;
            let sql = "SELECT v FROM texts ORDER BY id";
            let first = executor.query_row(sql, &[])?.expect("a first row").text(0)?.to_owned();
            Ok((first, executor.query(sql, &[]).map(|rows| rows.len()).map_err(|error| error.refusal())))
        })
        .await?;
    assert_eq!(first, "valid");
    assert!(matches!(every, Err(Some(Refusal::TextNotUtf8 { column: 0, .. }))), "the second row's TEXT is not UTF-8: {every:?}");
    Ok(())
}

/// `execute` runs a statement for its effect, and refuses one whose first
/// step yields a row, as rusqlite's does, so a query cannot pass for a write.
#[engine_test]
async fn execute_refuses_a_statement_that_yields_a_row() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let outcomes = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (k INTEGER PRIMARY KEY)", &[])?;
            Ok([
                "SELECT 1",
                "PRAGMA busy_timeout",
                "INSERT INTO things (k) VALUES (1) RETURNING k",
                "SELECT 1 WHERE 0",
                "INSERT INTO things (k) VALUES (2), (3)",
            ]
            .map(|sql| executor.execute(sql, &[]).map_err(|error| error.refusal())))
        })
        .await?;
    for refused in &outcomes[..3] {
        assert_eq!(refused, &Err(Some(Refusal::ExecuteReturnedRows)), "a statement that yields a row is refused");
    }
    assert!(outcomes[3].is_ok(), "a statement whose step yields no row runs: {:?}", outcomes[3]);
    assert_eq!(outcomes[4], Ok(2));
    Ok(())
}

/// `query` runs a statement that yields no rows for its effect and returns
/// none, as rusqlite's does, on every driver.
#[engine_test]
async fn query_runs_a_statement_that_yields_no_rows() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (rows, count) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (k INTEGER PRIMARY KEY)", &[])?;
            let rows = executor.query("INSERT INTO things (k) VALUES (1)", &[])?.len();
            Ok((rows, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    assert_eq!((rows, count), (0, 1), "the insert ran and yielded no rows");
    Ok(())
}

/// `execute` returns SQLite's changes() on every driver: the rows an INSERT,
/// UPDATE or DELETE changed, and after any other statement the number the
/// last of those changed, as rusqlite returns it (better-sqlite3's own count
/// for such a statement is 0).
#[engine_test]
async fn execute_returns_the_count_of_changes_rusqlite_returns() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let counts = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (k INTEGER PRIMARY KEY)", &[])?;
            Ok([
                executor.execute("INSERT INTO things (k) VALUES (1), (2), (3)", &[])?,
                executor.execute("CREATE TABLE others (k)", &[])?,
                executor.execute("BEGIN", &[])?,
                executor.execute("COMMIT", &[])?,
                executor.execute("UPDATE things SET k = k WHERE k > 5", &[])?,
                executor.execute("CREATE INDEX others_k ON others (k)", &[])?,
            ])
        })
        .await?;
    assert_eq!(counts, [3, 3, 3, 3, 0, 0]);
    Ok(())
}

/// A PRAGMA that sets a value and reports it as TEXT returns the TEXT on
/// every driver: on Node.js, which cannot nest a PRAGMA in its typed reader, as
/// the driver's string.
#[engine_test]
async fn a_pragma_that_sets_a_value_reports_it() -> Result<()> {
    let db = TempDb::new("journal_mode");
    let storage = common::open(db.path()).await?;
    let mode = storage
        .with_executor(|executor| Ok(executor.query_row("PRAGMA journal_mode=DELETE", &[])?.expect("one row").text(0)?.to_owned()))
        .await?;
    assert_eq!(mode, "delete");
    Ok(())
}

/// A statement read again after the schema changes reads as the schema now
/// stands, though the Node.js executor keeps the reader it prepared for the
/// statement: `SELECT *` gains the column its table gained, in the `main`
/// database or in an attached one, and a TEMP table of the same name takes
/// the table's place.
#[engine_test]
async fn a_statement_read_again_follows_a_schema_change() -> Result<()> {
    fn width_of_every_column(executor: &dyn SqliteExecutor, table: &str) -> Result<usize, SqliteError> {
        Ok(executor.query_row(&format!("SELECT * FROM {table}"), &[])?.expect("one row").values().len())
    }
    let storage = common::open_in_memory().await?;
    let widths = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (a)", &[])?;
            executor.execute("INSERT INTO things (a) VALUES (1)", &[])?;
            let before = width_of_every_column(executor, "things")?;
            executor.execute("ALTER TABLE things ADD COLUMN b", &[])?;
            let gained = width_of_every_column(executor, "things")?;
            executor.execute("CREATE TEMP TABLE things (a, b, c)", &[])?;
            executor.execute("INSERT INTO temp.things VALUES (1, 2, 3)", &[])?;
            let shadowed = width_of_every_column(executor, "things")?;
            // The Node.js executor watches the schema versions of `main` and
            // `temp` only, so its reader of the attached table fails, and is
            // prepared afresh.
            executor.execute("ATTACH DATABASE ':memory:' AS aux", &[])?;
            executor.execute("CREATE TABLE aux.more (a)", &[])?;
            executor.execute("INSERT INTO aux.more (a) VALUES (1)", &[])?;
            let attached = width_of_every_column(executor, "aux.more")?;
            executor.execute("ALTER TABLE aux.more ADD COLUMN b", &[])?;
            Ok([before, gained, shadowed, attached, width_of_every_column(executor, "aux.more")?])
        })
        .await?;
    assert_eq!(widths, [1, 2, 3, 1, 2]);
    Ok(())
}

/// An error SQLite raises carries its extended result code, the same on
/// every driver: better-sqlite3 names the code, and the name reads as its
/// number.
#[engine_test]
async fn errors_carry_the_extended_result_code() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let codes = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (k INTEGER PRIMARY KEY, v TEXT UNIQUE, n CHECK (n > 0))", &[])?;
            executor.execute("INSERT INTO things (k, v, n) VALUES (1, 'a', 1)", &[])?;
            let key = executor.execute("INSERT INTO things (k, v) VALUES (1, 'b')", &[]).expect_err("a duplicate key is refused");
            let unique = executor.execute("INSERT INTO things (k, v) VALUES (2, 'a')", &[]).expect_err("a duplicate value is refused");
            let check = executor.execute("INSERT INTO things (k, n) VALUES (3, 0)", &[]).expect_err("the CHECK is refused");
            let syntax = executor.query("SELEC 1", &[]).expect_err("a misspelt statement is refused");
            let row = executor.query_row("SELECT 'text'", &[])?.expect("one row").integer(0).expect_err("TEXT is not an INTEGER");
            let seven = SqliteValueRef::Integer(7);
            let too_many = executor.query("SELECT ?", &[seven, seven]).expect_err("a value no parameter takes is refused");
            let too_few = executor.query("SELECT ?, ?", &[seven]).expect_err("a parameter without a value is refused");
            Ok([key, unique, check, syntax, row, too_many, too_few].map(|error| error.sqlite_code()))
        })
        .await?;
    // SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE,
    // SQLITE_CONSTRAINT_CHECK, SQLITE_ERROR, and no code for an error SQLite
    // did not raise, such as a call passing too many or too few values, which
    // rusqlite and better-sqlite3 refuse themselves.
    assert_eq!(codes, [Some(1555), Some(2067), Some(275), Some(1), None, None, None]);
    Ok(())
}

/// A double-quoted name that matches no column is refused, in a query and in
/// DDL, on every driver, rather than read as the string literal SQLite makes
/// of it where double-quoted string literals are on.
#[engine_test]
async fn a_double_quoted_name_that_matches_no_column_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let refusals = storage
        .with_executor(|executor| {
            executor.execute(r#"CREATE TABLE things ("present" TEXT)"#, &[])?;
            executor.execute("INSERT INTO things VALUES ('x')", &[])?;
            Ok([
                executor.query(r#"SELECT "missing" FROM things"#, &[]).map(|rows| rows.len()),
                executor.execute(r#"CREATE INDEX things_missing ON things ("missing")"#, &[]),
            ]
            .map(|outcome| outcome.map_err(|error| (error.sqlite_code(), error.to_string()))))
        })
        .await?;
    for refusal in refusals {
        let (code, message) = refusal.expect_err("the name matches no column");
        assert!(message.contains(r#"no such column: "missing""#), "{message}");
        assert_eq!(code, Some(1), "SQLITE_ERROR");
    }
    Ok(())
}
