//! What every executor must agree on where values cross the driver boundary,
//! checked natively on rusqlite and on wasm32 on node:sqlite and better-sqlite3.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::{
    exec::{Behavior, SqliteExecutor},
    SqliteError, SqliteStorageEngine, SqliteValue, SqliteValueRef,
};
use anyhow::Result;
use common::{session_settings, LockHolder, Stopwatch, TempDb};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
// The same tests run natively under tokio, and on wasm32 under wasm-bindgen-test in Node.
#[cfg(not(target_arch = "wasm32"))]
use tokio::test as engine_test;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as engine_test;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Stamp {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub timestamp: i64,
}

/// INTEGER parameters bind exactly, so an i64 past 2^53 is stored, compared
/// and ordered exactly in SQL (on Node they cross the boundary as BigInt).
#[engine_test]
async fn integers_past_the_double_range_are_exact() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let values = [
        ("negative odd", -9_007_199_254_740_993_i64),
        ("negative even", -9_007_199_254_740_992),
        ("at", 9_007_199_254_740_992),
        ("odd", 9_007_199_254_740_993),
        ("next", 9_007_199_254_740_994),
    ];
    let trx = ctx.begin();
    for (name, timestamp) in values {
        trx.create(&Stamp { name: name.to_string(), timestamp }).await?;
    }
    trx.commit().await?;

    // The odd values cannot be represented as doubles. Each must remain
    // distinct from its even neighbor in both stored values and query binds.
    for (name, timestamp) in values {
        let query = format!("timestamp = {timestamp}");
        let exact: Vec<StampView> = ctx.fetch(query.as_str()).await?;
        assert_eq!(exact.len(), 1, "{query}");
        assert_eq!(exact[0].name()?, name);
    }

    let past: Vec<StampView> = ctx.fetch("timestamp > 9007199254740992").await?;
    assert_eq!(past.len(), 2);

    let ordered: Vec<StampView> = ctx.fetch("timestamp >= -9007199254740993 ORDER BY timestamp ASC").await?;
    let timestamps: Vec<i64> = ordered.iter().map(|stamp| stamp.timestamp().unwrap()).collect();
    assert_eq!(timestamps, values.map(|(_, timestamp)| timestamp));
    Ok(())
}

/// A REAL reads as a REAL even when its value is integral, and an INTEGER
/// read refuses it, as rusqlite's does: no read converts between storage
/// classes.
#[engine_test]
async fn integral_reals_read_as_reals() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let row = storage.with_executor(|executor| Ok(executor.query_row("SELECT 2.0, -0.0, 2", &[])?.expect("one row"))).await?;
    assert!(
        matches!(row.values(), [SqliteValue::Real(two), SqliteValue::Real(zero), SqliteValue::Integer(2)] if *two == 2.0 && *zero == 0.0),
        "{row:?}"
    );
    assert!(row.integer(0).is_err(), "an INTEGER read refuses the REAL 2.0");
    Ok(())
}

/// A connection the engine opens waits up to five seconds for another
/// writer's lock before failing, on every driver: rusqlite sets that timeout
/// on each connection it opens, and the node executor sets it with its
/// session pragmas.
#[engine_test]
async fn connections_wait_five_seconds_for_a_lock() -> Result<()> {
    // The engine opens no better-sqlite3 database, and one it adopts keeps
    // the busy timeout its host set, so the better-sqlite3 run skips this.
    if common::on_better_sqlite3() {
        return Ok(());
    }
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let timeout = storage.with_executor(|executor| executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)).await?;
    assert_eq!(timeout, 5000);
    Ok(())
}

/// Read the SQL values themselves; model getters decode canonical state and
/// would not detect a driver rounding its returned INTEGER values.
#[engine_test]
async fn integer_results_preserve_all_bits() -> Result<()> {
    let storage = common::open_in_memory().await?;
    storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE exact_integers (value INTEGER)", &[])?;
            for value in [i64::MIN, -9_007_199_254_740_993, 9_007_199_254_740_993, i64::MAX] {
                executor.execute("INSERT INTO exact_integers VALUES (?)", &[SqliteValueRef::Integer(value)])?;
                let row =
                    executor.query_row("SELECT value FROM exact_integers WHERE value = ?", &[SqliteValueRef::Integer(value)])?.unwrap();
                assert_eq!(row.integer(0)?, value);
            }
            Ok(())
        })
        .await?;
    Ok(())
}

#[engine_test]
async fn duplicate_column_names_keep_both_positions() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let row = storage.with_executor(|executor| Ok(executor.query_row("SELECT 1 AS x, 2 AS x", &[])?.unwrap())).await?;
    assert_eq!(row.values().len(), 2);
    assert_eq!(row.integer(0)?, 1);
    assert_eq!(row.integer(1)?, 2);
    Ok(())
}

/// TEXT crosses the boundary as its bytes: a NUL inside it is kept, and TEXT
/// whose bytes are not UTF-8 is refused, in rusqlite's words, on every
/// driver. (node:sqlite's own strings would cut the first value at its NUL
/// and turn the second into U+FFFD.)
#[engine_test]
async fn text_keeps_an_embedded_nul_and_refuses_bytes_that_are_not_utf8() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (bound, cast, stored, length, replacement) = storage
        .with_executor(|executor| {
            // The text is bound twice, to bare `?` placeholders: better-sqlite3
            // binds a positional value to no numbered one such as `?1`.
            let text = SqliteValueRef::Text("a\0b");
            let row = executor.query_row("SELECT ?, CAST(x'610062' AS TEXT), length(CAST(? AS BLOB))", &[text, text])?.expect("one row");
            executor.execute("CREATE TABLE texts (t TEXT)", &[])?;
            executor.execute("INSERT INTO texts (t) VALUES (?)", &[SqliteValueRef::Text("a\0b")])?;
            let stored = executor.query_row("SELECT t FROM texts", &[])?.expect("one row");
            let replacement = executor.query_row("SELECT ?", &[SqliteValueRef::Text("\u{FFFD}")])?.expect("one row");
            Ok((
                row.text(0)?.to_owned(),
                row.text(1)?.to_owned(),
                stored.text(0)?.to_owned(),
                row.integer(2)?,
                replacement.text(0)?.to_owned(),
            ))
        })
        .await?;
    assert_eq!([bound.as_str(), cast.as_str(), stored.as_str()], ["a\0b"; 3]);
    assert_eq!(length, 3, "the NUL is stored");
    assert_eq!(replacement, "\u{FFFD}", "a U+FFFD that is really there reads as itself");

    let error =
        storage.with_executor(|executor| executor.query("SELECT 'ok', CAST(x'ff41' AS TEXT)", &[])).await.expect_err("0xff is not UTF-8");
    assert!(
        error.to_string().ends_with("Conversion error from type Text at index: 1, invalid utf-8 sequence of 1 bytes from index 0"),
        "{error}"
    );
    Ok(())
}

/// A row lends its TEXT and BLOB values, each read pointing at the one copy
/// the row holds rather than at a fresh one.
#[engine_test]
async fn rows_lend_their_text_and_blob_values() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let row = storage.with_executor(|executor| Ok(executor.query_row("SELECT 'text', x'0102'", &[])?.expect("one row"))).await?;
    assert!(std::ptr::eq(row.text(0)?, row.text(0)?));
    assert!(std::ptr::eq(row.blob(1)?, row.blob(1)?));
    Ok(())
}

/// A statement read again after the schema changes reads as the schema now
/// stands, though the node executor keeps the reader it prepared for the
/// statement: `SELECT *` gains the column its table gained, and a TEMP table
/// of the same name takes the table's place.
#[engine_test]
async fn a_statement_read_again_follows_a_schema_change() -> Result<()> {
    fn width_of_every_column(executor: &dyn SqliteExecutor) -> Result<usize, SqliteError> {
        Ok(executor.query_row("SELECT * FROM things", &[])?.expect("one row").values().len())
    }
    let storage = common::open_in_memory().await?;
    let widths = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (a)", &[])?;
            executor.execute("INSERT INTO things (a) VALUES (1)", &[])?;
            let before = width_of_every_column(executor)?;
            executor.execute("ALTER TABLE things ADD COLUMN b", &[])?;
            let gained = width_of_every_column(executor)?;
            executor.execute("CREATE TEMP TABLE things (a, b, c)", &[])?;
            executor.execute("INSERT INTO temp.things VALUES (1, 2, 3)", &[])?;
            Ok([before, gained, width_of_every_column(executor)?])
        })
        .await?;
    assert_eq!(widths, [1, 2, 3]);
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
            .map(|sql| executor.execute(sql, &[]).map_err(|error| error.to_string())))
        })
        .await?;
    for refused in &outcomes[..3] {
        let error = refused.as_ref().expect_err("a statement that yields a row is refused");
        assert!(error.ends_with("Execute returned results - did you mean to call query?"), "{error}");
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

/// An error SQLite raises carries its extended result code on rusqlite and
/// on node:sqlite alike, and none on better-sqlite3, which names the code
/// without its number.
#[engine_test]
async fn errors_carry_sqlite_s_extended_result_code() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let codes = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (k INTEGER PRIMARY KEY, v TEXT UNIQUE)", &[])?;
            executor.execute("INSERT INTO things (k, v) VALUES (1, 'a')", &[])?;
            let key = executor.execute("INSERT INTO things (k, v) VALUES (1, 'b')", &[]).expect_err("a duplicate key is refused");
            let unique = executor.execute("INSERT INTO things (k, v) VALUES (2, 'a')", &[]).expect_err("a duplicate value is refused");
            let syntax = executor.query("SELEC 1", &[]).expect_err("a misspelt statement is refused");
            let row = executor.query_row("SELECT 'text'", &[])?.expect("one row").integer(0).expect_err("TEXT is not an INTEGER");
            Ok([key, unique, syntax, row].map(|error| error.sqlite_code()))
        })
        .await?;
    // SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE, SQLITE_ERROR, and
    // no code for an error SQLite did not raise.
    let expected = if common::on_better_sqlite3() { [None; 4] } else { [Some(1555), Some(2067), Some(1), None] };
    assert_eq!(codes, expected);
    Ok(())
}

/// A connection the engine opens on a file takes the session settings
/// connection.rs gives a native one, on every driver.
#[engine_test]
async fn a_connection_on_a_file_takes_the_session_settings() -> Result<()> {
    // The engine opens no better-sqlite3 database, and one it adopts keeps
    // the session settings its host chose, so the better-sqlite3 run skips this.
    if common::on_better_sqlite3() {
        return Ok(());
    }
    let db = TempDb::new("settings");
    let storage = SqliteStorageEngine::open(db.path()).await?;
    assert_eq!(session_settings(&storage).await?, (5000, "wal".to_owned(), 1, 1, -64000, 268_435_456, 2));
    Ok(())
}

/// How long the other connection in the lock tests holds its lock: long
/// enough for a wait to show, and well inside the 5000 ms busy timeout.
const HOLD_MS: u64 = 400;

/// A writer waits for another connection's write lock on the same file and
/// takes it once released, rather than failing at once.
#[engine_test]
async fn a_writer_waits_for_another_connection_s_lock() -> Result<()> {
    let db = TempDb::new("writer_waits");
    let storage = common::open(db.path()).await?;
    let holder = LockHolder::take(&db, "BEGIN IMMEDIATE", HOLD_MS).await;
    let waited = Stopwatch::start();
    storage.with_executor(|executor| executor.begin(Behavior::Immediate)?.commit()).await?;
    let waited = waited.elapsed_ms();
    holder.released().await;
    assert!(waited >= HOLD_MS as f64 / 2.0, "the writer took the lock after {waited} ms, before it was released");
    Ok(())
}

/// Opening a store waits for a lock another connection holds on its file
/// rather than failing at once: the busy timeout is set before any statement
/// that needs the file.
#[engine_test]
async fn opening_a_store_waits_for_a_lock_on_its_file() -> Result<()> {
    // The engine opens no better-sqlite3 database, and one it adopts keeps
    // the busy timeout its host set, so the better-sqlite3 run skips this.
    if common::on_better_sqlite3() {
        return Ok(());
    }
    let db = TempDb::new("open_waits");
    // EXCLUSIVE in the file's first, rollback journal keeps out readers too.
    let holder = LockHolder::take(&db, "BEGIN EXCLUSIVE", HOLD_MS).await;
    let waited = Stopwatch::start();
    let storage = SqliteStorageEngine::open(db.path()).await?;
    let waited = waited.elapsed_ms();
    holder.released().await;
    assert!(waited >= HOLD_MS as f64 / 2.0, "the store opened after {waited} ms, before the lock was released");
    assert_eq!(session_settings(&storage).await?.1, "wal");
    Ok(())
}
