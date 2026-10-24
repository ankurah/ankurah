//! What every executor must agree on where values cross the driver boundary,
//! checked natively on rusqlite and on wasm32 on node:sqlite and better-sqlite3.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::{
    executor::{Behavior, Row, SqliteExecutor},
    SqliteError, SqliteStorageEngine, SqliteValue, SqliteValueRef,
};
use anyhow::Result;
use common::{session_settings, LockHolder, Stopwatch, TempDb};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
// The same tests run natively under tokio, and on wasm32 under wasm-bindgen-test in Node.js.
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
/// and ordered exactly in SQL (on Node.js they cross the boundary as BigInt).
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

/// A connection the engine opens sets a busy timeout of five seconds, on
/// every driver: rusqlite sets it on each connection it opens, and the Node.js
/// executor before any other statement. That a writer then waits for
/// another's lock is `a_writer_waits_for_another_connection_s_lock`.
#[engine_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the busy timeout its host set"
)]
async fn connections_set_a_busy_timeout_of_five_seconds() -> Result<()> {
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

/// A query that yields more than half of SQLite's column limit, 1000 of the
/// default 2000 columns, is refused on Node.js before it runs, saying so, since
/// the typed reader reads each column as two; rusqlite reads it. A query of
/// 1000 columns reads on every driver.
#[engine_test]
async fn a_query_of_more_than_a_thousand_columns_is_refused_on_nodejs() -> Result<()> {
    let columns = |count: usize| format!("SELECT {}", (0..count).map(|index| index.to_string()).collect::<Vec<_>>().join(", "));
    let storage = common::open_in_memory().await?;
    let (thousand, more) = storage
        .with_executor(move |executor| {
            let width = |sql: String| {
                executor.query_row(&sql, &[]).map(|row| row.expect("one row").values().len()).map_err(|error| error.to_string())
            };
            Ok((width(columns(1000)), width(columns(1001))))
        })
        .await?;
    assert_eq!(thousand, Ok(1000));
    if cfg!(target_arch = "wasm32") {
        let refusal = more.expect_err("Node.js refuses a query of 1001 columns");
        assert!(refusal.contains("the statement yields 1001 columns") && refusal.contains("1000 of the default 2000"), "{refusal}");
    } else {
        assert_eq!(more, Ok(1001));
    }
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

/// Empty TEXT and an empty BLOB, bound or written as literals, read back as
/// they were stored on every driver: the Node.js executor's typed reader reads
/// TEXT as the BLOB of its bytes, and SQLite hands a zero-length BLOB over
/// with a null pointer, which a driver could read as NULL.
#[engine_test]
async fn empty_text_and_an_empty_blob_read_back_as_stored() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (literals, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE empties (t TEXT, b BLOB)", &[])?;
            executor.execute("INSERT INTO empties VALUES (?, ?)", &[SqliteValueRef::Text(""), SqliteValueRef::Blob(&[])])?;
            let literals = executor.query_row("SELECT '', x'', zeroblob(0)", &[])?.expect("one row");
            let stored = executor.query_row("SELECT t, b, typeof(t), typeof(b) FROM empties", &[])?.expect("one row");
            Ok((format!("{:?}", literals.values()), format!("{:?}", stored.values())))
        })
        .await?;
    assert_eq!(literals, r#"[Text(""), Blob([]), Blob([])]"#);
    assert_eq!(stored, r#"[Text(""), Blob([]), Text("text"), Text("blob")]"#);
    Ok(())
}

/// `query_row` reads a statement's first row and no row past it, on every
/// driver, as rusqlite's does: a later row that cannot be read fails `query`,
/// and not `query_row`.
#[engine_test]
async fn query_row_reads_no_row_past_the_first() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (first, every) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE texts (id INTEGER PRIMARY KEY, v TEXT)", &[])?;
            executor.execute("INSERT INTO texts VALUES (1, 'valid'), (2, CAST(x'ff41' AS TEXT))", &[])?;
            let sql = "SELECT v FROM texts ORDER BY id";
            let first = executor.query_row(sql, &[])?.expect("a first row").text(0)?.to_owned();
            Ok((first, executor.query(sql, &[]).map(|rows| rows.len()).map_err(|error| error.to_string())))
        })
        .await?;
    assert_eq!(first, "valid");
    let refusal = every.expect_err("the second row's TEXT is not UTF-8");
    assert!(refusal.ends_with("Conversion error from type Text at index: 0, invalid utf-8 sequence of 1 bytes from index 0"), "{refusal}");
    Ok(())
}

/// A statement read again after the schema changes reads as the schema now
/// stands, though the Node.js executor keeps the reader it prepared for the
/// statement: `SELECT *` gains the column its table gained, in main or in an
/// attached database, and a TEMP table of the same name takes the table's
/// place.
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
            // The Node.js executor watches the schema versions of main and temp
            // only, so its reader of the attached table fails, and is
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
            let seven = SqliteValueRef::Integer(7);
            let too_many = executor.query("SELECT ?", &[seven, seven]).expect_err("a value no parameter takes is refused");
            let too_few = executor.query("SELECT ?, ?", &[seven]).expect_err("a parameter without a value is refused");
            Ok([key, unique, syntax, row, too_many, too_few].map(|error| error.sqlite_code()))
        })
        .await?;
    // SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE, SQLITE_ERROR, no
    // code for an error SQLite did not raise, and for a call passing too many
    // values SQLITE_RANGE on node:sqlite alone, which lets SQLite refuse it.
    let expected = if common::on_better_sqlite3() {
        [None; 6]
    } else if cfg!(target_arch = "wasm32") {
        [Some(1555), Some(2067), Some(1), None, Some(25), None]
    } else {
        [Some(1555), Some(2067), Some(1), None, None, None]
    };
    assert_eq!(codes, expected);
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
        // SQLITE_ERROR, which better-sqlite3 names without its number.
        assert_eq!(code, if common::on_better_sqlite3() { None } else { Some(1) });
    }
    Ok(())
}

/// A compound SELECT at the top level of a query, or in a scalar subquery
/// there, is refused on Node.js before it runs, where rusqlite reads its values
/// directly: Node.js's typed reader would read a later SELECT's INTEGER past
/// 2^53 with the affinity of the first SELECT's REAL column, or of a scalar
/// subquery's last SELECT's, as the nearest REAL. A compound in an EXISTS is
/// refused on Node.js too, though it reads the same everywhere. Selected from a
/// materialized CTE, or from a subquery or CTE that SQLite does not flatten,
/// where SQLite applies the first SELECT's affinity on every driver, or in
/// an IN, the compound reads the same everywhere.
#[engine_test]
async fn a_compound_select_reads_exactly_or_is_refused() -> Result<()> {
    fn values(rows: Vec<Row>) -> Vec<String> {
        let mut values: Vec<String> = rows.iter().flat_map(|row| row.values().iter().map(|value| format!("{value:?}"))).collect();
        values.sort();
        values
    }
    let storage = common::open_in_memory().await?;
    let (refused, same) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE reals (r REAL)", &[])?;
            executor.execute("INSERT INTO reals VALUES (1.5)", &[])?;
            executor.execute("CREATE TABLE integers (i INTEGER)", &[])?;
            executor.execute("INSERT INTO integers VALUES (9007199254740993)", &[])?;
            // Each statement Node.js refuses, with the values rusqlite reads.
            static EXACT: [&str; 2] = ["Integer(9007199254740993)", "Real(1.5)"];
            let refused = [
                ("SELECT r FROM reals UNION ALL SELECT i FROM integers", &EXACT[..]),
                // SQLite plans these two as a merge of their SELECTs: the
                // first for its ORDER BY, the second in SQLite 3.53, the
                // release better-sqlite3 bundles.
                ("SELECT r FROM reals UNION ALL SELECT i FROM integers ORDER BY 1", &EXACT[..]),
                ("SELECT r FROM reals UNION SELECT i FROM integers", &EXACT[..]),
                ("SELECT (SELECT i FROM integers UNION ALL SELECT r FROM reals)", &EXACT[..1]),
                ("SELECT (SELECT (SELECT i FROM integers UNION ALL SELECT r FROM reals))", &EXACT[..1]),
                ("SELECT EXISTS (SELECT i FROM integers UNION ALL SELECT r FROM reals)", &["Integer(1)"][..]),
            ]
            .map(|(sql, native)| (executor.query(sql, &[]).map(values).map_err(|error| error.to_string()), native));
            // Each statement every driver reads, with the values it reads.
            static ROUNDED: [&str; 2] = ["Real(1.5)", "Real(9007199254740992.0)"];
            let same = [
                ("WITH x AS MATERIALIZED (SELECT r FROM reals UNION ALL SELECT i FROM integers) SELECT * FROM x", &ROUNDED[..]),
                // SQLite flattens neither, since the columns of the
                // compound's SELECTs differ in affinity.
                ("SELECT * FROM (SELECT r FROM reals UNION ALL SELECT i FROM integers)", &ROUNDED[..]),
                ("WITH x AS (SELECT r FROM reals UNION ALL SELECT i FROM integers) SELECT * FROM x", &ROUNDED[..]),
                ("SELECT v FROM (SELECT (SELECT i FROM integers UNION ALL SELECT r FROM reals) AS v)", &ROUNDED[1..]),
                ("SELECT 9007199254740993 IN (SELECT r FROM reals UNION ALL SELECT i FROM integers)", &["Integer(1)"][..]),
            ]
            .map(|(sql, expected)| (sql, executor.query(sql, &[]).map(values), expected));
            Ok((refused, same))
        })
        .await?;
    for (read, native) in refused {
        if cfg!(target_arch = "wasm32") {
            let refusal = read.expect_err("Node.js refuses the compound SELECT");
            assert!(refusal.contains("WITH x AS MATERIALIZED"), "{refusal}");
        } else {
            assert_eq!(read.expect("rusqlite reads a compound SELECT directly"), native);
        }
    }
    for (sql, read, expected) in same {
        assert_eq!(read?, expected, "{sql}");
    }
    Ok(())
}

/// A call passes a value for every parameter of its statement, and no more,
/// on every driver: one passing too few is refused before it runs, where
/// node:sqlite would bind NULL for each value missing, and so is one passing
/// too many. A numbered parameter counts as its number.
#[engine_test]
async fn a_call_passes_a_value_for_every_parameter() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (passed, refused, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE pairs (a, b)", &[])?;
            let seven = SqliteValueRef::Integer(7);
            // Each statement runs first with every value it takes, so that a
            // count kept from that run cannot let a shorter call through.
            let passed = [
                executor.query("SELECT ?, ?", &[seven, seven]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven, seven]),
            ];
            let refused = [
                executor.query("SELECT ?, ?", &[seven]).map(|rows| rows.len()),
                executor.query("SELECT ?, ?", &[]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven]),
                executor.query("SELECT ?3", &[seven, seven]).map(|rows| rows.len()),
                executor.query("SELECT ?, ?", &[seven; 3]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven; 3]),
            ];
            let stored = executor.query_row("SELECT count(*), count(b) FROM pairs", &[])?.expect("one row");
            Ok((
                passed.map(|outcome| outcome.map_err(|error| error.to_string())),
                refused.map(|outcome| outcome.map_err(|error| error.to_string())),
                (stored.integer(0)?, stored.integer(1)?),
            ))
        })
        .await?;
    assert_eq!(passed, [Ok(1), Ok(1)]);
    let refused = refused.map(|outcome| outcome.expect_err("the call is refused"));
    // rusqlite's words for too few values, which the Node.js executor uses too
    // on node:sqlite; better-sqlite3 refuses the call in its own.
    let too_few = if common::on_better_sqlite3() {
        "Too few parameter values were provided"
    } else {
        "Wrong number of parameters passed to query. Got 1"
    };
    assert!(refused[0].contains(too_few), "{}", refused[0]);
    assert_eq!(stored, (1, 1), "only the insert given both values wrote a row");
    Ok(())
}

/// A numbered parameter takes the value at its position on rusqlite and
/// node:sqlite, given a value for every position up to its number, one that
/// no parameter takes included; better-sqlite3 binds no positional value to
/// one, and refuses the call (see `SqliteExecutor`).
#[engine_test]
async fn a_numbered_parameter_takes_the_value_at_its_position() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (selected, inserted, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE pairs (a, b)", &[])?;
            let [seven, eight, nine] = [7, 8, 9].map(SqliteValueRef::Integer);
            let selected = executor.query("SELECT ?2, ?1", &[seven, eight]).map(|rows| format!("{:?}", rows[0].values()));
            let inserted = executor.execute("INSERT INTO pairs VALUES (?, ?3)", &[seven, eight, nine]);
            let stored = executor.query("SELECT a, b FROM pairs", &[])?;
            let stored = stored.iter().map(|row| Ok((row.integer(0)?, row.integer(1)?))).collect::<Result<Vec<_>, SqliteError>>()?;
            Ok((selected.map_err(|error| error.to_string()), inserted.map_err(|error| error.to_string()), stored))
        })
        .await?;
    if common::on_better_sqlite3() {
        for refusal in [selected.expect_err("better-sqlite3 refuses ?2"), inserted.expect_err("better-sqlite3 refuses ?3")] {
            assert!(refusal.ends_with("Too many parameter values were provided"), "{refusal}");
        }
        assert_eq!(stored, []);
    } else {
        assert_eq!(selected.as_deref(), Ok("[Integer(8), Integer(7)]"));
        assert_eq!(inserted, Ok(1));
        assert_eq!(stored, [(7, 9)]);
    }
    Ok(())
}

/// SQL that holds a second statement is refused before either runs, on
/// every driver, where rusqlite and node:sqlite ran the first and ignored the
/// rest. Whitespace, semicolons and comments after a statement are no second
/// statement, except that better-sqlite3 refuses a query with a comment after
/// its semicolon (see `SqliteExecutor`).
#[engine_test]
async fn sql_holding_a_second_statement_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (refused, created, accepted, commented) = storage
        .with_executor(|executor| {
            let refused = [
                executor.execute("CREATE TABLE a (x); CREATE TABLE b (x)", &[]),
                // The second statement cannot even be prepared until the first has run.
                executor.execute("CREATE TABLE c (x); INSERT INTO c VALUES (1)", &[]),
                executor.query("SELECT 1; SELECT 2", &[]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.map_err(|error| error.to_string()));
            let created = executor
                .query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('a', 'b', 'c')", &[])?
                .expect("one row")
                .integer(0)?;
            executor.execute("CREATE TABLE d (x)", &[])?;
            let accepted = [
                executor.execute("INSERT INTO d VALUES (1);  ;; /* a comment */ -- and another", &[]),
                executor.query("SELECT 1; \n", &[]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.map_err(|error| error.to_string()));
            let commented = executor.query("SELECT 1; -- a note", &[]).map(|rows| rows.len()).map_err(|error| error.to_string());
            Ok((refused, created, accepted, commented))
        })
        .await?;
    for outcome in refused {
        let error = outcome.expect_err("a second statement is refused");
        // rusqlite's words, which the Node.js executor uses too.
        assert!(error.ends_with("Multiple statements provided"), "{error}");
    }
    assert_eq!(created, 0, "no statement of a refused string ran");
    assert_eq!(accepted, [Ok(1), Ok(1)]);
    if common::on_better_sqlite3() {
        commented.expect_err("the typed reader cannot nest the comment better-sqlite3 keeps after the semicolon");
    } else {
        assert_eq!(commented, Ok(1));
    }
    Ok(())
}

/// SQL that holds a second statement is refused before SQLite prepares
/// either, on every driver: SQLite applies some PRAGMAs, foreign_keys and
/// query_only among them, while it prepares them, so a refused string leaves
/// both as they were, whichever of its statements would have changed them.
#[engine_test]
async fn a_refused_string_of_two_statements_changes_no_setting() -> Result<()> {
    fn settings(executor: &dyn SqliteExecutor) -> Result<(i64, i64), SqliteError> {
        let row = executor
            .query_row("SELECT (SELECT foreign_keys FROM pragma_foreign_keys), (SELECT query_only FROM pragma_query_only)", &[])?
            .expect("one row");
        Ok((row.integer(0)?, row.integer(1)?))
    }
    let storage = common::open_in_memory().await?;
    let (before, refused, after) = storage
        .with_executor(|executor| {
            let before = settings(executor)?;
            let (foreign_keys, query_only) = (1 - before.0, 1 - before.1);
            let refused = [
                executor.execute(&format!("SELECT 1; PRAGMA foreign_keys = {foreign_keys}"), &[]),
                executor.execute(&format!("PRAGMA foreign_keys = {foreign_keys}; SELECT 1"), &[]),
                executor.query(&format!("SELECT 1; PRAGMA query_only = {query_only}"), &[]).map(|rows| rows.len()),
                executor.query(&format!("PRAGMA query_only = {query_only}; SELECT 1"), &[]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.is_err());
            Ok((before, refused, settings(executor)?))
        })
        .await?;
    assert_eq!(refused, [true; 4]);
    assert_eq!(after, before, "a refused string changed foreign_keys or query_only");
    Ok(())
}

/// What SQLite skips around a statement includes a UTF-8 byte order mark
/// and semicolons, before a PRAGMA as before any statement, on every driver;
/// better-sqlite3 alone refuses a byte order mark after a statement, in its
/// own words.
#[engine_test]
async fn a_byte_order_mark_and_semicolons_are_skipped_around_a_statement() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (timeouts, trailing) = storage
        .with_executor(|executor| {
            let timeouts = ["\u{FEFF}PRAGMA busy_timeout", "; PRAGMA busy_timeout"].map(|sql| {
                executor.query_row(sql, &[]).and_then(|row| row.expect("one row").integer(0)).map_err(|error| error.to_string())
            });
            Ok((timeouts, executor.execute("CREATE TABLE marked (x);\u{FEFF}", &[]).map_err(|error| error.to_string())))
        })
        .await?;
    for timeout in timeouts {
        assert!(timeout.is_ok(), "{timeout:?}");
    }
    if common::on_better_sqlite3() {
        let refusal = trailing.expect_err("better-sqlite3 reads the byte order mark as a second statement");
        assert!(refusal.ends_with("The supplied SQL string contains more than one statement"), "{refusal}");
    } else {
        assert!(trailing.is_ok(), "{trailing:?}");
    }
    Ok(())
}

/// A connection the engine opens on a file takes the session settings
/// connection.rs gives a native one, on every driver.
#[engine_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the session settings its host chose"
)]
async fn a_connection_on_a_file_takes_the_session_settings() -> Result<()> {
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
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "the engine opens no better-sqlite3 database, and one it adopts keeps the busy timeout its host set"
)]
async fn opening_a_store_waits_for_a_lock_on_its_file() -> Result<()> {
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
