//! What every executor must agree on where values cross the driver boundary.

use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sqlite::{executor::Row, Refusal, SqliteValue, SqliteValueRef};
use anyhow::Result;
use std::sync::Arc;

use crate::{
    common::{self, engine_test},
    Stamp, StampView,
};

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
/// default 2000 columns, is refused on Node.js before it runs, naming its
/// width, since the typed reader reads each column as two; rusqlite reads it.
/// A query of 1000 columns reads on every driver.
#[engine_test]
async fn a_query_of_more_than_a_thousand_columns_is_refused_on_nodejs() -> Result<()> {
    let columns = |count: usize| format!("SELECT {}", (0..count).map(|index| index.to_string()).collect::<Vec<_>>().join(", "));
    let storage = common::open_in_memory().await?;
    let (thousand, more) = storage
        .with_executor(move |executor| {
            let width = |sql: String| {
                executor.query_row(&sql, &[]).map(|row| row.expect("one row").values().len()).map_err(|error| error.refusal())
            };
            Ok((width(columns(1000)), width(columns(1001))))
        })
        .await?;
    assert_eq!(thousand, Ok(1000));
    if cfg!(target_arch = "wasm32") {
        assert_eq!(more, Err(Some(Refusal::TooManyColumns { width: 1001 })));
    } else {
        assert_eq!(more, Ok(1001));
    }
    Ok(())
}

/// TEXT crosses the boundary as its bytes: a NUL inside it is kept, and TEXT
/// whose bytes are not UTF-8 is refused, as rusqlite refuses it, on every
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
    assert!(matches!(error.refusal(), Some(Refusal::TextNotUtf8 { column: 1, .. })), "{error}");
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

/// A JSON parameter binds as its JSON text, TEXT, on every driver, which
/// `jsonb(?)` turns into JSONB.
#[engine_test]
async fn a_json_parameter_binds_as_its_text() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let row = storage
        .with_executor(|executor| {
            let json = serde_json::json!({ "a": [1, "two"] });
            let json = SqliteValueRef::Jsonb(&json);
            Ok(executor.query_row("SELECT ?, typeof(?), typeof(jsonb(?)), json(jsonb(?))", &[json; 4])?.expect("one row"))
        })
        .await?;
    let text = r#"{"a":[1,"two"]}"#;
    assert_eq!([row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?], [text, "text", "blob", text]);
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
            .map(|(sql, native)| (executor.query(sql, &[]).map(values).map_err(|error| error.refusal()), native));
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
            assert_eq!(read, Err(Some(Refusal::CompoundSelect)), "Node.js refuses the compound SELECT");
        } else {
            assert_eq!(read.expect("rusqlite reads a compound SELECT directly"), native);
        }
    }
    for (sql, read, expected) in same {
        assert_eq!(read?, expected, "{sql}");
    }
    Ok(())
}
