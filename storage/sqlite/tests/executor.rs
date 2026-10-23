//! What every executor must agree on where values cross the driver boundary,
//! checked natively on rusqlite and on wasm32 on node:sqlite.

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::{SqliteStorageEngine, SqliteValue, SqliteValueRef};
use anyhow::Result;
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
    let storage = SqliteStorageEngine::open_in_memory().await?;
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
    let storage = SqliteStorageEngine::open_in_memory().await?;
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
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let timeout = storage.with_executor(|executor| executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)).await?;
    assert_eq!(timeout, 5000);
    Ok(())
}

/// Read the SQL values themselves; model getters decode canonical state and
/// would not detect a driver rounding its returned INTEGER values.
#[engine_test]
async fn integer_results_preserve_all_bits() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
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
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let row = storage.with_executor(|executor| Ok(executor.query_row("SELECT 1 AS x, 2 AS x", &[])?.unwrap())).await?;
    assert_eq!(row.values().len(), 2);
    assert_eq!(row.integer(0)?, 1);
    assert_eq!(row.integer(1)?, 2);
    Ok(())
}
