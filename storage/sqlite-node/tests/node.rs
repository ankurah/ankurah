//! What is particular to the node executor: adopting a database the host
//! built, refusing an object of the wrong shape, and integers that only
//! survive the JavaScript boundary as BigInt.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite_node::SqliteNodeStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView};
use js_sys::{Array, Function, Reflect};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

/// A `DatabaseSync` constructed by the host, the way an extension host or a
/// server would build one before handing it to the engine.
fn host_database(path: &str) -> JsValue {
    let global = js_sys::global();
    let process = Reflect::get(&global, &"process".into()).unwrap();
    let get_builtin: Function = Reflect::get(&process, &"getBuiltinModule".into()).unwrap().dyn_into().unwrap();
    let module = get_builtin.call1(&process, &"node:sqlite".into()).unwrap();
    let constructor: Function = Reflect::get(&module, &"DatabaseSync".into()).unwrap().dyn_into().unwrap();
    Reflect::construct(&constructor, &Array::of1(&path.into())).unwrap()
}

#[wasm_bindgen_test]
async fn adopts_a_host_built_database() -> Result<()> {
    let storage = SqliteNodeStorageEngine::from_database(host_database(":memory:")).await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let trx = ctx.begin();
    trx.create(&Album { name: "Adopted".to_string(), year: "2026".to_string() }).await?;
    trx.commit().await?;

    let albums: Vec<AlbumView> = ctx.fetch("year = '2026'").await?;
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].name().unwrap(), "Adopted");
    Ok(())
}

#[wasm_bindgen_test]
async fn refuses_an_object_without_the_database_shape() {
    let error =
        SqliteNodeStorageEngine::from_database(JsValue::from_str("not a database")).await.err().expect("a string is not a database");
    assert!(error.to_string().contains("prepare"), "the refusal names the shape it wanted: {error}");
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Stamp {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub timestamp: i64,
}

/// INTEGER parameters bind as BigInt and results read back as BigInt, so an
/// i64 past 2^53 is stored, compared and ordered exactly.
#[wasm_bindgen_test]
async fn integers_past_the_double_range_are_exact() -> Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let trx = ctx.begin();
    for (name, timestamp) in [
        ("below", 9_007_199_254_740_990_i64),
        ("at", 9_007_199_254_740_991),
        ("past", 9_007_199_254_740_992),
        ("far", 9_007_199_254_741_000),
        ("negative", -9_007_199_254_741_000),
    ] {
        trx.create(&Stamp { name: name.to_string(), timestamp }).await?;
    }
    trx.commit().await?;

    let past: Vec<StampView> = ctx.fetch("timestamp > 9007199254740991").await?;
    assert_eq!(past.len(), 2);

    // 2^53 + 1 is not representable as a double; as a BigInt it matches exactly one row.
    let exact: Vec<StampView> = ctx.fetch("timestamp = 9007199254740992").await?;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].name().unwrap(), "past");

    let ordered: Vec<StampView> = ctx.fetch("timestamp > -9007199254741001 ORDER BY timestamp ASC").await?;
    let timestamps: Vec<i64> = ordered.iter().map(|stamp| stamp.timestamp().unwrap()).collect();
    assert_eq!(
        timestamps,
        vec![-9_007_199_254_741_000, 9_007_199_254_740_990, 9_007_199_254_740_991, 9_007_199_254_740_992, 9_007_199_254_741_000]
    );
    Ok(())
}
