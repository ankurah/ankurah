//! What only the node executor offers: adopting a database the host built,
//! and refusing an object of the wrong shape.
#![cfg(target_arch = "wasm32")]

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView};
use js_sys::{Array, Function, Reflect};
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
    let storage = SqliteStorageEngine::from_database(host_database(":memory:")).await?;
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
    let error = SqliteStorageEngine::from_database(JsValue::from_str("not a database")).await.err().expect("a string is not a database");
    assert!(error.to_string().contains("prepare"), "the refusal names the shape it wanted: {error}");
}

// Hide one capability on an actual DatabaseSync statement to exercise older
// or incompatible host drivers without substituting their SQL behavior.
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = "
export function withoutStatementMethod(database, name) {
    return {
        prepare(sql) {
            const statement = database.prepare(sql);
            Object.defineProperty(statement, name, { value: undefined });
            return statement;
        },
        exec(sql) { return database.exec(sql); }
    };
}
")]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = withoutStatementMethod)]
    fn without_statement_method(database: JsValue, name: &str) -> JsValue;
}

#[wasm_bindgen_test]
async fn refuses_a_driver_without_positional_rows() {
    let database = without_statement_method(host_database(":memory:"), "setReturnArrays");
    let error = SqliteStorageEngine::from_database(database).await.err().expect("object rows are not positional");
    assert!(error.to_string().contains("positional rows"), "{error}");
}

#[wasm_bindgen_test]
async fn refuses_a_driver_without_exact_integers() {
    let database = without_statement_method(host_database(":memory:"), "setReadBigInts");
    let error = SqliteStorageEngine::from_database(database).await.err().expect("Number cannot represent every INTEGER");
    assert!(error.to_string().contains("exact INTEGER"), "{error}");
}
