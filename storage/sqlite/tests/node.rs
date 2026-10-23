//! What only the node executor offers: adopting a database the host built,
//! and refusing an object of the wrong shape.
#![cfg(target_arch = "wasm32")]

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sqlite::{exec::SqliteExecutor, NodeConnection, SqliteStorageEngine};
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

/// TEXT is read exactly only from a statement SQLite can nest in the typed
/// reader; from a PRAGMA it is refused, pointing at the table-valued
/// function, while the PRAGMA's other storage classes read as ever.
#[wasm_bindgen_test]
async fn text_from_a_statement_sqlite_cannot_nest_is_refused() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let (table_info, timeout) = storage
        .with_executor(|executor| {
            let table_info =
                executor.query(r#"PRAGMA table_info("_ankurah_meta")"#, &[]).map(|rows| rows.len()).map_err(|error| error.to_string());
            Ok((table_info, executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    let refusal = table_info.expect_err("TEXT from a PRAGMA is refused");
    assert!(refusal.contains("column 1 is TEXT") && refusal.contains("pragma_table_info"), "{refusal}");
    assert_eq!(timeout, 5000);
    Ok(())
}

/// A statement SQLite cannot nest that is neither a PRAGMA nor an EXPLAIN,
/// such as one with a RETURNING clause, is refused before it runs, so that
/// it writes nothing it could not then report.
#[wasm_bindgen_test]
async fn a_returning_statement_is_refused_before_it_writes() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let (refusals, count) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (v TEXT)", &[])?;
            let refusals = [
                "INSERT INTO things (v) VALUES ('x') RETURNING v",
                "/* a comment */ WITH value (v) AS (VALUES ('y')) INSERT INTO things (v) SELECT v FROM value RETURNING rowid",
            ]
            .map(|sql| executor.query(sql, &[]).map(|rows| rows.len()).map_err(|error| error.to_string()));
            Ok((refusals, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    for refusal in refusals {
        let refusal = refusal.expect_err("a RETURNING statement is refused on Node");
        assert!(refusal.contains("so it is not run"), "{refusal}");
    }
    assert_eq!(count, 0, "neither insert ran");
    Ok(())
}

#[wasm_bindgen_test]
fn refuses_a_utf16_database() {
    let database = host_database(":memory:");
    let exec: Function = Reflect::get(&database, &"exec".into()).unwrap().dyn_into().unwrap();
    exec.call1(&database, &"PRAGMA encoding = 'UTF-16le'; CREATE TABLE t (v TEXT);".into()).unwrap();
    let error = NodeConnection::from_database(database).err().expect("a UTF-16 database is refused");
    assert!(error.to_string().contains("not UTF-8"), "{error}");
}

/// A host database whose statements pass through `tamper`: the body of a
/// JavaScript function of `statement` returning what the executor gets in its
/// place, where `forward(target, name)` reaches the real statement's member.
fn tampered_database(tamper: &str) -> JsValue {
    let wrap = Function::new_with_args(
        "database",
        &format!(
            "const forward = (target, name) => {{
                 const value = target[name];
                 return typeof value === 'function' ? value.bind(target) : value;
             }};
             const tamper = (statement) => {{ {tamper} }};
             return {{ prepare: (sql) => tamper(database.prepare(sql)), exec: (sql) => database.exec(sql) }};"
        ),
    );
    wrap.call1(&JsValue::NULL, &host_database(":memory:")).unwrap()
}

/// The refusal `NodeConnection::from_database` gives a tampered database.
fn refusal(tamper: &str) -> String {
    NodeConnection::from_database(tampered_database(tamper)).err().expect("the database is refused").to_string()
}

/// An INTEGER handed over as a number cannot be told from a REAL, so such a
/// database is refused rather than read by guesswork.
#[wasm_bindgen_test]
fn refuses_a_database_that_hands_an_integer_over_as_a_number() {
    let error =
        refusal("return new Proxy(statement, { get: (target, name) => name === 'setReadBigInts' ? () => {} : forward(target, name) });");
    assert!(error.contains("storage class integer that arrived as Real(1.0)"), "{error}");
}

#[wasm_bindgen_test]
fn refuses_a_row_of_the_wrong_width() {
    let error = refusal(
        "return new Proxy(statement, { get: (target, name) => name === 'all'
             ? (...params) => target.all(...params).map((row) => [...row, 0n])
             : forward(target, name) });",
    );
    assert!(error.contains("returned a row of 3"), "{error}");
}

/// `undefined` is no storage class: it is refused, not read as NULL.
#[wasm_bindgen_test]
fn refuses_undefined_where_null_was_handed_over() {
    let connection = NodeConnection::from_database(tampered_database(
        "return new Proxy(statement, { get: (target, name) => name === 'all'
             ? (...params) => target.all(...params).map((row) => row.map((value) => value === null ? undefined : value))
             : forward(target, name) });",
    ))
    .expect("SELECT 1 holds no NULL to tamper with");
    let error = connection.query("SELECT NULL", &[]).expect_err("undefined is refused");
    assert!(error.to_string().contains("holds JsValue(undefined)"), "{error}");
}
