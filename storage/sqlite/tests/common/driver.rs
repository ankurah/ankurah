//! What the tests open their engines with. Natively that is rusqlite,
//! through `SqliteStorageEngine::open` and `open_in_memory`. On wasm32 it is
//! better-sqlite3, whose `Database` class the tests pass to those
//! constructors, as a host does: the release tests/package.json pins, which
//! `npm ci` in storage/sqlite/tests installs.
//!
//! The unit tests in src/ include this file as well (see src/lib.rs), so it
//! names this crate as the integration tests do and uses nothing else of
//! `common`. Each test binary uses only some of it.
#![allow(dead_code)]

use ankurah_storage_sqlite::{SqliteError, SqliteStorageEngine};
#[cfg(target_arch = "wasm32")]
use js_sys::{Function, Reflect};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};

/// The attribute of a test that runs on every target: natively under tokio,
/// and on wasm32 under wasm-bindgen-test in Node.js.
#[cfg(not(target_arch = "wasm32"))]
pub use tokio::test as engine_test;
#[cfg(target_arch = "wasm32")]
pub use wasm_bindgen_test::wasm_bindgen_test as engine_test;

/// The message of `error` when it is a JavaScript exception, as
/// better-sqlite3 throws for its own refusal of a call, such as of a call
/// that passes too few values: never natively, where rusqlite's refusals are
/// errors of its own.
#[cfg(target_arch = "wasm32")]
pub fn javascript_message(error: &SqliteError) -> Option<&str> {
    match error {
        SqliteError::JavaScript(message) => Some(message),
        _ => None,
    }
}

/// The message of `error` when it is a JavaScript exception: never natively.
#[cfg(not(target_arch = "wasm32"))]
pub fn javascript_message(_: &SqliteError) -> Option<&str> { None }

/// The engine a test runs on in memory.
#[cfg(not(target_arch = "wasm32"))]
pub async fn open_in_memory() -> anyhow::Result<SqliteStorageEngine> { SqliteStorageEngine::open_in_memory().await }

/// The engine a test runs on in memory, built with better-sqlite3.
#[cfg(target_arch = "wasm32")]
pub async fn open_in_memory() -> anyhow::Result<SqliteStorageEngine> { SqliteStorageEngine::open_in_memory(&better_sqlite3()).await }

/// The engine a test runs on over the file at `path`.
#[cfg(not(target_arch = "wasm32"))]
pub async fn open(path: &std::path::Path) -> anyhow::Result<SqliteStorageEngine> { SqliteStorageEngine::open(path).await }

/// The engine a test runs on over the file at `path`, built with better-sqlite3.
#[cfg(target_arch = "wasm32")]
pub async fn open(path: &std::path::Path) -> anyhow::Result<SqliteStorageEngine> {
    SqliteStorageEngine::open(path, &better_sqlite3()).await
}

/// better-sqlite3's `Database` class, as a host passes it to the engine.
#[cfg(target_arch = "wasm32")]
pub fn better_sqlite3() -> JsValue {
    tests_require()
        .call1(&JsValue::NULL, &"better-sqlite3".into())
        .expect("better-sqlite3 is installed: run `npm ci` in storage/sqlite/tests")
}

/// The path of better-sqlite3's entry point, for a worker thread to load it from.
#[cfg(target_arch = "wasm32")]
pub fn better_sqlite3_entry() -> String {
    let require = tests_require();
    let resolve: Function = Reflect::get(&require, &"resolve".into()).unwrap().dyn_into().unwrap();
    resolve.call1(&require, &"better-sqlite3".into()).unwrap().as_string().expect("a resolved path")
}

/// `require` as a module in tests/ has it, which finds better-sqlite3 where
/// `npm ci` there installs the release tests/package.json pins.
#[cfg(target_arch = "wasm32")]
fn tests_require() -> Function {
    let module = builtin("node:module");
    let create_require: Function = Reflect::get(&module, &"createRequire".into()).unwrap().dyn_into().unwrap();
    create_require.call1(&module, &concat!(env!("CARGO_MANIFEST_DIR"), "/tests/").into()).unwrap().dyn_into().unwrap()
}

/// The Node.js built-in module `name`, such as `node:fs`.
#[cfg(target_arch = "wasm32")]
pub fn builtin(name: &str) -> JsValue {
    let get_builtin: Function = Reflect::get(&process(), &"getBuiltinModule".into()).unwrap().dyn_into().unwrap();
    get_builtin.call1(&process(), &name.into()).unwrap()
}

#[cfg(target_arch = "wasm32")]
fn process() -> JsValue { Reflect::get(&js_sys::global(), &"process".into()).unwrap() }
