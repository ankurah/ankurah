//! The SQLite driver a run of the suite builds its databases with, so that
//! the engine's support for better-sqlite3, a driver its own constructors
//! never build, runs under the same tests as node:sqlite. Natively the driver
//! is rusqlite. On wasm32 it is Node.js's node:sqlite, or better-sqlite3 when
//! the suite is built with `--cfg ankurah_sqlite_better_sqlite3` and run
//! with `ANKURAH_SQLITE_TEST_DRIVER` set to `better-sqlite3`, as
//! scripts/run-wasm-tests.sh builds and runs it for the suite's second run in
//! Node. On that run a test's engine adopts a better-sqlite3 database through
//! `SqliteStorageEngine::from_database`, because `open` and `open_in_memory`
//! always build node:sqlite's. The driver is chosen when the suite is built,
//! so that a test that cannot run on better-sqlite3 reports as ignored on
//! that run, through `#[cfg_attr(ankurah_sqlite_better_sqlite3, ignore =
//! "…")]`, rather than pass without testing anything; the variable is
//! checked against the build, so that a build whose cfg went missing, as it
//! does when `RUSTFLAGS` replaces the target's rustflags, fails rather than
//! test node:sqlite again.
//!
//! The unit tests in src/ include this file as well (see src/lib.rs), so it
//! names this crate as the integration tests do and uses nothing else of
//! `common`. Each test binary uses only some of it.
#![allow(dead_code)]

use ankurah_storage_sqlite::SqliteStorageEngine;
#[cfg(target_arch = "wasm32")]
use js_sys::{Array, Function, Reflect};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue};

/// Whether this run builds its databases with better-sqlite3: never
/// natively, where rusqlite builds them.
#[cfg(not(target_arch = "wasm32"))]
pub fn on_better_sqlite3() -> bool { false }

/// Whether this run builds its databases with better-sqlite3 rather than
/// node:sqlite, as the suite was built to and `ANKURAH_SQLITE_TEST_DRIVER`
/// names the driver; a run whose variable names another driver than the
/// build's fails.
#[cfg(target_arch = "wasm32")]
pub fn on_better_sqlite3() -> bool {
    let env = Reflect::get(&process(), &"env".into()).unwrap();
    let named = match Reflect::get(&env, &"ANKURAH_SQLITE_TEST_DRIVER".into()).unwrap().as_string().as_deref() {
        None | Some("node:sqlite") => false,
        Some("better-sqlite3") => true,
        Some(other) => panic!("ANKURAH_SQLITE_TEST_DRIVER names node:sqlite or better-sqlite3, not {other:?}"),
    };
    let built = cfg!(ankurah_sqlite_better_sqlite3);
    assert_eq!(
        named, built,
        "ANKURAH_SQLITE_TEST_DRIVER names better-sqlite3 exactly when the suite is built with \
         `--cfg ankurah_sqlite_better_sqlite3`, as scripts/run-wasm-tests.sh does both"
    );
    built
}

/// The engine a test runs on in memory: the one `open_in_memory` opens, or on
/// the better-sqlite3 run the engine adopting a better-sqlite3 database.
pub async fn open_in_memory() -> anyhow::Result<SqliteStorageEngine> {
    #[cfg(target_arch = "wasm32")]
    if on_better_sqlite3() {
        return SqliteStorageEngine::from_database(host_database(":memory:")).await;
    }
    SqliteStorageEngine::open_in_memory().await
}

/// The engine a test runs on over the file at `path`: the one `open` opens,
/// or on the better-sqlite3 run the engine adopting a better-sqlite3 database
/// on the file.
pub async fn open(path: &std::path::Path) -> anyhow::Result<SqliteStorageEngine> {
    #[cfg(target_arch = "wasm32")]
    if on_better_sqlite3() {
        return SqliteStorageEngine::from_database(host_database(path.to_str().expect("a UTF-8 path"))).await;
    }
    SqliteStorageEngine::open(path).await
}

/// A database the host constructed on `path` (`:memory:` for one in memory),
/// the way an extension host or a server builds one before handing it to the
/// engine: node:sqlite's `DatabaseSync`, or on the better-sqlite3 run
/// better-sqlite3's `Database`, each with its driver's defaults.
#[cfg(target_arch = "wasm32")]
pub fn host_database(path: &str) -> JsValue {
    let constructor = if on_better_sqlite3() {
        tests_require()
            .call1(&JsValue::NULL, &"better-sqlite3".into())
            .expect("better-sqlite3 is installed: run `npm ci` in storage/sqlite/tests")
    } else {
        Reflect::get(&builtin("node:sqlite"), &"DatabaseSync".into()).unwrap()
    };
    Reflect::construct(&constructor.dyn_into().unwrap(), &Array::of1(&path.into())).unwrap()
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

#[cfg(target_arch = "wasm32")]
fn builtin(name: &str) -> JsValue {
    let get_builtin: Function = Reflect::get(&process(), &"getBuiltinModule".into()).unwrap().dyn_into().unwrap();
    get_builtin.call1(&process(), &name.into()).unwrap()
}

#[cfg(target_arch = "wasm32")]
fn process() -> JsValue { Reflect::get(&js_sys::global(), &"process".into()).unwrap() }
