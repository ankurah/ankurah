//! Opening node:sqlite's database for `SqliteStorageEngine::open` and
//! `open_in_memory`, through Node.js's `process` global, with the busy
//! timeout and the session settings of a native connection.

use js_sys::{Array, Function, Object, Reflect};
use wasm_bindgen::{JsCast, JsValue};

use super::{exception::js_error, statement::database_method, NodeJsConnection};
use crate::{
    error::{DatabaseRefusal, SqliteError},
    executor::SESSION_PRAGMAS,
};

/// The `process` global, which exists only inside Node.js.
fn nodejs_process() -> Result<JsValue, SqliteError> {
    let process = Reflect::get(&js_sys::global(), &"process".into()).map_err(js_error)?;
    if process.is_undefined() || process.is_null() {
        return Err(DatabaseRefusal::NoNodeSqlite { missing: "the `process` global" }.into());
    }
    Ok(process)
}

/// So that a connection waits up to five seconds for another's lock instead
/// of failing at once, the busy timeout rusqlite sets on every connection it
/// opens (node:sqlite's default is 0). `open` sets it before any other
/// statement, since on a file in a rollback journal even preparing
/// `SELECT 1` takes a lock.
const BUSY_TIMEOUT_PRAGMA: &str = "PRAGMA busy_timeout=5000;";

impl NodeJsConnection {
    /// Open a database through Node.js's built-in module; `":memory:"` for an
    /// in-memory one. Applies the session settings a native connection has,
    /// with SQLite's defensive flag off, as it is natively.
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let process = nodejs_process()?;
        // Node.js has had `process.getBuiltinModule` since 22.3, before any
        // release whose node:sqlite the executor runs on, so a runtime without
        // it is no Node.js the executor runs on.
        let get_builtin: Function = Reflect::get(&process, &"getBuiltinModule".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| DatabaseRefusal::NoNodeSqlite { missing: "`process.getBuiltinModule`" })?;
        let module = get_builtin.call1(&process, &"node:sqlite".into()).map_err(js_error)?;
        if module.is_undefined() || module.is_null() {
            return Err(DatabaseRefusal::NoNodeSqlite { missing: "the `node:sqlite` module" }.into());
        }
        // node:sqlite names its database class `Database` from Node.js
        // 26.11.0, which keeps `DatabaseSync` as a deprecated alias of it
        // (DEP0210), and `DatabaseSync` alone before.
        let class = match Reflect::get(&module, &"Database".into()).map_err(js_error)? {
            class if class.is_undefined() => Reflect::get(&module, &"DatabaseSync".into()).map_err(js_error)?,
            class => class,
        };
        let constructor: Function = class
            .dyn_into()
            .map_err(|_| DatabaseRefusal::NoNodeSqlite { missing: "`node:sqlite`'s `Database` or `DatabaseSync` constructor" })?;
        // SQLite's defensive flag, which node:sqlite turns on by default from
        // Node.js 24.14.0 and 25.5.0, makes statements such as `PRAGMA
        // writable_schema = ON` and `PRAGMA journal_mode = OFF` do nothing,
        // where a native connection has it off. A release before the option,
        // which came in 24.12.0 and 25.1.0, reads only the options it knows,
        // as 22.20.0 and 24.7.0 do, and so ignores it.
        let options = Object::new();
        Reflect::set(&options, &"defensive".into(), &JsValue::FALSE).map_err(js_error)?;
        let database = Reflect::construct(&constructor, &Array::of2(&path.into(), &options)).map_err(js_error)?;
        let set_up = || -> Result<Self, SqliteError> {
            // The busy timeout comes before any other statement, as rusqlite
            // sets it at open, since from_database's checks already read the
            // file.
            database_method(&database, "exec")?.call1(&database, &BUSY_TIMEOUT_PRAGMA.into()).map_err(js_error)?;
            let connection = Self::from_database(database.clone())?;
            // Only a database the executor accepts is switched to WAL.
            connection.exec(SESSION_PRAGMAS)?;
            Ok(connection)
        };
        // A database that fails its setup, such as one `from_database`
        // refuses, is closed before the error returns, rather than left open
        // on its file until it is collected. The setup's error is the one to
        // report, so an error of the close is dropped.
        set_up().inspect_err(|_| {
            let _ = database_method(&database, "close").and_then(|close| close.call0(&database).map_err(js_error));
        })
    }
}
