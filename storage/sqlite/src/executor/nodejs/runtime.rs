//! What the Node.js executor needs of Node.js itself: the `process` global,
//! and node:sqlite's `DatabaseSync`, opened with the busy timeout and the
//! session settings of a native connection.

use js_sys::{Array, Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};

use super::{exception::js_error, statement::method, NodeJsConnection};
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

/// The 5000 ms busy timeout rusqlite sets on every connection as it opens
/// it, before the connection runs any statement (node:sqlite's default is
/// 0): a connection then waits for another's lock instead of failing at
/// once, and on a file in a rollback journal even preparing `SELECT 1`
/// takes a lock.
const BUSY_TIMEOUT_PRAGMA: &str = "PRAGMA busy_timeout=5000;";

impl NodeJsConnection {
    /// Open a database through Node.js's built-in module; `":memory:"` for an
    /// in-memory one. Applies the session settings a native connection has.
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
        let constructor: Function = Reflect::get(&module, &"DatabaseSync".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| DatabaseRefusal::NoNodeSqlite { missing: "`node:sqlite`'s `DatabaseSync` constructor" })?;
        let database = Reflect::construct(&constructor, &Array::of1(&path.into())).map_err(js_error)?;
        // The busy timeout comes before any other statement, as rusqlite sets
        // it at open, since from_database's checks already read the file.
        method(&database, "exec")?.call1(&database, &BUSY_TIMEOUT_PRAGMA.into()).map_err(js_error)?;
        let connection = Self::from_database(database)?;
        // Only a database the executor accepts is switched to WAL.
        connection.exec(SESSION_PRAGMAS)?;
        Ok(connection)
    }
}
