//! What the Node.js executor needs of Node.js itself: the `process` global,
//! a Node.js at or past the floor, and node:sqlite's `DatabaseSync`, opened
//! with the busy timeout and the session settings of a native connection.

use js_sys::{Array, Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};

use super::{
    statement::{js_error, method},
    NodeJsConnection,
};
use crate::error::SqliteError;

/// The oldest Node.js the Node.js executor runs on, on either driver: from
/// 22.20.0, node:sqlite binds a positional value to a numbered parameter such
/// as `?2` as rusqlite does, where before it bound none, leaving the
/// parameter NULL.
const NODE_FLOOR: [u32; 3] = [22, 20, 0];

/// The `process` global, which exists only inside Node.js.
pub(super) fn nodejs_process() -> Result<JsValue, SqliteError> {
    let process = Reflect::get(&js_sys::global(), &"process".into()).map_err(js_error)?;
    if process.is_undefined() || process.is_null() {
        return Err(SqliteError::Node("no `process` global: the Node.js executor runs only inside Node.js".into()));
    }
    Ok(process)
}

/// Refuse a Node.js older than [`NODE_FLOOR`].
pub(super) fn require_node_floor(process: &JsValue) -> Result<(), SqliteError> {
    let version =
        Reflect::get(process, &"versions".into()).and_then(|versions| Reflect::get(&versions, &"node".into())).map_err(js_error)?;
    let version = version.as_string().ok_or_else(|| SqliteError::Node(format!("`process.versions.node` is {version:?}, not a version")))?;
    meets_node_floor(&version)
}

/// Refuse `version`, as `process.versions.node` gives it, when it is older
/// than [`NODE_FLOOR`].
fn meets_node_floor(version: &str) -> Result<(), SqliteError> {
    let parts: Option<Vec<u32>> = version.split('.').map(|part| part.parse().ok()).collect();
    match parts {
        Some(parts) if parts.len() == 3 && parts.as_slice() >= NODE_FLOOR.as_slice() => Ok(()),
        Some(parts) if parts.len() == 3 => Err(SqliteError::Node(format!(
            "the Node.js executor needs Node.js 22.20.0 or later, whose node:sqlite binds a positional value to a numbered \
             parameter such as `?2` as rusqlite does; this is Node.js {version}"
        ))),
        _ => Err(SqliteError::Node(format!("Node.js reports its version as {version:?}, which the Node.js executor cannot read"))),
    }
}

/// The 5000 ms busy timeout rusqlite sets on every connection as it opens
/// it, before the connection runs any statement (node:sqlite's default is
/// 0): a connection then waits for another's lock instead of failing at
/// once, and on a file in a rollback journal even preparing `SELECT 1`
/// takes a lock.
const BUSY_TIMEOUT: &str = "PRAGMA busy_timeout=5000;";

/// connection.rs's session pragmas, which make a Node.js connection behave like
/// a native one.
const SESSION_PRAGMAS: &str = "PRAGMA journal_mode=WAL;
     PRAGMA synchronous=NORMAL;
     PRAGMA foreign_keys=ON;
     PRAGMA cache_size=-64000;
     PRAGMA mmap_size=268435456;
     PRAGMA temp_store=MEMORY;";

impl NodeJsConnection {
    /// Open a database through Node.js's built-in module; `":memory:"` for an
    /// in-memory one. Applies the session settings a native connection has.
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let process = nodejs_process()?;
        require_node_floor(&process)?;
        // Node.js has had `process.getBuiltinModule` since 22.3, before the
        // floor, so a runtime that reports a version past the floor without
        // it is no Node.js the executor runs on.
        let get_builtin: Function = Reflect::get(&process, &"getBuiltinModule".into()).map_err(js_error)?.dyn_into().map_err(|_| {
            SqliteError::Node(
                "`process` has no `getBuiltinModule` method, which Node.js has from 22.3: the Node.js executor needs \
                 Node.js 22.20.0 or later"
                    .into(),
            )
        })?;
        let module = get_builtin.call1(&process, &"node:sqlite".into()).map_err(js_error)?;
        if module.is_undefined() || module.is_null() {
            return Err(SqliteError::Node("`node:sqlite` is unavailable in this build of Node.js".into()));
        }
        let constructor: Function = Reflect::get(&module, &"DatabaseSync".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteError::Node("`node:sqlite` exports no DatabaseSync constructor".into()))?;
        let database = Reflect::construct(&constructor, &Array::of1(&path.into())).map_err(js_error)?;
        // The busy timeout comes before any other statement, as rusqlite sets
        // it at open, since from_database's checks already read the file.
        method(&database, "exec")?.call1(&database, &BUSY_TIMEOUT.into()).map_err(js_error)?;
        let connection = Self::from_database(database)?;
        // Only a database the executor accepts is switched to WAL.
        connection.exec(SESSION_PRAGMAS)?;
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::*;

    /// The executor runs on Node.js 22.20.0 or later, whose node:sqlite binds
    /// numbered parameters as rusqlite does, and refuses an older one, naming
    /// both versions.
    #[wasm_bindgen_test]
    fn refuses_a_node_older_than_its_floor() {
        for version in ["22.20.0", "22.20.1", "22.21.0", "23.0.0", "24.13.0"] {
            assert!(meets_node_floor(version).is_ok(), "{version}");
        }
        for version in ["22.19.9", "22.16.0", "21.99.99", "20.18.0"] {
            let refusal = meets_node_floor(version).expect_err("an older Node.js is refused").to_string();
            assert!(refusal.contains("Node.js 22.20.0 or later") && refusal.contains(version), "{refusal}");
        }
        assert!(meets_node_floor("22.20").is_err());
        require_node_floor(&nodejs_process().unwrap()).expect("the Node.js running the tests meets the floor");
    }
}
