//! The node executor: Node.js's built-in `node:sqlite` module, driven from WASM.
//!
//! The WASM build of an Ankurah node has no IndexedDB inside Node.js (a VS
//! Code extension host, an Electron main process, a JavaScript CLI) and no
//! native library either, but Node.js ships SQLite as `node:sqlite`. The
//! engine reaches it through a `DatabaseSync`-shaped JavaScript object:
//! `prepare(sql)` giving statements with `run(...params)` and
//! `all(...params)`, plus `exec(sql)`. Node's own `DatabaseSync` has that
//! shape (Node.js 22.16 or later). Compatible drivers must also support
//! positional rows and BigInt INTEGER results; the adapter recognizes
//! better-sqlite3's `raw` and `safeIntegers` methods for these capabilities.
//! Hosts can adopt an existing database with [`SqliteStorageEngine::from_database`];
//! [`NodeConnection::open`] constructs Node's directly through
//! `process.getBuiltinModule`. Every call is synchronous, which suits the
//! single-threaded WASM host: the engine's closures simply run in place.

use std::path::Path;

use js_sys::{Array, BigInt, Function, Reflect, Uint8Array};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    error::SqliteError,
    exec::{Row, SqliteExecutor},
    value::SqliteValue,
    SqliteStorageEngine,
};

/// The engine names the connection its pool hands out `PooledConnection`;
/// here the pool holds a single database.
pub(crate) use NodeConnection as PooledConnection;
/// The engine's pool on Node.
pub(crate) type Pool = NodePool;

/// Session settings that make a Node connection behave like a native one:
/// connection.rs's pragmas, plus the 5000 ms busy timeout rusqlite gives every
/// connection it opens (node:sqlite's default is 0), so that a writer waits
/// for another's lock instead of failing at once.
const SESSION_PRAGMAS: &str = "PRAGMA journal_mode=WAL;
     PRAGMA synchronous=NORMAL;
     PRAGMA foreign_keys=ON;
     PRAGMA cache_size=-64000;
     PRAGMA mmap_size=268435456;
     PRAGMA temp_store=MEMORY;
     PRAGMA busy_timeout=5000;";

/// The pool behind `SqliteStorageEngine::open`.
pub(crate) async fn file_pool(path: &Path) -> anyhow::Result<Pool> {
    let path = path.to_str().ok_or_else(|| anyhow::anyhow!("the database path is not valid UTF-8"))?;
    Ok(NodePool::new(NodeConnection::open(path)?))
}

/// The pool behind `SqliteStorageEngine::open_in_memory`.
pub(crate) async fn memory_pool() -> anyhow::Result<Pool> { Ok(NodePool::new(NodeConnection::open(":memory:")?)) }

impl SqliteStorageEngine {
    /// Adopt a database object the host constructed: Node's `DatabaseSync`, or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    pub async fn from_database(database: JsValue) -> anyhow::Result<Self> {
        Self::new(NodePool::new(NodeConnection::from_database(database)?)).await
    }

    /// The JavaScript database object this engine runs on, for a host that
    /// wants to share it or reopen the engine over it.
    pub fn database(&self) -> &JsValue { self.pool().connection().database() }
}

/// One `DatabaseSync`-shaped database. Clones share the same JavaScript object.
#[derive(Clone)]
pub struct NodeConnection {
    // The JavaScript handle is bound to the thread that made it; WASM has one.
    database: SendWrapper<JsValue>,
}

impl NodeConnection {
    /// Adopt a database object the host constructed: Node's `DatabaseSync` or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    pub fn from_database(database: JsValue) -> Result<Self, SqliteError> {
        if !database.is_object() {
            return Err(SqliteError::Node(format!(
                "a database must be an object with prepare and exec methods, such as node:sqlite's DatabaseSync; got {database:?}"
            )));
        }
        method(&database, "prepare")?;
        method(&database, "exec")?;
        let connection = Self { database: SendWrapper::new(database) };
        // A database that cannot return rows as arrays is refused here rather
        // than at its first query.
        return_arrays(&connection.prepare("SELECT 1")?)?;
        Ok(connection)
    }

    /// Open a database through Node's built-in module; `":memory:"` for an
    /// in-memory one. Applies the session settings a native connection has.
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let global = js_sys::global();
        let process = Reflect::get(&global, &"process".into()).map_err(js_error)?;
        if process.is_undefined() || process.is_null() {
            return Err(SqliteError::Node("no `process` global: the node executor runs only inside Node.js".into()));
        }
        let get_builtin = method(&process, "getBuiltinModule")?;
        let module = get_builtin.call1(&process, &"node:sqlite".into()).map_err(js_error)?;
        if module.is_undefined() || module.is_null() {
            return Err(SqliteError::Node("`node:sqlite` is unavailable: it needs Node.js 22.16 or later".into()));
        }
        let constructor: Function = Reflect::get(&module, &"DatabaseSync".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteError::Node("`node:sqlite` exports no DatabaseSync constructor".into()))?;
        let database = Reflect::construct(&constructor, &Array::of1(&path.into())).map_err(js_error)?;
        let connection = Self::from_database(database)?;
        connection.exec(SESSION_PRAGMAS)?;
        Ok(connection)
    }

    /// The JavaScript database object.
    pub fn database(&self) -> &JsValue { &self.database }

    /// Run one or more statements that yield nothing, as `exec` does.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        method(&self.database, "exec")?.call1(&self.database, &sql.into()).map_err(js_error)?;
        Ok(())
    }

    /// Run a closure against this database through the executor boundary.
    /// Everything happens synchronously on the calling thread.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteError> + Send + 'static,
        T: Send + 'static,
    {
        f(self)
    }

    fn prepare(&self, sql: &str) -> Result<JsValue, SqliteError> {
        method(&self.database, "prepare")?.call1(&self.database, &sql.into()).map_err(js_error)
    }
}

/// The node executor's stand-in for a pool: one database, handed out by clone.
#[derive(Clone)]
pub struct NodePool {
    connection: NodeConnection,
}

impl NodePool {
    pub fn new(connection: NodeConnection) -> Self { Self { connection } }

    pub async fn get(&self) -> Result<NodeConnection, SqliteError> { Ok(self.connection.clone()) }

    pub fn connection(&self) -> &NodeConnection { &self.connection }
}

impl SqliteExecutor for NodeConnection {
    fn execute(&self, sql: &str, params: &[SqliteValue]) -> Result<usize, SqliteError> {
        let statement = self.prepare(sql)?;
        let result = method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
        let changes = Reflect::get(&result, &"changes".into()).map_err(js_error)?;
        Ok(integer_from_js(&changes)?.max(0) as usize)
    }

    fn query(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>, SqliteError> {
        let statement = self.prepare(sql)?;
        return_arrays(&statement)?;
        // INTEGERs as BigInt, so that a value past 2^53 comes back instead of throwing.
        if let Ok(set_read_big_ints) = method(&statement, "setReadBigInts") {
            set_read_big_ints.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        } else if let Ok(safe_integers) = method(&statement, "safeIntegers") {
            safe_integers.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        } else {
            return Err(SqliteError::Node("exact INTEGER results require setReadBigInts or safeIntegers".into()));
        }
        let rows: Array = method(&statement, "all")?
            .apply(&statement, &js_params(params))
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteError::Node("`all()` did not return an array".into()))?;
        rows.iter().map(row_from_js).collect()
    }
}

/// Ask a statement for its rows as arrays, which keep every column at its
/// position in the statement: node:sqlite's `setReturnArrays`, or
/// better-sqlite3's `raw`. Their default, objects keyed by column name,
/// cannot: a name that reads as an integer moves ahead of the others, and two
/// columns of one name collapse into one.
fn return_arrays(statement: &JsValue) -> Result<(), SqliteError> {
    if let Ok(set_return_arrays) = method(statement, "setReturnArrays") {
        set_return_arrays.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else if let Ok(raw) = method(statement, "raw") {
        raw.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else {
        return Err(SqliteError::Node("positional rows require setReturnArrays (Node.js 22.16+) or raw".into()));
    }
    Ok(())
}

fn row_from_js(row: JsValue) -> Result<Row, SqliteError> {
    let values = row.dyn_into::<Array>().map_err(|_| SqliteError::Node("expected a positional row array".into()))?;
    values.iter().map(value_from_js).collect::<Result<Vec<_>, _>>().map(Row::new)
}

fn js_params(params: &[SqliteValue]) -> Array { params.iter().map(js_param).collect() }

fn js_param(value: &SqliteValue) -> JsValue {
    match value {
        SqliteValue::Text(text) => JsValue::from_str(text),
        // BigInt binds as INTEGER exactly; a JavaScript number would bind as REAL.
        SqliteValue::Integer(integer) => BigInt::from(*integer).into(),
        SqliteValue::Real(real) => JsValue::from_f64(*real),
        SqliteValue::Blob(bytes) => Uint8Array::from(bytes.as_slice()).into(),
        // The SQL wraps this placeholder in jsonb(?), so the text form binds.
        SqliteValue::Jsonb(json) => JsValue::from_str(&json.to_string()),
        SqliteValue::Null => JsValue::NULL,
    }
}

/// INTEGERs arrive as BigInt, so Number values remain REAL even when integral.
fn value_from_js(value: JsValue) -> Result<SqliteValue, SqliteError> {
    if value.is_null() || value.is_undefined() {
        return Ok(SqliteValue::Null);
    }
    if let Some(text) = value.as_string() {
        return Ok(SqliteValue::Text(text));
    }
    if value.is_bigint() {
        return Ok(SqliteValue::Integer(integer_from_js(&value)?));
    }
    if let Some(number) = value.as_f64() {
        return Ok(SqliteValue::Real(number));
    }
    if value.is_instance_of::<Uint8Array>() {
        return Ok(SqliteValue::Blob(value.unchecked_into::<Uint8Array>().to_vec()));
    }
    if let Some(buffer) = value.dyn_ref::<js_sys::ArrayBuffer>() {
        return Ok(SqliteValue::Blob(Uint8Array::new(buffer).to_vec()));
    }
    Err(SqliteError::Node(format!("unsupported column value {value:?}")))
}

fn integer_from_js(value: &JsValue) -> Result<i64, SqliteError> {
    if value.is_bigint() {
        let big: BigInt = value.clone().unchecked_into();
        return i64::try_from(big).map_err(|_| SqliteError::Node("INTEGER does not fit in 64 bits".into()));
    }
    match value.as_f64() {
        Some(number) if number.fract() == 0.0 => Ok(number as i64),
        _ => Err(SqliteError::Node(format!("expected an integer, found {value:?}"))),
    }
}

fn method(target: &JsValue, name: &str) -> Result<Function, SqliteError> {
    Reflect::get(target, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| SqliteError::Node(format!("the database object has no `{name}` method")))
}

fn js_error(value: JsValue) -> SqliteError {
    let text = if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(text) = value.as_string() {
        text
    } else {
        format!("{value:?}")
    };
    SqliteError::Node(text)
}
