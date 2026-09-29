//! The node executor: Node.js's built-in `node:sqlite` module, driven from WASM.
//!
//! The engine reaches SQLite through a `DatabaseSync`-shaped JavaScript
//! object: `prepare(sql)` giving statements with `run(...params)` and
//! `all(...params)`, plus `exec(sql)`. Node's own `DatabaseSync` has that
//! shape (Node.js 22.5 or later), and so does a `better-sqlite3` database, so
//! a host may hand either to [`NodeConnection::from_database`];
//! [`NodeConnection::open`] constructs Node's directly through
//! `process.getBuiltinModule`. Every call is synchronous, which suits the
//! single-threaded WASM host: the engine's closures simply run in place.

use js_sys::{Array, BigInt, Function, Reflect, Uint8Array};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    error::SqliteNodeError,
    exec::{Row, SqliteExecutor},
    value::SqliteValue,
};

const SESSION_PRAGMAS: &str = "PRAGMA journal_mode=WAL;
     PRAGMA synchronous=NORMAL;
     PRAGMA foreign_keys=ON;
     PRAGMA cache_size=-64000;
     PRAGMA mmap_size=268435456;
     PRAGMA temp_store=MEMORY;";

/// One `DatabaseSync`-shaped database. Clones share the same JavaScript object.
#[derive(Clone)]
pub struct NodeConnection {
    // The JavaScript handle is bound to the thread that made it; WASM has one.
    database: SendWrapper<JsValue>,
}

impl NodeConnection {
    /// Adopt a database object the host constructed: Node's `DatabaseSync` or
    /// anything with the same `prepare` and `exec` methods.
    pub fn from_database(database: JsValue) -> Result<Self, SqliteNodeError> {
        if !database.is_object() {
            return Err(SqliteNodeError::Node(format!(
                "a database must be an object with prepare and exec methods, such as node:sqlite's DatabaseSync; got {database:?}"
            )));
        }
        method(&database, "prepare")?;
        method(&database, "exec")?;
        Ok(Self { database: SendWrapper::new(database) })
    }

    /// Open a database through Node's built-in module; `":memory:"` for an
    /// in-memory one. Applies the same session settings as the native executor.
    pub fn open(path: &str) -> Result<Self, SqliteNodeError> {
        let global = js_sys::global();
        let process = Reflect::get(&global, &"process".into()).map_err(js_error)?;
        if process.is_undefined() || process.is_null() {
            return Err(SqliteNodeError::Node("no `process` global: the node executor runs only inside Node.js".into()));
        }
        let get_builtin = method(&process, "getBuiltinModule")?;
        let module = get_builtin.call1(&process, &"node:sqlite".into()).map_err(js_error)?;
        if module.is_undefined() || module.is_null() {
            return Err(SqliteNodeError::Node("`node:sqlite` is unavailable: Node.js 22.5 or later is required".into()));
        }
        let constructor: Function = Reflect::get(&module, &"DatabaseSync".into())
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteNodeError::Node("`node:sqlite` exports no DatabaseSync constructor".into()))?;
        let database = Reflect::construct(&constructor, &Array::of1(&path.into())).map_err(js_error)?;
        let connection = Self::from_database(database)?;
        connection.exec(SESSION_PRAGMAS)?;
        Ok(connection)
    }

    /// The JavaScript database object.
    pub fn database(&self) -> &JsValue { &self.database }

    /// Run one or more statements that yield nothing, as `exec` does.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteNodeError> {
        method(&self.database, "exec")?.call1(&self.database, &sql.into()).map_err(js_error)?;
        Ok(())
    }

    /// Run a closure against this database through the executor boundary.
    /// Everything happens synchronously on the calling thread.
    pub async fn with_executor<F, T>(&self, f: F) -> Result<T, SqliteNodeError>
    where
        F: FnOnce(&dyn SqliteExecutor) -> Result<T, SqliteNodeError> + Send + 'static,
        T: Send + 'static,
    {
        let executor = NodeExecutor { database: &self.database };
        f(&executor)
    }
}

/// The node executor's stand-in for a pool: one database, handed out by clone.
#[derive(Clone)]
pub(crate) struct NodePool {
    connection: NodeConnection,
}

impl NodePool {
    pub(crate) fn new(connection: NodeConnection) -> Self { Self { connection } }

    pub(crate) async fn get(&self) -> Result<NodeConnection, SqliteNodeError> { Ok(self.connection.clone()) }

    pub(crate) fn connection(&self) -> &NodeConnection { &self.connection }
}

struct NodeExecutor<'d> {
    database: &'d JsValue,
}

impl NodeExecutor<'_> {
    fn prepare(&self, sql: &str) -> Result<JsValue, SqliteNodeError> {
        method(self.database, "prepare")?.call1(self.database, &sql.into()).map_err(js_error)
    }
}

impl SqliteExecutor for NodeExecutor<'_> {
    fn execute(&self, sql: &str, params: &[SqliteValue]) -> Result<usize, SqliteNodeError> {
        let statement = self.prepare(sql)?;
        let result = method(&statement, "run")?.apply(&statement, &js_params(params)).map_err(js_error)?;
        let changes = Reflect::get(&result, &"changes".into()).map_err(js_error)?;
        Ok(integer_from_js(&changes)?.max(0) as usize)
    }

    fn query(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>, SqliteNodeError> {
        let statement = self.prepare(sql)?;
        // Rows as arrays keep the column order of the SELECT (`node:sqlite`,
        // then better-sqlite3's spelling); INTEGERs as BigInt so a value past
        // 2^53 comes back instead of throwing.
        if let Ok(set_return_arrays) = method(&statement, "setReturnArrays") {
            set_return_arrays.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        } else if let Ok(raw) = method(&statement, "raw") {
            raw.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        }
        if let Ok(set_read_big_ints) = method(&statement, "setReadBigInts") {
            set_read_big_ints.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        } else if let Ok(safe_integers) = method(&statement, "safeIntegers") {
            safe_integers.call1(&statement, &JsValue::TRUE).map_err(js_error)?;
        }
        let rows: Array = method(&statement, "all")?
            .apply(&statement, &js_params(params))
            .map_err(js_error)?
            .dyn_into()
            .map_err(|_| SqliteNodeError::Node("`all()` did not return an array".into()))?;
        rows.iter().map(row_from_js).collect()
    }
}

fn row_from_js(row: JsValue) -> Result<Row, SqliteNodeError> {
    if let Some(values) = row.dyn_ref::<Array>() {
        return values.iter().map(value_from_js).collect::<Result<Vec<_>, _>>().map(Row::new);
    }
    // A database that cannot return arrays yields objects, whose string keys
    // keep insertion order, which is the SELECT's column order.
    let keys = js_sys::Object::keys(row.unchecked_ref::<js_sys::Object>());
    keys.iter().map(|key| Reflect::get(&row, &key).map_err(js_error).and_then(value_from_js)).collect::<Result<Vec<_>, _>>().map(Row::new)
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

fn value_from_js(value: JsValue) -> Result<SqliteValue, SqliteNodeError> {
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
        // Without BigInt results, SQLite INTEGERs arrive as numbers; keep the
        // integral ones INTEGER.
        return Ok(if number.fract() == 0.0 && number.abs() < 9_007_199_254_740_992.0 {
            SqliteValue::Integer(number as i64)
        } else {
            SqliteValue::Real(number)
        });
    }
    if value.is_instance_of::<Uint8Array>() {
        return Ok(SqliteValue::Blob(value.unchecked_into::<Uint8Array>().to_vec()));
    }
    if let Some(buffer) = value.dyn_ref::<js_sys::ArrayBuffer>() {
        return Ok(SqliteValue::Blob(Uint8Array::new(buffer).to_vec()));
    }
    Err(SqliteNodeError::Node(format!("unsupported column value {value:?}")))
}

fn integer_from_js(value: &JsValue) -> Result<i64, SqliteNodeError> {
    if value.is_bigint() {
        let big: BigInt = value.clone().unchecked_into();
        return i64::try_from(big).map_err(|_| SqliteNodeError::Node("INTEGER does not fit in 64 bits".into()));
    }
    match value.as_f64() {
        Some(number) if number.fract() == 0.0 => Ok(number as i64),
        _ => Err(SqliteNodeError::Node(format!("expected an integer, found {value:?}"))),
    }
}

fn method(target: &JsValue, name: &str) -> Result<Function, SqliteNodeError> {
    Reflect::get(target, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| SqliteNodeError::Node(format!("the database object has no `{name}` method")))
}

fn js_error(value: JsValue) -> SqliteNodeError {
    let text = if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(text) = value.as_string() {
        text
    } else {
        format!("{value:?}")
    };
    SqliteNodeError::Node(text)
}
