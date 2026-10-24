//! The JavaScript statement API the Node.js executor drives: preparing a
//! statement that hands rows over as the executor reads them, checking the
//! SQL SQLite prepared, running a statement for its rows, and the values
//! that cross between Rust and JavaScript.

use js_sys::{Array, BigInt, Function, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

use super::NodeJsConnection;
use crate::{
    error::SqliteError,
    executor::statement_scan,
    value::{SqliteValue, SqliteValueRef},
};

impl NodeJsConnection {
    /// Prepare a statement that hands rows over as the executor reads them.
    pub(super) fn prepare(&self, sql: &str) -> Result<JsValue, SqliteError> {
        let statement = method(&self.database, "prepare")?.call1(&self.database, &sql.into()).map_err(js_error)?;
        return_arrays(&statement)?;
        // INTEGERs as BigInt, so that each storage class arrives as a
        // JavaScript type of its own and a value past 2^53 arrives exactly.
        read_big_ints(&statement)?;
        Ok(statement)
    }
}

/// Ask a statement for its rows as arrays, which keep every column at its
/// position in the statement: node:sqlite's `setReturnArrays`, or
/// better-sqlite3's `raw`. Their default, objects keyed by column name,
/// cannot: a name that reads as an integer moves ahead of the others, and two
/// columns of one name collapse into one. better-sqlite3 refuses `raw` on a
/// statement that yields no rows, which has none to hand over.
fn return_arrays(statement: &JsValue) -> Result<(), SqliteError> {
    if let Ok(set_return_arrays) = method(statement, "setReturnArrays") {
        set_return_arrays.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else if let Ok(raw) = method(statement, "raw") {
        if yields_rows(statement)? {
            raw.call1(statement, &JsValue::TRUE).map_err(js_error)?;
        }
    } else {
        return Err(SqliteError::Node("positional rows require setReturnArrays (node:sqlite's) or raw (better-sqlite3's)".into()));
    }
    Ok(())
}

/// Ask a statement for its INTEGERs as BigInt: node:sqlite's
/// `setReadBigInts`, or better-sqlite3's `safeIntegers`.
fn read_big_ints(statement: &JsValue) -> Result<(), SqliteError> {
    if let Ok(set_read_big_ints) = method(statement, "setReadBigInts") {
        set_read_big_ints.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else if let Ok(safe_integers) = method(statement, "safeIntegers") {
        safe_integers.call1(statement, &JsValue::TRUE).map_err(js_error)?;
    } else {
        return Err(SqliteError::Node("exact INTEGER results require setReadBigInts or safeIntegers".into()));
    }
    Ok(())
}

/// Whether a statement can yield rows, as better-sqlite3 reports it in
/// `reader`. node:sqlite's statements have no `reader`, and are taken to.
fn yields_rows(statement: &JsValue) -> Result<bool, SqliteError> {
    Ok(Reflect::get(statement, &"reader".into()).map_err(js_error)?.as_bool() != Some(false))
}

/// How many columns each row of a statement holds: none for a statement
/// better-sqlite3 reports yields no rows, whose `columns()` it refuses.
pub(super) fn width(statement: &JsValue) -> Result<usize, SqliteError> {
    if !yields_rows(statement)? {
        return Ok(0);
    }
    let columns: Array = statement_method(statement, "columns")?
        .call0(statement)
        .map_err(js_error)?
        .dyn_into()
        .map_err(|_| SqliteError::Node("`columns()` did not return an array".into()))?;
    Ok(columns.length() as usize)
}

/// A statement's SQL, which is one statement alone: node:sqlite's
/// `sourceSQL`, the first statement as SQLite compiled it, or
/// better-sqlite3's `source`, the SQL it prepared, which it refuses to
/// prepare when it holds more than one.
pub(super) fn source(statement: &JsValue) -> Result<String, SqliteError> {
    for name in ["sourceSQL", "source"] {
        if let Some(source) = Reflect::get(statement, &name.into()).map_err(js_error)?.as_string() {
            return Ok(source);
        }
    }
    Err(SqliteError::Node(
        "the database's statements report no `sourceSQL` (better-sqlite3's `source`), which the Node.js executor needs".into(),
    ))
}

/// Refuse SQL that holds a second statement, before SQLite prepares either,
/// as the native executor refuses it: SQLite applies some PRAGMAs while it
/// prepares them, and node:sqlite runs the first statement and ignores the
/// rest without a word. Whitespace, semicolons and comments after the
/// statement are no statement.
pub(super) fn refuse_a_second_statement(sql: &str) -> Result<(), SqliteError> {
    if statement_scan::holds_a_second_statement(sql) {
        // rusqlite's words for the same refusal.
        return Err(SqliteError::Node("Multiple statements provided".into()));
    }
    Ok(())
}

/// Check where SQLite ended the statement it prepared from `sql`, which the
/// scan before preparing found to be followed only by what SQLite skips:
/// node:sqlite's `source` is the SQL up to the end of its first statement,
/// and better-sqlite3's is the whole SQL. Should SQLite have found more, the
/// SQL is refused.
pub(super) fn refuse_sql_past_its_statement(sql: &str, source: &str) -> Result<(), SqliteError> {
    let rest = sql
        .strip_prefix(source)
        .ok_or_else(|| SqliteError::Node(format!("the database reports {source:?} as the SQL of a statement prepared from {sql:?}")))?;
    if !statement_scan::holds_no_statement(rest) {
        return Err(SqliteError::Node("Multiple statements provided".into()));
    }
    Ok(())
}

/// Which rows of a statement a read takes.
#[derive(Clone, Copy)]
pub(super) enum RowSelection {
    /// Every row, through `all()`.
    All,
    /// The first row alone, through `get()`, which steps the statement once.
    First,
}

/// Run a statement and take the rows `selection` names.
pub(super) fn read_rows(
    statement: &JsValue,
    params: &[SqliteValueRef<'_>],
    width: usize,
    selection: RowSelection,
) -> Result<Vec<Array>, SqliteError> {
    match selection {
        RowSelection::All => all(statement, params, width),
        RowSelection::First => {
            let row = statement_method(statement, "get")?.apply(statement, &js_params(params)).map_err(js_error)?;
            if row.is_undefined() {
                return Ok(Vec::new());
            }
            Ok(vec![row_of_width(row, width)?])
        }
    }
}

/// Run a statement and take every row it yields, refusing any row that is
/// not an array of `width` values before a value of it is read.
pub(super) fn all(statement: &JsValue, params: &[SqliteValueRef<'_>], width: usize) -> Result<Vec<Array>, SqliteError> {
    let rows: Array = statement_method(statement, "all")?
        .apply(statement, &js_params(params))
        .map_err(js_error)?
        .dyn_into()
        .map_err(|_| SqliteError::Node("`all()` did not return an array".into()))?;
    rows.iter().map(|row| row_of_width(row, width)).collect()
}

/// A row a statement returned, refused unless it is an array of `width`
/// values.
fn row_of_width(row: JsValue, width: usize) -> Result<Array, SqliteError> {
    let row: Array =
        row.dyn_into().map_err(|row| SqliteError::Node(format!("a statement returned a row that is not an array: {row:?}")))?;
    if row.length() as usize != width {
        return Err(SqliteError::Node(format!("a statement of {width} columns returned a row of {}", row.length())));
    }
    Ok(row)
}

pub(super) fn js_params(params: &[SqliteValueRef<'_>]) -> Array { params.iter().map(js_param).collect() }

fn js_param(value: &SqliteValueRef<'_>) -> JsValue {
    match *value {
        SqliteValueRef::Text(text) => JsValue::from_str(text),
        // BigInt binds as INTEGER exactly; a JavaScript number would bind as REAL.
        SqliteValueRef::Integer(integer) => BigInt::from(integer).into(),
        SqliteValueRef::Real(real) => JsValue::from_f64(real),
        SqliteValueRef::Blob(bytes) => Uint8Array::from(bytes).into(),
        // The SQL wraps this placeholder in jsonb(?), so the text form binds.
        SqliteValueRef::Jsonb(json) => JsValue::from_str(&json.to_string()),
        SqliteValueRef::Null => JsValue::NULL,
    }
}

/// The value in column `index` of a row, as both Node.js drivers hand each
/// storage class but TEXT over to a statement reading BigInts: NULL as null,
/// INTEGER as BigInt, REAL as number (2.0 included), BLOB as Uint8Array. A
/// string, their lossy form of TEXT, is refused, and so is anything else,
/// `undefined` among it, rather than read as the nearest storage class.
pub(super) fn value_from_js(index: usize, value: JsValue) -> Result<SqliteValue, SqliteError> {
    if value.is_null() {
        Ok(SqliteValue::Null)
    } else if value.is_bigint() {
        integer_from_js(&value).map(SqliteValue::Integer)
    } else if let Some(real) = value.as_f64() {
        Ok(SqliteValue::Real(real))
    } else if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        Ok(SqliteValue::Blob(bytes.to_vec()))
    } else if value.is_string() {
        Err(SqliteError::Node(format!("column {index} holds TEXT as a string, which may have lost bytes, where its bytes were expected")))
    } else {
        Err(SqliteError::Node(format!("column {index} holds {value:?}, which stands for no SQLite storage class")))
    }
}

/// An INTEGER, which both Node.js drivers hand over as a BigInt.
pub(super) fn integer_from_js(value: &JsValue) -> Result<i64, SqliteError> {
    if !value.is_bigint() {
        return Err(SqliteError::Node(format!("expected an INTEGER as a BigInt, found {value:?}")));
    }
    i64::try_from(value.clone().unchecked_into::<BigInt>()).map_err(|_| SqliteError::Node(format!("{value:?} does not fit in 64 bits")))
}

/// A method of the database object, which the refusal of one it lacks names;
/// `open` checks `process.getBuiltinModule` with a refusal of its own.
pub(super) fn method(database: &JsValue, name: &str) -> Result<Function, SqliteError> {
    Reflect::get(database, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| SqliteError::Node(format!("the database object has no `{name}` method")))
}

/// A method of a statement the database prepared. node:sqlite's statements
/// have every method the executor calls, on every Node.js it runs on.
pub(super) fn statement_method(statement: &JsValue, name: &str) -> Result<Function, SqliteError> {
    Reflect::get(statement, &name.into()).map_err(js_error)?.dyn_into::<Function>().map_err(|_| {
        SqliteError::Node(format!(
            "the database's statements have no `{name}` method, which the Node.js executor needs; node:sqlite's statements have \
             every method it calls"
        ))
    })
}

/// A JavaScript exception as an error. One that node:sqlite raises for SQLite
/// carries SQLite's extended result code as `errcode`, which is kept, as
/// rusqlite keeps it natively. better-sqlite3's errors carry no `errcode`,
/// only the code's name as `code`, which is deliberately not read, since
/// turning a name back into its number takes a table of SQLite's codes: on
/// better-sqlite3 an error SQLite raised has no [`SqliteError::sqlite_code`].
pub(super) fn js_error(value: JsValue) -> SqliteError {
    let message = if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(text) = value.as_string() {
        text
    } else {
        format!("{value:?}")
    };
    let code = if value.is_object() { Reflect::get(&value, &"errcode".into()).ok().and_then(|code| code.as_f64()) } else { None };
    match code {
        Some(code) if code.fract() == 0.0 && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&code) => {
            SqliteError::NodeSqlite { code: code as i32, message }
        }
        _ => SqliteError::Node(message),
    }
}
