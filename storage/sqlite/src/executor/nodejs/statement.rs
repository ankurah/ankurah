//! The JavaScript statement API the Node.js executor drives: preparing a
//! statement that hands rows over as the executor reads them, checking the
//! SQL SQLite prepared, running a statement for its rows, and the values
//! that cross between Rust and JavaScript.

use js_sys::{Array, BigInt, Function, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

use super::{exception::js_error, NodeJsConnection};
use crate::{
    error::{DatabaseRefusal, Refusal, SqliteError},
    executor::statement_scan::{self, Statements},
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
        return Err(DatabaseRefusal::NoPositionalRows.into());
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
        return Err(DatabaseRefusal::NoExactIntegers.into());
    }
    Ok(())
}

/// Whether a statement can yield rows, as better-sqlite3 reports it in
/// `reader`. node:sqlite's statements have no `reader`, so each is taken to
/// yield rows.
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
        .map_err(|columns| unreadable(format!("{columns:?} from `columns()`, where an array was expected")))?;
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
    Err(DatabaseRefusal::NoStatementSource.into())
}

/// Refuse SQL that holds no statement, or a second one, before SQLite
/// prepares anything. Returns the statement's first named parameter, which
/// the same pass over the SQL finds, for `refuse_a_named_parameter`.
///
/// - SQL that holds a second statement is refused as the native executor
///   refuses it: SQLite applies some PRAGMAs while it prepares them, and
///   node:sqlite runs the first statement and ignores the rest without a
///   word. Whitespace, semicolons and comments after the statement are no
///   statement.
/// - SQL that holds no statement, only whitespace, semicolons and comments,
///   is refused as SQLite refuses to run it natively, with SQLITE_MISUSE,
///   which the refusal answers as its `sqlite_code`. It is refused before
///   it is prepared, since node:sqlite can crash the process, in a later
///   `sqlite3_step`, once it collects a statement prepared from such SQL
///   while the database is open, as Node.js 24.13.0 does.
pub(super) fn refuse_all_but_one_statement(sql: &str) -> Result<Option<&str>, SqliteError> {
    match statement_scan::statements(sql) {
        Statements::One { named_parameter } => Ok(named_parameter),
        Statements::Several => Err(Refusal::MultipleStatements.into()),
        Statements::None => Err(Refusal::NoStatement.into()),
    }
}

/// Check where SQLite ended the statement it prepared from `sql`, which the
/// scan before preparing found to be followed only by what SQLite skips:
/// node:sqlite's `source` is the SQL up to the end of its first statement,
/// and better-sqlite3's is the whole SQL. Should SQLite have found more, the
/// SQL is refused.
pub(super) fn refuse_sql_past_its_statement(sql: &str, source: &str) -> Result<(), SqliteError> {
    let rest = sql.strip_prefix(source).ok_or_else(|| unreadable(format!("{source:?} as the SQL of a statement prepared from {sql:?}")))?;
    if !statement_scan::holds_no_statement(rest) {
        return Err(Refusal::SqlPastItsStatement.into());
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

/// Run a statement and take its rows as arrays of `width` values: every row,
/// or only the first, for which `get()` steps the statement once, so that
/// `query_row` decodes no row past it.
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
        .map_err(|rows| unreadable(format!("{rows:?} from `all()`, where an array of rows was expected")))?;
    rows.iter().map(|row| row_of_width(row, width)).collect()
}

/// A row a statement returned, refused unless it is an array of `width`
/// values.
fn row_of_width(row: JsValue, width: usize) -> Result<Array, SqliteError> {
    let row: Array = row.dyn_into().map_err(|row| unreadable(format!("{row:?} as a row, which is not an array")))?;
    if row.length() as usize != width {
        return Err(unreadable(format!("a row of {} values from a statement of {width} columns", row.length())));
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
        // JSON binds as its JSON text, TEXT, as it does natively.
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
        Err(unreadable(format!("TEXT as a string, which may have lost bytes, in column {index}, where its bytes were expected")))
    } else {
        Err(unreadable(format!("{value:?} in column {index}, which stands for no SQLite storage class")))
    }
}

/// An INTEGER, which both Node.js drivers hand over as a BigInt to a
/// statement reading BigInts. One handed over as a number shows that the
/// database's statements do not read BigInts, and is refused as such.
pub(super) fn integer_from_js(value: &JsValue) -> Result<i64, SqliteError> {
    if !value.is_bigint() {
        if value.as_f64().is_some() {
            return Err(DatabaseRefusal::NoExactIntegers.into());
        }
        return Err(unreadable(format!("{value:?} where an INTEGER was expected, as a BigInt")));
    }
    i64::try_from(value.clone().unchecked_into::<BigInt>())
        .map_err(|_| unreadable(format!("the INTEGER {value:?}, which does not fit in 64 bits")))
}

/// A method of the database object; a missing one is refused by name.
pub(super) fn method(database: &JsValue, name: &'static str) -> Result<Function, SqliteError> {
    Reflect::get(database, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| DatabaseRefusal::NoDatabaseMethod { name }.into())
}

/// A method of a statement the database prepared. node:sqlite's statements
/// have every method the executor calls, on every Node.js it runs on.
pub(super) fn statement_method(statement: &JsValue, name: &'static str) -> Result<Function, SqliteError> {
    Reflect::get(statement, &name.into())
        .map_err(js_error)?
        .dyn_into::<Function>()
        .map_err(|_| DatabaseRefusal::NoStatementMethod { name }.into())
}

/// The refusal of `what`, something the database handed over that the
/// executor cannot read as SQLite's, such as a row that is not an array.
pub(super) fn unreadable(what: String) -> SqliteError { DatabaseRefusal::Unreadable(what).into() }
