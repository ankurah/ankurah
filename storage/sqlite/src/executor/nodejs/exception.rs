//! A JavaScript exception a driver throws, as the executor reports it: an
//! error SQLite raised, with SQLite's extended result code, or a JavaScript
//! exception of another kind.

use js_sys::Reflect;
use wasm_bindgen::{JsCast, JsValue};

use crate::error::{SqliteError, SqliteFailure};

/// A JavaScript exception as an error. node:sqlite throws an error SQLite
/// raised with SQLite's extended result code as `errcode`, which is kept,
/// as rusqlite keeps it natively; better-sqlite3 throws one as its own
/// `SqliteError`, which names the code as `code`. Any other exception, such
/// as a driver's own refusal of a call or a host function's exception, is a
/// [`SqliteError::JavaScript`].
pub(super) fn js_error(exception: JsValue) -> SqliteError {
    let message = if let Some(error) = exception.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(text) = exception.as_string() {
        text
    } else {
        format!("{exception:?}")
    };
    if !exception.is_object() {
        return SqliteError::JavaScript(message);
    }
    let property = |name: &str| Reflect::get(&exception, &name.into()).ok();
    if let Some(code) = property("errcode").and_then(|code| code.as_f64()) {
        if code.fract() == 0.0 && (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&code) {
            return SqliteError::Sqlite(SqliteFailure { code: Some(code as i32), message });
        }
    }
    let name = property("name").and_then(|name| name.as_string());
    if name.as_deref() == Some("SqliteError") && property("code").is_some_and(|code| code.is_string()) {
        return SqliteError::Sqlite(SqliteFailure { code: None, message });
    }
    SqliteError::JavaScript(message)
}
