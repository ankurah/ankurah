//! PostgreSQL-compatible JSONB ordering for SQLite's JSON text expressions.

use ankurah_core::value::ValueExt;
use ankurah_core_types::Value;
use rusqlite::{functions::FunctionFlags, types::ValueRef, Connection};

pub(super) fn register(connection: &Connection) -> rusqlite::Result<()> {
    connection.create_scalar_function(
        "ankurah_json_order_key",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        |context| match context.get_raw(0) {
            ValueRef::Null => Ok(None),
            ValueRef::Text(text) => {
                let json = serde_json::from_slice(text).map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
                let path: Vec<String> = serde_json::from_str(&context.get::<String>(1)?)
                    .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
                Ok(Value::Json(json).json_order_at_path(path).map(|json| ankurah_core_types::ordering::value_bytes(&Value::Json(json))))
            }
            _ => Err(rusqlite::Error::UserFunctionError("JSON ordering requires JSON text".into())),
        },
    )
}
