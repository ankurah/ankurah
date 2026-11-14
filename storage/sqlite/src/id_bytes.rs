//! Compare and sort entity ids by their decoded bytes.
//!
//! Tables store ids as URL-safe base64, matching the representation Ankurah prints. Its text
//! order differs from the byte order the in-memory evaluator uses for the 32-byte ids.
//! SQLite has no built-in base64 decoder, so the engine registers one on every connection.
//! Entity id range comparisons and ORDER BY id use this function, and the entity table indexes
//! the same expression. Queries and the index share [`SQL`] because SQLite requires matching
//! expressions to use an expression index.

use ankurah_proto::EntityId;
use rusqlite::{functions::FunctionFlags, types::ValueRef, Connection};

const FUNCTION: &str = "ankurah_id_bytes";

/// SQL expression that decodes the `id` column to bytes.
pub(crate) const SQL: &str = r#"ankurah_id_bytes("id")"#;

/// Register the id decoder used by this engine's SQL on `connection`.
///
/// The engine registers it on every connection it opens. A connection opened elsewhere, such
/// as a backup tool's, needs it before writing to a store or running `PRAGMA integrity_check`
/// on one, because both evaluate the index expression.
///
/// The function depends only on its argument and has no side effects. Mark it deterministic
/// for use in expression indexes, and innocuous for use in schemas with `trusted_schema` off.
pub fn register(connection: &Connection) -> rusqlite::Result<()> {
    connection.create_scalar_function(
        FUNCTION,
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        |context| match context.get_raw(0) {
            ValueRef::Null => Ok(None),
            ValueRef::Text(text) => EntityId::from_base64(text)
                .map(|id| Some(id.to_bytes().to_vec()))
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error))),
            other => Err(rusqlite::Error::UserFunctionError(
                format!("{FUNCTION} reads an id's base64 text, not a value of type {}", other.data_type()).into(),
            )),
        },
    )
}
