//! The checks a database a host hands over must pass before the engine runs
//! on it.

use std::rc::Rc;

use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

use super::{
    runtime::{nodejs_process, require_node_floor},
    statement::method,
    NodeJsConnection,
};
use crate::{error::SqliteError, executor::SqliteExecutor, value::SqliteValue};

impl NodeJsConnection {
    /// Adopt a database object the host constructed: Node.js's `DatabaseSync` or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    /// A driver with a copy of SQLite of its own must not share a file with
    /// another copy in the same process (see
    /// [`SqliteStorageEngine::from_database`](crate::SqliteStorageEngine::from_database)).
    pub fn from_database(database: JsValue) -> Result<Self, SqliteError> {
        require_node_floor(&nodejs_process()?)?;
        if !database.is_object() {
            return Err(SqliteError::Node(format!(
                "a database must be an object with prepare and exec methods, such as node:sqlite's DatabaseSync; got {database:?}"
            )));
        }
        method(&database, "prepare")?;
        method(&database, "exec")?;
        let mut connection = Self { database: SendWrapper::new(database), readers: SendWrapper::new(Rc::default()), parameters: None };
        // A database whose statements do not hand rows over as the executor
        // reads them is refused here rather than at its first query.
        let probe = connection.query("SELECT 1", &[])?;
        if !matches!(probe.as_slice(), [row] if matches!(row.values(), [SqliteValue::Integer(1)])) {
            return Err(SqliteError::Node(format!("`SELECT 1` read back as {probe:?}, not as one row holding the INTEGER 1")));
        }
        // On a database that binds NULL for a parameter a call does not pass,
        // the executor checks each statement's count of parameters itself.
        if connection.binds_null_for_a_missing_parameter()? {
            connection.parameters = Some(SendWrapper::new(Rc::default()));
        }
        // The typed reader takes TEXT as the bytes SQLite stores, which are
        // UTF-8 only in a UTF-8 database. rusqlite has SQLite convert the TEXT
        // of a UTF-16 database to UTF-8; neither Node.js driver offers a way to,
        // so a UTF-16 database is refused rather than misread.
        let utf8 = connection.query("SELECT (SELECT encoding FROM pragma_encoding) = 'UTF-8'", &[])?;
        if !matches!(utf8.as_slice(), [row] if matches!(row.values(), [SqliteValue::Integer(1)])) {
            return Err(SqliteError::Node(
                "the database's text encoding is not UTF-8, and the Node.js executor reads TEXT as UTF-8 bytes".into(),
            ));
        }
        // A double-quoted name that matches no column fails with "no such
        // column" on every connection the engine opens natively
        // (connection.rs), so a database that reads such a name as a string
        // literal is refused rather than let it read as data: the probe
        // prepares only on such a database.
        match connection.prepare(r#"SELECT "_ankurah_dqs_probe""#) {
            Ok(_) => {
                return Err(SqliteError::Node(
                    "the database reads a double-quoted name that matches no column as a string literal, where the engine \
                     refuses such a name: turn double-quoted string literals off (node:sqlite's DatabaseSync has them off \
                     unless it is constructed with `enableDoubleQuotedStringLiterals: true`)"
                        .into(),
                ));
            }
            Err(refusal) if refusal.to_string().contains("no such column") && refusal.to_string().contains("_ankurah_dqs_probe") => {}
            Err(error) => return Err(error),
        }
        Ok(connection)
    }
}
