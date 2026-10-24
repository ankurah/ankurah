//! The checks a database a host hands over must pass before the engine runs
//! on it.

use std::rc::Rc;

use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

use super::{statement::method, NodeJsConnection};
use crate::{error::SqliteError, executor::SqliteExecutor, value::SqliteValue};

impl NodeJsConnection {
    /// Adopt a database object the host constructed: Node.js's `DatabaseSync` or
    /// a compatible driver whose statements support positional arrays and BigInt
    /// INTEGER results. The host owns session settings, including busy timeout.
    /// A driver with a copy of SQLite of its own must not share a file with
    /// another copy in the same process (see
    /// [`SqliteStorageEngine::from_database`](crate::SqliteStorageEngine::from_database)).
    pub fn from_database(database: JsValue) -> Result<Self, SqliteError> {
        if !database.is_object() {
            return Err(SqliteError::Node(format!(
                "a database must be an object with prepare and exec methods, such as node:sqlite's DatabaseSync; got {database:?}"
            )));
        }
        method(&database, "prepare")?;
        method(&database, "exec")?;
        let mut connection = Self {
            database: SendWrapper::new(database),
            readers: SendWrapper::new(Rc::default()),
            statements: SendWrapper::new(Rc::default()),
            parameters: None,
        };
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
            // Such a database, node:sqlite, must also bind a numbered
            // parameter at its position, or that check misjudges a statement
            // that has one, and a parameter left without a value reads NULL.
            if !connection.binds_a_numbered_parameter_at_its_position()? {
                return Err(SqliteError::Node(
                    "the database skips a numbered parameter such as `?2` as it binds a call's values, which can leave the \
                     parameter NULL where rusqlite binds the value at its position, as node:sqlite did before Node.js \
                     22.20.0 on 22 and 24.7.0 on 24: the Node.js executor needs Node.js 22.20.0 or later on 22, 24.7.0 or \
                     later on 24, or a later release line"
                        .into(),
                ));
            }
        }
        // The typed reader takes TEXT as the bytes SQLite stores, which are
        // UTF-8 only in a UTF-8 database. rusqlite has SQLite convert the TEXT
        // of a UTF-16 database to UTF-8; neither Node.js driver offers a way to,
        // so a UTF-16 database is refused rather than misread. The encoding is
        // read through the PRAGMA itself, since a table named `pragma_encoding`
        // takes the place of the table-valued function of that name.
        let encoding = connection.query("PRAGMA encoding", &[])?;
        if !matches!(encoding.as_slice(), [row] if matches!(row.values(), [SqliteValue::Text(encoding)] if encoding == "UTF-8")) {
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
