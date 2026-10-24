//! The checks a database must pass before the engine runs on it, so that a
//! database the executor cannot read exactly is refused as the engine
//! adopts it, rather than misread later.

use std::rc::Rc;

use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

use super::{
    statement::{method, unreadable},
    NodeJsConnection,
};
use crate::{
    error::{DatabaseRefusal, SqliteError},
    executor::SqliteExecutor,
    value::SqliteValue,
};

impl NodeJsConnection {
    /// Adopt a database object the host constructed, as
    /// [`SqliteStorageEngine::from_database`](crate::SqliteStorageEngine::from_database)
    /// does: its doc lists the databases refused, and says why the host must
    /// not open the same file with another copy of SQLite in the process.
    pub fn from_database(database: JsValue) -> Result<Self, SqliteError> {
        if !database.is_object() {
            return Err(DatabaseRefusal::NotAnObject { found: format!("{database:?}") }.into());
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
            return Err(unreadable(format!("{probe:?} as the rows of `SELECT 1`, where one row holding the INTEGER 1 was expected")));
        }
        // On a database that binds NULL for a parameter a call does not pass,
        // the executor checks each statement's count of parameters itself.
        if connection.binds_null_for_a_missing_parameter()? {
            connection.parameters = Some(SendWrapper::new(Rc::default()));
            // Such a database, node:sqlite, must also bind a numbered
            // parameter at its position, or that check misjudges a statement
            // that has one, and a parameter left without a value reads NULL.
            if !connection.binds_a_numbered_parameter_at_its_position()? {
                return Err(DatabaseRefusal::SkipsNumberedParameters.into());
            }
        }
        // The typed reader takes TEXT as the bytes SQLite stores, which are
        // UTF-8 only in a UTF-8 database. rusqlite has SQLite convert the TEXT
        // of a UTF-16 database to UTF-8; neither Node.js driver offers a way to,
        // so a UTF-16 database is refused rather than misread. The encoding is
        // read through the PRAGMA itself, since a table named `pragma_encoding`
        // takes the place of the table-valued function of that name.
        let encoding = connection.query("PRAGMA encoding", &[])?;
        if !matches!(encoding.as_slice(), [row] if matches!(row.values(), [SqliteValue::Text(name)] if name == "UTF-8")) {
            return Err(DatabaseRefusal::NotUtf8.into());
        }
        // A double-quoted name that matches no column fails with "no such
        // column" on every connection the engine opens natively (see
        // native/connection.rs), so a database that reads such a name as a
        // string literal is refused rather than let it read as data: the
        // probe prepares only on such a database.
        match connection.prepare(r#"SELECT "_ankurah_dqs_probe""#) {
            Ok(_) => return Err(DatabaseRefusal::DoubleQuotedStringLiterals.into()),
            Err(SqliteError::Sqlite(failure))
                if failure.message.contains("no such column") && failure.message.contains("_ankurah_dqs_probe") => {}
            Err(error) => return Err(error),
        }
        Ok(connection)
    }
}
