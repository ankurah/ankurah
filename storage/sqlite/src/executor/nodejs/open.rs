//! Opening a database for `SqliteStorageEngine::open` and `open_in_memory`
//! with the better-sqlite3 `Database` class the host passes in: the check
//! that the class is better-sqlite3's, the settings of a native connection,
//! and the checks that refuse a database the executor cannot read exactly.

use std::rc::Rc;

use js_sys::{Array, Function, Object, Reflect};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use super::{exception::js_error, statement::database_method, NodeJsConnection};
use crate::{
    error::{DatabaseRefusal, SqliteError},
    executor::{SqliteExecutor, SESSION_PRAGMAS},
    value::SqliteValue,
};

/// So that a connection waits up to five seconds for another's lock instead
/// of failing at once: the busy timeout rusqlite sets on every connection it
/// opens. better-sqlite3 sets it as it opens a database, before any
/// statement can take a lock.
const BUSY_TIMEOUT_MS: u32 = 5000;

/// The methods of better-sqlite3's `Database` that the executor calls, which
/// the class defines on its prototype.
const DATABASE_METHODS: [&str; 4] = ["prepare", "exec", "unsafeMode", "close"];

impl NodeJsConnection {
    /// Open the database on the file at `path`, or in memory for
    /// `":memory:"`, with `driver`, better-sqlite3's `Database` class (the
    /// export of `require('better-sqlite3')`), and give it the busy timeout,
    /// the session settings and the defensive flag of a native connection.
    /// A `driver` that is not that class is refused before it builds
    /// anything. A database the executor cannot read exactly is closed and
    /// refused: one whose text encoding is not UTF-8, and one whose SQLite
    /// reads a double-quoted name that matches no column as a string.
    pub fn open(path: &str, driver: &JsValue) -> Result<Self, SqliteError> {
        let class = database_class(driver)?;
        let options = Object::new();
        Reflect::set(&options, &"timeout".into(), &BUSY_TIMEOUT_MS.into()).map_err(js_error)?;
        let database = Reflect::construct(class, &Array::of2(&path.into(), &options)).map_err(js_error)?;
        let connection = Self {
            database: SendWrapper::new(database),
            readers: SendWrapper::new(Rc::default()),
            statements: SendWrapper::new(Rc::default()),
        };
        // A database that fails its setup is closed before the error returns,
        // rather than left open on its file until it is collected. The
        // setup's error is the one to report, so an error of the close is
        // dropped.
        connection.set_up().inspect_err(|_| {
            let database = connection.database();
            let _ = database_method(database, "close").and_then(|close| close.call0(database).map_err(js_error));
        })?;
        Ok(connection)
    }

    /// Give a database just opened the settings of a native connection,
    /// refusing one the executor cannot read exactly.
    fn set_up(&self) -> Result<(), SqliteError> {
        // better-sqlite3 opens a database with SQLite's defensive flag on,
        // under which statements such as `PRAGMA writable_schema = ON` and
        // `PRAGMA journal_mode = OFF` do nothing, and `unsafeMode()` turns it
        // off, as it is on a native connection. `unsafeMode()` also lets a
        // statement write while an iterator over another statement's rows is
        // open, as SQLite lets a native connection; the executor opens no
        // iterator.
        database_method(&self.database, "unsafeMode")?.call0(&self.database).map_err(js_error)?;
        // The typed reader takes TEXT as the bytes SQLite stores, which are
        // UTF-8 only in a UTF-8 database. rusqlite has SQLite convert the TEXT
        // of a UTF-16 database to UTF-8; better-sqlite3 offers no way to, so a
        // UTF-16 database is refused rather than misread. The encoding is
        // read through the PRAGMA itself, since a table named `pragma_encoding`
        // takes the place of the table-valued function of that name.
        let encoding = self.query("PRAGMA encoding", &[])?;
        if !matches!(encoding.as_slice(), [row] if matches!(row.values(), [SqliteValue::Text(name)] if name == "UTF-8")) {
            return Err(DatabaseRefusal::NotUtf8.into());
        }
        // A double-quoted name that matches no column fails with "no such
        // column" on every connection the engine opens natively (see
        // native/connection.rs). better-sqlite3 builds its SQLite with
        // double-quoted string literals off and offers no way to turn them
        // on, but a better-sqlite3 built from source against an SQLite of
        // other options can have them on, so a database that reads such a
        // name as a string literal is refused rather than let it read as
        // data: the probe prepares only on such a database.
        match self.prepare(r#"SELECT "_ankurah_dqs_probe""#) {
            Ok(_) => return Err(DatabaseRefusal::DoubleQuotedStringLiterals.into()),
            Err(SqliteError::Sqlite(failure))
                if failure.message.contains("no such column") && failure.message.contains("_ankurah_dqs_probe") => {}
            Err(error) => return Err(error),
        }
        // Only a database the executor accepts is switched to WAL.
        self.exec(SESSION_PRAGMAS)
    }
}

/// `driver` as better-sqlite3's `Database` class: a constructor whose
/// prototype has each method the executor calls on a database. Any other
/// value, such as the module object an `import *` of better-sqlite3 gives,
/// one of its databases, or another driver's class, is refused before
/// anything is built.
fn database_class(driver: &JsValue) -> Result<&Function, SqliteError> {
    let not_a_class = || SqliteError::from(DatabaseRefusal::NotADatabaseClass { found: format!("{driver:?}") });
    let class = driver.dyn_ref::<Function>().ok_or_else(not_a_class)?;
    let prototype = Reflect::get(class, &"prototype".into()).map_err(js_error)?;
    if !prototype.is_object() {
        return Err(not_a_class());
    }
    for name in DATABASE_METHODS {
        database_method(&prototype, name)?;
    }
    Ok(class)
}
