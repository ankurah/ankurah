//! A JavaScript exception better-sqlite3 throws, as the executor reports it:
//! an error SQLite raised, with SQLite's extended result code, or a
//! JavaScript exception of another kind.

use js_sys::Reflect;
use wasm_bindgen::{JsCast, JsValue};

use crate::error::{SqliteError, SqliteFailure};

/// A JavaScript exception as an error. better-sqlite3 throws an error SQLite
/// raised as its own `SqliteError`, which names SQLite's extended result
/// code as `code`, and the name is read as its number. The code is kept, as
/// rusqlite keeps it natively. Any other exception, such as better-sqlite3's
/// own refusal of a call or a host function's exception, is a
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
    if property("name").and_then(|name| name.as_string()).as_deref() == Some("SqliteError") {
        if let Some(name) = property("code").and_then(|code| code.as_string()) {
            return SqliteError::Sqlite(SqliteFailure { code: result_code_named(&name), message });
        }
    }
    SqliteError::JavaScript(message)
}

/// The extended result code better-sqlite3 names `name`: the name sqlite3.h
/// gives the code, or, for a code better-sqlite3 has no name of its own for,
/// `UNKNOWN_SQLITE_ERROR_` and its number. `None` for any other name, such as
/// one an SQLite release later than the table's adds.
fn result_code_named(name: &str) -> Option<i32> {
    if let Some(number) = name.strip_prefix("UNKNOWN_SQLITE_ERROR_") {
        return number.parse().ok();
    }
    // SQLite's result codes and extended result codes, each by the name
    // sqlite3.h gives it, generated from the sqlite3.h of SQLite 3.53.4,
    // which better-sqlite3 13.0.3 bundles as deps/sqlite3/sqlite3.h (under
    // storage/sqlite/tests/node_modules after `npm ci` there): each
    // `#define` from SQLITE_OK to the header's `end-of-error-codes` marker,
    // then each under `CAPI3REF: Extended Result Codes`, whose
    // `(SQLITE_<code> | (<n><<8))` is computed to its number.
    Some(match name {
        "SQLITE_OK" => 0,
        "SQLITE_ERROR" => 1,
        "SQLITE_INTERNAL" => 2,
        "SQLITE_PERM" => 3,
        "SQLITE_ABORT" => 4,
        "SQLITE_BUSY" => 5,
        "SQLITE_LOCKED" => 6,
        "SQLITE_NOMEM" => 7,
        "SQLITE_READONLY" => 8,
        "SQLITE_INTERRUPT" => 9,
        "SQLITE_IOERR" => 10,
        "SQLITE_CORRUPT" => 11,
        "SQLITE_NOTFOUND" => 12,
        "SQLITE_FULL" => 13,
        "SQLITE_CANTOPEN" => 14,
        "SQLITE_PROTOCOL" => 15,
        "SQLITE_EMPTY" => 16,
        "SQLITE_SCHEMA" => 17,
        "SQLITE_TOOBIG" => 18,
        "SQLITE_CONSTRAINT" => 19,
        "SQLITE_MISMATCH" => 20,
        "SQLITE_MISUSE" => 21,
        "SQLITE_NOLFS" => 22,
        "SQLITE_AUTH" => 23,
        "SQLITE_FORMAT" => 24,
        "SQLITE_RANGE" => 25,
        "SQLITE_NOTADB" => 26,
        "SQLITE_NOTICE" => 27,
        "SQLITE_WARNING" => 28,
        "SQLITE_ROW" => 100,
        "SQLITE_DONE" => 101,
        "SQLITE_ERROR_MISSING_COLLSEQ" => 257,
        "SQLITE_ERROR_RETRY" => 513,
        "SQLITE_ERROR_SNAPSHOT" => 769,
        "SQLITE_ERROR_RESERVESIZE" => 1025,
        "SQLITE_ERROR_KEY" => 1281,
        "SQLITE_ERROR_UNABLE" => 1537,
        "SQLITE_IOERR_READ" => 266,
        "SQLITE_IOERR_SHORT_READ" => 522,
        "SQLITE_IOERR_WRITE" => 778,
        "SQLITE_IOERR_FSYNC" => 1034,
        "SQLITE_IOERR_DIR_FSYNC" => 1290,
        "SQLITE_IOERR_TRUNCATE" => 1546,
        "SQLITE_IOERR_FSTAT" => 1802,
        "SQLITE_IOERR_UNLOCK" => 2058,
        "SQLITE_IOERR_RDLOCK" => 2314,
        "SQLITE_IOERR_DELETE" => 2570,
        "SQLITE_IOERR_BLOCKED" => 2826,
        "SQLITE_IOERR_NOMEM" => 3082,
        "SQLITE_IOERR_ACCESS" => 3338,
        "SQLITE_IOERR_CHECKRESERVEDLOCK" => 3594,
        "SQLITE_IOERR_LOCK" => 3850,
        "SQLITE_IOERR_CLOSE" => 4106,
        "SQLITE_IOERR_DIR_CLOSE" => 4362,
        "SQLITE_IOERR_SHMOPEN" => 4618,
        "SQLITE_IOERR_SHMSIZE" => 4874,
        "SQLITE_IOERR_SHMLOCK" => 5130,
        "SQLITE_IOERR_SHMMAP" => 5386,
        "SQLITE_IOERR_SEEK" => 5642,
        "SQLITE_IOERR_DELETE_NOENT" => 5898,
        "SQLITE_IOERR_MMAP" => 6154,
        "SQLITE_IOERR_GETTEMPPATH" => 6410,
        "SQLITE_IOERR_CONVPATH" => 6666,
        "SQLITE_IOERR_VNODE" => 6922,
        "SQLITE_IOERR_AUTH" => 7178,
        "SQLITE_IOERR_BEGIN_ATOMIC" => 7434,
        "SQLITE_IOERR_COMMIT_ATOMIC" => 7690,
        "SQLITE_IOERR_ROLLBACK_ATOMIC" => 7946,
        "SQLITE_IOERR_DATA" => 8202,
        "SQLITE_IOERR_CORRUPTFS" => 8458,
        "SQLITE_IOERR_IN_PAGE" => 8714,
        "SQLITE_IOERR_BADKEY" => 8970,
        "SQLITE_IOERR_CODEC" => 9226,
        "SQLITE_LOCKED_SHAREDCACHE" => 262,
        "SQLITE_LOCKED_VTAB" => 518,
        "SQLITE_BUSY_RECOVERY" => 261,
        "SQLITE_BUSY_SNAPSHOT" => 517,
        "SQLITE_BUSY_TIMEOUT" => 773,
        "SQLITE_CANTOPEN_NOTEMPDIR" => 270,
        "SQLITE_CANTOPEN_ISDIR" => 526,
        "SQLITE_CANTOPEN_FULLPATH" => 782,
        "SQLITE_CANTOPEN_CONVPATH" => 1038,
        "SQLITE_CANTOPEN_DIRTYWAL" => 1294,
        "SQLITE_CANTOPEN_SYMLINK" => 1550,
        "SQLITE_CORRUPT_VTAB" => 267,
        "SQLITE_CORRUPT_SEQUENCE" => 523,
        "SQLITE_CORRUPT_INDEX" => 779,
        "SQLITE_READONLY_RECOVERY" => 264,
        "SQLITE_READONLY_CANTLOCK" => 520,
        "SQLITE_READONLY_ROLLBACK" => 776,
        "SQLITE_READONLY_DBMOVED" => 1032,
        "SQLITE_READONLY_CANTINIT" => 1288,
        "SQLITE_READONLY_DIRECTORY" => 1544,
        "SQLITE_ABORT_ROLLBACK" => 516,
        "SQLITE_CONSTRAINT_CHECK" => 275,
        "SQLITE_CONSTRAINT_COMMITHOOK" => 531,
        "SQLITE_CONSTRAINT_FOREIGNKEY" => 787,
        "SQLITE_CONSTRAINT_FUNCTION" => 1043,
        "SQLITE_CONSTRAINT_NOTNULL" => 1299,
        "SQLITE_CONSTRAINT_PRIMARYKEY" => 1555,
        "SQLITE_CONSTRAINT_TRIGGER" => 1811,
        "SQLITE_CONSTRAINT_UNIQUE" => 2067,
        "SQLITE_CONSTRAINT_VTAB" => 2323,
        "SQLITE_CONSTRAINT_ROWID" => 2579,
        "SQLITE_CONSTRAINT_PINNED" => 2835,
        "SQLITE_CONSTRAINT_DATATYPE" => 3091,
        "SQLITE_NOTICE_RECOVER_WAL" => 283,
        "SQLITE_NOTICE_RECOVER_ROLLBACK" => 539,
        "SQLITE_NOTICE_RBU" => 795,
        "SQLITE_WARNING_AUTOINDEX" => 284,
        "SQLITE_AUTH_USER" => 279,
        "SQLITE_OK_LOAD_PERMANENTLY" => 256,
        "SQLITE_OK_SYMLINK" => 512,
        _ => return None,
    })
}
