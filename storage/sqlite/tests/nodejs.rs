//! What only the Node.js executor does: refusing a driver that is not
//! better-sqlite3's `Database` class, refusing a database it cannot read
//! exactly, and refusing the statements it cannot read as rusqlite does,
//! such as a RETURNING statement or a PRAGMA whose TEXT holds U+FFFD.
#![cfg(target_arch = "wasm32")]

mod common;

use ankurah_storage_sqlite::{
    executor::SqliteExecutor, DatabaseRefusal, NodeJsConnection, Refusal, SqliteError, SqliteFailure, SqliteStorageEngine,
};
use anyhow::Result;
use common::better_sqlite3;
use js_sys::{Array, Function};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

/// The refusal an engine's constructor reports, if it reports one.
fn refusal_of(error: &anyhow::Error) -> Option<Refusal> { error.downcast_ref::<SqliteError>().and_then(SqliteError::refusal) }

/// The refusal of a driver or a database, as a [`Refusal::Database`].
fn refused_database(refusal: DatabaseRefusal) -> Option<Refusal> { Some(Refusal::Database(refusal)) }

/// A class standing in for better-sqlite3's `Database`, made by `make`: the
/// body of a JavaScript function of `Database`, better-sqlite3's own class,
/// returning the class the executor gets in its place.
fn class(make: &str) -> JsValue { Function::new_with_args("Database", make).call1(&JsValue::NULL, &better_sqlite3()).unwrap() }

/// A class that builds better-sqlite3's own databases, whose statements pass
/// through `tamper`: the body of a JavaScript function of `statement` and
/// the `sql` it was prepared from, returning what the executor gets in its
/// place, where `forward(target, name)` reaches the real statement's member.
/// A typed reader's SQL begins `WITH _ankurah_typed_rows`.
fn tampered_class(tamper: &str) -> JsValue {
    class(&format!(
        "const forward = (target, name) => {{
             const value = target[name];
             return typeof value === 'function' ? value.bind(target) : value;
         }};
         const tamper = (statement, sql) => {{ {tamper} }};
         return class extends Database {{ prepare(sql) {{ return tamper(super.prepare(sql), sql); }} }};"
    ))
}

/// A connection in memory on a database `tampered_class(tamper)` builds.
fn tampered(tamper: &str) -> Result<NodeJsConnection, SqliteError> { NodeJsConnection::open(":memory:", &tampered_class(tamper)) }

/// The refusal `NodeJsConnection::open` gives a database
/// `tampered_class(tamper)` builds.
fn refusal(tamper: &str) -> Option<Refusal> { tampered(tamper).err().expect("the database is refused").refusal() }

/// A driver that is not better-sqlite3's `Database` class is refused before
/// anything is built: a value that is not a class, such as a string, the
/// module object an `import *` of better-sqlite3 gives, one of its databases
/// or an arrow function, as not a class, and a class whose databases lack a
/// method the executor calls, here one whose constructor throws, by that
/// method's name.
#[wasm_bindgen_test]
async fn refuses_a_driver_that_is_not_the_database_class() {
    let drivers: Array = class(
        "return [
             'better-sqlite3',
             { default: Database },
             new Database(':memory:'),
             () => {},
             class { constructor() { throw new Error('constructed'); } prepare() {} exec() {} close() {} },
         ];",
    )
    .dyn_into()
    .unwrap();
    let refusals: Vec<_> =
        drivers.iter().map(|driver| NodeJsConnection::open(":memory:", &driver).err().and_then(|error| error.refusal())).collect();
    for refusal in &refusals[..4] {
        assert!(matches!(refusal, Some(Refusal::Database(DatabaseRefusal::NotADatabaseClass { .. }))), "{refusal:?}");
    }
    assert_eq!(refusals[4], refused_database(DatabaseRefusal::NoDatabaseMethod { name: "unsafeMode" }));
    let error = SqliteStorageEngine::open_in_memory(&JsValue::from_str("better-sqlite3")).await.err().expect("a string is not a class");
    assert!(matches!(refusal_of(&error), Some(Refusal::Database(DatabaseRefusal::NotADatabaseClass { .. }))), "{error}");
}

/// A database whose statements lack `raw` or `safeIntegers`, through which
/// the executor asks for rows as arrays and INTEGERs as BigInts, is refused
/// as it opens, naming the method.
#[wasm_bindgen_test]
fn refuses_a_database_whose_statements_lack_a_method_the_executor_calls() {
    for name in ["raw", "safeIntegers"] {
        let refusal = refusal(&format!("Object.defineProperty(statement, '{name}', {{ value: undefined }}); return statement;"));
        assert_eq!(refusal, refused_database(DatabaseRefusal::NoStatementMethod { name }));
    }
}

/// TEXT from a statement SQLite cannot nest in the typed reader, such as a
/// PRAGMA, is read as the driver's string, and the statement's other values
/// as their storage classes; a string holding U+FFFD, which better-sqlite3
/// may have put in place of bytes that are not UTF-8, is refused, naming its
/// column.
#[wasm_bindgen_test]
async fn text_from_a_statement_sqlite_cannot_nest_is_read_as_a_string() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (names, marked, timeout) = storage
        .with_executor(|executor| {
            let names = executor.query(r#"PRAGMA table_info("_ankurah_meta")"#, &[])?;
            let names = names.iter().map(|row| row.text(1).map(str::to_owned)).collect::<Result<Vec<_>, _>>()?;
            executor.execute("CREATE TABLE marked (\"\u{FFFD}\" TEXT)", &[])?;
            let marked = executor.query("PRAGMA table_info(marked)", &[]).map(|rows| rows.len()).map_err(|error| error.refusal());
            Ok((names, marked, executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    assert_eq!(names, ["key", "value"]);
    assert_eq!(marked, Err(Some(Refusal::ReplacementCharacter { column: 1 })), "a string holding U+FFFD is refused");
    assert_eq!(timeout, 5000);
    Ok(())
}

/// A statement SQLite cannot nest that is neither a PRAGMA nor an EXPLAIN,
/// such as one with a RETURNING clause, is refused before it runs, so that
/// it writes nothing it could not then report. The refusal keeps SQLite's
/// error, and so its code, SQLITE_ERROR.
#[wasm_bindgen_test]
async fn a_returning_statement_is_refused_before_it_writes() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (refusals, count) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (v TEXT)", &[])?;
            let refusals = [
                "INSERT INTO things (v) VALUES ('x') RETURNING v",
                "/* a comment */ WITH value (v) AS (VALUES ('y')) INSERT INTO things (v) SELECT v FROM value RETURNING rowid",
            ]
            .map(|sql| executor.query(sql, &[]).map(|rows| rows.len()).map_err(|error| (error.refusal(), error.sqlite_code())));
            Ok((refusals, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    for refusal in refusals {
        assert!(matches!(refusal, Err((Some(Refusal::NotNestable(_)), Some(1)))), "a RETURNING statement is refused: {refusal:?}");
    }
    assert_eq!(count, 0, "neither insert ran");
    Ok(())
}

/// A database whose SQLite reads a double-quoted name that matches no
/// column as a string literal is refused, since the engine refuses such a
/// name on every connection it opens natively. better-sqlite3 builds its
/// SQLite with such literals off, so the class here reads the probe's name
/// as the string an SQLite with them on makes of it.
#[wasm_bindgen_test]
fn refuses_a_database_that_reads_double_quoted_names_as_strings() {
    let reading_strings = class(
        r#"return class extends Database {
               prepare(sql) { return super.prepare(sql.replaceAll('"_ankurah_dqs_probe"', "'_ankurah_dqs_probe'")); }
           };"#,
    );
    let error = NodeJsConnection::open(":memory:", &reading_strings).err().expect("the database is refused");
    assert_eq!(error.refusal(), refused_database(DatabaseRefusal::DoubleQuotedStringLiterals), "{error}");
}

/// better-sqlite3's own database on `path`, built behind the engine's back,
/// runs `sql`, through `exec` or as a query whose first value it returns,
/// and closes.
fn run_on_file(path: &str, sql: &str, query: bool) -> Result<JsValue, JsValue> {
    Function::new_with_args(
        "Database, path, sql, query",
        "const database = new Database(path);
         try { return query ? database.prepare(sql).pluck().get() : database.exec(sql); }
         finally { database.close(); }",
    )
    .apply(&JsValue::NULL, &Array::of4(&better_sqlite3(), &path.into(), &sql.into(), &query.into()))
}

/// A UTF-16 file is refused, and the database `open` built on it is closed
/// before the refusal returns. The file is in WAL mode, whose WAL a database
/// left open would hold, so that no other connection could take the file out
/// of WAL mode.
#[wasm_bindgen_test]
fn open_refuses_a_utf16_file_and_closes_it() {
    let file = common::TempDb::new("refused_by_open");
    run_on_file(file.path_str(), "PRAGMA encoding = 'UTF-16le'; PRAGMA journal_mode = WAL; CREATE TABLE t (v TEXT);", false).unwrap();
    let error = NodeJsConnection::open(file.path_str(), &better_sqlite3()).err().expect("a UTF-16 file is refused");
    assert_eq!(error.refusal(), refused_database(DatabaseRefusal::NotUtf8), "{error}");
    let journal_mode = run_on_file(file.path_str(), "PRAGMA journal_mode = DELETE", true)
        .map(|mode| mode.as_string())
        .map_err(|error| String::from(error.unchecked_into::<js_sys::Error>().message()));
    assert_eq!(journal_mode, Ok(Some("delete".to_owned())), "no other connection holds the file's WAL");
}

/// A UTF-16 file is refused though it holds a table named `pragma_encoding`
/// whose one row reads 'UTF-8': the check runs the PRAGMA itself, which no
/// table can shadow, as such a table shadows the table-valued function of
/// its name.
#[wasm_bindgen_test]
fn refuses_a_utf16_file_that_holds_a_table_named_pragma_encoding() {
    let file = common::TempDb::new("pragma_encoding");
    let schema =
        "PRAGMA encoding = 'UTF-16le'; CREATE TABLE pragma_encoding (encoding TEXT); INSERT INTO pragma_encoding VALUES ('UTF-8');";
    run_on_file(file.path_str(), schema, false).unwrap();
    let error = NodeJsConnection::open(file.path_str(), &better_sqlite3()).err().expect("a UTF-16 file is refused");
    assert_eq!(error.refusal(), refused_database(DatabaseRefusal::NotUtf8), "{error}");
}

/// An INTEGER handed over as a number cannot be told from a REAL, so such a
/// database is refused rather than read by guesswork. The first INTEGER it
/// reads is the schema version the kept typed readers are checked against.
#[wasm_bindgen_test]
fn refuses_a_database_that_hands_an_integer_over_as_a_number() {
    let refusal =
        refusal("return new Proxy(statement, { get: (target, name) => name === 'safeIntegers' ? () => {} : forward(target, name) });");
    assert_eq!(refusal, refused_database(DatabaseRefusal::NoExactIntegers));
}

/// A row whose INTEGER arrives as a number is refused, the storage class the
/// typed reader reports for it showing that the number stands for an
/// INTEGER.
#[wasm_bindgen_test]
fn refuses_a_row_whose_integer_arrives_as_a_number() {
    let connection = tampered(
        "return new Proxy(statement, { get: (target, name) => name === 'all' && sql.startsWith('WITH _ankurah_typed_rows')
             ? (...params) => target.all(...params).map((row) => row.map((value) => typeof value === 'bigint' ? Number(value) : value))
             : forward(target, name) });",
    )
    .expect("opening the database reads no row through a typed reader");
    assert_eq!(
        connection.query("SELECT 1", &[]).err().and_then(|error| error.refusal()),
        refused_database(DatabaseRefusal::NoExactIntegers)
    );
}

#[wasm_bindgen_test]
fn refuses_a_row_of_the_wrong_width() {
    let connection = tampered(
        "return new Proxy(statement, { get: (target, name) => name === 'all' && sql.startsWith('WITH _ankurah_typed_rows')
             ? (...params) => target.all(...params).map((row) => [...row, 0n])
             : forward(target, name) });",
    )
    .expect("opening the database reads no row through a typed reader");
    let unreadable = "a row of 3 values from a statement of 2 columns".to_owned();
    assert_eq!(
        connection.query("SELECT 1", &[]).err().and_then(|error| error.refusal()),
        refused_database(DatabaseRefusal::Unreadable(unreadable))
    );
}

/// A typed reader the database fails to prepare for a reason other than
/// SQLite's, such as an exception the driver throws, fails with that
/// exception, not as a statement SQLite cannot nest.
#[wasm_bindgen_test]
fn a_typed_reader_that_fails_to_prepare_otherwise_than_in_sqlite_reports_the_failure() {
    let connection = tampered(
        "if (sql.startsWith('WITH _ankurah_typed_rows')) throw new TypeError('the typed reader is not prepared');
         return statement;",
    )
    .expect("opening the database prepares no typed reader");
    let error = connection.query("SELECT 1", &[]).expect_err("the typed reader is not prepared");
    assert_eq!(common::javascript_message(&error), Some("the typed reader is not prepared"), "{error}");
}

/// An error thrown as better-sqlite3 throws one SQLite raised, as its
/// `SqliteError`, is read as SQLite's error with the code its `code` names:
/// `UNKNOWN_SQLITE_ERROR_` and the number, as better-sqlite3 names a code it
/// has no name of its own for, gives the number, and a name that no SQLite
/// release up to the crate's table defines gives no code. The fake throws
/// each from `prepare`.
#[wasm_bindgen_test]
fn reads_the_code_a_better_sqlite3_error_names() {
    let connection = tampered(
        "const names = { 'SELECT 773': 'UNKNOWN_SQLITE_ERROR_773', 'SELECT 0': 'SQLITE_DEFINED_LATER' };
         if (Object.hasOwn(names, sql)) {
             class SqliteError extends Error {}
             Object.defineProperty(SqliteError.prototype, 'name', { value: 'SqliteError' });
             throw Object.assign(new SqliteError('raised by the fake'), { code: names[sql] });
         }
         return statement;",
    )
    .expect("the fake throws for none of the statements `open` runs");
    let failure = |sql: &str| match connection.query(sql, &[]) {
        Err(SqliteError::Sqlite(failure)) => failure,
        other => panic!("{sql}: {other:?}"),
    };
    assert_eq!(failure("SELECT 773"), SqliteFailure { code: Some(773), message: "raised by the fake".into() });
    assert_eq!(failure("SELECT 0"), SqliteFailure { code: None, message: "raised by the fake".into() });
}

/// `undefined` is no storage class: it is refused, not read as NULL.
#[wasm_bindgen_test]
fn refuses_undefined_where_null_was_handed_over() {
    let connection = tampered(
        "return new Proxy(statement, { get: (target, name) => name === 'all'
             ? (...params) => target.all(...params).map((row) => row.map((value) => value === null ? undefined : value))
             : forward(target, name) });",
    )
    .expect("opening the database reads no NULL");
    let error = connection.query("SELECT NULL", &[]).expect_err("undefined is refused");
    let unreadable = "JsValue(undefined) in column 0, which stands for no SQLite storage class".to_owned();
    assert_eq!(error.refusal(), refused_database(DatabaseRefusal::Unreadable(unreadable)), "{error}");
}
