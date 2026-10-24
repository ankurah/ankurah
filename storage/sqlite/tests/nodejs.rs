//! What only the Node.js executor offers: adopting a database the host built,
//! and refusing an object of the wrong shape.
#![cfg(target_arch = "wasm32")]

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sqlite::{executor::SqliteExecutor, NodeJsConnection, SqliteStorageEngine, SqliteValueRef};
use anyhow::Result;
use common::{host_database, on_better_sqlite3, Album, AlbumView};
use js_sys::{Function, Reflect};
use std::sync::Arc;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn adopts_a_host_built_database() -> Result<()> {
    let storage = SqliteStorageEngine::from_database(host_database(":memory:")).await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let trx = ctx.begin();
    trx.create(&Album { name: "Adopted".to_string(), year: "2026".to_string() }).await?;
    trx.commit().await?;

    let albums: Vec<AlbumView> = ctx.fetch("year = '2026'").await?;
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].name().unwrap(), "Adopted");
    Ok(())
}

#[wasm_bindgen_test]
async fn refuses_an_object_without_the_database_shape() {
    let error = SqliteStorageEngine::from_database(JsValue::from_str("not a database")).await.err().expect("a string is not a database");
    assert!(error.to_string().contains("prepare"), "the refusal names the shape it wanted: {error}");
}

// Hide one capability on an actual statement of the run's driver to exercise
// older or incompatible host drivers without substituting their SQL behavior.
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = "
export function withoutStatementMethod(database, name) {
    return {
        prepare(sql) {
            const statement = database.prepare(sql);
            Object.defineProperty(statement, name, { value: undefined });
            return statement;
        },
        exec(sql) { return database.exec(sql); }
    };
}
")]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = withoutStatementMethod)]
    fn without_statement_method(database: JsValue, name: &str) -> JsValue;
}

#[wasm_bindgen_test]
async fn refuses_a_driver_without_positional_rows() {
    let method = if on_better_sqlite3() { "raw" } else { "setReturnArrays" };
    let database = without_statement_method(host_database(":memory:"), method);
    let error = SqliteStorageEngine::from_database(database).await.err().expect("object rows are not positional");
    assert!(error.to_string().contains("positional rows"), "{error}");
}

#[wasm_bindgen_test]
async fn refuses_a_driver_without_exact_integers() {
    let method = if on_better_sqlite3() { "safeIntegers" } else { "setReadBigInts" };
    let database = without_statement_method(host_database(":memory:"), method);
    let error = SqliteStorageEngine::from_database(database).await.err().expect("Number cannot represent every INTEGER");
    assert!(error.to_string().contains("exact INTEGER"), "{error}");
}

// Hand over a copy of each statement of the run's driver that holds all but
// its SQL (node:sqlite's `sourceSQL`, better-sqlite3's `source`), each method
// bound to the actual statement: both drivers make the SQL a property that
// cannot be redefined, nor hidden behind a Proxy for better-sqlite3, whose
// `source` is a value that cannot change.
#[wasm_bindgen::prelude::wasm_bindgen(inline_js = "
export function withoutStatementSource(database) {
    return {
        prepare(sql) {
            const statement = database.prepare(sql);
            const copy = {};
            for (let object = statement; object && object !== Object.prototype; object = Object.getPrototypeOf(object)) {
                for (const name of Object.getOwnPropertyNames(object)) {
                    if (name === 'sourceSQL' || name === 'source' || name === 'constructor' || name in copy) continue;
                    const value = statement[name];
                    copy[name] = typeof value === 'function' ? value.bind(statement) : value;
                }
            }
            return copy;
        },
        exec(sql) { return database.exec(sql); }
    };
}
")]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = withoutStatementSource)]
    fn without_statement_source(database: JsValue) -> JsValue;
}

#[wasm_bindgen_test]
async fn refuses_a_driver_whose_statements_report_no_sql() {
    let database = without_statement_source(host_database(":memory:"));
    let error = SqliteStorageEngine::from_database(database).await.err().expect("the executor reads each statement's SQL");
    assert!(error.to_string().contains("report no `sourceSQL` (better-sqlite3's `source`)"), "{error}");
}

/// TEXT from a statement SQLite cannot nest in the typed reader, such as a
/// PRAGMA, is read as the driver's string, and the PRAGMA's other storage
/// classes as ever; a string holding U+FFFD, which a driver may have put in
/// place of bytes that are not UTF-8, is refused, pointing at the
/// table-valued function, which reads TEXT as its bytes.
#[wasm_bindgen_test]
async fn text_from_a_statement_sqlite_cannot_nest_is_read_as_a_string() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (names, marked, timeout) = storage
        .with_executor(|executor| {
            let names = executor.query(r#"PRAGMA table_info("_ankurah_meta")"#, &[])?;
            let names = names.iter().map(|row| row.text(1).map(str::to_owned)).collect::<Result<Vec<_>, _>>()?;
            executor.execute("CREATE TABLE marked (\"\u{FFFD}\" TEXT)", &[])?;
            let marked = executor.query("PRAGMA table_info(marked)", &[]).map(|rows| rows.len()).map_err(|error| error.to_string());
            Ok((names, marked, executor.query_row("PRAGMA busy_timeout", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    assert_eq!(names, ["key", "value"]);
    let refusal = marked.expect_err("a string holding U+FFFD is refused");
    assert!(refusal.contains("column 1 is TEXT holding U+FFFD") && refusal.contains("pragma_table_info"), "{refusal}");
    assert_eq!(timeout, 5000);
    Ok(())
}

/// A statement SQLite cannot nest that is neither a PRAGMA nor an EXPLAIN,
/// such as one with a RETURNING clause, is refused before it runs, so that
/// it writes nothing it could not then report.
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
            .map(|sql| executor.query(sql, &[]).map(|rows| rows.len()).map_err(|error| error.to_string()));
            Ok((refusals, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    for refusal in refusals {
        let refusal = refusal.expect_err("a RETURNING statement is refused on Node.js");
        assert!(refusal.contains("so it is not run"), "{refusal}");
    }
    assert_eq!(count, 0, "neither insert ran");
    Ok(())
}

/// A statement with a named parameter is refused before it runs, on both
/// drivers, where rusqlite binds a call's values to it in turn: node:sqlite
/// would bind none to it, leaving it NULL.
#[wasm_bindgen_test]
async fn a_named_parameter_is_refused_before_it_runs() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (refusals, count) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (a, b)", &[])?;
            let seven = SqliteValueRef::Integer(7);
            let refusals = [
                executor.execute("INSERT INTO things VALUES (:a, ?)", &[seven]),
                executor.execute("INSERT INTO things VALUES (?, @b)", &[seven, seven]),
                executor.query("SELECT a FROM things WHERE b = $c OR b = #d", &[seven, seven]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.map_err(|error| error.to_string()));
            Ok((refusals, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    for refusal in refusals {
        let refusal = refusal.expect_err("a named parameter is refused on Node.js");
        assert!(refusal.contains("write a positional `?`"), "{refusal}");
    }
    assert_eq!(count, 0, "neither insert ran");
    Ok(())
}

/// A database that reads a double-quoted name that matches no column as a
/// string literal is refused, since the engine refuses such a name on every
/// connection it opens natively; node:sqlite's DatabaseSync reads them so
/// when constructed with `enableDoubleQuotedStringLiterals`.
#[wasm_bindgen_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "better-sqlite3 is built with double-quoted string literals off, and offers no way to turn them on"
)]
fn refuses_a_database_that_reads_double_quoted_names_as_strings() {
    let construct = Function::new_no_args(
        "const { DatabaseSync } = process.getBuiltinModule('node:sqlite');
         return new DatabaseSync(':memory:', { enableDoubleQuotedStringLiterals: true });",
    );
    let error = NodeJsConnection::from_database(construct.call0(&JsValue::NULL).unwrap()).err().expect("the database is refused");
    assert!(error.to_string().contains("enableDoubleQuotedStringLiterals"), "{error}");
}

#[wasm_bindgen_test]
fn refuses_a_utf16_database() {
    let database = host_database(":memory:");
    let exec: Function = Reflect::get(&database, &"exec".into()).unwrap().dyn_into().unwrap();
    exec.call1(&database, &"PRAGMA encoding = 'UTF-16le'; CREATE TABLE t (v TEXT);".into()).unwrap();
    let error = NodeJsConnection::from_database(database).err().expect("a UTF-16 database is refused");
    assert!(error.to_string().contains("not UTF-8"), "{error}");
}

/// A UTF-16 database is refused though it holds a table named
/// `pragma_encoding` whose one row reads 'UTF-8': the check runs the PRAGMA
/// itself, which no table can shadow, as such a table shadows the
/// table-valued function of its name.
#[wasm_bindgen_test]
fn refuses_a_utf16_database_that_holds_a_table_named_pragma_encoding() {
    let database = host_database(":memory:");
    let exec: Function = Reflect::get(&database, &"exec".into()).unwrap().dyn_into().unwrap();
    let schema =
        "PRAGMA encoding = 'UTF-16le'; CREATE TABLE pragma_encoding (encoding TEXT); INSERT INTO pragma_encoding VALUES ('UTF-8');";
    exec.call1(&database, &schema.into()).unwrap();
    let error = NodeJsConnection::from_database(database).err().expect("a UTF-16 database is refused");
    assert!(error.to_string().contains("not UTF-8"), "{error}");
}

/// A host database whose statements pass through `tamper`: the body of a
/// JavaScript function of `statement` and the `sql` it was prepared from,
/// returning what the executor gets in its place, where `forward(target,
/// name)` reaches the real statement's member. A typed reader's SQL begins
/// `WITH _ankurah_typed_rows`.
fn tampered_database(tamper: &str) -> JsValue {
    let wrap = Function::new_with_args(
        "database",
        &format!(
            "const forward = (target, name) => {{
                 const value = target[name];
                 return typeof value === 'function' ? value.bind(target) : value;
             }};
             const tamper = (statement, sql) => {{ {tamper} }};
             return {{ prepare: (sql) => tamper(database.prepare(sql), sql), exec: (sql) => database.exec(sql) }};"
        ),
    );
    wrap.call1(&JsValue::NULL, &host_database(":memory:")).unwrap()
}

/// The refusal `NodeJsConnection::from_database` gives a tampered database.
fn refusal(tamper: &str) -> String {
    NodeJsConnection::from_database(tampered_database(tamper)).err().expect("the database is refused").to_string()
}

/// An INTEGER handed over as a number cannot be told from a REAL, so such a
/// database is refused rather than read by guesswork. The first INTEGER it
/// reads is the schema version the kept typed readers are checked against.
#[wasm_bindgen_test]
fn refuses_a_database_that_hands_an_integer_over_as_a_number() {
    let method = if on_better_sqlite3() { "safeIntegers" } else { "setReadBigInts" };
    let error = refusal(&format!(
        "return new Proxy(statement, {{ get: (target, name) => name === '{method}' ? () => {{}} : forward(target, name) }});"
    ));
    assert!(error.contains("expected an INTEGER as a BigInt, found JsValue(0)"), "{error}");
}

/// A row whose INTEGER arrives as a number is refused, the storage class the
/// typed reader reports for it standing against the number.
#[wasm_bindgen_test]
fn refuses_a_row_whose_integer_arrives_as_a_number() {
    let error = refusal(
        "return new Proxy(statement, { get: (target, name) => name === 'all' && sql.startsWith('WITH _ankurah_typed_rows')
             ? (...params) => target.all(...params).map((row) => row.map((value) => typeof value === 'bigint' ? Number(value) : value))
             : forward(target, name) });",
    );
    assert!(error.contains("storage class integer that arrived as Real(1.0)"), "{error}");
}

#[wasm_bindgen_test]
fn refuses_a_row_of_the_wrong_width() {
    let error = refusal(
        "return new Proxy(statement, { get: (target, name) => name === 'all' && sql.startsWith('WITH _ankurah_typed_rows')
             ? (...params) => target.all(...params).map((row) => [...row, 0n])
             : forward(target, name) });",
    );
    assert!(error.contains("returned a row of 3"), "{error}");
}

/// A database that skips a numbered parameter as it binds a call's values,
/// as node:sqlite did before Node.js 22.20.0 on 22 and 24.7.0 on 24, is
/// refused, naming the releases that bind one at its position. The fake's
/// `SELECT ?2` refuses its second value as those releases do, since they
/// bind it past the statement's last parameter: SQLITE_RANGE, 25.
#[wasm_bindgen_test]
#[cfg_attr(
    ankurah_sqlite_better_sqlite3,
    ignore = "better-sqlite3 binds no positional value to a numbered parameter and refuses the call itself, so the executor \
              checks no binding of one there"
)]
fn refuses_a_database_that_skips_a_numbered_parameter() {
    let error = refusal(
        "return sql !== 'SELECT ?2' ? statement : new Proxy(statement, { get: (target, name) => name === 'get'
             ? (...params) => {
                   if (params.length < 2) return target.get(...params);
                   throw Object.assign(new Error('column index out of range'), { code: 'ERR_SQLITE_ERROR', errcode: 25 });
               }
             : forward(target, name) });",
    );
    assert!(error.contains("Node.js 22.20.0 or later on 22, 24.7.0 or later on 24, or a later release line"), "{error}");
}

/// `undefined` is no storage class: it is refused, not read as NULL.
#[wasm_bindgen_test]
fn refuses_undefined_where_null_was_handed_over() {
    let connection = NodeJsConnection::from_database(tampered_database(
        "return new Proxy(statement, { get: (target, name) => name === 'all'
             ? (...params) => target.all(...params).map((row) => row.map((value) => value === null ? undefined : value))
             : forward(target, name) });",
    ))
    .expect("SELECT 1 holds no NULL to tamper with");
    let error = connection.query("SELECT NULL", &[]).expect_err("undefined is refused");
    assert!(error.to_string().contains("holds JsValue(undefined)"), "{error}");
}
