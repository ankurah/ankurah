//! Which SQL text one call accepts, and how its parameters bind, on every
//! executor.

use ankurah_storage_sqlite::{executor::SqliteExecutor, SqliteError, SqliteValueRef};
use anyhow::Result;

use crate::common::{self, engine_test};

/// SQL that holds a second statement is refused before either runs, on
/// every driver, where rusqlite and node:sqlite ran the first and ignored the
/// rest. Whitespace, semicolons and comments after a statement are no second
/// statement, except that better-sqlite3 refuses a query with a comment after
/// its semicolon (see `SqliteExecutor`); a byte SQLite cannot read after it
/// is.
#[engine_test]
async fn sql_holding_a_second_statement_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (refused, created, accepted, commented) = storage
        .with_executor(|executor| {
            let refused = [
                executor.execute("CREATE TABLE a (x); CREATE TABLE b (x)", &[]),
                // The second statement cannot even be prepared until the first has run.
                executor.execute("CREATE TABLE c (x); INSERT INTO c VALUES (1)", &[]),
                executor.query("SELECT 1; SELECT 2", &[]).map(|rows| rows.len()),
                // A vertical tab begins no whitespace for SQLite's tokenizer.
                executor.execute("CREATE TABLE e (x);\x0b", &[]),
            ]
            .map(|outcome| outcome.map_err(|error| error.to_string()));
            let created = executor
                .query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('a', 'b', 'c', 'e')", &[])?
                .expect("one row")
                .integer(0)?;
            executor.execute("CREATE TABLE d (x)", &[])?;
            let accepted = [
                executor.execute("INSERT INTO d VALUES (1);  ;; /* a comment */ -- and another", &[]),
                executor.query("SELECT 1; \n", &[]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.map_err(|error| error.to_string()));
            let commented = executor.query("SELECT 1; -- a note", &[]).map(|rows| rows.len()).map_err(|error| error.to_string());
            Ok((refused, created, accepted, commented))
        })
        .await?;
    for outcome in refused {
        let error = outcome.expect_err("a second statement is refused");
        // rusqlite's words, which the Node.js executor uses too.
        assert!(error.ends_with("Multiple statements provided"), "{error}");
    }
    assert_eq!(created, 0, "no statement of a refused string ran");
    assert_eq!(accepted, [Ok(1), Ok(1)]);
    if common::on_better_sqlite3() {
        commented.expect_err("the typed reader cannot nest the comment better-sqlite3 keeps after the semicolon");
    } else {
        assert_eq!(commented, Ok(1));
    }
    Ok(())
}

/// SQL that holds a second statement is refused before SQLite prepares
/// either, on every driver: SQLite applies some PRAGMAs, foreign_keys and
/// query_only among them, while it prepares them, so a refused string leaves
/// both as they were, whichever of its statements would have changed them.
#[engine_test]
async fn a_refused_string_of_two_statements_changes_no_setting() -> Result<()> {
    fn settings(executor: &dyn SqliteExecutor) -> Result<(i64, i64), SqliteError> {
        let row = executor
            .query_row("SELECT (SELECT foreign_keys FROM pragma_foreign_keys), (SELECT query_only FROM pragma_query_only)", &[])?
            .expect("one row");
        Ok((row.integer(0)?, row.integer(1)?))
    }
    let storage = common::open_in_memory().await?;
    let (before, refused, after) = storage
        .with_executor(|executor| {
            let before = settings(executor)?;
            let (foreign_keys, query_only) = (1 - before.0, 1 - before.1);
            let refused = [
                executor.execute(&format!("SELECT 1; PRAGMA foreign_keys = {foreign_keys}"), &[]),
                executor.execute(&format!("PRAGMA foreign_keys = {foreign_keys}; SELECT 1"), &[]),
                executor.query(&format!("SELECT 1; PRAGMA query_only = {query_only}"), &[]).map(|rows| rows.len()),
                executor.query(&format!("PRAGMA query_only = {query_only}; SELECT 1"), &[]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.is_err());
            Ok((before, refused, settings(executor)?))
        })
        .await?;
    assert_eq!(refused, [true; 4]);
    assert_eq!(after, before, "a refused string changed foreign_keys or query_only");
    Ok(())
}

/// What SQLite skips around a statement includes a UTF-8 byte order mark,
/// semicolons and comments, before a PRAGMA as before a query, which
/// `query` and `query_row` alike read past it, on every driver: the Node.js
/// executor nests the query past it in its typed reader. better-sqlite3
/// alone refuses a byte order mark after a statement, in its own words.
#[engine_test]
async fn a_byte_order_mark_and_semicolons_are_skipped_around_a_statement() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (timeouts, ones, trailing) = storage
        .with_executor(|executor| {
            let timeouts = ["\u{FEFF}PRAGMA busy_timeout", "; PRAGMA busy_timeout"].map(|sql| {
                executor.query_row(sql, &[]).and_then(|row| row.expect("one row").integer(0)).map_err(|error| error.to_string())
            });
            let ones = ["; SELECT 1", ";; VALUES (1)", "/* c */ SELECT 1"].map(|sql| {
                let every = executor.query(sql, &[]).and_then(|rows| rows.iter().map(|row| row.integer(0)).collect::<Result<Vec<_>, _>>());
                let first = executor.query_row(sql, &[]).and_then(|row| row.expect("one row").integer(0));
                (every.map_err(|error| error.to_string()), first.map_err(|error| error.to_string()))
            });
            Ok((timeouts, ones, executor.execute("CREATE TABLE marked (x);\u{FEFF}", &[]).map_err(|error| error.to_string())))
        })
        .await?;
    for timeout in timeouts {
        assert!(timeout.is_ok(), "{timeout:?}");
    }
    assert_eq!(ones, [(Ok(vec![1]), Ok(1)), (Ok(vec![1]), Ok(1)), (Ok(vec![1]), Ok(1))]);
    if common::on_better_sqlite3() {
        let refusal = trailing.expect_err("better-sqlite3 reads the byte order mark as a second statement");
        assert!(refusal.ends_with("The supplied SQL string contains more than one statement"), "{refusal}");
    } else {
        assert!(trailing.is_ok(), "{trailing:?}");
    }
    Ok(())
}

/// SQL that holds no statement, only what SQLite skips between statements,
/// is refused by `execute`, `query` and `query_row` alike, on every driver,
/// as SQLite refuses to run it: SQLITE_MISUSE, 21.
#[engine_test]
async fn sql_that_holds_no_statement_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let codes = storage
        .with_executor(|executor| {
            Ok([";", "", "-- c"].map(|sql| {
                [executor.execute(sql, &[]).map(drop), executor.query(sql, &[]).map(drop), executor.query_row(sql, &[]).map(drop)]
                    .map(|outcome| outcome.map_err(|error| error.sqlite_code()))
            }))
        })
        .await?;
    assert_eq!(codes, [[Err(Some(21)); 3]; 3]);
    Ok(())
}

/// A call passes a value for every parameter of its statement, and no more,
/// on every driver: one passing too few is refused before it runs, where
/// node:sqlite would bind NULL for each value missing, and so is one passing
/// too many. A numbered parameter counts as its number.
#[engine_test]
async fn a_call_passes_a_value_for_every_parameter() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (passed, refused, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE pairs (a, b)", &[])?;
            let seven = SqliteValueRef::Integer(7);
            // Each statement runs first with every value it takes, so that a
            // count kept from that run cannot let a shorter call through.
            let passed = [
                executor.query("SELECT ?, ?", &[seven, seven]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven, seven]),
            ];
            let refused = [
                executor.query("SELECT ?, ?", &[seven]).map(|rows| rows.len()),
                executor.query("SELECT ?, ?", &[]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven]),
                executor.query("SELECT ?3", &[seven, seven]).map(|rows| rows.len()),
                executor.query("SELECT ?, ?", &[seven; 3]).map(|rows| rows.len()),
                executor.execute("INSERT INTO pairs VALUES (?, ?)", &[seven; 3]),
            ];
            let stored = executor.query_row("SELECT count(*), count(b) FROM pairs", &[])?.expect("one row");
            Ok((
                passed.map(|outcome| outcome.map_err(|error| error.to_string())),
                refused.map(|outcome| outcome.map_err(|error| error.to_string())),
                (stored.integer(0)?, stored.integer(1)?),
            ))
        })
        .await?;
    assert_eq!(passed, [Ok(1), Ok(1)]);
    let refused = refused.map(|outcome| outcome.expect_err("the call is refused"));
    // rusqlite's words for too few values, which the Node.js executor uses too
    // on node:sqlite; better-sqlite3 refuses the call in its own.
    let too_few = if common::on_better_sqlite3() {
        "Too few parameter values were provided"
    } else {
        "Wrong number of parameters passed to query. Got 1"
    };
    assert!(refused[0].contains(too_few), "{}", refused[0]);
    assert_eq!(stored, (1, 1), "only the insert given both values wrote a row");
    Ok(())
}

/// A numbered parameter takes the value at its position on rusqlite and
/// node:sqlite, given a value for every position up to its number, one that
/// no parameter takes included; better-sqlite3 binds no positional value to
/// one, and refuses the call (see `SqliteExecutor`).
#[engine_test]
async fn a_numbered_parameter_takes_the_value_at_its_position() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (selected, inserted, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE pairs (a, b)", &[])?;
            let [seven, eight, nine] = [7, 8, 9].map(SqliteValueRef::Integer);
            let selected = executor.query("SELECT ?2, ?1", &[seven, eight]).map(|rows| format!("{:?}", rows[0].values()));
            let inserted = executor.execute("INSERT INTO pairs VALUES (?, ?3)", &[seven, eight, nine]);
            let stored = executor.query("SELECT a, b FROM pairs", &[])?;
            let stored = stored.iter().map(|row| Ok((row.integer(0)?, row.integer(1)?))).collect::<Result<Vec<_>, SqliteError>>()?;
            Ok((selected.map_err(|error| error.to_string()), inserted.map_err(|error| error.to_string()), stored))
        })
        .await?;
    if common::on_better_sqlite3() {
        for refusal in [selected.expect_err("better-sqlite3 refuses ?2"), inserted.expect_err("better-sqlite3 refuses ?3")] {
            assert!(refusal.ends_with("Too many parameter values were provided"), "{refusal}");
        }
        assert_eq!(stored, []);
    } else {
        assert_eq!(selected.as_deref(), Ok("[Integer(8), Integer(7)]"));
        assert_eq!(inserted, Ok(1));
        assert_eq!(stored, [(7, 9)]);
    }
    Ok(())
}
