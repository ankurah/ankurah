//! Which SQL text one call accepts, and how its parameters bind, on every
//! executor.

use ankurah_storage_sqlite::{executor::SqliteExecutor, Refusal, SqliteError, SqliteValueRef};
use anyhow::Result;

use crate::common::{self, engine_test};

/// SQL that holds a second statement is refused before either runs, on
/// every driver, where rusqlite on its own runs the first and ignores the
/// rest. Whitespace, semicolons and comments after a statement are no second
/// statement; a byte SQLite cannot read after it is.
#[engine_test]
async fn sql_holding_a_second_statement_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (refused, created, accepted) = storage
        .with_executor(|executor| {
            let refused = [
                executor.execute("CREATE TABLE a (x); CREATE TABLE b (x)", &[]),
                // The second statement cannot even be prepared until the first has run.
                executor.execute("CREATE TABLE c (x); INSERT INTO c VALUES (1)", &[]),
                executor.query("SELECT 1; SELECT 2", &[]).map(|rows| rows.len()),
                // A vertical tab begins no whitespace for SQLite's tokenizer.
                executor.execute("CREATE TABLE e (x);\x0b", &[]),
            ]
            .map(|outcome| outcome.map_err(|error| error.refusal()));
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
            Ok((refused, created, accepted))
        })
        .await?;
    for outcome in refused {
        assert_eq!(outcome, Err(Some(Refusal::MultipleStatements)), "a second statement is refused");
    }
    assert_eq!(created, 0, "no statement of a refused string ran");
    assert_eq!(accepted, [Ok(1), Ok(1)]);
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
            .map(|outcome| outcome.err().and_then(|error| error.refusal()));
            Ok((before, refused, settings(executor)?))
        })
        .await?;
    for refusal in refused {
        assert_eq!(refusal, Some(Refusal::MultipleStatements));
    }
    assert_eq!(after, before, "a refused string changed foreign_keys or query_only");
    Ok(())
}

/// `query` and `query_row` read past what SQLite skips before a statement,
/// a UTF-8 byte order mark, semicolons and comments among it, on every
/// driver, before a PRAGMA just as before a query: the Node.js executor nests
/// the query past it in its typed reader. They read a statement followed by
/// what SQLite skips after it, a comment that is never closed and a byte
/// order mark among it, as `execute` runs one, since the Node.js executor
/// prepares the statement alone.
#[engine_test]
async fn a_byte_order_mark_and_semicolons_are_skipped_around_a_statement() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (timeouts, ones, trailing) = storage
        .with_executor(|executor| {
            let timeouts = ["\u{FEFF}PRAGMA busy_timeout", "; PRAGMA busy_timeout"].map(|sql| {
                executor.query_row(sql, &[]).and_then(|row| row.expect("one row").integer(0)).map_err(|error| error.to_string())
            });
            let ones = [
                "; SELECT 1",
                ";; VALUES (1)",
                "/* c */ SELECT 1",
                "SELECT 1; -- a note",
                "SELECT 1;;",
                "SELECT 1; /* c */",
                "SELECT 1;\u{FEFF}",
                "SELECT 1 /* unterminated",
            ]
            .map(|sql| {
                let every = executor.query(sql, &[]).and_then(|rows| rows.iter().map(|row| row.integer(0)).collect::<Result<Vec<_>, _>>());
                let first = executor.query_row(sql, &[]).and_then(|row| row.expect("one row").integer(0));
                (sql, (every.map_err(|error| error.to_string()), first.map_err(|error| error.to_string())))
            });
            Ok((timeouts, ones, executor.execute("CREATE TABLE marked (x);\u{FEFF}", &[])))
        })
        .await?;
    for timeout in timeouts {
        assert!(timeout.is_ok(), "{timeout:?}");
    }
    for (sql, read) in ones {
        assert_eq!(read, (Ok(vec![1]), Ok(1)), "{sql:?}");
    }
    assert!(trailing.is_ok(), "{trailing:?}");
    Ok(())
}

/// SQL that holds no statement, only what SQLite skips between statements,
/// is refused by `execute`, `query` and `query_row` alike, on every driver:
/// natively SQLite refuses to run it, with SQLITE_MISUSE, 21, and on Node.js
/// better-sqlite3 refuses to prepare it, in its own words.
#[engine_test]
async fn sql_that_holds_no_statement_is_refused() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let outcomes = storage
        .with_executor(|executor| {
            Ok([";", "", "-- c"].map(|sql| {
                [executor.execute(sql, &[]).map(drop), executor.query(sql, &[]).map(drop), executor.query_row(sql, &[]).map(drop)]
                    .map(|outcome| outcome.map_err(|error| (error.sqlite_code(), common::javascript_message(&error).map(str::to_owned))))
            }))
        })
        .await?;
    let refused = if cfg!(target_arch = "wasm32") {
        (None, Some("The supplied SQL string contains no statements".to_owned()))
    } else {
        (Some(21), None)
    };
    for outcome in outcomes.iter().flatten() {
        assert_eq!(outcome, &Err(refused.clone()));
    }
    Ok(())
}

/// A named parameter (`:a`, `@a`, `#a` or `$a`) takes a call's value in
/// turn on rusqlite, which binds a call's values to every parameter in
/// order; better-sqlite3 binds one only by name, so it refuses, in its own
/// words and before the statement runs, a call that gives one a positional
/// value (see `SqliteExecutor`).
#[engine_test]
async fn a_named_parameter_takes_a_positional_value_on_rusqlite_alone() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (outcomes, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE things (a, b)", &[])?;
            let seven = SqliteValueRef::Integer(7);
            let outcomes = [
                executor.execute("INSERT INTO things VALUES (:a, ?)", &[seven, seven]),
                executor.execute("INSERT INTO things VALUES (?, @b)", &[seven, seven]),
                executor.query("SELECT a FROM things WHERE b = $c OR b = #d", &[seven, seven]).map(|rows| rows.len()),
            ]
            .map(|outcome| outcome.map_err(|error| common::javascript_message(&error).map(str::to_owned).ok_or(error.to_string())));
            Ok((outcomes, executor.query_row("SELECT count(*) FROM things", &[])?.expect("one row").integer(0)?))
        })
        .await?;
    if cfg!(target_arch = "wasm32") {
        let many = Err(Ok("Too many parameter values were provided".to_owned()));
        assert_eq!(outcomes, [many.clone(), many.clone(), many]);
        assert_eq!(stored, 0, "neither insert ran");
    } else {
        assert_eq!(outcomes, [Ok(1), Ok(1), Ok(2)]);
        assert_eq!(stored, 2);
    }
    Ok(())
}

/// A call passes a value for every parameter of its statement, and no more,
/// on every driver: one passing too few is refused before it runs, and so is
/// one passing too many. A numbered parameter counts as its number. rusqlite
/// and better-sqlite3 refuse the count of values themselves, better-sqlite3
/// in its own words.
#[engine_test]
async fn a_call_passes_a_value_for_every_parameter() -> Result<()> {
    let storage = common::open_in_memory().await?;
    let (passed, refused, stored) = storage
        .with_executor(|executor| {
            executor.execute("CREATE TABLE pairs (a, b)", &[])?;
            let seven = SqliteValueRef::Integer(7);
            // Each statement runs first with every value it takes, so that a
            // typed reader kept from that run cannot let a shorter call
            // through.
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
            Ok((passed.map(|outcome| outcome.map_err(|error| error.to_string())), refused, (stored.integer(0)?, stored.integer(1)?)))
        })
        .await?;
    assert_eq!(passed, [Ok(1), Ok(1)]);
    let refused = refused.map(|outcome| outcome.expect_err("the call is refused"));
    #[cfg(target_arch = "wasm32")]
    {
        // better-sqlite3 binds `?3` only by name, as it binds a named
        // parameter, once the call's two values fill the positions before it.
        let (few, many) = (Some("Too few parameter values were provided"), Some("Too many parameter values were provided"));
        let by_name = Some("Missing named parameters");
        assert_eq!(refused.each_ref().map(common::javascript_message), [few, few, few, by_name, many, many]);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        for (error, passed) in refused[..4].iter().zip([1, 0, 1, 2]) {
            assert_eq!(error.refusal(), Some(Refusal::TooFewValues { passed }), "{error}");
        }
        for error in &refused[4..] {
            assert!(matches!(error, SqliteError::Rusqlite(rusqlite::Error::InvalidParameterCount(3, 2))), "{error:?}");
        }
    }
    assert_eq!(stored, (1, 1), "only the insert given both values wrote a row");
    Ok(())
}

/// A numbered parameter takes the value at its position on rusqlite, given a
/// value for every position up to its number, one that no parameter takes
/// included; better-sqlite3 binds no positional value to one, and refuses the
/// call (see `SqliteExecutor`).
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
            Ok((selected, inserted, stored))
        })
        .await?;
    if cfg!(target_arch = "wasm32") {
        for refusal in [selected.expect_err("better-sqlite3 refuses ?2"), inserted.expect_err("better-sqlite3 refuses ?3")] {
            assert_eq!(common::javascript_message(&refusal), Some("Too many parameter values were provided"), "{refusal}");
        }
        assert_eq!(stored, []);
    } else {
        assert_eq!(selected.map_err(|error| error.to_string()).as_deref(), Ok("[Integer(8), Integer(7)]"));
        assert_eq!(inserted.map_err(|error| error.to_string()), Ok(1));
        assert_eq!(stored, [(7, 9)]);
    }
    Ok(())
}
