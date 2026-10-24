//! The typed readers a connection keeps prepared between queries, and the
//! schema versions that tell when they go stale.

use std::collections::HashMap;

use js_sys::Array;
use wasm_bindgen::{JsCast, JsValue};

use super::{
    statement::{integer_from_js, js_error, statement_method, width},
    NodeJsConnection,
};
use crate::error::SqliteError;

/// How many typed readers a connection keeps prepared.
const KEPT_READERS_MAX: usize = 64;

/// The typed readers a connection has prepared, by the SQL text of the
/// statement each reads, so that a query run again skips both prepares,
/// which cost a point lookup several times what reading its row does. At
/// most [`KEPT_READERS_MAX`] are kept, the least recently used leaving first.
/// SQLite prepares a kept statement again when the schema changes, but a
/// typed reader names its statement's columns, which a `SELECT *` may have
/// gained or lost; so every kept reader is dropped when the schema version
/// of main or temp changes, and one that fails because its statement now
/// yields another number of columns, as a reader of an attached database's
/// table can, is dropped and its statement read afresh (see `read`). A
/// reader is kept only for a statement that `refuse_a_compound_select` lets
/// through.
#[derive(Default)]
pub(super) struct KeptReaders {
    /// The schema versions the kept readers were prepared at.
    versions: Option<[i64; 2]>,
    kept: HashMap<String, KeptReader>,
    /// Counts the uses of kept readers, to find the least recently used.
    clock: u64,
}

struct KeptReader {
    statement: JsValue,
    width: usize,
    used: u64,
}

impl NodeJsConnection {
    /// The typed reader kept for `sql` and its statement's width, once every
    /// kept reader has been dropped if the schema changed since they were
    /// prepared.
    pub(super) fn kept_reader(&self, sql: &str) -> Result<Option<(JsValue, usize)>, SqliteError> {
        let versions = self.schema_versions()?;
        let mut readers = self.readers.borrow_mut();
        if readers.versions != Some(versions) {
            readers.kept.clear();
            readers.versions = Some(versions);
        }
        readers.clock += 1;
        let clock = readers.clock;
        Ok(readers.kept.get_mut(sql).map(|reader| {
            reader.used = clock;
            (reader.statement.clone(), reader.width)
        }))
    }

    /// Keep a typed reader just prepared for `sql`, making room by dropping
    /// the least recently used one.
    pub(super) fn keep_reader(&self, sql: &str, statement: &JsValue, width: usize) {
        let mut readers = self.readers.borrow_mut();
        if readers.kept.len() >= KEPT_READERS_MAX {
            let oldest = readers.kept.iter().min_by_key(|(_, reader)| reader.used).map(|(sql, _)| sql.clone());
            if let Some(oldest) = oldest {
                readers.kept.remove(&oldest);
            }
        }
        readers.clock += 1;
        let used = readers.clock;
        readers.kept.insert(sql.to_owned(), KeptReader { statement: statement.clone(), width, used });
    }

    /// The schema versions of main and temp, which change whenever their schemas do.
    fn schema_versions(&self) -> Result<[i64; 2], SqliteError> {
        let prepared = self.statements.borrow().version_statements.clone();
        let statements = match prepared {
            Some(statements) => statements,
            None => {
                let statements = [self.prepare("PRAGMA main.schema_version")?, self.prepare("PRAGMA temp.schema_version")?];
                self.statements.borrow_mut().version_statements = Some(statements.clone());
                statements
            }
        };
        let mut versions = [0; 2];
        for (version, statement) in versions.iter_mut().zip(&statements) {
            let row: Array = statement_method(statement, "get")?
                .call0(statement)
                .map_err(js_error)?
                .dyn_into()
                .map_err(|row| SqliteError::Node(format!("`PRAGMA schema_version` returned {row:?}, not a row")))?;
            *version = integer_from_js(&row.get(0))?;
        }
        Ok(versions)
    }

    /// Drop the typed reader kept for `sql`, so that its statement is read
    /// afresh, as on its first run.
    pub(super) fn drop_reader(&self, sql: &str) { self.readers.borrow_mut().kept.remove(sql); }

    /// Whether the statement `sql`, prepared afresh, yields another number of
    /// columns than the `kept_width` its kept reader names, which is why
    /// SQLite fails to prepare that reader again. A statement that no longer
    /// prepares yields no other width, so that the reader's own failure is
    /// reported.
    pub(super) fn yields_another_width(&self, sql: &str, kept_width: usize) -> bool {
        self.prepare(sql).and_then(|statement| width(&statement)).is_ok_and(|width| width != kept_width)
    }
}

#[cfg(test)]
mod tests {
    use js_sys::{Function, Reflect};
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::{executor::SqliteExecutor, test_driver};

    use super::*;

    /// A connection on a database that has a host function `tick()`, which
    /// returns NULL and throws on its fourth call, over a table `f` of three
    /// rows; and a count of the function's calls so far.
    fn ticking_connection() -> (NodeJsConnection, impl Fn() -> f64) {
        let database = test_driver::host_database(":memory:");
        let register = Function::new_with_args(
            "database",
            "let calls = 0;
             database.function('tick', () => {
                 calls += 1;
                 if (calls === 4) throw new Error('tick refuses its fourth call');
                 return null;
             });
             return () => calls;",
        );
        let calls: Function = register.call1(&JsValue::NULL, &database).unwrap().dyn_into().unwrap();
        let connection = NodeJsConnection::from_database(database).unwrap();
        connection.execute("CREATE TABLE f (a)", &[]).unwrap();
        connection.execute("INSERT INTO f VALUES (1), (2), (3)", &[]).unwrap();
        (connection, move || calls.call0(&JsValue::NULL).unwrap().as_f64().unwrap())
    }

    /// The query the tests run on `f`, which calls `tick()` once for each row.
    const TICKING_QUERY: &str = "SELECT a, tick() FROM f ORDER BY a";

    /// A kept reader whose statement fails as it runs, in a host function
    /// that throws on one call, reports that failure, and does not run the
    /// statement again, which would call the function once more for each row.
    /// The reader stays kept, and reads the statement once the function
    /// stops throwing.
    #[wasm_bindgen_test]
    fn a_kept_reader_that_fails_as_it_runs_reports_the_failure() {
        let (connection, calls) = ticking_connection();
        assert_eq!(connection.query(TICKING_QUERY, &[]).unwrap().len(), 3, "the first run calls tick() three times");
        let error = connection.query(TICKING_QUERY, &[]).expect_err("the kept reader's run calls tick() a fourth time");
        assert!(error.to_string().contains("tick refuses its fourth call"), "{error}");
        assert_eq!(calls(), 4.0, "the failed run called tick() once, and nothing ran it again");
        assert!(connection.readers.borrow().kept.contains_key(TICKING_QUERY));
        assert_eq!(connection.query(TICKING_QUERY, &[]).unwrap().len(), 3);
    }

    /// A kept reader that fails inside an open transaction reports the
    /// failure without running its statement again, and leaves the
    /// transaction as SQLite left it, open after a host function's failure:
    /// node:sqlite reports that as `isTransaction`, better-sqlite3 as
    /// `inTransaction`.
    #[wasm_bindgen_test]
    fn a_kept_reader_that_fails_in_a_transaction_leaves_it_as_sqlite_left_it() {
        let (connection, calls) = ticking_connection();
        connection.query(TICKING_QUERY, &[]).unwrap();
        connection.execute("BEGIN", &[]).unwrap();
        connection.execute("INSERT INTO f VALUES (4)", &[]).unwrap();
        connection.query(TICKING_QUERY, &[]).expect_err("the kept reader's run calls tick() a fourth time");
        assert_eq!(calls(), 4.0, "the failed run called tick() once, and nothing ran it again");
        let open = if test_driver::on_better_sqlite3() { "inTransaction" } else { "isTransaction" };
        assert_eq!(Reflect::get(connection.database(), &open.into()).unwrap().as_bool(), Some(true), "the transaction is open");
        connection.execute("ROLLBACK", &[]).unwrap();
        assert_eq!(connection.query_row("SELECT count(*) FROM f", &[]).unwrap().unwrap().integer(0).unwrap(), 3);
    }

    /// A connection keeps the typed readers of at most [`KEPT_READERS_MAX`]
    /// queries, making room by dropping the least recently used, and reads a
    /// query whose reader it dropped as before, keeping it again.
    #[wasm_bindgen_test]
    fn keeps_the_typed_readers_of_the_most_recently_used_queries() {
        let connection = NodeJsConnection::from_database(crate::test_driver::host_database(":memory:")).unwrap();
        // SQL texts of the test's own, so that the readers of the queries
        // `from_database` ran leave first.
        let sql = |index: usize| format!("SELECT {index} AS kept");
        let read = |index: usize| connection.query(&sql(index), &[]).unwrap()[0].integer(0).unwrap();
        let kept = |index: usize| connection.readers.borrow().kept.contains_key(&sql(index));
        for index in 0..KEPT_READERS_MAX {
            read(index);
        }
        // The first query becomes the most recently used, so the second leaves first.
        read(0);
        read(KEPT_READERS_MAX);
        assert_eq!(connection.readers.borrow().kept.len(), KEPT_READERS_MAX);
        assert!(kept(0) && !kept(1) && kept(KEPT_READERS_MAX));
        assert_eq!(read(1), 1);
        assert!(kept(1) && !kept(2));
    }
}
