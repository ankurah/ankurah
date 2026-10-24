//! The typed readers a connection keeps prepared between queries, and the
//! schema versions that tell when they go stale.

use std::collections::HashMap;

use js_sys::Array;
use wasm_bindgen::{JsCast, JsValue};

use super::{
    statement::{integer_from_js, js_error, statement_method},
    NodeJsConnection,
};
use crate::error::SqliteError;

/// How many typed readers a connection keeps prepared.
const KEPT_READERS: usize = 64;

/// The typed readers a connection has prepared, by the SQL text of the
/// statement each reads, so that a query run again skips both prepares,
/// which cost a point lookup several times what reading its row does. At
/// most [`KEPT_READERS`] are kept, the least recently used leaving first.
/// SQLite prepares a kept statement again when the schema changes, but a
/// typed reader names its statement's columns, which a `SELECT *` may have
/// gained or lost; so every kept reader is dropped when the schema version
/// of main or temp changes, and one that fails, as a reader of an attached
/// database's table can, is dropped and its statement read afresh (see
/// `read`). A reader is kept only for a statement that
/// `refuse_a_compound_select` lets through.
#[derive(Default)]
pub(super) struct Readers {
    /// `PRAGMA main.schema_version` and `PRAGMA temp.schema_version`, prepared once.
    version_statements: Option<[JsValue; 2]>,
    /// `SELECT changes()`, prepared once.
    pub(super) changes_statement: Option<JsValue>,
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
        if readers.kept.len() >= KEPT_READERS {
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
        let prepared = self.readers.borrow().version_statements.clone();
        let statements = match prepared {
            Some(statements) => statements,
            None => {
                let statements = [self.prepare("PRAGMA main.schema_version")?, self.prepare("PRAGMA temp.schema_version")?];
                self.readers.borrow_mut().version_statements = Some(statements.clone());
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
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::executor::SqliteExecutor;

    use super::*;

    /// A connection keeps the typed readers of at most [`KEPT_READERS`]
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
        for index in 0..KEPT_READERS {
            read(index);
        }
        // The first query becomes the most recently used, so the second leaves first.
        read(0);
        read(KEPT_READERS);
        assert_eq!(connection.readers.borrow().kept.len(), KEPT_READERS);
        assert!(kept(0) && !kept(1) && kept(KEPT_READERS));
        assert_eq!(read(1), 1);
        assert!(kept(1) && !kept(2));
    }
}
