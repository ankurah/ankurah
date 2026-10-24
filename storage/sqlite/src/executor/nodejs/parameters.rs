//! The check of a call's values against its statement's parameters: the
//! counts kept by SQL text, the probe of how a database binds a parameter a
//! call leaves out, and the refusal of a named parameter.

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use js_sys::Array;
use wasm_bindgen::JsValue;

use super::{
    exception::js_error,
    statement::{integer_from_js, method, read_rows, statement_method, RowSelection},
    NodeJsConnection,
};
use crate::{
    error::{Refusal, SqliteError, SqliteFailure},
    executor::statement_scan,
    value::SqliteValueRef,
};

/// On a database that binds NULL for a positional parameter a call does not
/// pass, how many values each SQL text's statement was shown to take at
/// most, by that text (see `refuse_too_few_values`).
pub(super) type ParameterCounts = Rc<RefCell<HashMap<String, usize>>>;

/// How many SQL texts' parameter counts a connection keeps; past that it
/// forgets them all, and checks each statement again when it next runs.
const PARAMETER_COUNTS_MAX: usize = 256;

/// SQLite's result code for a parameter index out of range.
const SQLITE_RANGE: i32 = 25;

impl NodeJsConnection {
    /// Whether the database binds NULL for a positional parameter a call does
    /// not pass, as node:sqlite does, rather than refusing the call, as
    /// rusqlite and better-sqlite3 do.
    pub(super) fn binds_null_for_a_missing_parameter(&self) -> Result<bool, SqliteError> {
        let statement = method(&self.database, "prepare")?.call1(&self.database, &"SELECT ?".into()).map_err(js_error)?;
        let get = statement_method(&statement, "get")?;
        if get.call0(&statement).is_ok() {
            return Ok(true);
        }
        // The call was refused for the value it did not pass only if the same
        // call passing that value runs.
        get.call1(&statement, &JsValue::NULL).map_err(js_error)?;
        Ok(false)
    }

    /// Whether the database binds a call's values to a numbered parameter
    /// such as `?2` at its position, as rusqlite does: node:sqlite does from
    /// Node.js 22.20.0 on 22 and 24.7.0 on 24, and on every later line. Before,
    /// it skipped each numbered parameter as it bound a call's values in turn,
    /// so that `?, ?3` given two values left `?3` NULL, and the check of a
    /// call's values (`refuse_too_few_values`) misjudged it. Checking the
    /// binding itself, rather than the version, holds for every release line
    /// and build: given two values, `SELECT ?2` reads the second where the
    /// binding is right, and where it skips `?2`, its second value lands past
    /// the statement's last parameter, which SQLite refuses (SQLITE_RANGE).
    pub(super) fn binds_a_numbered_parameter_at_its_position(&self) -> Result<bool, SqliteError> {
        let statement = self.prepare("SELECT ?2")?;
        match read_rows(&statement, &[SqliteValueRef::Integer(1), SqliteValueRef::Integer(2)], 1, RowSelection::First) {
            Ok(rows) => match rows.as_slice() {
                [row] => Ok(integer_from_js(&row.get(0))? == 2),
                _ => Ok(false),
            },
            Err(SqliteError::Sqlite(SqliteFailure { code: Some(SQLITE_RANGE), .. })) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Refuse a call to `sql` that passes fewer values than its statement
    /// takes parameters, on a database that would bind NULL for each one
    /// missing, before it runs; rusqlite refuses such a call. The statement's
    /// EXPLAIN, which runs nothing, is bound one value more than the call
    /// passes, and SQLite refuses the extra value (SQLITE_RANGE) exactly when
    /// the statement takes no more parameters than were passed. A call that
    /// passes too many, node:sqlite refuses itself. The count shown is kept by
    /// SQL text, so a statement run again with as many values is only looked up.
    pub(super) fn refuse_too_few_values(&self, sql: &str, passed: usize) -> Result<(), SqliteError> {
        let Some(checked) = &self.parameters else { return Ok(()) };
        if checked.borrow().get(sql).is_some_and(|&most| most <= passed) {
            return Ok(());
        }
        // EXPLAIN goes before the statement, past what SQLite skips before it,
        // and an EXPLAIN takes no second one.
        let statement = statement_scan::skip_to_statement(sql);
        let explain = if statement_scan::first_keyword(statement).eq_ignore_ascii_case("EXPLAIN") {
            statement.into()
        } else {
            format!("EXPLAIN {statement}")
        };
        let probe = method(&self.database, "prepare")?.call1(&self.database, &explain.into()).map_err(js_error)?;
        let values: Array = (0..=passed).map(|_| JsValue::NULL).collect();
        match statement_method(&probe, "get")?.apply(&probe, &values).map_err(js_error) {
            Err(SqliteError::Sqlite(SqliteFailure { code: Some(SQLITE_RANGE), .. })) => {}
            Err(error) => return Err(error),
            Ok(_) => return Err(Refusal::TooFewValues { passed }.into()),
        }
        let mut checked = checked.borrow_mut();
        if checked.len() >= PARAMETER_COUNTS_MAX {
            checked.clear();
        }
        checked.insert(sql.to_owned(), passed);
        Ok(())
    }
}

/// Refuse a statement that has a named parameter, the first of which the
/// scan before preparing found (see `refuse_all_but_one_statement`), before it
/// runs: node:sqlite binds no positional value to one, leaving it NULL, where
/// rusqlite binds a call's values to every parameter in turn. better-sqlite3
/// binds a named parameter only by name, which a positional call cannot give.
/// It is refused once SQLite has prepared the statement, so that SQL SQLite
/// cannot prepare fails with SQLite's own error, as it does natively.
pub(super) fn refuse_a_named_parameter(named_parameter: Option<&str>) -> Result<(), SqliteError> {
    match named_parameter {
        None => Ok(()),
        Some(name) => Err(Refusal::NamedParameter { name: name.to_owned() }.into()),
    }
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::{executor::SqliteExecutor, value::SqliteValueRef};

    use super::*;

    /// A node:sqlite connection keeps the parameter counts of at most
    /// [`PARAMETER_COUNTS_MAX`] SQL texts, forgetting them all to keep the next,
    /// and shows the count of a statement it forgot again.
    #[wasm_bindgen_test]
    #[cfg_attr(
        ankurah_sqlite_better_sqlite3,
        ignore = "better-sqlite3 refuses a call that passes too few values itself, so its connections keep no counts"
    )]
    fn forgets_the_parameter_counts_past_its_limit() {
        let connection = NodeJsConnection::from_database(crate::test_driver::host_database(":memory:")).unwrap();
        let checked = connection.parameters.as_ref().expect("node:sqlite binds NULL for a value a call does not pass");
        // Without the counts of the queries `from_database` ran.
        checked.borrow_mut().clear();
        let seven = SqliteValueRef::Integer(7);
        let read = |sql: &str| connection.query(sql, &[seven]).unwrap();
        for index in 0..PARAMETER_COUNTS_MAX {
            read(&format!("SELECT ?, {index}"));
        }
        assert_eq!(checked.borrow().len(), PARAMETER_COUNTS_MAX);
        read("SELECT ?, 'one more'");
        assert_eq!(checked.borrow().len(), 1);
        read("SELECT ?, 0");
        assert_eq!(checked.borrow().get("SELECT ?, 0"), Some(&1));
    }
}
