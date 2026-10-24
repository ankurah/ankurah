//! Both Node.js drivers hand TEXT over only as a JavaScript string, made by
//! decoding the bytes and replacing each byte that is not UTF-8 with U+FFFD
//! (node:sqlite decodes them only up to the first NUL), where rusqlite hands
//! over the bytes themselves. So a query reads its statement through a typed
//! reader that takes TEXT as the bytes of a BLOB, and decodes them as
//! strictly as rusqlite does (see `typed_reader`). The reader's nesting can
//! change a compound SELECT's values, so the check that refuses such a
//! SELECT lives here too.

use js_sys::Array;

use super::{
    statement::{all, integer_from_js, unreadable, value_from_js},
    NodeJsConnection,
};
use crate::{
    error::{DatabaseRefusal, Refusal, SqliteError},
    executor::Row,
    value::{SqliteValue, SqliteValueRef},
};

/// A typed reader's SQL, and the name of the CTE it nests its statement in,
/// which SQLite names in its error when the statement no longer yields as
/// many columns as the reader names.
pub(super) struct TypedReader {
    pub(super) sql: String,
    pub(super) name: String,
}

/// A typed reader for `statement`, whose rows have `width` columns: the
/// statement nested in a WITH clause, and each of its columns read as two,
/// the value's storage class from `typeof()` and the value itself, TEXT cast
/// to the BLOB of its bytes. MATERIALIZED has SQLite compute each value once,
/// so that `typeof()` and the value read come from one evaluation; SQLite
/// keeps a nested ORDER BY when the query around it has none, and a
/// materialized statement's rows are read in the order it yielded them.
pub(super) fn typed_reader(statement: &str, width: usize) -> TypedReader {
    // A statement's source keeps the semicolon that ended it, which a nested
    // statement cannot have.
    let statement = statement.trim_end().strip_suffix(';').unwrap_or(statement);
    let names = (0..width).map(|index| format!("c{index}")).collect::<Vec<_>>().join(", ");
    let columns = (0..width)
        .map(|index| format!("typeof(c{index}), CASE typeof(c{index}) WHEN 'text' THEN CAST(c{index} AS BLOB) ELSE c{index} END"))
        .collect::<Vec<_>>()
        .join(", ");
    // The reader's name occurs nowhere in the statement's text, so that it
    // cannot stand in for a table the statement names, such as a model's
    // table of the same name; SQLite reads names without regard to ASCII case.
    let text = statement.to_ascii_lowercase();
    let name = std::iter::once("_ankurah_typed_rows".to_owned())
        .chain((1..).map(|suffix| format!("_ankurah_typed_rows_{suffix}")))
        .find(|name| !text.contains(name.as_str()))
        .expect("a finite statement leaves some suffix unused");
    // The statement stands on lines of its own, so that a comment ending it
    // cannot swallow the parenthesis that closes it.
    let sql = format!("WITH {name}({names}) AS MATERIALIZED (\n{statement}\n) SELECT {columns} FROM {name}");
    TypedReader { sql, name }
}

/// A row of a typed reader, a storage class and a value for each column of
/// its statement, read as the statement's row. TEXT comes as its bytes and
/// is decoded as strictly as rusqlite decodes it.
pub(super) fn typed_row(row: Array) -> Result<Row, SqliteError> {
    (0..row.length() / 2)
        .map(|column| {
            let index = column as usize;
            let class = row.get(2 * column);
            let class = class.as_string().ok_or_else(|| unreadable(format!("{class:?} as the storage class of column {index}")))?;
            match (class.as_str(), value_from_js(index, row.get(2 * column + 1))?) {
                ("text", SqliteValue::Blob(bytes)) => String::from_utf8(bytes)
                    .map(SqliteValue::Text)
                    .map_err(|error| Refusal::TextNotUtf8 { column: index, error: error.utf8_error() }.into()),
                ("null", value @ SqliteValue::Null)
                | ("integer", value @ SqliteValue::Integer(_))
                | ("real", value @ SqliteValue::Real(_))
                | ("blob", value @ SqliteValue::Blob(_)) => Ok(value),
                // An INTEGER handed over as a number, as a statement that does
                // not read BigInts hands it over.
                ("integer", SqliteValue::Real(_)) => Err(DatabaseRefusal::NoExactIntegers.into()),
                (class, value) => Err(unreadable(format!("{value:?} as a value of storage class {class} in column {index}"))),
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Row::new)
}

/// A row of a PRAGMA or an EXPLAIN, which SQLite would not nest in a typed
/// reader, read as the database hands it over. Its TEXT arrives as a
/// JavaScript string, read as the TEXT: SQLite makes a PRAGMA's or an
/// EXPLAIN's TEXT of C strings, so it holds no NUL for node:sqlite to cut it
/// at, but a string holding U+FFFD is refused, since both Node.js drivers put
/// that character in place of each byte that is not UTF-8.
pub(super) fn plain_row(row: Array) -> Result<Row, SqliteError> {
    row.iter()
        .enumerate()
        .map(|(index, value)| match value.as_string() {
            Some(text) if text.contains('\u{FFFD}') => Err(Refusal::ReplacementCharacter { column: index }.into()),
            Some(text) => Ok(SqliteValue::Text(text)),
            None => value_from_js(index, value),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Row::new)
}

impl NodeJsConnection {
    /// Refuse a compound SELECT (UNION, UNION ALL, INTERSECT or EXCEPT) at the
    /// top level of a query, or as the SELECT of a scalar subquery there,
    /// before it runs, since the typed reader could change one of its values.
    /// Nested in the typed reader, each column of the query takes an
    /// affinity: a compound's column takes its first SELECT's, and a scalar
    /// subquery takes its last SELECT's, whichever SELECT the value came
    /// from. So an INTEGER past 2^53 read through a REAL one reads as the
    /// nearest REAL, and a small one as a REAL, where rusqlite reads each
    /// value as SQLite computed it. SQLite's query plan says where a compound
    /// stands, so one in a subquery that SQLite flattens into the query is
    /// refused as one at its top level. A compound in a subquery or a CTE
    /// that SQLite does not flatten, which the plan shows under a CO-ROUTINE
    /// or a MATERIALIZE, takes its first SELECT's affinity natively too, and
    /// a compound in an IN has its values only compared, so both are let
    /// through. The plan does not show whether a scalar subquery computes a
    /// column of the query, so one that computes a value no row holds, in a
    /// WHERE clause or as an EXISTS, is refused too. The plan shows a
    /// compound as a COMPOUND QUERY, or as a MERGE when SQLite codes it as a
    /// merge of its SELECTs, as it does under an ORDER BY and, in SQLite
    /// 3.53, for UNION, INTERSECT and EXCEPT.
    pub(super) fn refuse_a_compound_select(&self, statement: &str, params: &[SqliteValueRef<'_>]) -> Result<(), SqliteError> {
        // Each step of the plan is id, parent, notused, detail. better-sqlite3
        // plans a statement only with its parameters bound.
        let plan = self.prepare(&format!("EXPLAIN QUERY PLAN {statement}"))?;
        let mut steps = Vec::new();
        for step in all(&plan, params, 4)? {
            let detail = step.get(3);
            let detail = detail.as_string().ok_or_else(|| unreadable(format!("{detail:?} as the detail of a step of a query plan")))?;
            steps.push((integer_from_js(&step.get(0))?, integer_from_js(&step.get(1))?, detail));
        }
        // The query (0), and each scalar subquery that stands in the query or
        // in another such subquery, found without relying on the order of
        // the steps.
        let mut top_level = vec![0];
        loop {
            let found = top_level.len();
            for (id, parent, detail) in &steps {
                let scalar = detail.strip_prefix("CORRELATED ").unwrap_or(detail).starts_with("SCALAR SUBQUERY");
                if scalar && top_level.contains(parent) && !top_level.contains(id) {
                    top_level.push(*id);
                }
            }
            if top_level.len() == found {
                break;
            }
        }
        let compound = |detail: &String| detail == "COMPOUND QUERY" || detail.starts_with("MERGE (");
        if steps.iter().any(|(_, parent, detail)| top_level.contains(parent) && compound(detail)) {
            return Err(Refusal::CompoundSelect.into());
        }
        Ok(())
    }
}
