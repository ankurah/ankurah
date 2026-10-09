//! How SQLite lists and creates the indexes on a materialization table. The
//! materialization decides when a query needs one
//! (`Materialization::assure_index_exists`); this module reads the table's
//! index catalog back as key specs and renders the DDL.

use std::collections::BTreeMap;

use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core::value::ValueType;
use ankurah_storage_common::materialization_index::ExistingIndex;
use rusqlite::Connection;

use crate::error::SqliteError;

fn quote_identifier(identifier: &str) -> String { format!(r#""{}""#, identifier.replace('"', "\"\"")) }

/// The statement that creates the index named `name` for `spec` on `table`.
/// Each key part is its materialization column, or for a JSON sub-path the
/// `json_extract` expression the query's WHERE clause uses, so that SQLite
/// matches the index to the query; its direction follows. SQLite's CREATE
/// INDEX has no NULLS clause, so a part's nulls order is not rendered (the
/// planner sets none). SQLite builds the index inside this statement.
pub(super) fn create_index_sql(table: &str, name: &str, spec: &KeySpec<String>) -> String {
    let columns: Vec<String> = spec.keyparts.iter().map(key_part_sql).collect();
    format!("CREATE INDEX IF NOT EXISTS {} ON {} ({})", quote_identifier(name), quote_identifier(table), columns.join(", "))
}

fn key_part_sql(part: &IndexKeyPart<String>) -> String {
    let mut sql = match &part.sub_path {
        None => quote_identifier(&part.key),
        Some(steps) => json_extract_sql(&part.key, steps),
    };
    if let Some(collation) = &part.collation {
        sql.push_str(&format!(" COLLATE {}", quote_identifier(collation)));
    }
    sql.push_str(if part.direction.is_desc() { " DESC" } else { " ASC" });
    sql
}

/// `json_extract("column", '$.step.step')`, as `SqlBuilder` renders a sub-path.
fn json_extract_sql(column: &str, steps: &[String]) -> String {
    let path = steps.iter().map(|step| step.replace('\'', "''")).collect::<Vec<_>>().join(".");
    format!("json_extract({}, '$.{}')", quote_identifier(column), path)
}

/// The indexes the catalog lists on `table`, each as the key spec it serves.
/// A partial index holds only some rows and serves nothing. A key part's
/// column and direction come from `pragma_index_xinfo`; an expression column,
/// which the pragma reports without its text, is read back from the index's
/// statement in `sqlite_master`, where only this engine's `json_extract` form
/// is recognized: an index with any other expression serves nothing. The
/// catalog records no value type and `KeySpec::matches` reads none, so each
/// part carries `ValueType::String`, as the planner's own ORDER BY parts do.
pub(super) fn list_indexes(conn: &Connection, table: &str) -> Result<Vec<ExistingIndex>, SqliteError> {
    let mut statements = conn.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?1")?;
    let statements: BTreeMap<String, Option<String>> =
        statements.query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    let mut columns = conn.prepare(
        "SELECT il.name, il.partial, ix.name, ix.desc FROM pragma_index_list(?1) AS il JOIN pragma_index_xinfo(il.name) AS ix \
         WHERE ix.key ORDER BY il.name, ix.seqno",
    )?;
    let mut indexes: BTreeMap<String, (bool, Vec<(Option<String>, bool)>)> = BTreeMap::new();
    for row in columns.query_map([table], |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?, row.get(2)?, row.get(3)?)))? {
        let (name, partial, column, descending) = row?;
        indexes.entry(name).or_insert((partial, Vec::new())).1.push((column, descending));
    }

    let mut existing = Vec::new();
    for (name, (partial, columns)) in indexes {
        if partial {
            continue;
        }
        let Some(expressions) = statements.get(&name).and_then(|sql| sql.as_deref()).map_or(Some(Vec::new()), json_extract_calls) else {
            continue;
        };
        let mut expressions = expressions.into_iter();
        let mut keyparts = Vec::with_capacity(columns.len());
        for (column, descending) in columns {
            let (key, sub_path) = match column {
                Some(column) => (column, None),
                None => match expressions.next() {
                    Some((column, steps)) => (column, Some(steps)),
                    None => break,
                },
            };
            let direction = if descending { IndexDirection::Desc } else { IndexDirection::Asc };
            keyparts.push(IndexKeyPart { key, sub_path, direction, value_type: ValueType::String, nulls: None, collation: None });
        }
        if keyparts.len() == keyparts.capacity() {
            existing.push(ExistingIndex { name, spec: KeySpec::new(keyparts) });
        }
    }
    Ok(existing)
}

/// Every `json_extract("column", '$.step.step')` call in an index statement,
/// in order, as the column and steps it reads; None when a call is not in
/// that exact form, which this engine never writes.
fn json_extract_calls(sql: &str) -> Option<Vec<(String, Vec<String>)>> {
    let mut calls = Vec::new();
    let mut rest = sql;
    while let Some(start) = rest.find("json_extract(") {
        let (column, after) = quoted(&rest[start + "json_extract(".len()..], '"')?;
        let (path, after) = quoted(after.strip_prefix(", ")?, '\'')?;
        rest = after.strip_prefix(')')?;
        calls.push((column, path.strip_prefix("$.")?.split('.').map(str::to_owned).collect()));
    }
    Some(calls)
}

/// The token `delimiter` quotes at the start of `text`, its doubled
/// delimiters read as one, and the text after the closing delimiter.
fn quoted(text: &str, delimiter: char) -> Option<(String, &str)> {
    let mut rest = text.strip_prefix(delimiter)?;
    let mut token = String::new();
    loop {
        let end = rest.find(delimiter)?;
        token.push_str(&rest[..end]);
        rest = &rest[end + delimiter.len_utf8()..];
        match rest.strip_prefix(delimiter) {
            Some(after) => {
                token.push(delimiter);
                rest = after;
            }
            None => return Some((token, rest)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_storage_common::materialization_index::{index_name, serving_index};

    fn part(path: &str, direction: IndexDirection) -> IndexKeyPart<String> {
        IndexKeyPart::from_flat_path(path, direction, ValueType::String)
    }

    #[test]
    fn the_statement_renders_each_part_s_column_or_expression_with_its_direction() {
        let spec = KeySpec::new(vec![part("status", IndexDirection::Asc), part("detail.kind", IndexDirection::Desc)]);
        assert_eq!(
            create_index_sql("notification", &index_name("notification", &spec), &spec),
            r#"CREATE INDEX IF NOT EXISTS "notification__status asc__detail.kind desc" ON "notification" ("status" ASC, json_extract("detail", '$.kind') DESC)"#
        );
    }

    #[test]
    fn the_catalog_reads_back_as_the_specs_the_indexes_serve() -> Result<(), SqliteError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(r#"CREATE TABLE "notification" ("id" TEXT PRIMARY KEY, "status" TEXT, "detail" BLOB, "it's" BLOB)"#)?;
        let specs = [
            KeySpec::new(vec![part("status", IndexDirection::Asc), part("id", IndexDirection::Desc)]),
            KeySpec::new(vec![
                part("detail.kind", IndexDirection::Asc),
                part("it's.a.b", IndexDirection::Desc),
                part("status", IndexDirection::Asc),
            ]),
        ];
        for spec in &specs {
            conn.execute(&create_index_sql("notification", &index_name("notification", spec), spec), [])?;
        }
        conn.execute_batch(
            r#"CREATE INDEX "partial" ON "notification" ("status") WHERE "status" IS NOT NULL;
               CREATE INDEX "foreign" ON "notification" ("status", lower("status"));"#,
        )?;

        let existing = list_indexes(&conn, "notification")?;
        let mut names: Vec<_> = existing.iter().map(|index| index.name.as_str()).collect();
        names.sort();
        assert_eq!(
            names,
            [
                "notification__detail.kind asc__it's.a.b desc__status asc",
                "notification__status asc__id desc",
                "sqlite_autoindex_notification_1"
            ]
        );
        for spec in &specs {
            assert_eq!(serving_index(&existing, spec).map(|index| &index.spec), Some(spec), "{spec:?}");
        }
        assert!(
            serving_index(&existing, &KeySpec::new(vec![part("status", IndexDirection::Asc)])).is_some(),
            "a trailing id serves a prefix"
        );
        assert!(serving_index(&existing, &KeySpec::new(vec![part("detail.kind", IndexDirection::Asc)])).is_none());
        assert!(list_indexes(&conn, "absent")?.is_empty());
        Ok(())
    }
}
