//! How SQLite lists and creates the indexes on a materialization table. The
//! materialization decides when a query needs one
//! (`Materialization::assure_index_exists`); this module reads the table's
//! index catalog back as key specs and renders the DDL.

use std::collections::BTreeMap;

use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec};
use ankurah_core::value::ValueType;
use ankurah_storage_common::materialization_index::ExistingIndex;
use rusqlite::Connection;

use super::quote_identifier;
use crate::error::SqliteError;

/// The statement that creates the index named `name` for `spec` on `table`.
/// Each key part is its materialization column, or for a JSON sub-path the
/// `json_extract` expression the query's WHERE clause uses, so that SQLite
/// matches the index to the query; its collation, when the part has one, and
/// its direction follow. SQLite's CREATE INDEX has no NULLS clause, so a
/// part's nulls order is not rendered (the planner sets none). SQLite builds
/// the index inside this statement.
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

/// One key column of an index as `pragma_index_xinfo` lists it: its column
/// name (None for an expression), whether it sorts descending, and its
/// collation.
struct CatalogColumn {
    name: Option<String>,
    descending: bool,
    collation: String,
}

/// The indexes the catalog lists on `table`, each as the key spec it serves.
/// A partial index holds only some rows and serves nothing. A key column's
/// name, direction and collation come from `pragma_index_xinfo`, the default
/// BINARY collation reading as none. An expression column, which the pragma
/// reports without its text, is read from the index's statement in
/// `sqlite_master`: the statement's column list is split at its top-level
/// commas, and the element at the column's position must be exactly the form
/// `key_part_sql` renders for a sub-path part, so that `lower(json_extract(..))`,
/// arithmetic around it, or any other expression leaves the whole index
/// unrecognized, serving nothing. SQLite creates no such index itself (see
/// `Materialization::assure_index_exists`); an application's index in exactly
/// that form serves a sub-path plan. The catalog records no value type and
/// `KeySpec::matches` reads none, so each part carries `ValueType::String`,
/// as the planner's own ORDER BY parts do.
pub(super) fn list_indexes(conn: &Connection, table: &str) -> Result<Vec<ExistingIndex>, SqliteError> {
    let mut statements = conn.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?1")?;
    let statements: BTreeMap<String, Option<String>> =
        statements.query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<Result<_, _>>()?;
    let mut columns = conn.prepare(
        "SELECT il.name, il.partial, ix.name, ix.desc, ix.coll FROM pragma_index_list(?1) AS il JOIN pragma_index_xinfo(il.name) AS ix \
         WHERE ix.key ORDER BY il.name, ix.seqno",
    )?;
    let mut indexes: BTreeMap<String, (bool, Vec<CatalogColumn>)> = BTreeMap::new();
    for row in columns.query_map([table], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, bool>(1)?,
            CatalogColumn { name: row.get(2)?, descending: row.get(3)?, collation: row.get(4)? },
        ))
    })? {
        let (name, partial, column) = row?;
        indexes.entry(name).or_insert((partial, Vec::new())).1.push(column);
    }

    let mut existing = Vec::new();
    'index: for (name, (partial, columns)) in indexes {
        if partial {
            continue;
        }
        // An index without a statement is one SQLite made for a constraint; its columns are all named.
        let elements = match statements.get(&name).and_then(|statement| statement.as_deref()) {
            Some(statement) => key_elements(statement),
            None => Some(Vec::new()),
        };
        let Some(elements) = elements else { continue };
        if !elements.is_empty() && elements.len() != columns.len() {
            continue;
        }
        let mut keyparts = Vec::with_capacity(columns.len());
        for (position, column) in columns.into_iter().enumerate() {
            let (key, sub_path) = match column.name {
                Some(key) => (key, None),
                None => match elements.get(position).and_then(|element| sub_path_element(element)) {
                    Some((key, steps)) => (key, Some(steps)),
                    None => continue 'index,
                },
            };
            let direction = if column.descending { IndexDirection::Desc } else { IndexDirection::Asc };
            let collation = (!column.collation.eq_ignore_ascii_case("BINARY")).then_some(column.collation);
            keyparts.push(IndexKeyPart { key, sub_path, direction, value_type: ValueType::String, nulls: None, collation });
        }
        existing.push(ExistingIndex { name, spec: KeySpec::new(keyparts) });
    }
    Ok(existing)
}

/// The elements of a CREATE INDEX statement's column list, split at the
/// list's top-level commas, with quoted text kept whole; None when the
/// statement holds no balanced list.
fn key_elements(statement: &str) -> Option<Vec<String>> {
    let mut elements = Vec::new();
    let mut element = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = statement.chars().peekable();
    while let Some(c) = chars.next() {
        let inside_list = depth >= 1;
        if let Some(delimiter) = quote {
            if inside_list {
                element.push(c);
            }
            if c == delimiter {
                match chars.peek() {
                    Some(next) if *next == delimiter => {
                        if inside_list {
                            element.push(delimiter);
                        }
                        chars.next();
                    }
                    _ => quote = None,
                }
            }
            continue;
        }
        match c {
            '"' | '\'' => quote = Some(c),
            '(' => depth += 1,
            ')' if depth == 0 => return None,
            ')' => depth -= 1,
            ',' if depth == 1 => {
                elements.push(element.trim().to_owned());
                element.clear();
                continue;
            }
            _ => {}
        }
        if depth == 0 && c == ')' {
            elements.push(element.trim().to_owned());
            return Some(elements);
        }
        if inside_list && !(c == '(' && depth == 1) {
            element.push(c);
        }
    }
    None
}

/// The column and steps of a column-list element that is exactly a sub-path
/// part as `key_part_sql` renders one: `json_extract("column", '$.a.b')`,
/// followed by nothing but its collation and direction clauses. None for any
/// other element.
fn sub_path_element(element: &str) -> Option<(String, Vec<String>)> {
    let (column, rest) = quoted(element.strip_prefix("json_extract(")?, '"')?;
    let (path, rest) = quoted(rest.strip_prefix(", ")?, '\'')?;
    let rest = rest.strip_prefix(')')?;
    let clauses: Vec<String> = rest.split_whitespace().map(str::to_ascii_uppercase).collect();
    let clauses: Vec<&str> = clauses.iter().map(String::as_str).collect();
    match clauses.as_slice() {
        [] | ["ASC"] | ["DESC"] | ["COLLATE", _] | ["COLLATE", _, "ASC"] | ["COLLATE", _, "DESC"] => {}
        _ => return None,
    }
    Some((column, path.strip_prefix("$.")?.split('.').map(str::to_owned).collect()))
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

    fn names(existing: &[ExistingIndex]) -> Vec<&str> { existing.iter().map(|index| index.name.as_str()).collect() }

    #[test]
    fn the_statement_renders_each_part_s_column_or_expression_with_its_direction() {
        let spec = KeySpec::new(vec![part("status", IndexDirection::Asc), part("detail.kind", IndexDirection::Desc)]);
        assert_eq!(
            create_index_sql("notification", &index_name("notification", &spec), &spec),
            r#"CREATE INDEX IF NOT EXISTS "_ankurah_index__notification__status asc__detail.kind desc" ON "notification" ("status" ASC, json_extract("detail", '$.kind') DESC)"#
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
        conn.execute_batch(r#"CREATE INDEX "partial" ON "notification" ("status") WHERE "status" IS NOT NULL;"#)?;

        let existing = list_indexes(&conn, "notification")?;
        let mut listed = names(&existing);
        listed.sort();
        assert_eq!(
            listed,
            [
                "_ankurah_index__notification__detail.kind asc__it's.a.b desc__status asc",
                "_ankurah_index__notification__status asc__id desc",
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

    /// An expression column is recognized only as the whole element, in the
    /// engine's own form: wrapped, combined, or preceded by an unrelated
    /// expression, the index serves nothing.
    #[test]
    fn an_expression_is_recognized_only_as_the_whole_element_in_the_engine_s_form() -> Result<(), SqliteError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            r#"CREATE TABLE "notification" ("id" TEXT PRIMARY KEY, "status" TEXT, "detail" BLOB);
               CREATE INDEX "wrapped" ON "notification" (lower(json_extract("detail", '$.kind')));
               CREATE INDEX "combined" ON "notification" (json_extract("detail", '$.kind') + 1);
               CREATE INDEX "preceded" ON "notification" ("status", lower("status"), json_extract("detail", '$.kind'));
               CREATE INDEX "upper" ON "notification" (JSON_EXTRACT("detail", '$.kind'));
               CREATE INDEX "exact" ON "notification" ("status" DESC, json_extract("detail", '$.a, (b)') COLLATE NOCASE ASC);"#,
        )?;
        let existing = list_indexes(&conn, "notification")?;
        assert_eq!(names(&existing), ["exact", "sqlite_autoindex_notification_1"]);
        let exact = &existing[0].spec.keyparts;
        assert_eq!((exact[1].key.as_str(), exact[1].sub_path.as_deref()), ("detail", Some(["a, (b)".to_owned()].as_slice())));
        assert_eq!(exact[1].collation.as_deref(), Some("NOCASE"));
        assert_eq!(exact[0].collation, None, "BINARY is the default collation");
        Ok(())
    }

    /// An index collated otherwise than the plan asks does not serve it.
    #[test]
    fn a_nocase_index_does_not_serve_a_plan_on_the_default_collation() -> Result<(), SqliteError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            r#"CREATE TABLE "notification" ("id" TEXT PRIMARY KEY, "status" TEXT);
               CREATE INDEX "nocase" ON "notification" ("status" COLLATE NOCASE);"#,
        )?;
        let existing = list_indexes(&conn, "notification")?;
        let status = KeySpec::new(vec![part("status", IndexDirection::Asc)]);
        assert!(serving_index(&existing, &status).is_none());
        let mut nocase = status.clone();
        nocase.keyparts[0].collation = Some("NOCASE".to_owned());
        assert_eq!(serving_index(&existing, &nocase).map(|index| index.name.as_str()), Some("nocase"));
        Ok(())
    }

    #[test]
    fn a_column_list_splits_at_its_top_level_commas_only() {
        assert_eq!(
            key_elements(r#"CREATE INDEX "i (x)" ON "t, u" ("a", f("b", 'c,d'), json_extract("e", '$.f') DESC)"#).unwrap(),
            [r#""a""#, r#"f("b", 'c,d')"#, r#"json_extract("e", '$.f') DESC"#]
        );
        assert_eq!(key_elements("CREATE INDEX i ON t (a").as_deref(), None);
        assert_eq!(sub_path_element(r#"json_extract("e", '$.f') DESC"#), Some(("e".to_owned(), vec!["f".to_owned()])));
        assert_eq!(sub_path_element(r#"json_extract("e", '$.f') COLLATE "x" ASC"#), Some(("e".to_owned(), vec!["f".to_owned()])));
        assert_eq!(sub_path_element(r#"json_extract("e", '$.f') + 1"#), None);
        assert_eq!(sub_path_element(r#"lower(json_extract("e", '$.f'))"#), None);
    }
}
