//! How PostgreSQL lists and creates the indexes on a materialization table.
//! The materialization decides when a query needs one
//! (`Materialization::assure_index_exists`); this module reads the table's
//! index catalog back as key specs and renders the DDL.

use std::collections::BTreeMap;

use ankurah_core::indexing::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
use ankurah_core::value::ValueType;
use ankurah_storage_common::materialization_index::{self, ExistingIndex};
use sha2::{Digest, Sha256};
use tokio_postgres::Client;

use crate::{quote_identifier, IDENTIFIER_MAX_BYTES};

/// The name of the index for `spec` on `table`, within PostgreSQL's
/// identifier limit: the shared name when it fits, else its head followed by
/// a stable hash of the whole name, so that every node derives one name and
/// two long specs sharing a head get two indexes.
pub(super) fn index_name(table: &str, spec: &KeySpec<String>) -> String {
    let name = materialization_index::index_name(table, spec);
    if name.len() <= IDENTIFIER_MAX_BYTES {
        return name;
    }
    let digest: String = Sha256::digest(name.as_bytes())[..8].iter().map(|byte| format!("{byte:02x}")).collect();
    let mut head = IDENTIFIER_MAX_BYTES - digest.len() - 1;
    while !name.is_char_boundary(head) {
        head -= 1;
    }
    format!("{}_{digest}", &name[..head])
}

/// The statement that creates the index named `name` for `spec` on `table`.
/// Each key part is its materialization column, or for a JSON sub-path the
/// `->` chain the query's WHERE clause uses, so that PostgreSQL matches the
/// index to the query; its direction follows, and its nulls order when the
/// part asks for one. PostgreSQL builds the index inside this statement, and
/// never CONCURRENTLY: that form cannot run inside a transaction and leaves
/// an invalid index behind when it fails.
pub(super) fn create_index_sql(table: &str, name: &str, spec: &KeySpec<String>) -> String {
    let columns: Vec<String> = spec.keyparts.iter().map(key_part_sql).collect();
    format!("CREATE INDEX IF NOT EXISTS {} ON {} ({})", quote_identifier(name), quote_identifier(table), columns.join(", "))
}

fn key_part_sql(part: &IndexKeyPart<String>) -> String {
    let mut sql = match &part.sub_path {
        None => quote_identifier(&part.key),
        Some(steps) => format!("({})", jsonb_path_sql(&part.key, steps)),
    };
    if let Some(collation) = &part.collation {
        sql.push_str(&format!(" COLLATE {}", quote_identifier(collation)));
    }
    sql.push_str(if part.direction.is_desc() { " DESC" } else { " ASC" });
    match part.nulls {
        Some(NullsOrder::First) => sql.push_str(" NULLS FIRST"),
        Some(NullsOrder::Last) => sql.push_str(" NULLS LAST"),
        None => {}
    }
    sql
}

/// `"column"->'step'->'step'`, as `SqlBuilder` renders a sub-path.
fn jsonb_path_sql(column: &str, steps: &[String]) -> String {
    let mut sql = quote_identifier(column);
    for step in steps {
        sql.push_str(&format!("->'{}'", step.replace('\'', "''")));
    }
    sql
}

/// The indexes the catalog lists on `table`, each as the key spec it serves.
/// A partial or invalid index serves nothing. Each key column and its
/// direction come from `pg_index`; an expression column is read back from
/// `pg_get_indexdef`'s rendering of it, where only this engine's `->` chain
/// is recognized: an index with any other expression serves nothing. The
/// catalog records no value type and `KeySpec::matches` reads none, so each
/// part carries `ValueType::String`, as the planner's own ORDER BY parts do;
/// a part's nulls order is likewise not read back, as matching ignores it.
pub(super) async fn list_indexes(client: &Client, table: &str) -> Result<Vec<ExistingIndex>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT ic.relname AS name, i.indkey[k - 1] = 0 AS expression, (i.indoption[k - 1] & 1) = 1 AS descending, \
                    pg_get_indexdef(i.indexrelid, k, true) AS definition \
             FROM pg_index AS i JOIN pg_class AS ic ON ic.oid = i.indexrelid, generate_series(1, i.indnkeyatts) AS k \
             WHERE i.indrelid = to_regclass($1) AND i.indisvalid AND i.indpred IS NULL \
             ORDER BY ic.relname, k",
            &[&quote_identifier(table)],
        )
        .await?;
    let mut columns: BTreeMap<String, Vec<(bool, bool, String)>> = BTreeMap::new();
    for row in rows {
        columns.entry(row.get("name")).or_default().push((row.get("expression"), row.get("descending"), row.get("definition")));
    }

    let mut existing = Vec::new();
    'index: for (name, columns) in columns {
        let mut keyparts = Vec::with_capacity(columns.len());
        for (expression, descending, definition) in columns {
            let (key, sub_path) = if expression {
                match jsonb_path(&definition) {
                    Some((column, steps)) => (column, Some(steps)),
                    None => continue 'index,
                }
            } else {
                (unquote_identifier(&definition), None)
            };
            let direction = if descending { IndexDirection::Desc } else { IndexDirection::Asc };
            keyparts.push(IndexKeyPart { key, sub_path, direction, value_type: ValueType::String, nulls: None, collation: None });
        }
        existing.push(ExistingIndex { name, spec: KeySpec::new(keyparts) });
    }
    Ok(existing)
}

/// The column and steps of a `->` chain as `pg_get_indexdef` renders one this
/// engine wrote: `(column -> 'step'::text)`, nested as
/// `((column -> 'a'::text) -> 'b'::text)`. None for any other expression.
fn jsonb_path(definition: &str) -> Option<(String, Vec<String>)> {
    let mut tokens = definition.split(" -> ");
    let column = unquote_identifier(tokens.next()?.trim_start_matches('('));
    let steps: Vec<String> =
        tokens.map(|token| unquote_literal(token.trim_end_matches(')').strip_suffix("::text")?)).collect::<Option<_>>()?;
    (!steps.is_empty()).then_some((column, steps))
}

/// An identifier as `pg_get_indexdef` renders it: quoted only when needed.
fn unquote_identifier(identifier: &str) -> String {
    match identifier.strip_prefix('"').and_then(|rest| rest.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => identifier.to_owned(),
    }
}

fn unquote_literal(literal: &str) -> Option<String> { Some(literal.strip_prefix('\'')?.strip_suffix('\'')?.replace("''", "'")) }

#[cfg(test)]
mod tests {
    use super::*;

    fn part(path: &str, direction: IndexDirection) -> IndexKeyPart<String> {
        IndexKeyPart::from_flat_path(path, direction, ValueType::String)
    }

    fn part_spec(column: &str) -> KeySpec<String> { KeySpec::new(vec![part(column, IndexDirection::Asc)]) }

    #[test]
    fn the_statement_renders_each_part_s_column_or_expression_with_its_direction() {
        let mut nulls_first = part("status", IndexDirection::Desc);
        nulls_first.nulls = Some(NullsOrder::First);
        let spec = KeySpec::new(vec![nulls_first, part("detail.kind.name", IndexDirection::Asc)]);
        assert_eq!(
            create_index_sql("notification", &index_name("notification", &spec), &spec),
            r#"CREATE INDEX IF NOT EXISTS "notification__status desc(nulls=first)__detail.kind.name asc" ON "notification" ("status" DESC NULLS FIRST, ("detail"->'kind'->'name') ASC)"#
        );
    }

    #[test]
    fn a_name_past_the_identifier_limit_is_shortened_by_a_stable_hash() {
        let long = |column: &str| KeySpec::new(vec![part(&format!("{column}_{}", "x".repeat(60)), IndexDirection::Asc)]);
        let [a, b] = [index_name("notification", &long("a")), index_name("notification", &long("b"))];
        assert_eq!(a.len(), IDENTIFIER_MAX_BYTES);
        assert!(a.starts_with("notification__a_xxx"), "{a}");
        assert_ne!(a, b, "two long specs with one head get two names");
        assert_eq!(a, index_name("notification", &long("a")));
        assert_eq!(index_name("notification", &part_spec("status")), "notification__status asc");
    }

    #[test]
    fn an_expression_reads_back_only_in_the_engine_s_own_form() {
        assert_eq!(jsonb_path("(detail -> 'kind'::text)"), Some(("detail".to_owned(), vec!["kind".to_owned()])));
        assert_eq!(
            jsonb_path(r#"(("Detail" -> 'it''s'::text) -> 'b'::text)"#),
            Some(("Detail".to_owned(), vec!["it's".to_owned(), "b".to_owned()]))
        );
        assert_eq!(jsonb_path("lower((status)::text)"), None);
        assert_eq!(jsonb_path("(detail ->> 'kind'::text)"), None);
    }
}
