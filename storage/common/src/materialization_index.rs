//! The index a SQL engine keeps on a model's materialization table for a
//! query plan: what it is named, and when an index already on the table
//! serves the plan's key spec instead. Each engine reads its table's indexes
//! from the database catalog and runs the DDL itself; this module holds the
//! rule the SQL engines share, which is sled's.

use ankurah_core::indexing::KeySpec;

/// An index the catalog lists on a materialization table, as the key spec it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingIndex {
    pub name: String,
    pub spec: KeySpec<String>,
}

/// The name of the index for `spec` on `table`: sled's rendering of the spec
/// behind the table's name, since a SQL database keeps every table's index
/// names in one namespace. PostgreSQL shortens a name past its identifier
/// limit.
pub fn index_name(table: &str, spec: &KeySpec<String>) -> String { spec.name_with(table, "__") }

/// The existing index that serves `spec`, by sled's rule: `spec` is a prefix
/// of the index's key, with every direction equal or every direction flipped
/// (a database scans an index either way), and each key part past the prefix
/// is the `id` column. Sled's guard also admits its materialization column
/// there; a materialization table has none, each model having its own table.
/// Any other trailing part refuses the reuse. Sled refuses because its shared
/// index omits an entity missing a trailing property; a SQL index holds every
/// row, and the guard is kept so that the SQL engines create exactly the
/// indexes sled creates for the same plans.
pub fn serving_index<'a>(existing: &'a [ExistingIndex], spec: &KeySpec<String>) -> Option<&'a ExistingIndex> {
    existing
        .iter()
        .find(|index| spec.matches(&index.spec).is_some() && index.spec.keyparts[spec.keyparts.len()..].iter().all(|part| part.key == "id"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::{indexing::IndexKeyPart, value::ValueType};

    fn spec(parts: &[&str]) -> KeySpec<String> {
        KeySpec::new(parts.iter().map(|part| IndexKeyPart::from_flat_path(&part[1..], direction(part), ValueType::String)).collect())
    }

    fn direction(part: &str) -> ankurah_core::indexing::IndexDirection {
        match part.as_bytes()[0] {
            b'+' => ankurah_core::indexing::IndexDirection::Asc,
            b'-' => ankurah_core::indexing::IndexDirection::Desc,
            _ => panic!("a part starts with its direction"),
        }
    }

    fn existing(parts: &[&str]) -> ExistingIndex { ExistingIndex { name: parts.join("__"), spec: spec(parts) } }

    #[test]
    fn an_index_is_named_for_its_table_and_key() {
        assert_eq!(index_name("notification", &spec(&["+recipient", "-status"])), "notification__recipient asc__status desc");
        assert_eq!(index_name("notification", &spec(&["+detail.kind"])), "notification__detail.kind asc");
    }

    #[test]
    fn an_index_serves_its_own_spec_and_the_flipped_one() {
        let indexes = [existing(&["+a", "-b"])];
        assert!(serving_index(&indexes, &spec(&["+a", "-b"])).is_some());
        assert!(serving_index(&indexes, &spec(&["-a", "+b"])).is_some());
        assert!(serving_index(&indexes, &spec(&["+a", "+b"])).is_none(), "one direction flipped is another key");
        assert!(serving_index(&indexes, &spec(&["+a", "-b", "+c"])).is_none(), "a longer spec needs its own index");
        assert!(serving_index(&indexes, &spec(&["+b"])).is_none());
    }

    #[test]
    fn a_prefix_is_served_only_past_trailing_id_parts() {
        let indexes = [existing(&["+a", "+id"]), existing(&["+b", "+c"]), existing(&["+d.kind", "+id"])];
        assert!(serving_index(&indexes, &spec(&["+a"])).is_some());
        assert!(serving_index(&indexes, &spec(&["-a"])).is_some());
        assert!(serving_index(&indexes, &spec(&["+b"])).is_none(), "a trailing property may be missing on an entity");
        assert!(serving_index(&indexes, &spec(&["+d.kind"])).is_some());
        assert!(serving_index(&indexes, &spec(&["+d"])).is_none(), "a sub-path is another key than its column");
    }
}
