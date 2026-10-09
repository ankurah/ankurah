//! The index a SQL engine keeps on a model's materialization table for a
//! query plan: what it is named, when an index already on the table serves
//! the plan's key spec instead, what assuring one decided, and what a
//! materialization handle remembers of its table's indexes. Each engine reads
//! its table's indexes from the database catalog and runs the DDL itself;
//! this module holds what the SQL engines share, and the reuse rule is sled's.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use ankurah_core::indexing::{IndexKeyPart, KeySpec};

/// An index the catalog lists on a materialization table, as the key spec it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingIndex {
    pub name: String,
    pub spec: KeySpec<String>,
}

/// The prefix of every index name the SQL engines choose, so that an
/// application's index cannot occupy one by accident.
pub const INDEX_NAME_PREFIX: &str = "_ankurah_index";

/// The name of the index for `spec` on `table`: sled's rendering of the spec
/// behind the engines' prefix and the table's name, since a SQL database
/// keeps every table's index names in one namespace. PostgreSQL shortens a
/// name past its identifier limit.
pub fn index_name(table: &str, spec: &KeySpec<String>) -> String { spec.name_with(&format!("{INDEX_NAME_PREFIX}__{table}"), "__") }

/// The existing index that serves `spec`, by sled's rule: `spec` is a prefix
/// of the index's key, with every direction equal or every direction flipped
/// (a database scans an index either way), and each key part past the prefix
/// is the `id` column. Sled's guard also admits its materialization column
/// there; a materialization table has none, each model having its own table.
/// Any other trailing part refuses the reuse. Sled refuses because its shared
/// index omits an entity missing a trailing property; a SQL index holds every
/// row, and the guard is kept so that the SQL engines create exactly the
/// indexes sled creates for the same plans. Beyond sled's rule, which reads
/// columns and directions, each part within the prefix must sort as the
/// plan's does: an index collated, or ordering NULLs, differently from what
/// the plan asks does not serve it.
pub fn serving_index<'a>(existing: &'a [ExistingIndex], spec: &KeySpec<String>) -> Option<&'a ExistingIndex> {
    existing.iter().find(|index| {
        spec.matches(&index.spec).is_some()
            && spec.keyparts.iter().zip(&index.spec.keyparts).all(|(wanted, part)| sorts_alike(wanted, part))
            && index.spec.keyparts[spec.keyparts.len()..].iter().all(|part| part.key == "id")
    })
}

fn sorts_alike(wanted: &IndexKeyPart<String>, part: &IndexKeyPart<String>) -> bool {
    wanted.collation == part.collation && wanted.nulls == part.nulls
}

/// What assuring an index for a plan decided, under the lock that decided
/// it. Nothing consumes it yet: it is returned so that the digest trees'
/// index creation hook has one place to attach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexOutcome {
    /// This call created the index for the plan's spec.
    Created(ExistingIndex),
    /// An index already on the table serves the plan's spec.
    Reused(ExistingIndex),
    /// No index serves the plan's spec and none is created for it: a part the
    /// engine cannot index, another index on the name the engine would use,
    /// or a build that failed and waits out its backoff. The query runs
    /// without an index, which its SQL allows.
    Unservable(KeySpec<String>),
}

/// How long after a failed build the engine answers without the index before
/// trying to build it again.
pub const DDL_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// What a materialization handle remembers of its table's indexes: the
/// catalog's last listing, and the specs it runs no DDL for.
#[derive(Debug, Default)]
pub struct TableIndexes {
    listed: Option<Vec<ExistingIndex>>,
    refused: HashSet<KeySpec<String>>,
    retry_after: HashMap<KeySpec<String>, Instant>,
}

impl TableIndexes {
    /// The listed index that serves `spec`, once the catalog has been read.
    pub fn serving(&self, spec: &KeySpec<String>) -> Option<&ExistingIndex> {
        self.listed.as_deref().and_then(|listed| serving_index(listed, spec))
    }

    /// Whether the handle runs no DDL for `spec` at `now`: the spec was
    /// refused, or its build failed and the backoff is not over.
    pub fn refuses(&self, spec: &KeySpec<String>, now: Instant) -> bool {
        self.refused.contains(spec) || self.retry_after.get(spec).is_some_and(|until| now < *until)
    }

    /// Replace the listing with what the catalog lists now.
    pub fn list(&mut self, existing: Vec<ExistingIndex>) { self.listed = Some(existing); }

    /// Run no more DDL for `spec` while this handle lives.
    pub fn refuse(&mut self, spec: KeySpec<String>) { self.refused.insert(spec); }

    /// Run no DDL for `spec` until `until`.
    pub fn fail(&mut self, spec: KeySpec<String>, until: Instant) { self.retry_after.insert(spec, until); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::{
        indexing::{IndexDirection, NullsOrder},
        value::ValueType,
    };

    /// A spec from parts written as `+column` or `-column.step`.
    fn spec(parts: &[&str]) -> KeySpec<String> {
        KeySpec::new(parts.iter().map(|part| IndexKeyPart::from_flat_path(&part[1..], direction(part), ValueType::String)).collect())
    }

    fn direction(part: &str) -> IndexDirection {
        match part.as_bytes()[0] {
            b'+' => IndexDirection::Asc,
            b'-' => IndexDirection::Desc,
            _ => panic!("a part starts with its direction"),
        }
    }

    fn existing(parts: &[&str]) -> ExistingIndex { ExistingIndex { name: parts.join("__"), spec: spec(parts) } }

    #[test]
    fn an_index_is_named_for_its_table_and_key_behind_the_engines_prefix() {
        assert_eq!(
            index_name("notification", &spec(&["+recipient", "-status"])),
            "_ankurah_index__notification__recipient asc__status desc"
        );
        assert_eq!(index_name("notification", &spec(&["+detail.kind"])), "_ankurah_index__notification__detail.kind asc");
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

    #[test]
    fn an_index_sorting_differently_from_the_plan_does_not_serve_it() {
        let mut collated = existing(&["+a"]);
        collated.spec.keyparts[0].collation = Some("NOCASE".to_owned());
        let mut nulls_first = existing(&["+b"]);
        nulls_first.spec.keyparts[0].nulls = Some(NullsOrder::First);
        let indexes = [collated, nulls_first];
        assert!(serving_index(&indexes, &spec(&["+a"])).is_none(), "the plan asks for the default collation");
        assert!(serving_index(&indexes, &spec(&["+b"])).is_none(), "the plan asks for the default nulls order");
        let mut wanted = spec(&["+a"]);
        wanted.keyparts[0].collation = Some("NOCASE".to_owned());
        assert!(serving_index(&indexes, &wanted).is_some());
    }

    #[test]
    fn a_handle_remembers_the_listing_and_the_specs_it_runs_no_ddl_for() {
        let mut indexes = TableIndexes::default();
        let now = Instant::now();
        assert!(indexes.serving(&spec(&["+a"])).is_none(), "nothing serves before the catalog is read");
        indexes.list(vec![existing(&["+a"])]);
        assert!(indexes.serving(&spec(&["+a"])).is_some());

        assert!(!indexes.refuses(&spec(&["+b"]), now));
        indexes.refuse(spec(&["+b"]));
        assert!(indexes.refuses(&spec(&["+b"]), now));

        indexes.fail(spec(&["+c"]), now + DDL_RETRY_BACKOFF);
        assert!(indexes.refuses(&spec(&["+c"]), now));
        assert!(!indexes.refuses(&spec(&["+c"]), now + DDL_RETRY_BACKOFF), "the backoff is over");
    }
}
