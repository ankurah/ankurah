//! Index scans through real fetches: each comparison shape must return
//! exactly the matching entities in ORDER BY order, honour LIMIT, and build
//! the index its shape calls for. A fetch always carries the membership
//! equality as its leading prefix, so the two shapes without one (no
//! equality prefix, an all-0xFF key) are covered at the range conversion
//! in the workspace tests instead.

mod support;

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core::{indexing::IndexDirection, storage::StorageEngine, value::Value};
use ankurah_proto::PropertyId;
use ankurah_storage_sled::SledStorageEngine;
use support::{index_specs, insert, model, property};

/// Entities by id byte: (group, name). Names are unique, so name order is total.
const ROWS: [(u8, &str, &str); 6] = [(1, "a", "a"), (2, "a", "a\0"), (3, "a", "b"), (4, "a", "c"), (5, "a\0", "d"), (6, "b", "e")];

fn group() -> PropertyId { property(201) }
fn name() -> PropertyId { property(202) }

fn compare(property: PropertyId, operator: ComparisonOperator, literal: &str) -> Predicate<Resolved> {
    Predicate::Comparison {
        left: Box::new(Expr::Path(property.into())),
        operator,
        right: Box::new(Expr::Literal(Value::String(literal.into()))),
    }
}

fn name_is(operator: ComparisonOperator, literal: &str) -> Predicate<Resolved> { compare(name(), operator, literal) }

fn both(a: Predicate<Resolved>, b: Predicate<Resolved>) -> Predicate<Resolved> { Predicate::And(Box::new(a), Box::new(b)) }

struct Case {
    what: &'static str,
    predicate: Predicate<Resolved>,
    order: OrderDirection,
    /// Entity id bytes in ORDER BY order.
    expected: Vec<u8>,
    /// Key parts of the one index the fetch builds, and the direction of its
    /// last part; `None` when the planner answers without an index.
    index: Option<(usize, IndexDirection)>,
}

fn cases() -> Vec<Case> {
    use ComparisonOperator::*;
    use IndexDirection::{Asc, Desc};
    use OrderDirection as O;
    vec![
        Case { what: "name < 'b'", predicate: name_is(LessThan, "b"), order: O::Asc, expected: vec![1, 2], index: Some((2, Asc)) },
        Case {
            what: "name >= 'b'",
            predicate: name_is(GreaterThanOrEqual, "b"),
            order: O::Asc,
            expected: vec![3, 4, 5, 6],
            index: Some((2, Asc)),
        },
        Case {
            what: "name > 'a' AND name < 'd'",
            predicate: both(name_is(GreaterThan, "a"), name_is(LessThan, "d")),
            order: O::Asc,
            expected: vec![2, 3, 4],
            index: Some((2, Asc)),
        },
        Case {
            what: "name >= 'a\\0' AND name <= 'c'",
            predicate: both(name_is(GreaterThanOrEqual, "a\0"), name_is(LessThanOrEqual, "c")),
            order: O::Asc,
            expected: vec![2, 3, 4],
            index: Some((2, Asc)),
        },
        Case {
            what: "name > 'e' (empty range)",
            predicate: name_is(GreaterThan, "e"),
            order: O::Asc,
            expected: vec![],
            index: Some((2, Asc)),
        },
        Case {
            what: "name > 'c' AND name < 'c' (empty bounds)",
            predicate: both(name_is(GreaterThan, "c"), name_is(LessThan, "c")),
            order: O::Asc,
            expected: vec![],
            index: None,
        },
        Case {
            what: "group = 'a' AND name >= 'a' (the neighbouring prefix 'a\\0' must not leak in)",
            predicate: both(compare(group(), Equal, "a"), name_is(GreaterThanOrEqual, "a")),
            order: O::Asc,
            expected: vec![1, 2, 3, 4],
            index: Some((3, Asc)),
        },
        Case {
            what: "group = 'a\\0' AND name >= 'a' (the neighbouring prefix 'a' must not leak in)",
            predicate: both(compare(group(), Equal, "a\0"), name_is(GreaterThanOrEqual, "a")),
            order: O::Asc,
            expected: vec![5],
            index: Some((3, Asc)),
        },
        Case {
            what: "name >= 'b' DESC",
            predicate: name_is(GreaterThanOrEqual, "b"),
            order: O::Desc,
            expected: vec![6, 5, 4, 3],
            index: Some((2, Desc)),
        },
        Case {
            what: "group = 'a' AND name < 'c' DESC",
            predicate: both(compare(group(), Equal, "a"), name_is(LessThan, "c")),
            order: O::Desc,
            expected: vec![3, 2, 1],
            index: Some((3, Desc)),
        },
    ]
}

#[tokio::test]
async fn every_scan_shape_returns_exactly_its_rows_in_order() -> anyhow::Result<()> {
    for case in cases() {
        let engine = SledStorageEngine::new_test()?;
        for (byte, group_value, name_value) in ROWS {
            insert(&engine, model(), byte, &[(group(), Value::String(group_value.into())), (name(), Value::String(name_value.into()))])
                .await?;
        }
        for limit in [None, Some(2)] {
            let order_by = Some(vec![OrderByItem { path: name().into(), direction: case.order.clone() }]);
            let selection = Selection { predicate: case.predicate.clone(), order_by, limit }.and_member_of(model());
            let found: Vec<u8> = engine.fetch_states(&selection).await?.iter().map(|state| state.payload.entity_id.to_bytes()[0]).collect();
            let expected: Vec<u8> = case.expected.iter().copied().take(limit.map_or(usize::MAX, |n| n as usize)).collect();
            assert_eq!(found, expected, "{} with limit {limit:?}", case.what);
        }
        let specs = index_specs(&engine);
        match case.index {
            None => assert!(specs.is_empty(), "{}: {specs:?}", case.what),
            Some((parts, last_direction)) => {
                assert_eq!(specs.len(), 1, "{}: {specs:?}", case.what);
                assert_eq!(specs[0].keyparts.len(), parts, "{}: {specs:?}", case.what);
                assert_eq!(specs[0].keyparts.last().unwrap().direction, last_direction, "{}: {specs:?}", case.what);
            }
        }
    }
    Ok(())
}
