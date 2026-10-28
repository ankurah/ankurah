//! Key order: a tree reads its members in their keys' order, each part in its
//! own direction with missing values where PostgreSQL puts them, ties broken
//! by id; a range of a part's values holds the members holding values within
//! it, whichever way the part sorts; and a tree keyed by id alone reads in id
//! order.

mod common;

use std::cmp::Ordering;

use ankurah_core_types::key_spec::{IndexDirection, KeySpec};
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{KeyRange, ScanRange};
use common::{asc, desc, item, tree_of, Item, ItemTree, Property};
use futures::TryStreamExt;

/// A member's values for `A` and `B`, `None` when missing.
type Values = (Option<i64>, Option<i64>);

/// 90 members over few values, so that ties are many and ids break them,
/// with some values missing.
fn members() -> Vec<(u64, Values)> {
    (0..90u64)
        .map(|n| {
            let first = (n % 11 != 0).then_some((n % 5) as i64);
            let second = (n % 13 != 0).then_some((n / 5 % 3) as i64);
            (n, (first, second))
        })
        .collect()
}

fn with_values((id, (first, second)): (u64, Values)) -> Item { item!(id, [first, second], b"") }

/// How two values compare in a part's order, a missing value after every
/// present one ascending, and so before them descending.
fn compare(a: Option<i64>, b: Option<i64>, direction: IndexDirection) -> Ordering {
    let rank = |value: Option<i64>| value.map_or((1, 0), |n| (0, n));
    let ordering = rank(a).cmp(&rank(b));

    match direction {
        IndexDirection::Asc => ordering,
        IndexDirection::Desc => ordering.reverse(),
    }
}

/// A tree read forward gives each part in its own order, missing values last
/// ascending and first descending, ties broken by id.
#[tokio::test]
async fn a_mix_of_orders_reads_each_part_in_its_own() {
    for key_spec in [
        KeySpec::new(vec![asc!(Property::A, ValueType::I64), asc!(Property::B, ValueType::I64)]),
        KeySpec::new(vec![asc!(Property::A, ValueType::I64), desc!(Property::B, ValueType::I64)]),
        KeySpec::new(vec![desc!(Property::A, ValueType::I64), asc!(Property::B, ValueType::I64)]),
        KeySpec::new(vec![desc!(Property::A, ValueType::I64), desc!(Property::B, ValueType::I64)]),
    ] {
        let tree: ItemTree = tree_of!(key_spec.clone(), members().into_iter().map(with_values));
        let read: Vec<u64> = tree.view().range(..).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

        let mut ordered = members();
        let (first, second) = (key_spec.keyparts[0].direction, key_spec.keyparts[1].direction);
        ordered.sort_by(|(a_id, a), (b_id, b)| compare(a.0, b.0, first).then(compare(a.1, b.1, second)).then(a_id.cmp(b_id)));

        assert_eq!(read, ordered.into_iter().map(|(id, _)| id).collect::<Vec<_>>(), "{key_spec:?}");
    }
}

/// Under an equal first value, a range of a descending second part holds the
/// members holding values within it, read largest first, and none that holds
/// no value there: member 52, which holds 2 and nothing, is left out.
#[tokio::test]
async fn a_range_of_a_descending_part_holds_its_values_and_no_missing_ones() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64), desc!(Property::B, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec, members().into_iter().map(with_values));

    let span = tree.span_for(KeyRange::prefix([2i64]).and(0i64..2)).unwrap();
    let read: Vec<u64> = tree.view().range(span).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

    let mut held: Vec<(u64, Values)> =
        members().into_iter().filter(|(_, (a, b))| *a == Some(2) && b.is_some_and(|b| (0..2).contains(&b))).collect();
    held.sort_by(|(a_id, a), (b_id, b)| compare(a.1, b.1, IndexDirection::Desc).then(a_id.cmp(b_id)));

    assert_eq!(members()[52], (52, (Some(2), None)));
    assert_eq!(read, held.into_iter().map(|(id, _)| id).collect::<Vec<_>>());
}

/// A tree keyed by id alone, under a key spec of no parts, reads in id order:
/// forward from the smallest id up, and in reverse from the largest down.
#[tokio::test]
async fn a_tree_keyed_by_id_alone_reads_in_id_order() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![]), (0..20).map(|id| item!(id, [], b"")));
    let view = tree.view();

    let forward: Vec<u64> = view.range(..).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();
    let reverse: Vec<u64> = view.range(ScanRange::reverse(..)).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

    assert_eq!(forward, (0..20).collect::<Vec<_>>());
    assert_eq!(reverse, (0..20).rev().collect::<Vec<_>>());
}
