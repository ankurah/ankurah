//! Key order: a tree reads its members in the byte order of their keys, ties
//! broken by id, whatever mix of orders the owner's keys carry in their parts;
//! a tree keyed by id alone reads in id order.

mod common;

use std::cmp::Ordering;

use ankurah_digest_tree::{Ristretto255, ScanRange, Span};
use common::{built, spec, Item, Order, Spec};
use futures::TryStreamExt;

/// A member's two key values, `None` when missing.
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

fn item((id, (first, second)): (u64, Values)) -> Item { Item::new(id, [first, second], b"") }

/// Every mix of orders for the two key parts.
fn mixes() -> Vec<Spec> {
    let orders = [Order::Ascending, Order::Descending];
    orders.iter().flat_map(|&first| orders.iter().map(move |&second| spec(&[first, second]))).collect()
}

/// How two values compare in a part's order, a missing value after every
/// present one ascending, and so before them descending.
fn compare(a: Option<i64>, b: Option<i64>, order: Order) -> Ordering {
    let rank = |value: Option<i64>| value.map_or((1, 0), |n| (0, n));
    let ordering = rank(a).cmp(&rank(b));

    match order {
        Order::Ascending => ordering,
        Order::Descending => ordering.reverse(),
    }
}

/// The owner's keys carry its mix of orders, so a tree read forward gives
/// each part in its own order, missing values last ascending and first
/// descending, ties broken by id.
#[tokio::test]
async fn a_mix_of_orders_is_the_keys_own() {
    for key_spec in mixes() {
        let tree = built::<Ristretto255>(key_spec.clone(), members().into_iter().map(item)).await;
        let read: Vec<u64> = tree.view().range(ScanRange::forward(Span::all())).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

        let mut ordered = members();
        let (first, second) = (key_spec.0[0], key_spec.0[1]);
        ordered.sort_by(|(a_id, a), (b_id, b)| compare(a.0, b.0, first).then(compare(a.1, b.1, second)).then(a_id.cmp(b_id)));

        assert_eq!(read, ordered.into_iter().map(|(id, _)| id).collect::<Vec<_>>(), "{key_spec:?}");
    }
}

/// A tree keyed by id alone, under a key spec of no parts, reads in id order:
/// forward from the smallest id up, and in reverse from the largest down.
#[tokio::test]
async fn a_tree_keyed_by_id_alone_reads_in_id_order() {
    let tree = built::<Ristretto255>(spec(&[]), (0..20).map(|id| Item::new(id, [], b""))).await;
    let view = tree.view();

    let forward: Vec<u64> = view.range(ScanRange::forward(Span::all())).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();
    let reverse: Vec<u64> = view.range(ScanRange::reverse(Span::all())).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

    assert_eq!(forward, (0..20).collect::<Vec<_>>());
    assert_eq!(reverse, (0..20).rev().collect::<Vec<_>>());
}
