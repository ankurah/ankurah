//! Windows: the members a plan's limit admits, read forward or backward in
//! address order, whatever mix of orders the owner's keys carry in their parts.

mod common;

use std::cmp::Ordering;
use std::ops::Bound;

use ankurah_digest_tree::{leaf_address, Ristretto255, ScanDirection, ScanRange, Span, ViewPlan};
use common::{built, key_of, key_part, spec, Id, Item, Order, Spec};
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

/// The ids of the first `limit` members within `spans` in address order, or
/// the last `limit`, read backward: what a window must cover.
fn expected(key_spec: &Spec, spans: &[Span], direction: ScanDirection, limit: usize) -> Vec<u64> {
    let mut addresses: Vec<(Vec<u8>, u64)> = members()
        .into_iter()
        .map(|(id, (first, second))| (leaf_address(&key_of(&[first, second], key_spec).unwrap(), &Id(id)), id))
        .filter(|(address, _)| spans.iter().any(|span| span.contains(address)))
        .collect();

    addresses.sort();

    if direction == ScanDirection::Reverse {
        addresses.reverse();
    }

    let mut ids: Vec<u64> = addresses.into_iter().take(limit).map(|(_, id)| id).collect();
    ids.sort_unstable();

    ids
}

/// Every mix of orders in the keys, both scan directions, limits short of
/// and past the members, and spans whole and disjoint: a window covers
/// exactly the first or last members its limit admits.
#[tokio::test]
async fn a_window_covers_the_first_or_last_members_its_limit_admits() {
    for key_spec in mixes() {
        let tree = built::<Ristretto255>(key_spec.clone(), members().into_iter().map(item)).await;

        // Keys before one first value and after another, in byte order.
        let mut cuts = [key_part(Some(1), key_spec.0[0]), key_part(Some(3), key_spec.0[0])];
        cuts.sort();

        let disjoint = vec![
            Span::for_key_range(Bound::Unbounded, Bound::Excluded(&cuts[0])),
            Span::for_key_range(Bound::Excluded(&cuts[1]), Bound::Unbounded),
        ];

        for spans in [vec![Span::all()], disjoint] {
            for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
                for limit in [0u64, 1, 9, 37, 89, 200] {
                    let plan = ViewPlan { spans: spans.clone(), direction, limit: Some(limit) };
                    let view = tree.select(&plan).await.unwrap();

                    let mut covered: Vec<u64> =
                        view.range(ScanRange::forward(Span::all())).map_ok(|leaf| leaf.id.0).try_collect().await.unwrap();

                    covered.sort_unstable();
                    let expected = expected(&key_spec, &spans, direction, limit as usize);

                    assert_eq!(covered, expected, "{key_spec:?}, {plan:?}");
                    assert_eq!(view.len().await.unwrap(), expected.len() as u64);
                }
            }
        }
    }
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

/// A limit the members don't fill keeps the plan's spans whole, since a peer
/// may hold the members missing here: the view covers members that come
/// later, an empty tree's view included.
#[tokio::test]
async fn an_unfilled_limit_keeps_the_plans_spans_whole() {
    let key_spec = spec(&[Order::Ascending, Order::Ascending]);
    for count in [0, 3] {
        let tree = built::<Ristretto255>(key_spec.clone(), members().into_iter().take(count).map(item)).await;
        let plan = ViewPlan { spans: vec![Span::all()], direction: ScanDirection::Reverse, limit: Some(10) };

        let view = tree.select(&plan).await.unwrap();
        assert_eq!(view.len().await.unwrap(), count as u64);

        let mut trx = tree.transaction();
        for member in members().into_iter().skip(count).take(20) {
            trx.insert(item(member)).unwrap();
        }

        trx.commit().await.unwrap();
        assert_eq!(view.len().await.unwrap(), count as u64 + 20, "the view's spans stayed whole");
    }
}

/// A tree keyed by id alone windows by id. The plan picks which members, the
/// first three ids or the last three, and the read picks their order: the
/// last three come from the largest id down when read in reverse, and from
/// the smallest of them up when read forward.
#[tokio::test]
async fn a_tree_keyed_by_id_alone_windows_by_id() {
    let tree = built::<Ristretto255>(spec(&[]), (0..20).map(|id| Item::new(id, [], b""))).await;
    let reads = [
        (ScanDirection::Forward, ScanRange::forward(Span::all()), [0, 1, 2]),
        (ScanDirection::Reverse, ScanRange::reverse(Span::all()), [19, 18, 17]),
        (ScanDirection::Reverse, ScanRange::forward(Span::all()), [17, 18, 19]),
    ];

    for (direction, range, expected) in reads {
        let plan = ViewPlan { spans: vec![Span::all()], direction, limit: Some(3) };
        let ids: Vec<Id> = tree.select(&plan).await.unwrap().range(range.clone()).map_ok(|leaf| leaf.id).try_collect().await.unwrap();

        assert_eq!(ids, expected.map(Id), "the {direction:?} window, read by {range:?}");
    }
}
