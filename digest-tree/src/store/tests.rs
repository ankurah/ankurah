//! The contract every store keeps, run against both reference stores.

use super::*;
use crate::fixtures::Tag;
use crate::MemoryStore;

/// Keys of each kind, in key order: kinds sort together, in declared order.
fn keys() -> Vec<RowKey> {
    vec![
        RowKey::Meta,
        RowKey::Node(NodePrefix::root()),
        RowKey::Node(NodePrefix::root().child(false)),
        RowKey::Node(NodePrefix::root().child(false).child(true)),
        RowKey::Node(NodePrefix::root().child(true)),
        RowKey::Leaf(vec![]),
        RowKey::Leaf(vec![1]),
        RowKey::Leaf(vec![1, 0]),
        RowKey::Leaf(vec![2]),
        RowKey::Leaf(vec![0xFF, 0xFF]),
    ]
}

fn row(n: usize) -> Tag { Tag(vec![n as u8]) }

/// A store holding a row under every key of [`keys`], the nth holding `n`.
async fn filled<S: Store<RowKey, Tag>>(store: S) -> S {
    store.write(keys().into_iter().enumerate().map(|(n, key)| WriteItem::Put(key, row(n))).collect()).await.unwrap();
    store
}

async fn a_row_reads_back_after_a_write_and_is_gone_after_a_delete<S: Store<RowKey, Tag>>(store: S) {
    assert_eq!(store.get(&RowKey::Meta).await.unwrap(), None);
    store.write(vec![WriteItem::Put(RowKey::Meta, row(1))]).await.unwrap();

    assert_eq!(store.get(&RowKey::Meta).await.unwrap(), Some(row(1)));
    store.write(vec![WriteItem::Put(RowKey::Meta, row(2))]).await.unwrap();

    assert_eq!(store.get(&RowKey::Meta).await.unwrap(), Some(row(2)), "a put replaces the row");
    store.write(vec![WriteItem::Delete(RowKey::Meta)]).await.unwrap();

    assert_eq!(store.get(&RowKey::Meta).await.unwrap(), None);
    store.write(vec![WriteItem::Delete(RowKey::Meta)]).await.unwrap();

    assert_eq!(store.get(&RowKey::Meta).await.unwrap(), None, "deleting an absent row changes nothing");
}

async fn range_reads_in_key_order_or_reversed_up_to_a_limit<S: Store<RowKey, Tag>>(store: S) {
    let store = filled(store).await;
    let keys = keys();
    let all: Vec<(RowKey, Tag)> = keys.iter().cloned().enumerate().map(|(n, key)| (key, row(n))).collect();
    let read = |start, end, direction, limit| store.range(start, end, direction, limit);

    assert_eq!(read(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap(), all);
    let reversed: Vec<_> = all.iter().rev().cloned().collect();

    assert_eq!(read(Bound::Unbounded, Bound::Unbounded, ScanDirection::Reverse, usize::MAX).await.unwrap(), reversed);
    assert_eq!(read(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 3).await.unwrap(), all[..3]);
    assert_eq!(read(Bound::Unbounded, Bound::Unbounded, ScanDirection::Reverse, 2).await.unwrap(), reversed[..2]);
    assert_eq!(read(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, 0).await.unwrap(), []);

    // Every pair of bounds over the leaf keys, each kind of bound.
    let (first_leaf, second_leaf, last_leaf) = (&keys[5], &keys[6], &keys[8]);
    for (start, end, expected) in [
        (Bound::Included(first_leaf), Bound::Included(last_leaf), &all[5..9]),
        (Bound::Excluded(first_leaf), Bound::Included(last_leaf), &all[6..9]),
        (Bound::Included(first_leaf), Bound::Excluded(last_leaf), &all[5..8]),
        (Bound::Excluded(first_leaf), Bound::Excluded(second_leaf), &all[6..6]),
        (Bound::Included(second_leaf), Bound::Unbounded, &all[6..]),
        (Bound::Unbounded, Bound::Excluded(first_leaf), &all[..5]),
    ] {
        assert_eq!(read(start, end, ScanDirection::Forward, usize::MAX).await.unwrap(), expected, "{start:?} to {end:?}");
        let reversed: Vec<_> = expected.iter().rev().cloned().collect();
        assert_eq!(read(start, end, ScanDirection::Reverse, usize::MAX).await.unwrap(), reversed, "{start:?} to {end:?}, reversed");
    }
}

async fn bounds_that_hold_nothing_read_nothing<S: Store<RowKey, Tag>>(store: S) {
    let store = filled(store).await;
    let (low, high) = (RowKey::Leaf(vec![1]), RowKey::Leaf(vec![2]));

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        for (start, end) in [
            (Bound::Included(&high), Bound::Included(&low)),
            (Bound::Included(&low), Bound::Excluded(&low)),
            (Bound::Excluded(&low), Bound::Included(&low)),
            (Bound::Excluded(&low), Bound::Excluded(&low)),
        ] {
            assert_eq!(store.range(start, end, direction, usize::MAX).await.unwrap(), [], "{start:?} to {end:?}");
        }
    }
}

/// A caller reads on from the last key it was given, a page at a time, and
/// so reads every row once.
async fn a_reader_pages_on_from_the_last_key<S: Store<RowKey, Tag>>(store: S) {
    let store = filled(store).await;
    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        let mut read = Vec::new();
        let mut last: Option<RowKey> = None;

        loop {
            let (start, end) = match (&last, direction) {
                (None, _) => (Bound::Unbounded, Bound::Unbounded),
                (Some(last), ScanDirection::Forward) => (Bound::Excluded(last), Bound::Unbounded),
                (Some(last), ScanDirection::Reverse) => (Bound::Unbounded, Bound::Excluded(last)),
            };

            let page = store.range(start, end, direction, 3).await.unwrap();
            let Some((key, _)) = page.last() else { break };
            last = Some(key.clone());
            read.extend(page.into_iter().map(|(key, _)| key));
        }

        let mut expected = keys();
        if direction == ScanDirection::Reverse {
            expected.reverse();
        }

        assert_eq!(read, expected, "{direction:?}");
    }
}

/// Every put and delete of a batch lands. That a batch lands whole or not
/// at all, each store promises by applying it under one lock or as one sled
/// batch; no test can watch a write midway.
async fn every_put_and_delete_of_a_batch_lands<S: Store<RowKey, Tag>>(store: S) {
    let store = filled(store).await;
    let keys = keys();

    store
        .write(vec![
            WriteItem::Delete(keys[1].clone()),
            WriteItem::Put(keys[2].clone(), row(20)),
            WriteItem::Delete(keys[3].clone()),
            WriteItem::Put(keys[4].clone(), row(40)),
        ])
        .await
        .unwrap();

    let nodes = store.range(Bound::Included(&keys[1]), Bound::Included(&keys[4]), ScanDirection::Forward, usize::MAX).await.unwrap();
    assert_eq!(nodes, [(keys[2].clone(), row(20)), (keys[4].clone(), row(40))]);
}

async fn clones_share_rows<S: Store<RowKey, Tag>>(store: S) {
    let clone = store.clone();
    store.write(vec![WriteItem::Put(RowKey::Leaf(vec![7]), row(7))]).await.unwrap();

    assert_eq!(clone.get(&RowKey::Leaf(vec![7])).await.unwrap(), Some(row(7)));
    clone.write(vec![WriteItem::Delete(RowKey::Leaf(vec![7]))]).await.unwrap();

    assert_eq!(store.get(&RowKey::Leaf(vec![7])).await.unwrap(), None);
    assert_eq!(store.range(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward, usize::MAX).await.unwrap(), []);
}

/// A store over a sled tree in a database that is deleted when dropped.
#[cfg(feature = "sled")]
pub(super) fn sled_store<V: Clone>() -> crate::SledStore<V> {
    let db = ::sled::Config::new().temporary(true).open().unwrap();
    crate::SledStore::new(db.open_tree("rows").unwrap())
}

/// Runs each named test, generic over the store, against both reference
/// stores.
macro_rules! against_each_store {
    ($($test:ident),* $(,)?) => {
        mod memory_store {
            $(#[tokio::test]
            async fn $test() { super::$test(super::MemoryStore::new()).await })*
        }

        #[cfg(feature = "sled")]
        mod sled_store {
            $(#[tokio::test]
            async fn $test() { super::$test(super::sled_store()).await })*
        }
    };
}

against_each_store!(
    a_row_reads_back_after_a_write_and_is_gone_after_a_delete,
    range_reads_in_key_order_or_reversed_up_to_a_limit,
    bounds_that_hold_nothing_read_nothing,
    a_reader_pages_on_from_the_last_key,
    every_put_and_delete_of_a_batch_lands,
    clones_share_rows,
);
