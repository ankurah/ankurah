//! Trees of items the top-level tests share: ten items and a transaction that
//! changes them, the comparison that tells whether two trees hold the same
//! rows, and the leaf read the tests keep for themselves.

use std::future;

use ankurah_core_types::Value;
use ankurah_digest_tree::{
    DigestGroup, DigestTree, DigestTreeView, KeyRange, Leaf, MemberId, Payload, Ristretto255, Row, RowKey, Store, Transaction,
};
use futures::TryStreamExt;

use super::members::{item, Id, Item, ItemTree, Property, Tag};
use super::stores::stored_rows;

/// Items 0 to 9, each holding its own number for property `A`.
pub fn ten_items() -> impl Iterator<Item = Item> { (0..10).map(|n| item!(n, n as i64, b"")) }

/// A transaction on a tree of [`ten_items`] that removes item 3, adds item
/// 10, and moves item 5 to key 50.
pub fn remove_add_and_move<S: Store<RowKey, Row<Property, Tag, Ristretto255>>>(
    tree: &DigestTree<Property, Id, Tag, Ristretto255, S>,
) -> Transaction<Property, Id, Tag, Ristretto255, S> {
    let mut trx = tree.transaction();
    trx.remove(item!(3, 3, b"")).unwrap();
    trx.insert(item!(10, 10, b"")).unwrap();
    trx.update(item!(5, 5, b""), item!(5, 50, b"")).unwrap();

    trx
}

/// The members of a tree of [`ten_items`] once [`remove_add_and_move`] has
/// committed.
pub fn removed_added_and_moved() -> impl Iterator<Item = Item> {
    (0..10).filter(|&n| n != 3 && n != 5).map(|n| item!(n, n as i64, b"")).chain([item!(5, 50, b""), item!(10, 10, b"")])
}

/// Whether two trees hold the same rows, metadata, members and nodes alike,
/// as two layouts of one set of members must, since a trie's shape follows
/// from its members alone; and a clean audit of each.
pub async fn assert_same_rows<G: DigestGroup>(a: &ItemTree<G>, b: &ItemTree<G>) {
    assert_eq!(stored_rows(a.store()).await, stored_rows(b.store()).await, "rows");
    a.audit().await.unwrap();
    b.audit().await.unwrap();
}

/// The leaf `view` holds for the member with `id` whose key holds `values`,
/// one for each part of the key: the tests' point read, through the key's
/// own range, since the API reads by range.
pub async fn leaf_at<K, I, L, G, S>(
    view: &DigestTreeView<K, I, L, G, S>,
    values: impl IntoIterator<Item = impl Into<Value>>,
    id: &I,
) -> Option<Leaf<I, L>>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    let at_key = view.range(KeyRange::prefix(values)).unwrap();
    at_key.try_filter(|leaf| future::ready(&leaf.id == id)).try_next().await.unwrap()
}
