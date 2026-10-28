//! Trees of items the top-level tests share: ten items and a transaction that
//! changes them, and the comparison that tells whether two trees hold the
//! same members with the same digests.

use ankurah_digest_tree::{DigestGroup, DigestTree, Ristretto255, Row, RowKey, Span, Store, Transaction};
use futures::TryStreamExt;

use super::members::{item, Id, Item, ItemTree, Property, Tag};

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

/// Whether two trees hold the same members with the same digests, as far as
/// reads can tell: equal members, an equal digest over the whole tree, and a
/// clean audit of each.
pub async fn assert_same_tree<G: DigestGroup>(a: &ItemTree<G>, b: &ItemTree<G>) {
    let (a_view, b_view) = (a.view(), b.view());
    let a_members: Vec<_> = a_view.range(..).try_collect().await.unwrap();
    let b_members: Vec<_> = b_view.range(..).try_collect().await.unwrap();

    assert_eq!(a_members, b_members, "members");
    assert_eq!(a_view.span_digest(&Span::all()).await.unwrap().digest, b_view.span_digest(&Span::all()).await.unwrap().digest, "digest");

    a.audit().await.unwrap();
    b.audit().await.unwrap();
}
