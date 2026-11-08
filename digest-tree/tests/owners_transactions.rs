//! Commits in an owner's transaction: prepared without the store, applied
//! through the transaction the owner lends the tree, and landing only when
//! the owner commits it.

mod common;

use std::pin::pin;
use std::task::Poll;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::Error;
use common::{
    asc, item, leaf_at, poll_once, remove_add_and_move, removed_added_and_moved, stored_rows, ten_items, tree_of, Id, Item, ItemTree,
    Property,
};

/// A tree in memory holding `members`.
async fn tree_holding(members: impl IntoIterator<Item = Item>) -> ItemTree {
    tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), members)
}

/// A commit applied in the owner's transaction lands with it, and makes the
/// tree a commit of the same changes on a store of its own makes.
#[tokio::test]
async fn a_commit_applied_in_an_owners_transaction_lands_when_it_commits() {
    let tree = tree_holding(ten_items()).await;
    let before = stored_rows(tree.store()).await;

    let mut owner_trx = tree.store().begin().await;
    let applied = remove_add_and_move(&tree).prepare().await.apply(&mut owner_trx).await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, before, "nothing before the owner commits");

    owner_trx.commit().await.unwrap();
    applied.landed();

    let committed = tree_holding(ten_items()).await;
    remove_add_and_move(&committed).commit().await.unwrap();

    assert_eq!(stored_rows(tree.store()).await, stored_rows(committed.store()).await);
    tree.audit().await.unwrap();
}

/// A commit applied in an owner's transaction that is dropped, as when the
/// owner's records fail to commit, leaves the tree as it was, and the tree
/// takes the same changes afterwards.
#[tokio::test]
async fn a_commit_whose_owners_transaction_is_dropped_changes_nothing() {
    let tree = tree_holding(ten_items()).await;
    let before = stored_rows(tree.store()).await;

    let mut owner_trx = tree.store().begin().await;
    let applied = remove_add_and_move(&tree).prepare().await.apply(&mut owner_trx).await.unwrap();
    drop((applied, owner_trx));

    assert_eq!(stored_rows(tree.store()).await, before, "the dropped commit changed no row");

    remove_add_and_move(&tree).commit().await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, stored_rows(tree_holding(removed_added_and_moved()).await.store()).await);
}

/// A commit the tree refuses writes nothing into the owner's transaction, so
/// the owner may still commit its own records, and the tree is left as it
/// was.
#[tokio::test]
async fn a_refused_commit_writes_nothing_into_the_owners_transaction() {
    let tree = tree_holding(ten_items()).await;
    let before = stored_rows(tree.store()).await;

    let mut trx = tree.transaction();
    trx.insert(item!(10, 10, b"")).unwrap();
    trx.remove(item!(3, 3, b"stale")).unwrap();

    let mut owner_trx = tree.store().begin().await;
    let refused = trx.prepare().await.apply(&mut owner_trx).await;
    assert!(matches!(refused, Err(Error::NotHeld { .. })));

    owner_trx.commit().await.unwrap();
    assert_eq!(stored_rows(tree.store()).await, before);
}

/// A commit given another store's transaction would land its rows beside
/// another tree's, so it is refused, and writes nothing to either.
#[tokio::test]
async fn a_commit_refuses_a_transaction_on_another_store() {
    let (tree, other) = (tree_holding(ten_items()).await, tree_holding(ten_items()).await);
    let before = stored_rows(other.store()).await;

    let mut others_trx = other.store().begin().await;
    let refused = remove_add_and_move(&tree).prepare().await.apply(&mut others_trx).await;
    assert!(matches!(refused, Err(Error::OtherTree)));

    others_trx.commit().await.unwrap();
    assert_eq!(stored_rows(other.store()).await, before);
}

/// While the owner's transaction is open, reads of the tree go on, seeing it
/// as it was, and another commit waits for the transaction to end, then
/// sees its rows.
#[tokio::test]
async fn an_open_owners_transaction_holds_off_other_commits_but_not_reads() {
    let tree = tree_holding(ten_items()).await;

    let mut owner_trx = tree.store().begin().await;
    let applied = remove_add_and_move(&tree).prepare().await.apply(&mut owner_trx).await.unwrap();

    let view = tree.view();
    match poll_once(pin!(leaf_at(&view, [10i64], &Id(10)))) {
        Poll::Ready(leaf) => assert_eq!(leaf, None, "the read sees the tree before the owner's commit"),
        Poll::Pending => panic!("the read waited for the owner's open transaction"),
    }

    let mut other = tree.transaction();
    other.insert(item!(11, 11, b"")).unwrap();

    let mut committing = pin!(other.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the owner's transaction");

    owner_trx.commit().await.unwrap();
    applied.landed();

    committing.await.unwrap();
    assert_eq!(tree.view().count(..).await.unwrap(), 11);
}
