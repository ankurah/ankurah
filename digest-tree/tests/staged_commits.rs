//! Staged commits: a transaction's rows worked out for its owner to write in
//! a storage transaction of its own, with the tree held until the staged
//! commit is dropped.

mod common;

use std::pin::pin;

use ankurah_digest_tree::Store;
use common::{
    integer_spec, item, poll_once, remove_add_and_move, removed_added_and_moved, stored_rows, ten_items, Id, Item, ItemStore, ItemTree,
};

/// A tree in memory holding `members`, and its store, which a test writes as
/// an owner's storage transaction would.
async fn tree_and_store(members: impl IntoIterator<Item = Item>) -> (ItemTree, ItemStore) {
    let store = ItemStore::new();
    (ItemTree::build(store.clone(), integer_spec(), members).await.unwrap(), store)
}

/// The rows a staged commit hands its owner, written in one batch as the
/// owner's storage transaction would write them, make the tree a commit of
/// the same changes makes.
#[tokio::test]
async fn a_staged_commits_rows_make_the_tree_a_commit_makes() {
    let (tree, store) = tree_and_store(ten_items()).await;

    let staged = remove_add_and_move(&tree).stage().await.unwrap();
    store.write(staged.rows().to_vec()).await.unwrap();
    drop(staged);

    let (committed, committed_store) = tree_and_store(ten_items()).await;
    remove_add_and_move(&committed).commit().await.unwrap();

    assert_eq!(stored_rows(&store).await, stored_rows(&committed_store).await);
    tree.audit().await.unwrap();
}

/// A staged commit dropped with its rows unwritten, as when its owner's
/// storage transaction fails, leaves the tree as it was, and the tree takes
/// the same changes afterwards.
#[tokio::test]
async fn a_staged_commit_dropped_unwritten_changes_nothing() {
    let (tree, store) = tree_and_store(ten_items()).await;
    let before = stored_rows(&store).await;

    let staged = remove_add_and_move(&tree).stage().await.unwrap();
    assert!(!staged.rows().is_empty());
    drop(staged);

    assert_eq!(stored_rows(&store).await, before, "the dropped commit changed no row");

    remove_add_and_move(&tree).commit().await.unwrap();

    let (_, expected) = tree_and_store(removed_added_and_moved()).await;
    assert_eq!(stored_rows(&store).await, stored_rows(&expected).await);
}

/// A staged commit holds the tree until it is dropped: a read and another
/// commit wait out its owner's storage transaction, then see its rows.
#[tokio::test]
async fn a_staged_commit_holds_off_reads_and_commits_until_dropped() {
    let (tree, store) = tree_and_store(ten_items()).await;
    let staged = remove_add_and_move(&tree).stage().await.unwrap();

    let (view, key, id) = (tree.view(), integer_spec().collate([10]), Id(10));
    let mut reading = pin!(view.member(&key, &id));
    assert!(poll_once(reading.as_mut()).is_pending(), "the read waits for the staged commit");

    let mut other = tree.transaction();
    other.insert(item!(11, Some(11), b"")).unwrap();

    let mut committing = pin!(other.commit());
    assert!(poll_once(committing.as_mut()).is_pending(), "the commit waits for the staged commit");

    // The owner's storage transaction writes the staged rows, then finishes.
    store.write(staged.rows().to_vec()).await.unwrap();
    drop(staged);

    assert!(reading.await.unwrap().is_some(), "the read sees the staged commit's rows");
    committing.await.unwrap();

    assert_eq!(tree.view().len().await.unwrap(), 11);
}
