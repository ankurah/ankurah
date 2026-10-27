//! Transactions: each kind of change, how changes to one member chain, the
//! checks a commit makes, and what a commit writes.

mod common;

use ankurah_digest_tree::{Error, MemberId, MemoryStore, Ristretto255, Transaction};
use common::{
    asc, assert_same_tree, built, counted, integer_spec, item, remove_add_and_move, removed_added_and_moved, stored_rows, ten_items,
    CountedStore, CountedTree, CountingStore, FailingStore, FailingTree, Id, Item, KeySpec, Named, NamedTree, Tag,
};
use futures::TryStreamExt;

/// A transaction on a tree over a store that counts its writes.
type CountedTransaction = Transaction<KeySpec, Id, Tag, Ristretto255, CountedStore>;

#[tokio::test]
async fn an_insert_files_a_new_member() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a")]).await;

    let mut trx = tree.transaction();
    trx.insert(item!(2, Some(5), b"b")).unwrap();
    trx.commit().await.unwrap();

    let leaf = tree.view().member(&integer_spec().collate([5]), &Id(2)).await.unwrap().unwrap();
    assert_eq!((leaf.id, leaf.payload), (Id(2), Tag(b"b".to_vec())));
    assert_same_tree(&tree, &built(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(5), b"b")]).await).await;
}

#[tokio::test]
async fn an_update_under_the_same_key_replaces_the_payload() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"b")]).await;

    let mut trx = tree.transaction();
    trx.update(item!(2, Some(2), b"b"), item!(2, Some(2), b"changed")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(tree.view().member(&integer_spec().collate([2]), &Id(2)).await.unwrap().unwrap().payload, Tag(b"changed".to_vec()));
    assert_same_tree(&tree, &built(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"changed")]).await).await;
}

#[tokio::test]
async fn an_update_under_another_key_moves_the_member() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"b")]).await;

    let mut trx = tree.transaction();
    trx.update(item!(2, Some(2), b"b"), item!(2, None, b"b")).unwrap();
    trx.commit().await.unwrap();

    let view = tree.view();
    assert_eq!(view.member(&integer_spec().collate([2]), &Id(2)).await.unwrap(), None, "gone from its old key");
    assert!(view.member(&integer_spec().collate([None]), &Id(2)).await.unwrap().is_some(), "filed under a missing value");
    assert_eq!(view.len().await.unwrap(), 2);
    assert_same_tree(&tree, &built(integer_spec(), [item!(1, Some(1), b"a"), item!(2, None, b"b")]).await).await;
}

#[tokio::test]
async fn a_removal_takes_the_member_out() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"b")]).await;

    let mut trx = tree.transaction();
    trx.remove(item!(1, Some(1), b"a")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(tree.view().member(&integer_spec().collate([1]), &Id(1)).await.unwrap(), None);
    assert_same_tree(&tree, &built(integer_spec(), [item!(2, Some(2), b"b")]).await).await;
}

/// A leaf read from the tree is the version the tree holds, so a reader can
/// name it in a change, as a peer repairing its tree from another's does.
#[tokio::test]
async fn a_leaf_read_from_the_tree_names_the_version_a_change_replaces() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"b")]).await;
    let leaves: Vec<_> = tree.view().range(..).try_collect().await.unwrap();

    let mut trx = tree.transaction();
    trx.update(leaves[0].clone(), item!(1, Some(1), b"changed")).unwrap();
    trx.remove(leaves[1].clone()).unwrap();
    trx.commit().await.unwrap();

    assert_same_tree(&tree, &built(integer_spec(), [item!(1, Some(1), b"changed")]).await).await;
}

/// Each change to one member starts from the version the one before it left,
/// so a member's changes in a transaction sum to one, and one that starts
/// elsewhere is refused at its call.
#[tokio::test]
async fn changes_to_one_member_chain_within_a_transaction() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a"), item!(2, Some(2), b"b")]).await;

    let mut trx = tree.transaction();
    trx.update(item!(1, Some(1), b"a"), item!(1, Some(7), b"first")).unwrap();
    trx.update(item!(1, Some(7), b"first"), item!(1, Some(8), b"last")).unwrap();
    trx.remove(item!(2, Some(2), b"b")).unwrap();
    trx.insert(item!(3, Some(3), b"gone")).unwrap();
    trx.remove(item!(3, Some(3), b"gone")).unwrap();

    assert!(matches!(trx.update(item!(1, Some(1), b"a"), item!(1, Some(9), b"")), Err(Error::NotHeld { .. })), "member 1 moved on");
    assert!(matches!(trx.insert(item!(1, Some(8), b"last")), Err(Error::AlreadyHeld { .. })), "member 1 is held");

    trx.commit().await.unwrap();
    assert_same_tree(&tree, &built(integer_spec(), [item!(1, Some(8), b"last")]).await).await;
}

#[tokio::test]
async fn an_update_keeps_its_members_id() {
    let tree = built::<Ristretto255>(integer_spec(), [item!(1, Some(1), b"a")]).await;
    let mut trx = tree.transaction();

    match trx.update(item!(1, Some(1), b"a"), item!(2, Some(1), b"a")) {
        Err(Error::IdChanged { old, new }) => assert_eq!((old, new), (Id(1).to_bytes().into_owned(), Id(2).to_bytes().into_owned())),
        other => panic!("expected another member's id refused, got {other:?}"),
    }
}

/// A member that can't make its key fails at its own call, named, with the
/// member's reason; the transaction keeps the rest.
#[tokio::test]
async fn a_member_without_a_key_is_refused_at_its_change() {
    let key_spec = KeySpec::new(vec![asc!("a"), asc!("b")]);
    let tree = built::<Ristretto255>(key_spec.clone(), [Item::new(1, [Some(1), Some(1)], b"a")]).await;

    let mut trx = tree.transaction();
    trx.insert(Item::new(2, [Some(2), None], b"b")).unwrap();

    match trx.insert(Item::new(3, [Some(3)], b"c")) {
        Err(Error::Key { member, source }) => {
            assert_eq!((member, source.to_string()), (Id(3).to_bytes().into_owned(), "no value for key part 1".into()))
        }
        other => panic!("expected a member without a key, got {other:?}"),
    }

    trx.commit().await.unwrap();
    assert_same_tree(&tree, &built(key_spec, [Item::new(1, [Some(1), Some(1)], b"a"), Item::new(2, [Some(2), None], b"b")]).await).await;
}

/// The error the commit gives for a transaction that inserts a new member
/// and makes one more change, `change`, to a tree holding member 1 at key 1
/// with payload "a", once the commit is checked to have written nothing.
async fn refusal(change: impl FnOnce(&mut CountedTransaction) -> Result<(), Error>) -> Error {
    let (tree, store) = counted(integer_spec(), [item!(1, Some(1), b"a")]).await;
    let (before, writes) = (stored_rows(&store).await, store.writes());

    let mut trx = tree.transaction();
    trx.insert(item!(5, Some(5), b"new")).unwrap();
    change(&mut trx).unwrap();
    let error = trx.commit().await.unwrap_err();

    assert_eq!((stored_rows(&store).await, store.writes()), (before, writes), "the refused transaction wrote nothing");
    error
}

/// A change naming a version the tree doesn't hold means the tree and its
/// owner disagree, so its commit fails, names the member, and writes none of
/// the transaction.
#[tokio::test]
async fn a_commit_refuses_a_change_naming_a_version_the_tree_doesnt_hold() {
    let stale = refusal(|trx| trx.update(item!(1, Some(1), b"stale"), item!(1, Some(1), b"new"))).await;
    let elsewhere = refusal(|trx| trx.update(item!(1, Some(9), b"a"), item!(1, Some(1), b"new"))).await;
    let absent = refusal(|trx| trx.remove(item!(9, Some(9), b""))).await;

    for (error, id) in [(stale, 1), (elsewhere, 1), (absent, 9)] {
        assert!(matches!(&error, Error::NotHeld { member } if *member == Id(id).to_bytes().into_owned()), "{error:?}");
    }
}

#[tokio::test]
async fn a_commit_refuses_an_insert_of_a_member_the_tree_holds() {
    let error = refusal(|trx| trx.insert(item!(1, Some(1), b"a"))).await;
    assert!(matches!(&error, Error::AlreadyHeld { member } if *member == Id(1).to_bytes().into_owned()), "{error:?}");
}

/// An update that moves a member to a key the tree already holds it under,
/// as after an insert under a second key went unnoticed, is refused as an
/// insert into that slot is, and writes nothing.
#[tokio::test]
async fn a_commit_refuses_a_move_to_a_key_the_tree_holds_the_member_under() {
    let (tree, store) = counted(integer_spec(), [item!(1, Some(1), b"a")]).await;

    // The owner's mistake an insert can't see: the member again, under a
    // second key.
    let mut trx = tree.transaction();
    trx.insert(item!(1, Some(5), b"a")).unwrap();
    trx.commit().await.unwrap();

    let (before, writes) = (stored_rows(&store).await, store.writes());
    let mut trx = tree.transaction();
    trx.update(item!(1, Some(1), b"a"), item!(1, Some(5), b"a")).unwrap();

    let error = trx.commit().await.unwrap_err();
    assert!(matches!(&error, Error::AlreadyHeld { member } if *member == Id(1).to_bytes().into_owned()), "{error:?}");
    assert_eq!((stored_rows(&store).await, store.writes()), (before, writes), "the refused transaction wrote nothing");
}

/// A transaction large enough to lay a tree out anew, had it changed
/// anything, writes nothing when its changes sum to none: every member it
/// inserts it removes again.
#[tokio::test]
async fn a_large_transaction_whose_changes_sum_to_none_writes_nothing() {
    let (tree, store) = counted(integer_spec(), (0..100).map(|n| item!(n, Some(0), b""))).await;
    let (before, writes) = (stored_rows(&store).await, store.writes());

    let mut trx = tree.transaction();
    for n in 1000..1200 {
        trx.insert(item!(n, Some(1), b"")).unwrap();
        trx.remove(item!(n, Some(1), b"")).unwrap();
    }

    trx.commit().await.unwrap();
    assert_eq!((stored_rows(&store).await, store.writes()), (before, writes));
}

#[tokio::test]
async fn an_empty_or_dropped_transaction_writes_nothing() {
    let (tree, store) = counted(integer_spec(), [item!(1, Some(1), b"a")]).await;
    let writes = store.writes();

    tree.transaction().commit().await.unwrap();

    let mut dropped = tree.transaction();
    dropped.insert(item!(2, Some(2), b"b")).unwrap();
    drop(dropped);

    assert_eq!((store.writes(), tree.view().len().await.unwrap()), (writes, 1));
}

/// Each operation that changes a tree writes its rows in one batch, which the
/// store makes all or nothing: a build, a create, and a commit alike.
#[tokio::test]
async fn every_change_to_a_tree_is_one_write() {
    let (tree, store) = counted(integer_spec(), (0..300).map(|n| item!(n, Some((n % 5) as i64), b""))).await;
    assert_eq!(store.writes(), 1, "a build");

    let mut trx = tree.transaction();
    for n in 0..200 {
        trx.update(item!(n, Some((n % 5) as i64), b""), item!(n, Some(9), b"moved")).unwrap();
    }

    trx.commit().await.unwrap();
    assert_eq!(store.writes(), 2, "a commit that lays the tree out anew");

    let mut trx = tree.transaction();
    trx.update(item!(7, Some(9), b"moved"), item!(7, Some(1), b"")).unwrap();
    trx.remove(item!(8, Some(9), b"moved")).unwrap();
    trx.commit().await.unwrap();
    assert_eq!(store.writes(), 3, "a commit node by node");

    let created = CountingStore::new();
    CountedTree::create(created.clone(), integer_spec()).await.unwrap();

    assert_eq!(created.writes(), 1, "a create");
}

/// A commit whose batch its store fails to write hands back the store's
/// failure and changes nothing, so the same changes commit once the store
/// writes again.
#[tokio::test]
async fn a_commit_its_store_fails_to_write_changes_nothing() {
    let store = FailingStore::new();
    let tree = FailingTree::build(store.clone(), integer_spec(), ten_items()).await.unwrap();
    let before = stored_rows(&store).await;

    store.fail_writes(true);

    match remove_add_and_move(&tree).commit().await {
        Err(Error::Store(failure)) => assert!(failure.is_retryable()),
        other => panic!("expected the store's failure, got {other:?}"),
    }

    assert_eq!(stored_rows(&store).await, before, "the failed commit changed no row");

    store.fail_writes(false);
    remove_add_and_move(&tree).commit().await.unwrap();

    let (_, expected) = counted(integer_spec(), removed_added_and_moved()).await;
    assert_eq!(stored_rows(&store).await, stored_rows(&expected).await);
}

/// Ids whose bytes begin one another, under one key, are held apart like any
/// others: an address ends its id, so no address begins another, and a 65th
/// member splits a bucket where their addresses first differ.
#[tokio::test]
async fn ids_that_begin_one_another_are_held_apart() {
    // "a" begins every other name, and is the first address of the bucket
    // the 65th member splits.
    let names: Vec<Named> =
        std::iter::once(b"a".to_vec()).chain((0..64u8).map(|n| vec![b'a', n])).map(|name| Named(name, Tag(Vec::new()))).collect();

    let built = NamedTree::build(MemoryStore::new(), KeySpec::new(vec![]), names.clone()).await.unwrap();
    let changed = NamedTree::build(MemoryStore::new(), KeySpec::new(vec![]), names[1..].to_vec()).await.unwrap();

    let mut trx = changed.transaction();
    trx.insert(names[0].clone()).unwrap();
    trx.commit().await.unwrap();

    for tree in [&built, &changed] {
        assert_eq!(tree.view().len().await.unwrap(), 65);
        assert_eq!(tree.audit().await.unwrap().nodes, 3, "a root and the two sides of its split");
    }
}
