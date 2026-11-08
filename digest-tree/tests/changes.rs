//! Transactions: each kind of change, how changes to one member chain, the
//! checks a commit makes, and what a commit writes.

mod common;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;
use ankurah_digest_tree::{Error, KeyRange, MemberId, MemoryStore, Ristretto255, Transaction};
use common::{
    asc, assert_same_rows, counted, counted_store, failing_store, item, leaf_at, no_items, remove_add_and_move, removed_added_and_moved,
    stored_rows, ten_items, tree_of, CountedStore, CountedTree, FailingTree, Id, ItemTree, Named, NamedTree, Property, Tag,
};
use futures::TryStreamExt;

/// A transaction on a tree over a store that counts its writes.
type CountedTransaction = Transaction<Property, Id, Tag, Ristretto255, CountedStore>;

#[tokio::test]
async fn an_insert_files_a_new_member() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a")]);

    let mut trx = tree.transaction();
    trx.insert(item!(2, 5, b"b")).unwrap();
    trx.commit().await.unwrap();

    let leaf = leaf_at(&tree.view(), [5i64], &Id(2)).await.unwrap();
    assert_eq!((leaf.id, leaf.payload), (Id(2), Tag(b"b".to_vec())));
    assert_same_rows(&tree, &tree_of!(spec, [item!(1, 1, b"a"), item!(2, 5, b"b")])).await;
}

#[tokio::test]
async fn an_update_under_the_same_key_replaces_the_payload() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a"), item!(2, 2, b"b")]);

    let mut trx = tree.transaction();
    trx.update(item!(2, 2, b"b"), item!(2, 2, b"changed")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(leaf_at(&tree.view(), [2i64], &Id(2)).await.unwrap().payload, Tag(b"changed".to_vec()));
    assert_same_rows(&tree, &tree_of!(spec, [item!(1, 1, b"a"), item!(2, 2, b"changed")])).await;
}

#[tokio::test]
async fn an_update_under_another_key_moves_the_member() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a"), item!(2, 2, b"b")]);

    let mut trx = tree.transaction();
    trx.update(item!(2, 2, b"b"), item!(2, None, b"b")).unwrap();
    trx.commit().await.unwrap();

    let view = tree.view();
    let missing = tree.span_for(KeyRange::new(vec![None], None)).unwrap();
    let filed: Vec<Id> = view.range(missing).unwrap().map_ok(|leaf| leaf.id).try_collect().await.unwrap();

    assert_eq!(leaf_at(&view, [2i64], &Id(2)).await, None, "gone from its old key");
    assert_eq!(filed, [Id(2)], "filed under a missing value");
    assert_eq!(view.count(..).await.unwrap(), 2);
    assert_same_rows(&tree, &tree_of!(spec, [item!(1, 1, b"a"), item!(2, None, b"b")])).await;
}

#[tokio::test]
async fn a_removal_takes_the_member_out() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a"), item!(2, 2, b"b")]);

    let mut trx = tree.transaction();
    trx.remove(item!(1, 1, b"a")).unwrap();
    trx.commit().await.unwrap();

    assert_eq!(leaf_at(&tree.view(), [1i64], &Id(1)).await, None);
    assert_same_rows(&tree, &tree_of!(spec, [item!(2, 2, b"b")])).await;
}

/// A leaf read from the tree is the version the tree holds, so a reader can
/// name it in a change, as a peer repairing its tree from another's does.
#[tokio::test]
async fn a_leaf_read_from_the_tree_names_the_version_a_change_replaces() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a"), item!(2, 2, b"b")]);
    let leaves: Vec<_> = tree.view().range(..).unwrap().try_collect().await.unwrap();

    let mut trx = tree.transaction();
    trx.update_leaf(leaves[0].clone(), item!(1, 1, b"changed")).unwrap();
    trx.remove_leaf(leaves[1].clone()).unwrap();
    trx.commit().await.unwrap();

    assert_same_rows(&tree, &tree_of!(spec, [item!(1, 1, b"changed")])).await;
}

/// Each change to one member starts from the version the one before it left,
/// so a member's changes in a transaction sum to one, and one that starts
/// elsewhere is refused at its call.
#[tokio::test]
async fn changes_to_one_member_chain_within_a_transaction() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, 1, b"a"), item!(2, 2, b"b")]);

    let mut trx = tree.transaction();
    trx.update(item!(1, 1, b"a"), item!(1, 7, b"first")).unwrap();
    trx.update(item!(1, 7, b"first"), item!(1, 8, b"last")).unwrap();
    trx.remove(item!(2, 2, b"b")).unwrap();
    trx.insert(item!(3, 3, b"gone")).unwrap();
    trx.remove(item!(3, 3, b"gone")).unwrap();

    assert!(matches!(trx.update(item!(1, 1, b"a"), item!(1, 9, b"")), Err(Error::NotHeld { .. })), "member 1 moved on");
    assert!(matches!(trx.insert(item!(1, 8, b"last")), Err(Error::AlreadyHeld { .. })), "member 1 is held");

    trx.commit().await.unwrap();
    assert_same_rows(&tree, &tree_of!(spec, [item!(1, 8, b"last")])).await;
}

#[tokio::test]
async fn an_update_keeps_its_members_id() {
    let tree: ItemTree = tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 1, b"a")]);
    let mut trx = tree.transaction();

    match trx.update(item!(1, 1, b"a"), item!(2, 1, b"a")) {
        Err(Error::IdChanged { old, new }) => assert_eq!((old, new), (Id(1).to_bytes().into_owned(), Id(2).to_bytes().into_owned())),
        other => panic!("expected another member's id refused, got {other:?}"),
    }
}

/// A member the tree can't make a key for, as one without a property a key
/// part names, fails at its own call, named, with the member's reason; the
/// transaction keeps the rest.
#[tokio::test]
async fn a_member_without_a_key_is_refused_at_its_change() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64), asc!(Property::B, ValueType::I64)]);
    let tree: ItemTree = tree_of!(spec.clone(), [item!(1, [1, 1], b"a")]);

    let mut trx = tree.transaction();
    trx.insert(item!(2, [Some(2), None], b"b")).unwrap();

    match trx.insert(item!(3, [3], b"c")) {
        Err(Error::Key { member, source }) => {
            assert_eq!((member, source.to_string()), (Id(3).to_bytes().into_owned(), "no property B".into()))
        }
        other => panic!("expected a member without a key, got {other:?}"),
    }

    trx.commit().await.unwrap();
    assert_same_rows(&tree, &tree_of!(spec, [item!(1, [1, 1], b"a"), item!(2, [Some(2), None], b"b")])).await;
}

/// The error the commit gives for a transaction that inserts a new member
/// and makes one more change, `change`, to a tree holding member 1 at key 1
/// with payload "a", once the commit is checked to have written nothing.
async fn refusal(change: impl FnOnce(&mut CountedTransaction) -> Result<(), Error>) -> Error {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 1, b"a")]).await;
    let (before, writes) = (stored_rows(tree.store()).await, counts.writes());

    let mut trx = tree.transaction();
    trx.insert(item!(5, 5, b"new")).unwrap();
    change(&mut trx).unwrap();
    let error = trx.commit().await.unwrap_err();

    assert_eq!((stored_rows(tree.store()).await, counts.writes()), (before, writes), "the refused transaction wrote nothing");
    error
}

/// A change naming a version the tree doesn't hold means the tree and its
/// owner disagree, so its commit fails, names the member, and writes none of
/// the transaction.
#[tokio::test]
async fn a_commit_refuses_a_change_naming_a_version_the_tree_doesnt_hold() {
    let stale = refusal(|trx| trx.update(item!(1, 1, b"stale"), item!(1, 1, b"new"))).await;
    let elsewhere = refusal(|trx| trx.update(item!(1, 9, b"a"), item!(1, 1, b"new"))).await;
    let absent = refusal(|trx| trx.remove(item!(9, 9, b""))).await;

    for (error, id) in [(stale, 1), (elsewhere, 1), (absent, 9)] {
        assert!(matches!(&error, Error::NotHeld { member } if *member == Id(id).to_bytes().into_owned()), "{error:?}");
    }
}

/// An insert of a member the tree holds is refused, whatever the transaction
/// does with the member after it: moves it to another key, or removes it
/// again.
#[tokio::test]
async fn a_commit_refuses_an_insert_of_a_member_the_tree_holds() {
    let alone = refusal(|trx| trx.insert(item!(1, 1, b"a"))).await;
    let moved = refusal(|trx| {
        trx.insert(item!(1, 1, b"a"))?;
        trx.update(item!(1, 1, b"a"), item!(1, 2, b"a"))
    })
    .await;
    let removed = refusal(|trx| {
        trx.insert(item!(1, 1, b"a"))?;
        trx.remove(item!(1, 1, b"a"))
    })
    .await;

    for error in [alone, moved, removed] {
        assert!(matches!(&error, Error::AlreadyHeld { member } if *member == Id(1).to_bytes().into_owned()), "{error:?}");
    }
}

/// An update that moves a member to a key the tree already holds it under,
/// as after an insert under a second key went unnoticed, is refused as an
/// insert into that slot is, even when a later change moves the member on,
/// and writes nothing.
#[tokio::test]
async fn a_commit_refuses_a_move_to_a_key_the_tree_holds_the_member_under() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 1, b"a")]).await;

    // The owner's mistake an insert can't see: the member again, under a
    // second key.
    let mut trx = tree.transaction();
    trx.insert(item!(1, 5, b"a")).unwrap();
    trx.commit().await.unwrap();
    let (before, writes) = (stored_rows(tree.store()).await, counts.writes());

    for moves_on in [false, true] {
        let mut trx = tree.transaction();
        trx.update(item!(1, 1, b"a"), item!(1, 5, b"a")).unwrap();
        if moves_on {
            trx.update(item!(1, 5, b"a"), item!(1, 6, b"a")).unwrap();
        }

        let error = trx.commit().await.unwrap_err();
        assert!(matches!(&error, Error::AlreadyHeld { member } if *member == Id(1).to_bytes().into_owned()), "{error:?}");
        assert_eq!((stored_rows(tree.store()).await, counts.writes()), (before.clone(), writes), "the refused transaction wrote nothing");
    }
}

/// A transaction large enough to lay a tree out anew, had it changed
/// anything, writes nothing when its changes sum to none: every member it
/// inserts it removes again.
#[tokio::test]
async fn a_large_transaction_whose_changes_sum_to_none_writes_nothing() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), (0..100).map(|n| item!(n, 0, b""))).await;
    let (before, writes) = (stored_rows(tree.store()).await, counts.writes());

    let mut trx = tree.transaction();
    for n in 1000..1200 {
        trx.insert(item!(n, 1, b"")).unwrap();
        trx.remove(item!(n, 1, b"")).unwrap();
    }

    trx.commit().await.unwrap();
    assert_eq!((stored_rows(tree.store()).await, counts.writes()), (before, writes));
}

#[tokio::test]
async fn an_empty_or_dropped_transaction_writes_nothing() {
    let (tree, counts) = counted(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 1, b"a")]).await;
    let writes = counts.writes();

    tree.transaction().commit().await.unwrap();

    let mut dropped = tree.transaction();
    dropped.insert(item!(2, 2, b"b")).unwrap();
    drop(dropped);

    assert_eq!((counts.writes(), tree.view().count(..).await.unwrap()), (writes, 1));
}

/// Each operation that changes a tree writes its rows in one batch, which the
/// store makes all or nothing: a creation and a commit alike.
#[tokio::test]
async fn every_change_to_a_tree_is_one_write() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let (tree, counts) = counted(spec.clone(), (0..300).map(|n| item!(n, (n % 5) as i64, b""))).await;
    assert_eq!(counts.writes(), 1, "a creation");

    let mut trx = tree.transaction();
    for n in 0..200 {
        trx.update(item!(n, (n % 5) as i64, b""), item!(n, 9, b"moved")).unwrap();
    }

    trx.commit().await.unwrap();
    assert_eq!(counts.writes(), 2, "a commit that lays the tree out anew");

    let mut trx = tree.transaction();
    trx.update(item!(7, 9, b"moved"), item!(7, 1, b"")).unwrap();
    trx.remove(item!(8, 9, b"moved")).unwrap();
    trx.commit().await.unwrap();
    assert_eq!(counts.writes(), 3, "a commit node by node");

    let (created, counts) = counted_store();
    CountedTree::create(created, spec, no_items!()).await.unwrap();

    assert_eq!(counts.writes(), 1, "a creation of no members");
}

/// A commit whose batch its store fails to write hands back the store's
/// failure and changes nothing, so the same changes commit once the store
/// writes again.
#[tokio::test]
async fn a_commit_its_store_fails_to_write_changes_nothing() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64)]);
    let (store, faults) = failing_store();
    let tree = FailingTree::create(store, spec.clone(), ten_items()).await.unwrap();
    let before = stored_rows(tree.store()).await;

    faults.fail_writes(true);

    match remove_add_and_move(&tree).commit().await {
        Err(Error::Store(failure)) => assert!(failure.is_retryable()),
        other => panic!("expected the store's failure, got {other:?}"),
    }

    assert_eq!(stored_rows(tree.store()).await, before, "the failed commit changed no row");

    faults.fail_writes(false);
    remove_add_and_move(&tree).commit().await.unwrap();

    let (expected, _) = counted(spec, removed_added_and_moved()).await;
    assert_eq!(stored_rows(tree.store()).await, stored_rows(expected.store()).await);
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

    let created = NamedTree::create(MemoryStore::new(), KeySpec::new(vec![]), names.clone()).await.unwrap();
    let changed = NamedTree::create(MemoryStore::new(), KeySpec::new(vec![]), names[1..].to_vec()).await.unwrap();

    let mut trx = changed.transaction();
    trx.insert(names[0].clone()).unwrap();
    trx.commit().await.unwrap();

    for tree in [&created, &changed] {
        assert_eq!(tree.view().count(..).await.unwrap(), 65);
        assert_eq!(tree.audit().await.unwrap().nodes, 3, "a root and the two sides of its split");
    }
}
