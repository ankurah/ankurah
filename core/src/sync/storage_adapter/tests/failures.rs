//! Changes through the storage adapter that fail: the storage transaction is left unable
//! to commit, and an engine's own error leaves as itself.

use std::pin::pin;
use std::sync::Arc;

use ankurah_digest_tree::DigestGroup;

use super::{pause, Tree};
use crate::error::{MutationError, RetrievalError};
use crate::storage::{StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{assert_holds, by_number, cause, created, item, poll_once, stores, Id};
use crate::sync::conformance::in_both_groups;
use crate::sync::{Error, TreeChange, TreeId};
use crate::test_utils::TestStorage;

/// A storage engine's write conflict, met by a read the crate makes in an
/// apply, leaves the apply as a write conflict, for core to retry: here, a
/// reset of the engine lands while the apply is under way.
async fn a_write_conflict_met_inside_the_crate_leaves_an_apply_as_one<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 2))?;

    // The apply's own request for locks and reads is its first map call, and
    // the crate's first read of the tree's rows its second.
    let (hold, mut entered, release) = pause!();
    *engine.hold_map_call.lock().unwrap() = Some((1, hold));

    let mut trx = engine.transaction();
    let mut applying = pin!(trees.apply(&mut trx, vec![TreeChange::Changes(changes)]));
    assert!(poll_once(applying.as_mut()).is_pending());
    entered.try_recv()?;

    engine.delete_all().await?;
    release.send(()).unwrap();

    assert!(matches!(applying.await, Err(MutationError::WriteConflict)));
    Ok(())
}

/// A write conflict met by the crate's read in a creation leaves the creation
/// as one.
async fn a_write_conflict_met_inside_the_crate_leaves_a_creation_as_one<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);

    // The creation's request for the lock and the entry is its first map
    // call, and the crate's check that the tree's rows are empty its second.
    let (hold, mut entered, release) = pause!();
    *engine.hold_map_call.lock().unwrap() = Some((1, hold));

    let mut trx = engine.transaction();
    let mut creating = pin!(trees.create::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), by_number(), [item!(1, 1)]));
    assert!(poll_once(creating.as_mut()).is_pending());
    entered.try_recv()?;

    engine.delete_all().await?;
    release.send(()).unwrap();

    assert!(matches!(creating.await, Err(MutationError::WriteConflict)));
    Ok(())
}

/// A tree's write that the engine fails leaves the apply with the engine's
/// own error, not the crate's report of it.
async fn a_tree_write_the_engine_fails_leaves_an_apply_with_the_engines_error<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 2))?;

    let mut trx = engine.transaction();
    engine.fail_next_map_write();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await.map(|_| ());

    assert!(matches!(&applied, Err(MutationError::General(error)) if error.to_string() == "test storage map write failed"), "{applied:?}");
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    Ok(())
}

/// A snapshot that fails retryably, as an engine's does when the state it
/// reads moved, fails the tree's read retryably, and the opening of a tree
/// with the engine's own error.
async fn a_snapshot_that_fails_retryably_fails_the_read_retryably<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    engine.fail_next_snapshot_read();
    let read = tree.view().count(..).await;
    assert!(matches!(&read, Err(ankurah_digest_tree::Error::Store(failure)) if failure.is_retryable()), "{read:?}");

    engine.fail_next_snapshot_read();
    let opened: Result<Tree<G>, _> = trees.open(&TreeId::new("tree")).await;
    let opened = opened.map(|_| ());
    assert!(matches!(&opened, Err(RetrievalError::MutationError(error)) if matches!(**error, MutationError::WriteConflict)), "{opened:?}");

    assert_eq!(tree.view().count(..).await?, 1, "the next read begins another snapshot");
    Ok(())
}

/// An apply given a tree from another engine fails, naming the tree, and leaves its
/// storage transaction unable to commit.
async fn an_apply_given_another_engines_tree_fails<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let other_engine = Arc::new(TestStorage::default());
    let (here, there) = (stores(&engine), stores(&other_engine));
    let (ours, theirs): (Tree<G>, Tree<G>) =
        (created(&here, &engine, "ours", [item!(1, 1)]).await?, created(&there, &other_engine, "theirs", [item!(1, 1)]).await?);

    let (mut our_changes, mut their_changes) = (ours.transaction(), theirs.transaction());
    our_changes.insert(item!(2, 2))?;
    their_changes.insert(item!(2, 2))?;

    let mut trx = engine.transaction();
    let applied = here.apply(&mut trx, vec![TreeChange::Changes(our_changes), TreeChange::Changes(their_changes)]).await;
    assert!(matches!(cause(&applied), Some(Error::ForeignTree(id)) if id.as_str() == "theirs"));

    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert_holds(&ours, &[item!(1, 1)]).await?;
    assert_holds(&theirs, &[item!(1, 1)]).await
}

in_both_groups!(
    #[tokio::test]
    a_write_conflict_met_inside_the_crate_leaves_an_apply_as_one,
    a_write_conflict_met_inside_the_crate_leaves_a_creation_as_one,
    a_tree_write_the_engine_fails_leaves_an_apply_with_the_engines_error,
    a_snapshot_that_fails_retryably_fails_the_read_retryably,
    an_apply_given_another_engines_tree_fails,
);
