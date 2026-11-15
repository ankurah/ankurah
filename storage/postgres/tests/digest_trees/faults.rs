//! Storage transactions that stall or give up while holding or waiting for a
//! tree's lock: the database's lock timeout and idle limit end them, landing
//! nothing, and one dropped while it waits leaves the lock's queue at once.
//! The two tests of the database's own timers set each to its smallest, and
//! a lock the test holds decides what each one meets.

use std::time::Duration;

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, commit, created, item, stores};
use ankurah::core::sync::{TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use ankurah_storage_postgres::{MapError, MapSettings};
use tokio::time::timeout;

use crate::database::{map_error, Database};
use crate::Tree;

/// An apply that waits for a tree's lock past its node's lock timeout, here
/// PostgreSQL's smallest, while another node's transaction holds it, fails
/// with the database's lock timeout and lands nothing; once the holder has
/// committed, the impatient node commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_that_waits_past_its_nodes_lock_timeout_fails_and_lands_nothing() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let holder = database.node().await?;
    let impatient = database.node_with(MapSettings { lock_timeout: Duration::from_millis(1), ..MapSettings::default() }).await?;
    let (holder_trees, impatient_trees) = (stores(&holder), stores(&impatient));
    let tree: Tree = created(&holder_trees, &holder, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = impatient_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = holder.transaction();
    let applied = holder_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let mut waiting = elsewhere.transaction();
    waiting.insert(item!(3, 30))?;
    let mut trx = impatient.transaction();
    let failed = impatient_trees.apply(&mut trx, vec![TreeChange::Changes(waiting)]).await.map(drop);
    drop(trx);

    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);
    commit(&impatient_trees, &impatient, &elsewhere, |changes| changes.insert(item!(3, 30))).await?;

    assert!(matches!(failed.as_ref().map_err(map_error), Err(Some(MapError::LockTimeout(_)))), "{failed:?}");
    assert_holds(&elsewhere, &[item!(1, 10), item!(2, 20), item!(3, 30)]).await
}

/// A storage transaction that stalls holding a tree's lock, here a drop's,
/// loses its session once it has sat idle past its node's idle limit, here
/// PostgreSQL's smallest: the tree is free for another node's commit, and the
/// stalled commit fails, knowing it landed nothing.
///
/// The drop takes the lock in its one request and holds its writes for the
/// commit, so the stalled session's limit can end it only once its setup is
/// done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_left_idle_past_its_nodes_idle_limit_loses_its_session_and_frees_the_tree() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let other = database.node().await?;
    let stalled =
        database.node_with(MapSettings { idle_in_transaction_timeout: Duration::from_millis(1), ..MapSettings::default() }).await?;
    let other_trees = stores(&other);
    let tree: Tree = created(&other_trees, &other, "albums", [item!(1, 10)]).await?;

    let mut dropping = stalled.transaction();
    stores(&stalled).drop(&mut dropping, &TreeId::new("albums")).await?;

    database.sessions_idle_in_a_transaction(0).await?;
    commit(&other_trees, &other, &tree, |changes| changes.insert(item!(2, 20))).await?;
    let failed = dropping.commit().await.map(drop);

    assert!(matches!(failed.as_ref().map_err(map_error), Err(Some(MapError::SessionEnded))), "{failed:?}");
    assert_holds(&tree, &[item!(1, 10), item!(2, 20)]).await
}

/// A storage transaction dropped while its apply waits at the database for a
/// tree's lock cancels the wait at once, rather than wait its turn for
/// nothing, and leaves no lock behind for the nodes after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_dropped_while_it_waits_for_a_trees_lock_leaves_the_locks_queue_at_once() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let (holder, gave_up, last) = (database.node().await?, database.node().await?, database.node().await?);
    let (holder_trees, gave_up_trees, last_trees) = (stores(&holder), stores(&gave_up), stores(&last));
    let tree: Tree = created(&holder_trees, &holder, "albums", [item!(1, 10)]).await?;
    let (abandoned, after): (Tree, Tree) =
        (gave_up_trees.open(&TreeId::new("albums")).await?, last_trees.open(&TreeId::new("albums")).await?);

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = holder.transaction();
    let applied = holder_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let waiting = tokio::spawn({
        let (gave_up, gave_up_trees, abandoned) = (gave_up.clone(), gave_up_trees.clone(), abandoned.clone());
        async move { commit(&gave_up_trees, &gave_up, &abandoned, |changes| changes.insert(item!(3, 30))).await }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    waiting.abort();
    database.sessions_waiting_for_a_lock(0).await?;

    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);
    timeout(HANG, commit(&last_trees, &last, &after, |changes| changes.insert(item!(4, 40)))).await??;
    timeout(HANG, commit(&gave_up_trees, &gave_up, &abandoned, |changes| changes.insert(item!(3, 30)))).await??;

    assert_holds(&after, &[item!(1, 10), item!(2, 20), item!(3, 30), item!(4, 40)]).await
}
