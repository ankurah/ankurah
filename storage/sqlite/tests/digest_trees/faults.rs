//! Storage transactions that fail, or give up, while holding or waiting for
//! the database's write lock: each lands nothing, and none leaves the lock
//! behind for the next. The test of SQLite's own busy timeout sets it to its
//! smallest, a millisecond, on a node whose setup is done before another node
//! takes the lock the test holds, which decides what the timeout meets.

use std::time::Duration;

use ankurah::core::error::MutationError;
use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, commit, created, item, stores};
use ankurah::core::sync::{TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::support::{gave_up_on_the_write_lock, refusal, Busy, Database, Tree};

/// An apply that waits for the write lock past its node's busy timeout,
/// here SQLite's smallest, while another node's storage transaction holds
/// the lock with a change to another tree, fails with SQLite's busy error and
/// lands nothing: the write lock is the database's, not a tree's. Once the
/// holder has committed, the impatient node commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_that_waits_past_its_nodes_busy_timeout_fails_and_lands_nothing() -> anyhow::Result<()> {
    let database = Database::start()?;
    let holder = database.node().await?;
    let impatient = database.node_with(Busy::Timeout(Duration::from_millis(1))).await?;
    let (holder_trees, impatient_trees) = (stores(&holder), stores(&impatient));
    let albums: Tree = created(&holder_trees, &holder, "albums", [item!(1, 10)]).await?;
    let songs: Tree = created(&impatient_trees, &impatient, "songs", [item!(1, 10)]).await?;

    let mut changes = albums.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = holder.transaction();
    let applied = holder_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let mut waiting = songs.transaction();
    waiting.insert(item!(3, 30))?;
    let mut trx = impatient.transaction();
    let failed = impatient_trees.apply(&mut trx, vec![TreeChange::Changes(waiting)]).await.map(drop);
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);
    commit(&impatient_trees, &impatient, &songs, |changes| changes.insert(item!(3, 30))).await?;

    assert!(failed.as_ref().is_err_and(gave_up_on_the_write_lock), "{failed:?}");
    assert_holds(&albums, &[item!(1, 10), item!(2, 20)]).await?;
    assert_holds(&songs, &[item!(1, 10), item!(3, 30)]).await
}

/// A storage transaction dropped while its apply waits at the database for
/// the write lock lands nothing, and leaves no lock behind: the abandoned
/// call takes the lock once the holder ends, and its transaction rolls back
/// as soon as the call returns, so the nodes after it commit, the abandoning
/// node among them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_dropped_while_it_waits_for_the_write_lock_leaves_no_lock_behind() -> anyhow::Result<()> {
    static WAITING: Notify = Notify::const_new();

    /// The abandoning node's busy handler: it tells the test the node waits,
    /// and tries again a moment later, for as long as it takes. The moment
    /// paces the tries, and decides nothing.
    fn waiting(_tries: i32) -> bool {
        WAITING.notify_one();
        std::thread::sleep(Duration::from_millis(1));
        true
    }

    let database = Database::start()?;
    let (holder, gave_up, last) = (database.node().await?, database.node_with(Busy::Handler(waiting)).await?, database.node().await?);
    let (holder_trees, gave_up_trees, last_trees) = (stores(&holder), stores(&gave_up), stores(&last));
    let tree: Tree = created(&holder_trees, &holder, "albums", [item!(1, 10)]).await?;
    let (abandoned, after): (Tree, Tree) =
        (gave_up_trees.open(&TreeId::new("albums")).await?, last_trees.open(&TreeId::new("albums")).await?);

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = holder.transaction();
    let applied = holder_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let abandoning = tokio::spawn({
        let (gave_up, gave_up_trees, abandoned) = (gave_up.clone(), gave_up_trees.clone(), abandoned.clone());
        async move { commit(&gave_up_trees, &gave_up, &abandoned, |changes| changes.insert(item!(3, 30))).await }
    });

    timeout(HANG, WAITING.notified()).await?;
    abandoning.abort();

    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);
    timeout(HANG, commit(&last_trees, &last, &after, |changes| changes.insert(item!(4, 40)))).await??;
    timeout(HANG, commit(&gave_up_trees, &gave_up, &abandoned, |changes| changes.insert(item!(3, 30)))).await??;

    assert_holds(&after, &[item!(1, 10), item!(2, 20), item!(3, 30), item!(4, 40)]).await
}

/// A storage transaction dropped once its apply holds the write lock lands
/// nothing, and its transaction rolls back, freeing the lock for another
/// node's commit, and then for its own node's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_storage_transaction_dropped_after_its_apply_frees_the_write_lock() -> anyhow::Result<()> {
    let database = Database::start()?;
    let (dropping, other) = (database.node().await?, database.node().await?);
    let (dropping_trees, other_trees) = (stores(&dropping), stores(&other));
    let tree: Tree = created(&dropping_trees, &dropping, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = other_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut trx = dropping.transaction();
    let applied = dropping_trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    drop((applied, trx));

    timeout(HANG, commit(&other_trees, &other, &elsewhere, |changes| changes.insert(item!(3, 30)))).await??;
    timeout(HANG, commit(&dropping_trees, &dropping, &tree, |changes| changes.insert(item!(4, 40)))).await??;

    assert_holds(&tree, &[item!(1, 10), item!(3, 30), item!(4, 40)]).await
}

/// A write SQLite refuses, here by a trigger on the maps' table, fails the
/// apply with SQLite's own error, leaves the storage transaction unable to
/// commit, and lands nothing; once SQLite takes the write again, it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_sqlite_refuses_fails_the_apply_with_its_own_error_and_lands_nothing() -> anyhow::Result<()> {
    let database = Database::start()?;
    let engine = database.node().await?;
    let trees = stores(&engine);
    let tree: Tree = created(&trees, &engine, "albums", [item!(1, 10)]).await?;

    let inspector = database.inspector()?;
    inspector.execute_batch(
        r#"CREATE TRIGGER refuse_map_writes BEFORE INSERT ON "_ankurah_map"
           BEGIN SELECT RAISE(ABORT, 'map writes refused'); END;"#,
    )?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut trx = engine.transaction();
    let refused = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await.map(drop);
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    let code = refused.as_ref().err().and_then(refusal).and_then(rusqlite::Error::sqlite_error).map(|refusal| refusal.extended_code);
    assert_eq!(code, Some(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER), "{refused:?}");
    assert_holds(&tree, &[item!(1, 10)]).await?;

    inspector.execute_batch("DROP TRIGGER refuse_map_writes")?;
    commit(&trees, &engine, &tree, |changes| changes.insert(item!(2, 20))).await?;
    assert_holds(&tree, &[item!(1, 10), item!(2, 20)]).await
}
