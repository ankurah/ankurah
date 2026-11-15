//! A reset of the engine while a storage transaction with a tree's lock and a
//! read of another tree are open: the reset waits for both to end, and then
//! retires every tree.

use std::pin::pin;

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{cause, created, item, stores};
use ankurah::core::sync::{Error, TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::{poll_once, HANG};
use ankurah_digest_tree::Store;
use tokio::time::timeout;

use crate::support::{Database, Tree};

/// A reset waits for an open storage transaction that applied changes to one
/// tree, which then commits, and for an open read of another tree, which
/// then ends; the reset then empties the maps, and both trees are gone.
#[tokio::test]
async fn a_reset_waits_for_an_open_map_transaction_and_an_open_read_and_then_retires_their_trees() -> anyhow::Result<()> {
    let database = Database::start()?;
    let engine = database.engine();
    let trees = stores(engine);
    let (changed, read): (Tree, Tree) =
        (created(&trees, engine, "changed", [item!(1, 10)]).await?, created(&trees, engine, "read", [item!(1, 10)]).await?);

    let mut changes = changed.transaction();
    changes.insert(item!(2, 20))?;
    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    let reading = read.store().begin_read().await?;

    let mut resetting = pin!(engine.delete_all());
    assert!(poll_once(resetting.as_mut()).is_pending(), "the reset waits for the open transaction and read");

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);
    assert!(poll_once(resetting.as_mut()).is_pending(), "the reset waits for the read still open");

    drop(reading);
    timeout(HANG, resetting).await??;

    for tree in [&changed, &read] {
        assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))), "a handle's read");
    }

    let opened: Result<Tree, _> = trees.open(&TreeId::new("changed")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))), "the tree no longer opens");
    Ok(())
}
