//! A reset of the engine on one node while another node's storage
//! transaction is open with a tree's lock: the reset waits for that
//! transaction to end, and then retires every tree.

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{cause, created, item, stores};
use ankurah::core::sync::{Error, TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use tokio::time::timeout;

use crate::database::Database;
use crate::Tree;

/// A reset on one node waits at the database for another node's open map
/// transaction, which then commits and lands; the reset then empties the
/// maps, and the tree is gone for both nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_waits_for_another_nodes_open_map_transaction_and_then_retires_its_tree() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let (here, there) = (database.node().await?, database.node().await?);
    let (here_trees, there_trees) = (stores(&here), stores(&there));
    let tree: Tree = created(&here_trees, &here, "albums", [item!(1, 10)]).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut open = here.transaction();
    let applied = here_trees.apply(&mut open, vec![TreeChange::Changes(changes)]).await?;

    let resetting = tokio::spawn({
        let there = there.clone();
        async move { there.delete_all().await }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    let committed = open.commit().await?.committed()?;
    applied.landed(&committed);
    timeout(HANG, resetting).await???;

    let read = tree.view().count(..).await;
    let opened: Result<Tree, _> = there_trees.open(&TreeId::new("albums")).await;

    assert!(matches!(cause(&read), Some(Error::Retired(_))), "the tree is retired");
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))), "the tree no longer opens");
    Ok(())
}
