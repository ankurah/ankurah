//! Several nodes on one database, each with connections and a record cache of
//! its own: writers wait their turn at a tree's lock, and each node's cache
//! follows the others' commits.

use std::sync::Arc;

use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, commit, created, item, stores, Item};
use ankurah::core::sync::{TreeChange, TreeId};
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use tokio::time::timeout;

use crate::database::Database;
use crate::Tree;

/// A node whose apply asks for a tree's lock while another node's storage
/// transaction holds it waits at the database until that transaction ends,
/// and then applies its changes to the tree as the other's commit left it:
/// here it removes the member the other inserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_node_waits_for_the_first_ones_lock_and_applies_on_top_of_its_commit() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let (first, second) = (database.node().await?, database.node().await?);
    let (first_trees, second_trees) = (stores(&first), stores(&second));
    let tree: Tree = created(&first_trees, &first, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = second_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut holding = first.transaction();
    let applied = first_trees.apply(&mut holding, vec![TreeChange::Changes(changes)]).await?;

    let waiting = tokio::spawn({
        let (second, elsewhere) = (Arc::clone(&second), elsewhere.clone());
        async move {
            let mut changes = elsewhere.transaction();
            changes.remove(item!(2, 20))?;
            changes.insert(item!(3, 30))?;

            let mut trx = second.transaction();
            let applied = second_trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
            let committed = trx.commit().await?.committed()?;
            anyhow::Ok((applied, committed))
        }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    let committed = holding.commit().await?.committed()?;
    applied.landed(&committed);

    let (second_applied, second_committed) = timeout(HANG, waiting).await???;
    assert!(second_applied.refused().is_empty(), "the second found the member the first inserted");
    second_applied.landed(&second_committed);

    assert_holds(&tree, &[item!(1, 10), item!(3, 30)]).await?;
    assert_holds(&elsewhere, &[item!(1, 10), item!(3, 30)]).await
}

/// Four nodes commit to one tree at once, each inserting members of its own:
/// each commit waits its turn at the tree's lock and lands, each node's cache
/// catching up from the others' commits, and every node's tree holds every
/// member.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn four_nodes_committing_to_one_tree_at_once_land_every_commit() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let creator = database.node().await?;
    let _: Tree = created(&stores(&creator), &creator, "shared", Vec::<Item>::new()).await?;

    let mut writers = Vec::new();
    for writer in 0..4u64 {
        let engine = database.node().await?;
        let trees = stores(&engine);
        let tree: Tree = trees.open(&TreeId::new("shared")).await?;

        writers.push(tokio::spawn(async move {
            for n in 0..25 {
                let id = writer * 100 + n;
                commit(&trees, &engine, &tree, |changes| changes.insert(item!(id, id as i64))).await?;
            }

            anyhow::Ok(tree)
        }));
    }

    let mut written = Vec::new();
    for writer in writers {
        written.push(timeout(HANG, writer).await???);
    }

    let members: Vec<Item> = (0..4u64).flat_map(|writer| (0..25).map(move |n| writer * 100 + n)).map(|id| item!(id, id as i64)).collect();
    for tree in &written {
        assert_holds(tree, &members).await?;
    }

    Ok(())
}
