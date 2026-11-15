//! Several tabs on one database, each with an engine of its own. A storage
//! transaction waits for none in another tab under one tree's lock, and
//! another tab's commit that lands first fails this tab's commit, or its next
//! read, as a conflict to retry.

use ankurah::core::error::MutationError;
use ankurah::core::storage::{StorageEngine, StorageTransaction};
use ankurah::core::sync::conformance::fixtures::{assert_holds, by_number, cause, commit, created, item, stores, Id, Item};
use ankurah::core::sync::{Error, TreeChange, TreeId};
use ankurah_digest_tree::Ristretto255;
use futures::future::try_join_all;
use wasm_bindgen_test::wasm_bindgen_test;

use crate::support::{Database, Tree};

/// Two tabs apply changes to one tree at once, neither waiting for the other:
/// the second to commit finds the tree's lock version moved by the first's
/// commit, fails as a conflict and lands nothing, and lands once applied
/// again.
#[wasm_bindgen_test]
async fn of_two_tabs_applying_to_one_tree_the_second_to_commit_conflicts_and_lands_once_applied_again() -> anyhow::Result<()> {
    let database = Database::new();
    let (first, second) = (database.tab().await?, database.tab().await?);
    let (first_trees, second_trees) = (stores(&first), stores(&second));
    let tree: Tree = created(&first_trees, &first, "albums", [item!(1, 10)]).await?;
    let elsewhere: Tree = second_trees.open(&TreeId::new("albums")).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 20))?;
    let mut first_trx = first.transaction();
    let first_applied = first_trees.apply(&mut first_trx, vec![TreeChange::Changes(changes)]).await?;

    let mut changes = elsewhere.transaction();
    changes.insert(item!(3, 30))?;
    let mut second_trx = second.transaction();
    let second_applied = second_trees.apply(&mut second_trx, vec![TreeChange::Changes(changes)]).await?;

    let committed = first_trx.commit().await?.committed()?;
    first_applied.landed(&committed);

    assert!(matches!(second_trx.commit().await, Err(MutationError::WriteConflict)));
    drop(second_applied);
    assert_holds(&elsewhere, &[item!(1, 10), item!(2, 20)]).await?;

    commit(&second_trees, &second, &elsewhere, |changes| changes.insert(item!(3, 30))).await?;
    assert_holds(&tree, &[item!(1, 10), item!(2, 20), item!(3, 30)]).await
}

/// Two tabs create one tree at once: both find no tree under its name, the
/// first's creation lands, and the second's commit fails as a conflict, so
/// the tree is the first's.
#[wasm_bindgen_test]
async fn of_two_tabs_creating_one_tree_at_once_the_second_to_commit_fails_as_a_conflict() -> anyhow::Result<()> {
    let database = Database::new();
    let (first, second) = (database.tab().await?, database.tab().await?);
    let (first_trees, second_trees) = (stores(&first), stores(&second));

    let mut first_trx = first.transaction();
    let creating =
        first_trees.create::<_, Id, _, Ristretto255, _>(&mut first_trx, TreeId::new("albums"), by_number(), [item!(1, 10)]).await?;

    let mut second_trx = second.transaction();
    let racing =
        second_trees.create::<_, Id, _, Ristretto255, _>(&mut second_trx, TreeId::new("albums"), by_number(), [item!(2, 20)]).await?;

    first_trx.commit().await?.committed()?;
    assert!(matches!(second_trx.commit().await, Err(MutationError::WriteConflict)));
    drop(racing);

    let opened: Tree = second_trees.open(&TreeId::new("albums")).await?;
    assert_holds(&creating.landed(), &[item!(1, 10)]).await?;
    assert_holds(&opened, &[item!(1, 10)]).await
}

/// A reset in one tab while another tab's creation of a tree waits to
/// commit: the creation noted the reset epoch before the reset moved it, so
/// its commit fails as a conflict, and no tree is left.
#[wasm_bindgen_test]
async fn a_reset_in_one_tab_fails_another_tabs_pending_creation_as_a_conflict() -> anyhow::Result<()> {
    let database = Database::new();
    let (here, there) = (database.tab().await?, database.tab().await?);
    let (here_trees, there_trees) = (stores(&here), stores(&there));

    let mut creating_trx = there.transaction();
    let creating =
        there_trees.create::<_, Id, _, Ristretto255, _>(&mut creating_trx, TreeId::new("albums"), by_number(), [item!(1, 10)]).await?;

    here.delete_all().await?;

    assert!(matches!(creating_trx.commit().await, Err(MutationError::WriteConflict)));
    drop(creating);

    let opened: Result<Tree, _> = here_trees.open(&TreeId::new("albums")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))));
    Ok(())
}

/// Another tab's commit to a tree lands while this tab's storage transaction,
/// which changed the tree, is still open: the transaction's next read finds
/// the tree's lock version moved, so its apply to another tree fails with
/// the engine's write conflict, and the transaction can't commit.
#[wasm_bindgen_test]
async fn another_tabs_commit_while_this_tabs_storage_transaction_is_open_fails_its_next_apply_as_a_conflict() -> anyhow::Result<()> {
    let database = Database::new();
    let (here, there) = (database.tab().await?, database.tab().await?);
    let (here_trees, there_trees) = (stores(&here), stores(&there));
    let first: Tree = created(&here_trees, &here, "first", [item!(1, 10)]).await?;
    let second: Tree = created(&here_trees, &here, "second", [item!(1, 10)]).await?;
    let elsewhere: Tree = there_trees.open(&TreeId::new("first")).await?;

    let mut changes = first.transaction();
    changes.insert(item!(2, 20))?;
    let mut trx = here.transaction();
    let applied = here_trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;

    commit(&there_trees, &there, &elsewhere, |changes| changes.insert(item!(3, 30))).await?;

    let mut changes = second.transaction();
    changes.insert(item!(2, 20))?;
    let next_apply = here_trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await;

    assert!(matches!(next_apply, Err(MutationError::WriteConflict)), "{:?}", next_apply.map(|_| ()));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    drop(applied);

    assert_holds(&first, &[item!(1, 10), item!(3, 30)]).await?;
    assert_holds(&second, &[item!(1, 10)]).await
}

/// Four tabs commit to one tree at once, each inserting ten members of its
/// own: a commit that finds another tab's landed first fails as a conflict
/// and is applied again, until every commit lands, and every tab's tree holds
/// every member. A tab's commit fails only for another tab's that landed
/// while it was applied, so the other tabs' thirty commits fail each tab's
/// at most thirty times.
#[wasm_bindgen_test]
async fn four_tabs_committing_to_one_tree_at_once_land_every_commit_applying_again_after_each_conflict() -> anyhow::Result<()> {
    let database = Database::new();
    let creator = database.tab().await?;
    let _: Tree = created(&stores(&creator), &creator, "shared", Vec::<Item>::new()).await?;

    let mut tabs = Vec::new();
    for tab in 0..4u64 {
        let engine = database.tab().await?;
        let trees = stores(&engine);
        let tree: Tree = trees.open(&TreeId::new("shared")).await?;
        let members: Vec<Item> = (0..10).map(|n| tab * 100 + n).map(|id| item!(id, id as i64)).collect();
        tabs.push((engine, trees, tree, members));
    }

    let other_tabs_commits = 3 * 10;
    let writers = tabs.iter().map(|(engine, trees, tree, members)| async move {
        let mut conflicts = 0;
        for member in members {
            while let Err(failure) = commit(trees, engine, tree, |changes| changes.insert(member.clone())).await {
                anyhow::ensure!(matches!(failure.downcast_ref(), Some(MutationError::WriteConflict)), failure);
                conflicts += 1;
                anyhow::ensure!(conflicts <= other_tabs_commits, "a tab's commits failed more often than the other tabs committed");
            }
        }

        anyhow::Ok(conflicts)
    });
    let conflicts: usize = try_join_all(writers).await?.into_iter().sum();

    let every_member: Vec<Item> = tabs.iter().flat_map(|(_, _, _, members)| members.clone()).collect();
    for (_, _, tree, _) in &tabs {
        assert_holds(tree, &every_member).await?;
    }

    assert!(conflicts > 0, "no commit found another tab's commit landed first");
    Ok(())
}
