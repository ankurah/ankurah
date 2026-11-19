//! Bytes the storage adapter keeps, damaged behind its back where no suite
//! scenario reaches: a change-log entry, in its framing or in the rows it
//! lists, which serves only the cache, and an entry at the last generation
//! there is.

use std::sync::Arc;

use ankurah_digest_tree::DigestGroup;

use super::Tree;
use crate::storage::{MapWrite, StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{assert_holds, cached, cause, commit, created, item, rows_now, stores, Carries, Item, Rows};
use crate::sync::conformance::in_both_groups;
use crate::sync::storage_adapter::layout::entry::{Status, TreeEntry};
use crate::sync::storage_adapter::layout::log::{self, Changed};
use crate::sync::storage_adapter::layout::names::trees_map;
use crate::sync::{Error, TreeChange, TreeId, TreePart};
use crate::test_utils::TestStorage;

/// A change-log entry damaged behind the tree's back costs a node whose
/// cache is behind it only the rows it has cached: its reads and its next
/// commit go on through storage, as on a node with nothing cached.
async fn a_damaged_change_log_entry_costs_a_node_only_its_cached_rows<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let (here, there) = (stores(&engine), stores(&engine));
    let tree: Tree<G> = created(&here, &engine, "tree", (0..100u64).map(|n| item!(n, n as i64))).await?;
    let elsewhere: Tree<G> = there.open(&TreeId::new("tree")).await?;

    commit(&there, &engine, &elsewhere, |changes| changes.insert(item!(1000, 50))).await?;

    let mut damage = engine.transaction();
    damage.map_write(vec![MapWrite::Put { map: tree.store().inner().log.clone(), key: log::key(1), value: vec![0xFF] }]).await?;
    damage.commit().await?.committed()?;

    let mut expected: Vec<Item> = (0..100u64).map(|n| item!(n, n as i64)).chain([item!(1000, 50)]).collect();
    assert_holds(&tree, &expected).await?;

    commit(&here, &engine, &tree, |changes| changes.insert(item!(1001, 60))).await?;
    expected.push(item!(1001, 60));
    assert_holds(&tree, &expected).await
}

/// A row damaged inside a change-log entry whose framing is whole costs a
/// node whose cache is behind it only its cached rows: its read reads the
/// row through storage, and the tree opens valid and reads again.
async fn a_read_meeting_a_damaged_row_in_a_change_log_entry_reads_the_row_through_storage<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let (here, there) = (stores(&engine), stores(&engine));
    let tree: Tree<G> = created(&here, &engine, "tree", (0..100u64).map(|n| item!(n, n as i64))).await?;
    let elsewhere: Tree<G> = there.open(&TreeId::new("tree")).await?;

    let before = rows_now(&engine, &tree).await?;
    commit(&there, &engine, &elsewhere, |changes| changes.insert(item!(1000, 50))).await?;
    damage_the_rows_in_the_first_change_log_entry(&engine, &tree, &before).await?;

    let expected: Vec<Item> = (0..100u64).map(|n| item!(n, n as i64)).chain([item!(1000, 50)]).collect();
    assert_holds(&tree, &expected).await?;
    assert_holds::<_, G>(&here.open(&TreeId::new("tree")).await?, &expected).await
}

/// A row damaged inside a change-log entry whose framing is whole costs a
/// node's commit, with no read before it, only the node's cached rows: the
/// commit reads the row through storage, the tree applies its changes, and
/// they land.
async fn a_commit_meeting_a_damaged_row_in_a_change_log_entry_reads_the_row_through_storage<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let (here, there) = (stores(&engine), stores(&engine));
    let tree: Tree<G> = created(&here, &engine, "tree", (0..100u64).map(|n| item!(n, n as i64))).await?;
    let elsewhere: Tree<G> = there.open(&TreeId::new("tree")).await?;

    let before = rows_now(&engine, &tree).await?;
    commit(&there, &engine, &elsewhere, |changes| changes.insert(item!(1000, 50))).await?;
    damage_the_rows_in_the_first_change_log_entry(&engine, &tree, &before).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(1001, 60))?;

    let mut trx = engine.transaction();
    let applied = here.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    assert!(applied.refused().is_empty(), "{:?}", applied.refused());

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    let expected: Vec<Item> = (0..100u64).map(|n| item!(n, n as i64)).chain([item!(1000, 50), item!(1001, 60)]).collect();
    assert_holds(&tree, &expected).await?;
    assert_holds::<_, G>(&here.open(&TreeId::new("tree")).await?, &expected).await
}

/// Write over the change-log entry for `tree`'s generation 1 the entry its
/// commit wrote, from `before`, the tree's rows before that commit, each row
/// it puts written as bytes that are no row: an entry whole in its framing
/// and damaged in its rows.
async fn damage_the_rows_in_the_first_change_log_entry<G: DigestGroup>(
    engine: &Arc<TestStorage>,
    tree: &Tree<G>,
    before: &Rows,
) -> anyhow::Result<()> {
    let after = rows_now(engine, tree).await?;
    let put = after.iter().filter(|(key, row)| before.get(*key) != Some(*row)).map(|(key, _)| (key.clone(), Some(vec![0xFF])));
    let deleted = before.keys().filter(|key| !after.contains_key(*key)).map(|key| (key.clone(), None));
    let changed: Changed = put.chain(deleted).filter(|(key, _)| cached(key)).collect();

    let mut damage = engine.transaction();
    damage.map_write(vec![MapWrite::Put { map: tree.store().inner().log.clone(), key: log::key(1), value: log::entry(&changed) }]).await?;
    damage.commit().await?.committed()?;
    Ok(())
}

/// A tree whose entry holds the last generation there is, as only damage
/// can leave it, refuses its next commit, naming the damage, and is marked
/// invalid.
async fn a_tree_at_the_last_generation_refuses_its_next_commit<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let last = TreeEntry { incarnation: tree.store().inner().incarnation, generation: u64::MAX, status: Status::Ready };
    let mut damage = engine.transaction();
    damage.map_write(vec![MapWrite::Put { map: trees_map(), key: tree.store().id().entry_key(), value: last.encode() }]).await?;
    damage.commit().await?.committed()?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 2))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    assert!(
        matches!(applied.refused(), [(_, error)] if matches!(error.sync_error(), Some(Error::Damaged { part: TreePart::Entry, .. }))),
        "{:?}",
        applied.refused()
    );

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    let opened: Result<Tree<G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&opened), Some(Error::Invalid(_))));
    Ok(())
}

in_both_groups!(
    #[tokio::test]
    a_damaged_change_log_entry_costs_a_node_only_its_cached_rows,
    a_read_meeting_a_damaged_row_in_a_change_log_entry_reads_the_row_through_storage,
    a_commit_meeting_a_damaged_row_in_a_change_log_entry_reads_the_row_through_storage,
    a_tree_at_the_last_generation_refuses_its_next_commit,
);
