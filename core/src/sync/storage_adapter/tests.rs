//! The full storage-adapter suite over the test engine, plus scenarios that
//! use its hooks to pause, fail or watch map calls.

use std::sync::Arc;

use ankurah_digest_tree::Ristretto255;

use super::{TreeChange, TreeId};
use crate::storage::{StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{self, by_number, created, item, stores, Id, Label, Number};
use crate::sync::conformance::{self, TestDatabase};
use crate::test_utils::TestStorage;

mod buffered;
mod cache;
mod cancellation;
mod damage;
mod failures;
mod key_migration;

/// A tree of items on the test engine, summed in the group `G`.
type Tree<G> = fixtures::Tree<TestStorage, G>;

/// The test engine keeps its data in this process's memory, so each node
/// reaches it through the one engine.
impl TestDatabase for Arc<TestStorage> {
    type Engine = TestStorage;

    async fn engine(&self) -> anyhow::Result<Arc<TestStorage>> { Ok(Arc::clone(self)) }

    /// A snapshot holds the committed maps it began with, which a later
    /// commit replaces rather than changes.
    fn keeps_a_reads_state_across_commits(&self) -> bool { true }
}

conformance::suite!(#[tokio::test], Arc::new(TestStorage::default()));

/// A pause for one of the test engine's hooks, with the test's two ends of
/// it: `let (pause, mut entered, release) = pause!();` hands the engine
/// `pause`, then `entered` says the paused call has reached it, and sending on
/// `release` lets the call go on.
macro_rules! pause {
    () => {{
        let (entered, reached) = ::tokio::sync::oneshot::channel::<()>();
        let (release, held) = ::tokio::sync::oneshot::channel::<()>();
        ((entered, held), reached, release)
    }};
}

use pause;

/// A tree opens and reads on an engine that holds one snapshot open at a
/// time: the opening is done with the snapshot it read the tree's entry in
/// before the tree begins its own.
#[tokio::test]
async fn a_tree_opens_and_reads_on_an_engine_that_holds_one_snapshot_at_a_time() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    created::<_, Ristretto255>(&trees, &engine, "tree", [item!(1, 1), item!(2, 2)]).await?;

    *engine.most_open_snapshots.lock().unwrap() = Some(1);
    let tree: Tree<Ristretto255> = trees.open(&TreeId::new("tree")).await?;

    assert_eq!(tree.view().count(..).await?, 2);
    Ok(())
}

/// An apply given no changes makes no map call: its storage transaction
/// commits as one that touched no map, which a reset before its commit
/// doesn't fail.
#[tokio::test]
async fn an_apply_given_no_changes_makes_no_map_call() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);

    let mut trx = engine.transaction();
    let applied = trees.apply::<Number, Id, Label, Ristretto255>(&mut trx, Vec::new()).await?;
    assert!(applied.retired().is_empty() && applied.refused().is_empty());

    engine.delete_all().await?;
    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);
    Ok(())
}

/// Creating, rebuilding, opening, changing, dropping and reading digest trees can each be
/// awaited on any thread of a multi-threaded executor.
#[tokio::test]
async fn every_call_of_the_stores_can_be_awaited_on_any_thread() -> anyhow::Result<()> {
    fn sendable<T: Send>(_: &T) {}

    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<Ristretto255> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let mut trx = engine.transaction();

    sendable(&trees.create::<Number, Id, Label, Ristretto255, _>(&mut trx, TreeId::new("other"), by_number(), [item!(1, 1)]));
    sendable(&trees.rebuild::<Number, Id, Label, Ristretto255, _>(&mut trx, TreeId::new("tree"), by_number(), [item!(2, 2)]));
    sendable(&trees.open::<Number, Id, Label, Ristretto255>(&TreeId::new("tree")));
    sendable(&trees.apply(&mut trx, vec![TreeChange::Changes(tree.transaction())]));
    sendable(&trees.drop(&mut trx, &TreeId::new("tree")));
    sendable(&tree.view().count(..));
    Ok(())
}
