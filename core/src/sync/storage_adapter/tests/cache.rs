//! The record cache as the storage adapter uses it: what it answers, and what it
//! never serves.

use crate::sync::conformance::fixtures::TestStores;

use std::ops::Bound;
use std::pin::pin;
use std::sync::Arc;

use ankurah_digest_tree::{DigestGroup, Encode, ReadTrx, RowKey, Span, Store};

use super::{pause, Tree};
use ankurah_proto::Clock;

use crate::storage::{MapRead, StorageCommitOutcome, StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{
    assert_holds, cached, commit, created, entity, item, poll_once, rows_now, stores, uncached_stores, Item,
};
use crate::sync::conformance::{cache_model, in_both_groups};
use crate::sync::{CacheSettings, TreeChange, TreeId};
use crate::test_utils::TestStorage;

/// Members numbered 0 to 299, enough for a trie of several levels.
macro_rules! several_levels {
    () => {
        (0..300u64).map(|n| item!(n, n as i64))
    };
}

/// How many of `reads` read node rows of `tree` through storage: a get of a
/// node's key, or a range that begins before the leaves. Node keys sort after
/// the metadata row's and before every leaf's, the first of which is the
/// empty address's.
fn node_row_reads<G: DigestGroup>(reads: &[MapRead], tree: &Tree<G>) -> usize {
    let rows = &tree.store().inner().rows;
    let (meta, first_leaf) = (RowKey::Meta.encode(), RowKey::Leaf(Vec::new()).encode());

    let reads_nodes = |read: &&MapRead| match read {
        MapRead::Get { map, keys } => map == rows && keys.iter().any(|key| *key > meta && *key < first_leaf),

        MapRead::Range { map, start, .. } => {
            map == rows
                && match start {
                    Bound::Unbounded => true,
                    Bound::Included(key) | Bound::Excluded(key) => *key < first_leaf,
                }
        }
    };

    reads.iter().filter(reads_nodes).count()
}

/// How many of `reads` read `tree`'s change log.
fn log_reads<G: DigestGroup>(reads: &[MapRead], tree: &Tree<G>) -> usize {
    let log = &tree.store().inner().log;
    reads.iter().filter(|read| matches!(read, MapRead::Range { map, .. } if map == log)).count()
}

/// A commit to a tree whose node rows the cache keeps, as a creation's
/// landing leaves them, reads no node row through storage, and neither does
/// a read of the tree's digests after it lands.
async fn a_tree_whose_node_rows_are_cached_reads_none_through_storage<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", several_levels!()).await?;

    engine.record_map_reads();
    commit(&trees, &engine, &tree, |changes| changes.insert(item!(1000, 150))).await?;
    assert_eq!(node_row_reads(&engine.recorded_map_reads(), &tree), 0, "the commit");

    engine.record_map_reads();
    tree.view().span_digest(Span::all()).await?;
    tree.select(10i64..250)?.span_digest(Span::all()).await?;
    assert_eq!(node_row_reads(&engine.recorded_map_reads(), &tree), 0, "the reads");

    assert_holds(&tree, &several_levels!().chain([item!(1000, 150)]).collect::<Vec<_>>()).await
}

/// After another node's commit to a tree, a node whose cache keeps the
/// tree's rows from before it reads the change log in its next commit's first
/// request, and still no row of a trie node through storage.
async fn after_another_nodes_commit_the_next_commit_reads_the_change_log_and_no_trie_nodes_row<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let (here, there) = (stores(&engine), stores(&engine));
    let tree: Tree<G> = created(&here, &engine, "tree", several_levels!()).await?;
    let elsewhere: Tree<G> = there.open(&TreeId::new("tree")).await?;

    commit(&there, &engine, &elsewhere, |changes| changes.remove(item!(20, 20))).await?;
    commit(&there, &engine, &elsewhere, |changes| changes.insert(item!(1000, 250))).await?;

    engine.record_map_reads();
    commit(&here, &engine, &tree, |changes| changes.insert(item!(1001, 40))).await?;
    let reads = engine.recorded_map_reads();

    assert_eq!(log_reads(&reads, &tree), 1, "the change log, in the commit's first request");
    assert_eq!(node_row_reads(&reads, &tree), 0);

    let mut expected: Vec<Item> = several_levels!().filter(|item| item.id.0 != 20).collect();
    expected.extend([item!(1000, 250), item!(1001, 40)]);
    assert_holds(&tree, &expected).await?;
    assert_holds(&elsewhere, &expected).await
}

/// A tree dropped and created again under its id meets none of the old
/// tree's rows, though the cache kept them all.
async fn a_tree_dropped_and_created_again_meets_none_of_the_old_trees_rows<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let old: Tree<G> = created(&trees, &engine, "tree", several_levels!()).await?;
    old.view().span_digest(Span::all()).await?;

    let mut trx = engine.transaction();
    trees.drop(&mut trx, &TreeId::new("tree")).await?;
    trx.commit().await?.committed()?;

    let again: Tree<G> = created(&trees, &engine, "tree", (0..300u64).map(|n| item!(n, n as i64 * 2))).await?;
    assert_holds(&again, &(0..300u64).map(|n| item!(n, n as i64 * 2)).collect::<Vec<_>>()).await?;

    let reopened: Tree<G> = trees.open(&TreeId::new("tree")).await?;
    assert_holds(&reopened, &(0..300u64).map(|n| item!(n, n as i64 * 2)).collect::<Vec<_>>()).await
}

/// With a cache of no bytes, every commit and every read reads the node rows
/// it needs through storage.
async fn a_cache_of_no_bytes_reads_every_node_row_through_storage<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = uncached_stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", several_levels!()).await?;

    engine.record_map_reads();
    commit(&trees, &engine, &tree, |changes| changes.insert(item!(1000, 150))).await?;
    let reads = engine.recorded_map_reads();
    assert!(node_row_reads(&reads, &tree) > 1, "a walk from the root reads a node row per level");
    assert_eq!(log_reads(&reads, &tree), 0, "a tree with nothing cached reads no change log");

    engine.record_map_reads();
    tree.view().span_digest(Span::all()).await?;
    assert!(node_row_reads(&engine.recorded_map_reads(), &tree) > 0);

    assert_holds(&tree, &several_levels!().chain([item!(1000, 150)]).collect::<Vec<_>>()).await
}

/// A cache that falls back while an apply waits for its tree's lock, the
/// tree's rows evicted and then refilled by a read older than the ones the
/// apply found, is not caught up from where the apply found it: the apply
/// reads through storage, and its commit leaves the tree right.
async fn a_cache_that_fell_back_while_an_apply_waited_is_caught_up_only_from_where_it_was<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let elsewhere = stores(&engine);
    let (tree_elsewhere, other_elsewhere): (Tree<G>, Tree<G>) =
        (created(&elsewhere, &engine, "tree", several_levels!()).await?, created(&elsewhere, &engine, "other", several_levels!()).await?);

    // Room for the tree's rows the cache keeps, and no more.
    let first = rows_now(&engine, &tree_elsewhere).await?;
    let room = first.iter().filter(|(key, _)| cached(key)).map(|(key, row)| key.len() + row.len()).sum();
    let here = TestStores::new(Arc::clone(&engine), CacheSettings { bytes: room });
    let (tree, other): (Tree<G>, Tree<G>) = (here.open(&TreeId::new("tree")).await?, here.open(&TreeId::new("other")).await?);

    let node_keys = first.keys().filter(|key| cached(key)).map(|key| RowKey::decode(key)).collect::<Result<Vec<_>, _>>()?;
    let mut old_read = tree.store().begin_read().await?;

    // Two commits elsewhere beneath the low numbers, which this node's
    // cache takes in at the second, then one beneath the high numbers.
    commit(&elsewhere, &engine, &tree_elsewhere, |changes| changes.remove(item!(10, 10))).await?;
    commit(&elsewhere, &engine, &tree_elsewhere, |changes| changes.remove(item!(11, 11))).await?;
    tree.select(0i64..50)?.span_digest(Span::all()).await?;
    commit(&elsewhere, &engine, &tree_elsewhere, |changes| changes.remove(item!(290, 290))).await?;

    let (hold, mut entered, release) = pause!();
    *engine.hold_map_call.lock().unwrap() = Some((0, hold));

    let mut changes = tree.transaction();
    changes.insert(item!(1000, 12))?;

    let mut trx = engine.transaction();
    let applied = {
        let mut applying = pin!(here.apply(&mut trx, vec![TreeChange::Changes(changes)]));
        assert!(poll_once(applying.as_mut()).is_pending());
        entered.try_recv()?;

        // While the apply waits: the other tree's rows evict this tree's,
        // and the read begun before every commit refills them, as they were.
        let other_keys = rows_now(&engine, &other_elsewhere)
            .await?
            .into_keys()
            .filter(|key| cached(key))
            .map(|key| RowKey::decode(&key))
            .collect::<Result<Vec<_>, _>>()?;
        other.store().begin_read().await?.get_many(&other_keys).await?;
        old_read.get_many(&node_keys).await?;

        release.send(()).unwrap();
        applying.await?
    };

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    let expected: Vec<Item> = several_levels!().filter(|item| ![10, 11, 290].contains(&item.id.0)).chain([item!(1000, 12)]).collect();
    assert_holds(&tree, &expected).await?;
    assert_holds(&tree_elsewhere, &expected).await
}

/// An owner that lands a commit that never landed, here one whose entity
/// record conflicted, with another commit's result, leaves the cache ahead
/// of storage: the tree's next
/// commit finds it so under the tree's lock, forgets the tree's cached rows,
/// and leaves the tree right.
async fn a_landing_of_a_commit_that_never_landed_is_forgotten_at_the_trees_next_commit<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", several_levels!()).await?;

    let (state, event) = entity!(1);
    let mut trx = engine.transaction();
    trx.add_events(&[event]).await?;
    trx.set_state(&Clock::default(), &state).await?;
    let anothers = trx.commit().await?.committed()?;

    let mut conflicting = tree.transaction();
    conflicting.insert(item!(1000, 150))?;

    let mut stale = engine.transaction();
    stale.set_state(&Clock::default(), &state).await?;
    let never_landed = trees.apply(&mut stale, vec![TreeChange::Changes(conflicting)]).await?;
    assert!(matches!(stale.commit().await?, StorageCommitOutcome::Conflict { .. }));
    never_landed.landed(&anothers);

    commit(&trees, &engine, &tree, |changes| changes.insert(item!(1001, 40))).await?;
    assert_holds(&tree, &several_levels!().chain([item!(1001, 40)]).collect::<Vec<_>>()).await
}

/// The suite's model of the cache, run longer over the test engine:
/// many runs of reads, commits, landings, other nodes' commits
/// and evictions in random orders, none of which serves a row of another
/// generation.
async fn the_cache_model_run_long_never_serves_a_row_of_another_generation<G: DigestGroup>() -> anyhow::Result<()> {
    cache_model::runs::<G, _>(&Arc::new(TestStorage::default()), 0..6, 150).await
}

in_both_groups!(
    #[tokio::test]
    a_landing_of_a_commit_that_never_landed_is_forgotten_at_the_trees_next_commit,
    a_cache_that_fell_back_while_an_apply_waited_is_caught_up_only_from_where_it_was,
    a_tree_whose_node_rows_are_cached_reads_none_through_storage,
    after_another_nodes_commit_the_next_commit_reads_the_change_log_and_no_trie_nodes_row,
    a_tree_dropped_and_created_again_meets_none_of_the_old_trees_rows,
    a_cache_of_no_bytes_reads_every_node_row_through_storage,
    the_cache_model_run_long_never_serves_a_row_of_another_generation,
);
