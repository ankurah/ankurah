//! A tree's reads on IndexedDB, whose readonly transactions end once control
//! goes back to the event loop: a read held across a commit to its tree
//! fails retryably rather than mix two states. The trees keep no record cache,
//! so that every read reaches the engine's snapshot: one with a cache answers
//! the rows it keeps from memory, and never meets the commit.

use std::pin::pin;

use ankurah::core::storage::StorageEngine;
use ankurah::core::sync::conformance::fixtures::{commit, created, item, uncached_stores};
use ankurah::core::test_helpers::map_conformance::fixtures::poll_once;
use ankurah_digest_tree::{ReadTrx, RowKey, Store};
use wasm_bindgen_test::wasm_bindgen_test;

use crate::support::{Database, Tree};

/// A read of a tree, held while another tree is committed to, reads on: its
/// tree's lock version hasn't moved. Held across a commit to its own tree,
/// its next read fails retryably, and a read begun after sees the commit.
#[wasm_bindgen_test]
async fn a_read_held_across_a_commit_to_its_tree_fails_retryably_and_one_begun_after_sees_the_commit() -> anyhow::Result<()> {
    let database = Database::new();
    let engine = database.tab().await?;
    let trees = uncached_stores(&engine);
    let tree: Tree = created(&trees, &engine, "albums", [item!(1, 10)]).await?;
    let other: Tree = created(&trees, &engine, "songs", [item!(1, 10)]).await?;

    let mut read = tree.store().begin_read().await?;
    commit(&trees, &engine, &other, |changes| changes.insert(item!(2, 20))).await?;
    assert!(read.get(&RowKey::Meta).await?.is_some(), "a commit to another tree");

    commit(&trees, &engine, &tree, |changes| changes.insert(item!(2, 20))).await?;
    let failure = read.get(&RowKey::Meta).await.expect_err("a read across a commit to its tree");
    assert!(failure.is_retryable(), "{failure}");

    assert_eq!(tree.view().count(..).await?, 2);
    Ok(())
}

/// A read held across a commit to its tree fails retryably, and so does the
/// read after it, though that one is made while the readonly transaction the
/// failed read opened is still active.
#[wasm_bindgen_test]
async fn a_read_after_one_that_failed_across_a_commit_to_its_tree_fails_retryably_too() -> anyhow::Result<()> {
    let database = Database::new();
    let engine = database.tab().await?;
    let trees = uncached_stores(&engine);
    let tree: Tree = created(&trees, &engine, "albums", [item!(1, 10)]).await?;

    let mut read = tree.store().begin_read().await?;
    commit(&trees, &engine, &tree, |changes| changes.insert(item!(2, 20))).await?;
    assert!(read.get(&RowKey::Meta).await.expect_err("a read across a commit to its tree").is_retryable());

    let failure = read.get(&RowKey::Meta).await.expect_err("a read after one that failed");
    assert!(failure.is_retryable(), "{failure}");
    Ok(())
}

/// A read held across a commit to its tree, dropped once it has made its
/// requests in a new readonly transaction, leaves the read after it to fail
/// retryably, though that one is made while the transaction is still active.
#[wasm_bindgen_test]
async fn a_read_after_one_dropped_part_way_across_a_commit_to_its_tree_fails_retryably() -> anyhow::Result<()> {
    let database = Database::new();
    let engine = database.tab().await?;
    let trees = uncached_stores(&engine);
    let tree: Tree = created(&trees, &engine, "albums", [item!(1, 10)]).await?;

    let mut read = tree.store().begin_read().await?;
    commit(&trees, &engine, &tree, |changes| changes.insert(item!(2, 20))).await?;
    {
        let mut dropped = pin!(read.get(&RowKey::Meta));
        assert!(poll_once(dropped.as_mut()).is_pending(), "a read waiting on its requests");
    }

    let failure = read.get(&RowKey::Meta).await.expect_err("a read after one dropped part way");
    assert!(failure.is_retryable(), "{failure}");
    Ok(())
}

/// A reset clears every lock's version, so a tree created anew under its old
/// id brings its lock back to the version a read held across the reset
/// noted. The reset epoch has moved, and the read fails retryably.
#[wasm_bindgen_test]
async fn a_read_held_across_a_reset_and_its_trees_creation_anew_fails_retryably() -> anyhow::Result<()> {
    let database = Database::new();
    let engine = database.tab().await?;
    let trees = uncached_stores(&engine);
    let tree: Tree = created(&trees, &engine, "albums", [item!(1, 10)]).await?;

    let mut read = tree.store().begin_read().await?;
    engine.delete_all().await?;
    let _: Tree = created(&trees, &engine, "albums", [item!(2, 20)]).await?;

    let failure = read.get(&RowKey::Meta).await.expect_err("a read across a reset");
    assert!(failure.is_retryable(), "{failure}");
    Ok(())
}
