//! A tree's reads beside commits to it.

use ankurah_digest_tree::DigestGroup;
use futures::{StreamExt, TryStreamExt};

use super::fixtures::{before_hang, commit, created, item, stores, Id, Item, Tree};
use super::TestDatabase;
use crate::sync::TreeId;

/// A range read begins a read for each batch, so a commit made between two
/// batches, here on another node, waits for neither, and the batches after
/// it see it: the first batch has read the first 64 members, and the commit
/// removes a member the next batches read and inserts one before every
/// member, which they don't read.
pub async fn a_range_reads_later_batches_see_a_commit_made_between_them<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let (here, there) = (db.engine().await?, db.engine().await?);
    let (here_trees, there_trees) = (stores(&here), stores(&there));
    let start: Vec<Item> = (0..200u64).map(|n| item!(n, n as i64)).collect();
    let tree: Tree<D::Engine, G> = created(&here_trees, &here, "tree", start.clone()).await?;
    let elsewhere: Tree<D::Engine, G> = there_trees.open(&TreeId::new("tree")).await?;

    let mut scan = tree.view().range(..)?;
    let first: Vec<Id> = scan.by_ref().take(64).map_ok(|leaf| leaf.id).try_collect().await?;

    let committing = commit(&there_trees, &there, &elsewhere, |changes| {
        changes.remove(item!(100, 100))?;
        changes.insert(item!(1000, -1))
    });

    before_hang("the commit between batches, as for a read left open", committing).await?;

    let rest: Vec<Id> = scan.map_ok(|leaf| leaf.id).try_collect().await?;

    let read: Vec<Id> = first.into_iter().chain(rest).collect();
    assert_eq!(read, start.iter().map(|item| item.id).filter(|id| *id != Id(100)).collect::<Vec<_>>());
    Ok(())
}
