//! Captures use short storage snapshots and retain their meaning across commits.

use std::sync::{atomic::AtomicU64, Arc};

use ankurah_digest_tree::{DigestGroup, DigestTree, Store};
use futures::TryStreamExt;

use super::fixtures::{assert_holds, commit, created, item, stores, Item, Tree};
use super::TestDatabase;
use crate::storage::StateCursor;
use crate::sync::TreeId;

/// A capture freezes both its durable base and private edits. Flushing the
/// owner and committing through an independent engine/cache cannot alter it.
/// Historical reads also leave the live cache at the current generation.
pub async fn a_capture_survives_flushes_and_other_managers_commits<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let local = stores(&engine);
    let initial: Vec<_> = (0..300).map(|id| item!(id, id as i64, "original")).collect();
    let tree: Tree<D::Engine, G> = created(&local, &engine, "capture", initial.clone()).await?;
    tree.store().load_buffered(Arc::new(AtomicU64::new(0))).await?;
    tree.store().advance(StateCursor::new(vec![0]), 0, 0);
    let durable_capture: Tree<D::Engine, G> = DigestTree::open(tree.store().capture()).await?;

    let mut changes = tree.transaction();
    changes.update(initial[3].clone(), item!(3, -30, "pending"))?;
    changes.remove(initial[5].clone())?;
    changes.insert(item!(1000, 150, "inserted"))?;
    changes.prepare().await.apply(tree.store().buffered_transaction().await?).await?.landed();
    let mut expected: Vec<Item> = initial.iter().filter(|item| item.id.0 != 5 && item.id.0 != 3).cloned().collect();
    expected.extend([item!(3, -30, "pending"), item!(1000, 150, "inserted")]);
    let capture: Tree<D::Engine, G> = DigestTree::open(tree.store().capture()).await?;
    tree.store().flush().await?;

    let remote_engine = db.engine().await?;
    let remote = stores(&remote_engine);
    let writer: Tree<D::Engine, G> = remote.open(&TreeId::new("capture")).await?;
    commit(&remote, &remote_engine, &writer, |changes| {
        changes.update(item!(3, -30, "pending"), item!(3, 400, "remote"))?;
        changes.remove(item!(1000, 150, "inserted"))?;
        changes.update(initial[10].clone(), item!(10, 10, "new payload"))?;
        changes.insert(item!(5, 500, "reinserted"))
    })
    .await?;
    // The same row changes in several generations: rewind must use its oldest
    // before-image, rather than the most recent one.
    commit(&remote, &remote_engine, &writer, |changes| changes.update(item!(3, 400, "remote"), item!(3, 401, "latest"))).await?;

    assert_holds(&durable_capture, &initial).await?;
    assert_holds(&capture, &expected).await?;
    let leaves: Vec<_> = capture.view().range(..)?.try_collect().await?;
    for leaf in leaves {
        assert_eq!(leaf.payload, expected.iter().find(|item| item.id == leaf.id).unwrap().label, "captured leaf payload");
    }
    let stale = tree.store().begin_read().await;
    assert!(matches!(stale, Err(error) if error.is_retryable()), "ordinary managed reads still require reconciliation");
    let mut current: Vec<_> = initial.into_iter().filter(|item| ![3, 5, 10].contains(&item.id.0)).collect();
    current.extend([item!(3, 401, "latest"), item!(5, 500, "reinserted"), item!(10, 10, "new payload")]);
    let live: Tree<D::Engine, G> = local.open(&TreeId::new("capture")).await?;
    assert_holds(&live, &current).await?;
    assert_holds(&capture, &expected).await?;
    assert!(capture.store().buffered_transaction().await.is_err());
    assert!(capture.store().flush().await.is_err());
    Ok(())
}
