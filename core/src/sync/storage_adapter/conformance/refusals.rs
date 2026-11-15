//! Trees that refuse their changes: each is marked invalid in the same
//! storage transaction, and then neither reads, commits nor opens.

use crate::sync::conformance::fixtures::TestStores;

use ankurah_digest_tree::{DigestGroup, Encode, Error as DigestTreeError, RowKey, Span};

use super::fixtures::{assert_holds, cached, cause, created, item, rows_now, stores, uncached_stores, Carries, Tree};
use super::TestDatabase;
use crate::storage::{MapWrite, StorageEngine, StorageTransaction};
use crate::sync::storage_adapter::layout::names::trees_map;
use crate::sync::{Error, TreeChange, TreeId, TreePart};

/// A tree that refuses a change, here the removal of a member it doesn't
/// hold, is marked invalid in the apply's storage transaction, and writes
/// nothing else, while the other trees' changes apply. Once the transaction
/// commits, the tree neither reads, commits nor opens.
pub async fn a_tree_that_refuses_its_changes_is_marked_invalid<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let refusing: Tree<D::Engine, G> = created(&trees, &engine, "refusing", [item!(1, 1)]).await?;
    let other: Tree<D::Engine, G> = created(&trees, &engine, "other", [item!(1, 1)]).await?;

    let mut refused = refusing.transaction();
    refused.remove(item!(2, 2))?;
    let mut changed = other.transaction();
    changed.insert(item!(2, 2))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(refused), TreeChange::Changes(changed)]).await?;
    assert!(matches!(applied.refused(), [(id, DigestTreeError::NotHeld { .. })] if id == refusing.store().id()), "{:?}", applied.refused());
    assert!(applied.retired().is_empty());

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    assert_holds(&other, &[item!(1, 1), item!(2, 2)]).await?;
    assert_invalid(&trees, &engine, &refusing).await
}

/// A tree the owner reports refused, since the crate refused a change at its
/// own call, is marked invalid in the apply's storage transaction.
pub async fn a_tree_its_owner_reports_refused_is_marked_invalid<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let refusal = tree.transaction().insert(item!(2, None)).expect_err("a member without a number has no key");

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Refused(tree.clone(), refusal)]).await?;
    assert!(matches!(applied.refused(), [(_, DigestTreeError::Key { .. })]));

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    assert_invalid(&trees, &engine, &tree).await
}

/// A tree marked invalid in a storage transaction that is dropped stays valid:
/// the mark lands with the transaction or not at all.
pub async fn a_refusal_whose_transaction_is_dropped_leaves_the_tree_valid<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut refused = tree.transaction();
    refused.remove(item!(2, 2))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(refused)]).await?;
    assert_eq!(applied.refused().len(), 1);
    drop(trx);

    assert_holds(&tree, &[item!(1, 1)]).await?;
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(1, 1)]).await
}

/// A node row damaged behind the tree's back, read with the cache off, since
/// a cached row can't show it: a read of the tree fails naming the damage,
/// and a commit's apply finds the tree refusing, and marks it invalid.
pub async fn a_damaged_row_fails_reads_and_marks_the_tree_invalid_at_its_next_commit<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = uncached_stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", (0..300u64).map(|n| item!(n, n as i64))).await?;

    let meta = RowKey::Meta.encode();
    let root = rows_now(&engine, &tree).await?.into_keys().find(|key| cached(key) && *key != meta).expect("the root's row");

    let mut damage = engine.transaction();
    damage.map_write(vec![MapWrite::Put { map: tree.store().inner().rows.clone(), key: root, value: vec![0xFF] }]).await?;
    damage.commit().await?.committed()?;

    let read = tree.view().span_digest(Span::all()).await;
    assert!(matches!(cause(&read), Some(Error::Damaged { part: TreePart::Row, .. })), "{:?}", read.map(|_| ()));

    let mut changes = tree.transaction();
    changes.insert(item!(1000, 1))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    assert!(matches!(applied.refused(), [(_, error)] if matches!(error.sync_error(), Some(Error::Damaged { .. }))));

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    assert_invalid(&trees, &engine, &tree).await
}

/// A tree whose entry is damaged behind its back refuses its changes, naming
/// the damage, and fails no apply: the other trees' changes apply, and
/// nothing is written for it. It neither reads nor opens, naming the damage,
/// until a drop deletes the entry and frees its id.
pub async fn a_tree_whose_entry_is_damaged_refuses_its_changes_until_it_is_dropped<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let damaged: Tree<D::Engine, G> = created(&trees, &engine, "damaged", [item!(1, 1)]).await?;
    let other: Tree<D::Engine, G> = created(&trees, &engine, "other", [item!(1, 1)]).await?;

    let mut damage = engine.transaction();
    damage.map_write(vec![MapWrite::Put { map: trees_map(), key: damaged.store().id().entry_key(), value: vec![0xFF] }]).await?;
    damage.commit().await?.committed()?;

    let (mut refused, mut changed) = (damaged.transaction(), other.transaction());
    refused.insert(item!(2, 2))?;
    changed.insert(item!(2, 2))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(refused), TreeChange::Changes(changed)]).await?;
    assert!(
        matches!(applied.refused(), [(id, error)] if id == damaged.store().id() && is_damaged_entry(error.sync_error())),
        "{:?}",
        applied.refused()
    );

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);
    assert_holds(&other, &[item!(1, 1), item!(2, 2)]).await?;

    assert!(is_damaged_entry(cause(&damaged.view().count(..).await)), "a read");
    let opened: Result<Tree<D::Engine, G>, _> = trees.open(damaged.store().id()).await;
    assert!(is_damaged_entry(cause(&opened)), "an opening");

    let mut dropping = engine.transaction();
    trees.drop(&mut dropping, damaged.store().id()).await?;
    dropping.commit().await?.committed()?;

    let again: Tree<D::Engine, G> = created(&trees, &engine, "damaged", [item!(3, 3)]).await?;
    assert_holds(&again, &[item!(3, 3)]).await
}

fn is_damaged_entry(error: Option<&Error>) -> bool { matches!(error, Some(Error::Damaged { part: TreePart::Entry, .. })) }

/// Check that `tree` is marked invalid: its reads fail as retired, its next
/// apply writes nothing and reports it retired, and it doesn't open.
async fn assert_invalid<SE: StorageEngine + 'static, G: DigestGroup>(
    trees: &TestStores<SE>,
    engine: &SE,
    tree: &Tree<SE, G>,
) -> anyhow::Result<()> {
    assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))), "a read");

    let mut changes = tree.transaction();
    changes.insert(item!(3, 3))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    assert_eq!(applied.retired(), [tree.store().id().clone()], "a commit");
    assert!(applied.refused().is_empty());
    drop(trx);

    let opened: Result<Tree<SE, G>, _> = trees.open(tree.store().id()).await;
    assert!(matches!(cause(&opened), Some(Error::Invalid(_))), "an opening");
    Ok(())
}
