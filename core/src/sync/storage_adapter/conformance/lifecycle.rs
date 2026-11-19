//! Creations, drops and commits of one tree racing each other in concurrent
//! storage transactions, and resets of the engine under them.

use std::ops::Bound;
use std::pin::pin;
use std::task::Poll;

use ankurah_core_types::key_spec::IndexDirection;
use ankurah_digest_tree::{DigestGroup, Encode, Error as DigestTreeError, ReadTrx, RowKey, Store};
use ankurah_proto::Clock;

use super::fixtures::{
    assert_holds, by_number, cached, cause, commit, created, entity, item, poll_once, rows_now, stores, Changes, Id, Tree,
};
use super::TestDatabase;
use crate::error::MutationError;
use crate::storage::{MapRead, MapRows, MapSnapshot, MapWrite, ScanDirection, StorageCommitOutcome, StorageEngine, StorageTransaction};
use crate::sync::storage_adapter::layout::{entry, log, names::trees_map};
use crate::sync::{Error, TreeChange, TreeId, TreePart};

/// Two creations of one tree race: the second waits for the first's lock,
/// then finds the tree the first created, and fails, leaving its storage
/// transaction unable to commit.
pub async fn of_two_creations_racing_the_second_finds_the_tree_the_first_created<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);

    let mut first = engine.transaction();
    let creating = trees.create::<_, Id, _, G, _>(&mut first, TreeId::new("tree"), by_number(), [item!(1, 1)]).await?;

    let mut second = engine.transaction();
    let raced = {
        let mut racing = pin!(trees.create::<_, Id, _, G, _>(&mut second, TreeId::new("tree"), by_number(), [item!(2, 2)]));
        assert!(poll_once(racing.as_mut()).is_pending(), "the second waits for the tree's lock");

        first.commit().await?.committed()?;
        racing.await.map(|_| ())
    };

    assert!(matches!(cause(&raced), Some(Error::Exists(_))));
    assert!(matches!(second.commit().await, Err(MutationError::TransactionFailed)));

    assert_holds(&creating.landed(), &[item!(1, 1)]).await
}

/// A creation racing a drop of the tree it creates again waits for the drop's
/// lock, and then creates the tree anew.
pub async fn a_creation_racing_a_drop_creates_the_tree_anew_once_the_drop_commits<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut dropping = engine.transaction();
    trees.drop(&mut dropping, &TreeId::new("tree")).await?;

    let mut creating = engine.transaction();
    let created = {
        let mut racing = pin!(trees.create::<_, Id, _, G, _>(&mut creating, TreeId::new("tree"), by_number(), [item!(2, 2)]));
        assert!(poll_once(racing.as_mut()).is_pending(), "the creation waits for the drop's lock");

        dropping.commit().await?.committed()?;
        racing.await?
    };

    creating.commit().await?.committed()?;
    assert_holds(&created.landed(), &[item!(2, 2)]).await?;
    assert!(matches!(cause(&old.view().count(..).await), Some(Error::Retired(_))));
    Ok(())
}

/// A drop racing a commit to the tree waits for the commit's lock, and then
/// drops the tree the commit left.
pub async fn a_drop_racing_a_commit_drops_the_tree_once_the_commit_lands<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(2, 2))?;

    let mut committing = engine.transaction();
    let applied = trees.apply(&mut committing, vec![TreeChange::Changes(changes)]).await?;

    let (mut dropping, id) = (engine.transaction(), TreeId::new("tree"));
    {
        let mut racing = pin!(trees.drop(&mut dropping, &id));
        assert!(poll_once(racing.as_mut()).is_pending(), "the drop waits for the commit's lock");

        let committed = committing.commit().await?.committed()?;
        applied.landed(&committed);
        assert_holds(&tree, &[item!(1, 1), item!(2, 2)]).await?;

        racing.await?;
    }

    dropping.commit().await?.committed()?;
    assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))));
    Ok(())
}

/// A commit racing a drop of its tree, or a drop and a creation anew, waits
/// for the lock, and then finds its tree retired: it writes nothing, and its
/// storage transaction commits.
pub async fn a_commit_racing_a_drop_finds_its_tree_retired_and_writes_nothing<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let (dropped, recreated): (Tree<D::Engine, G>, Tree<D::Engine, G>) =
        (created(&trees, &engine, "dropped", [item!(1, 1)]).await?, created(&trees, &engine, "recreated", [item!(1, 1)]).await?);

    let mut dropping = engine.transaction();
    trees.drop(&mut dropping, &TreeId::new("dropped")).await?;
    dropping.commit().await?.committed()?;

    let mut dropping = engine.transaction();
    trees.drop(&mut dropping, &TreeId::new("recreated")).await?;
    dropping.commit().await?.committed()?;

    let mut creating = engine.transaction();
    let anew = trees.create(&mut creating, TreeId::new("recreated"), by_number(), [item!(9, 9)]).await?;

    let mut committing = engine.transaction();
    let applied = {
        let changes: Vec<TreeChange<_, _, _, _, G>> =
            [&dropped, &recreated].into_iter().map(|tree| TreeChange::Changes(inserting(tree))).collect();
        let mut racing = pin!(trees.apply(&mut committing, changes));
        assert!(poll_once(racing.as_mut()).is_pending(), "the commit waits for the creation's lock");

        creating.commit().await?.committed()?;
        racing.await?
    };

    assert_eq!(applied.retired(), [dropped.store().id().clone(), recreated.store().id().clone()]);
    let committed = committing.commit().await?.committed()?;
    applied.landed(&committed);

    assert_holds::<_, G>(&anew.landed(), &[item!(9, 9)]).await
}

/// Changes inserting member 2 into `tree`.
fn inserting<SE: StorageEngine + 'static, G: DigestGroup>(tree: &Tree<SE, G>) -> Changes<SE, G> {
    let mut changes = tree.transaction();
    changes.insert(item!(2, 2)).expect("a member with a key");
    changes
}

/// A reset of the engine under open handles retires their trees: reads fail
/// as retired, an apply writes nothing for them, and they no longer open.
pub async fn a_reset_under_open_handles_retires_their_trees<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    tree.view().count(..).await?;

    engine.delete_all().await?;
    assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))));

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(inserting(&tree))]).await?;
    assert_eq!(applied.retired(), [tree.store().id().clone()]);
    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    let opened: Result<Tree<D::Engine, G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))));
    Ok(())
}

/// A reset of the engine begun between an apply and its storage
/// transaction's commit lands nothing of the apply that outlives the reset:
/// it waits for the commit, and then empties the tree, or it goes first and
/// fails the commit, as a conflict to retry. A reset that finished at once
/// can only have gone first; one still under way may have done either.
/// Either way, once both have finished, nothing of the commit is left and
/// the tree is gone for every handle.
pub async fn a_reset_between_an_apply_and_its_commit_waits_for_the_commit_or_fails_it<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(inserting(&tree))]).await?;

    let mut resetting = pin!(engine.delete_all());
    match poll_once(resetting.as_mut()) {
        Poll::Ready(reset) => {
            reset?;
            assert!(matches!(trx.commit().await, Err(MutationError::WriteConflict)), "a reset that didn't wait fails the commit");
        }

        // A reset still under way goes on beside the commit, as one in a
        // task of its own does.
        Poll::Pending => {
            let (committed, reset) = futures::future::join(trx.commit(), resetting).await;
            match committed {
                Ok(outcome) => {
                    outcome.committed()?;
                }

                Err(MutationError::WriteConflict) => {}

                Err(error) => return Err(error.into()),
            }

            reset?;
        }
    }

    drop(applied);

    assert!(rows_now(&engine, &tree).await?.is_empty(), "nothing of the commit is left");
    assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))), "the tree is gone for its handle");

    let opened: Result<Tree<D::Engine, G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))), "the tree no longer opens");
    Ok(())
}

/// A rebuild changes the same tree at its next generation. Old handles on
/// this node and another node keep working, and obsolete rows leave both
/// storage and their warm caches even when the replacement is much smaller.
pub async fn a_rebuild_preserves_handles_and_removes_obsolete_rows<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let members: Vec<_> = (0..400).map(|n| item!(n, n as i64)).collect();
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", members.clone()).await?;
    let other_engine = db.engine().await?;
    let other = stores(&other_engine);
    let remote: Tree<D::Engine, G> = other.open(&TreeId::new("tree")).await?;
    let before = rows_now(&engine, &old).await?;
    let keys: Vec<RowKey> = before.keys().filter(|key| cached(key)).map(|key| RowKey::decode(key)).collect::<Result<_, _>>()?;
    assert!(keys.len() > 2, "a multi-level tree with obsolete node rows");
    assert!(keys.len() + 2 < log::MOST_ROWS, "small enough to exercise the per-row change log");
    for tree in [&old, &remote] {
        tree.store().begin_read().await?.get_many(&keys).await?;
    }

    let mut stale_read = match db.keeps_a_reads_state_across_commits() {
        true => Some(remote.store().begin_read().await?),
        false => None,
    };

    let mut trx = engine.transaction();
    let rebuilding = trees.rebuild(&mut trx, TreeId::new("tree"), by_number(), [item!(1000, 1000)]).await?;
    assert_holds(&old, &members).await?;
    trx.commit().await?.committed()?;

    let rebuilt: Tree<D::Engine, G> = rebuilding.landed();
    assert_eq!(rebuilt.store().inner().incarnation, old.store().inner().incarnation);
    for tree in [&old, &remote, &rebuilt] {
        assert_holds(tree, &[item!(1000, 1000)]).await?;
        assert_eq!(tree.select(100i64..200i64)?.count(..).await?, 0);
    }
    let after = rows_now(&engine, &rebuilt).await?;
    assert!(before.keys().any(|key| cached(key) && !after.contains_key(key)), "the rebuild removed cached nodes");
    assert_eq!(after.len(), 3, "only metadata, the root, and the replacement leaf remain");
    let found = engine.map_snapshot(&[]).await?.map_read(vec![entry::read([old.store().id()])]).await?;
    assert_eq!(entry::found_one(old.store().id(), found)?.unwrap().generation, 1);

    if let Some(read) = &mut stale_read {
        for (key, row) in keys.iter().zip(read.get_many(&keys).await?) {
            assert_eq!(row.map(|row| row.encode()).as_ref(), before.get(&key.encode()), "the old snapshot keeps its rows");
        }
        assert_holds(&remote, &[item!(1000, 1000)]).await?;
    }
    drop(stale_read);
    commit(&other, &other_engine, &remote, |changes| changes.insert(item!(1001, 1001))).await?;
    assert_holds(&old, &[item!(1000, 1000), item!(1001, 1001)]).await?;
    Ok(())
}

/// A rebuild can create an absent tree, with the same commit boundary as a
/// creation.
pub async fn a_rebuild_of_an_absent_tree_creates_it<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let mut trx = engine.transaction();
    let rebuilding = trees.rebuild(&mut trx, TreeId::new("tree"), by_number(), [item!(1, 1)]).await?;

    let opened: Result<Tree<D::Engine, G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))));
    trx.commit().await?.committed()?;

    assert_holds::<_, G>(&rebuilding.landed(), &[item!(1, 1)]).await?;
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(1, 1)]).await
}

/// Discarding a rebuilt tree's transaction preserves the old tree, including
/// the rows the rebuild would remove and its ability to accept later changes.
pub async fn a_rebuild_whose_transaction_is_dropped_preserves_the_old_tree<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;

    let mut trx = engine.transaction();
    let rebuilding = trees.rebuild::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), by_number(), [item!(2, 2)]).await?;
    drop((rebuilding, trx));

    assert_eq!(rows_now(&engine, &old).await?, before);
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(1, 1)]).await?;
    commit(&trees, &engine, &old, |changes| changes.insert(item!(3, 3))).await?;
    assert_holds(&old, &[item!(1, 1), item!(3, 3)]).await
}

/// A rebuild in a transaction whose entity state conflicts lands neither
/// the replacement nor the removal of the old tree's rows.
pub async fn a_rebuild_whose_commit_conflicts_preserves_the_old_tree<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;
    let (state, _) = entity!(1);
    let mut first = engine.transaction();
    first.set_state(&Clock::default(), &state).await?;
    first.commit().await?.committed()?;

    let mut stale = engine.transaction();
    stale.set_state(&Clock::default(), &state).await?;
    let rebuilding = trees.rebuild::<_, Id, _, G, _>(&mut stale, TreeId::new("tree"), by_number(), [item!(2, 2)]).await?;
    assert!(matches!(stale.commit().await?, StorageCommitOutcome::Conflict { .. }));
    drop(rebuilding);

    assert_eq!(rows_now(&engine, &old).await?, before);
    assert_holds(&old, &[item!(1, 1)]).await?;
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(1, 1)]).await
}

/// Invalid replacement members fail the rebuild. The transaction cannot
/// commit, and the old tree remains usable.
pub async fn a_failed_rebuild_preserves_the_old_tree<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;

    let mut trx = engine.transaction();
    let failed = trees.rebuild::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), by_number(), [item!(2, None)]).await;
    assert!(matches!(failed, Err(MutationError::General(error)) if matches!(error.downcast_ref(), Some(DigestTreeError::Key { .. }))));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    assert_eq!(rows_now(&engine, &old).await?, before);
    assert_holds(&old, &[item!(1, 1)]).await?;
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(1, 1)]).await
}

/// A marked-invalid tree with readable rows can be rebuilt in place. Its
/// original handles become usable again with the rebuilt membership.
pub async fn a_rebuild_restores_a_tree_marked_invalid<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let refusal = old.transaction().insert(item!(2, None)).expect_err("a member without a key");
    let mut invalidating = engine.transaction();
    let applied = trees.apply(&mut invalidating, vec![TreeChange::Refused(old.clone(), refusal)]).await?;
    let committed = invalidating.commit().await?.committed()?;
    applied.landed(&committed);

    let mut trx = engine.transaction();
    let rebuilding = trees.rebuild(&mut trx, TreeId::new("tree"), by_number(), [item!(3, 3)]).await?;
    trx.commit().await?.committed()?;

    assert_holds::<_, G>(&rebuilding.landed(), &[item!(3, 3)]).await?;
    assert_holds(&old, &[item!(3, 3)]).await?;
    assert_holds::<_, G>(&trees.open(&TreeId::new("tree")).await?, &[item!(3, 3)]).await
}

/// A damaged entry cannot identify the old maps to clean up, so a rebuild
/// refuses it and leaves both that entry and the maps unchanged.
pub async fn a_rebuild_refuses_a_damaged_entry_without_replacing_it<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;
    let id = old.store().id();
    let mut damaging = engine.transaction();
    damaging.map_write(vec![MapWrite::Put { map: trees_map(), key: id.entry_key(), value: vec![0xFF] }]).await?;
    damaging.commit().await?.committed()?;

    let mut trx = engine.transaction();
    let failed = trees.rebuild::<_, Id, _, G, _>(&mut trx, id.clone(), by_number(), [item!(2, 2)]).await;
    assert!(matches!(cause(&failed), Some(Error::Damaged { part: TreePart::Entry, .. })));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    let entry = MapRead::Get { map: trees_map(), keys: vec![id.entry_key()] };
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![entry]).await?, [MapRows::Got(vec![Some(vec![0xFF])])]);
    assert_eq!(rows_now(&engine, &old).await?, before);
    Ok(())
}

/// Rebuilding reads every old row, including rows absent from the replacement.
/// An unreadable leaf fails that scan after metadata checks have succeeded;
/// it must not be silently discarded or change the tree's entry or log.
pub async fn a_rebuild_refuses_a_damaged_row_without_changing_stored_state<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    commit(&trees, &engine, &old, |changes| changes.insert(item!(2, 2))).await?;
    let leaf = rows_now(&engine, &old)
        .await?
        .into_keys()
        .find(|key| matches!(RowKey::decode(key), Ok(RowKey::Leaf(_))))
        .expect("the tree holds leaves");
    let tree = old.store().inner();
    let mut damaging = engine.transaction();
    damaging.map_write(vec![MapWrite::Put { map: tree.rows.clone(), key: leaf, value: vec![0xFF] }]).await?;
    damaging.commit().await?.committed()?;

    let mut reads = vec![entry::read([old.store().id()])];
    reads.extend([tree.rows.clone(), tree.log.clone()].into_iter().map(|map| MapRead::Range {
        map,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        direction: ScanDirection::Forward,
        limit: usize::MAX,
    }));
    let before = engine.map_snapshot(&[]).await?.map_read(reads.clone()).await?;

    let mut trx = engine.transaction();
    let failed = trees.rebuild::<_, Id, _, G, _>(&mut trx, old.store().id().clone(), by_number(), [item!(3, 3)]).await;
    assert!(matches!(cause(&failed), Some(Error::Damaged { part: TreePart::Row, .. })));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(reads).await?, before, "the entry, rows and log remain unchanged");
    Ok(())
}

/// Late landings from before and during a rebuild cannot overwrite the
/// cache's newer rows, including a change made through an original handle.
pub async fn late_landings_do_not_restore_rows_removed_by_a_rebuild<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let mut changes = old.transaction();
    changes.insert(item!(2, 2))?;
    let mut trx = engine.transaction();
    let earlier = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    let earlier_commit = trx.commit().await?.committed()?;

    let mut trx = engine.transaction();
    let rebuilding = trees.rebuild(&mut trx, TreeId::new("tree"), by_number(), [item!(3, 3)]).await?;
    trx.commit().await?.committed()?;
    assert_holds(&old, &[item!(3, 3)]).await?;
    earlier.landed(&earlier_commit);

    commit(&trees, &engine, &old, |changes| changes.insert(item!(4, 4))).await?;
    let rebuilt: Tree<D::Engine, G> = rebuilding.landed();
    assert_holds(&rebuilt, &[item!(3, 3), item!(4, 4)]).await?;
    assert_holds(&old, &[item!(3, 3), item!(4, 4)]).await
}

/// A rebuild cannot change the key specification beneath existing handles.
/// Refusing the request poisons its transaction and leaves the old tree intact.
pub async fn a_rebuild_refuses_a_different_key_spec<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;
    let mut reversed = by_number();
    reversed.keyparts[0].direction = IndexDirection::Desc;

    let mut trx = engine.transaction();
    let failed = trees.rebuild::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), reversed, [item!(2, 2)]).await;
    assert!(matches!(failed, Err(MutationError::General(error)) if matches!(error.downcast_ref(), Some(DigestTreeError::KeySpecMismatch))));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert_eq!(rows_now(&engine, &old).await?, before);
    assert_holds(&old, &[item!(1, 1)]).await
}

/// A damaged generation at the counter's limit must not wrap to an earlier
/// cache generation or mint a new incarnation during a rebuild.
pub async fn a_rebuild_refuses_generation_overflow<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let old: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let before = rows_now(&engine, &old).await?;
    let id = old.store().id();
    let entry = entry::TreeEntry { incarnation: old.store().inner().incarnation, generation: u64::MAX, status: entry::Status::Ready };
    let mut damaging = engine.transaction();
    damaging.map_write(vec![MapWrite::Put { map: trees_map(), key: id.entry_key(), value: entry.encode() }]).await?;
    damaging.commit().await?.committed()?;

    let mut trx = engine.transaction();
    let failed = trees.rebuild::<_, Id, _, G, _>(&mut trx, id.clone(), by_number(), [item!(2, 2)]).await;
    assert!(matches!(cause(&failed), Some(Error::Damaged { part: TreePart::Entry, .. })));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert_eq!(rows_now(&engine, &old).await?, before);
    let found = engine.map_snapshot(&[]).await?.map_read(vec![entry::read([id])]).await?;
    assert_eq!(entry::found_one(id, found)?, Some(entry));
    Ok(())
}
