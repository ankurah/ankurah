//! An apply, a creation, a rebuild or a drop dropped part way: the storage transaction
//! it was given fails to commit, whatever the call had reached.
//!
//! The sweeps drop each call at its map awaits: they pause it at its first
//! map call, then its second, and so on, through the test engine's hold, drop
//! it there, and commit the transaction, until the call finishes before
//! reaching the pause. With the cache off, a commit reads every row it needs
//! through storage, so every read is an await to drop it at. An apply and a
//! creation also wait where no map call is under way, at the crate's yields
//! between chunks of members, and are dropped there too.

use crate::sync::conformance::fixtures::TestStores;

use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use ankurah_digest_tree::DigestGroup;
use ankurah_proto::Clock;

use super::{pause, Tree};
use crate::error::{MutationError, RetrievalError};
use crate::storage::{StorageEngine, StorageTransaction};
use crate::sync::conformance::fixtures::{assert_holds, by_number, cause, created, entity, item, poll_once, uncached_stores, Id, Item};
use crate::sync::conformance::in_both_groups;
use crate::sync::{Error, TreeChange, TreeId};
use crate::test_utils::{TestStorage, TestStorageTransaction};

/// Members numbered 0 to 199, enough for a trie of several levels.
macro_rules! several_levels {
    () => {
        (0..200u64).map(|n| item!(n, n as i64)).collect::<Vec<Item>>()
    };
}

/// Members numbered 1,000 to 2,024: one more than the crate works through
/// between two yields.
macro_rules! more_than_a_chunk {
    () => {
        (1000..2025u64).map(|n| item!(n, n as i64))
    };
}

/// Run `call` on a storage transaction of `engine`'s, dropping it at its map
/// call `step`, or as it holds its tree's write part way when `mid_write`:
/// whether the call finished before it got there, and then the transaction's
/// commit, which must fail if the call was dropped.
async fn dropped_at<T>(
    engine: &TestStorage,
    step: usize,
    mid_write: bool,
    call: impl AsyncFnOnce(&mut TestStorageTransaction<'_>) -> Result<T, MutationError>,
) -> anyhow::Result<bool> {
    let (hold, mut entered, _release) = pause!();
    match mid_write {
        false => *engine.hold_map_call.lock().unwrap() = Some((step, hold)),
        true => *engine.hold_map_write.lock().unwrap() = Some(hold),
    }

    let mut trx = engine.transaction();
    let finished = {
        let mut calling = pin!(call(&mut trx));

        match poll_once(calling.as_mut()) {
            Poll::Ready(outcome) => {
                outcome?;
                true
            }

            Poll::Pending => {
                entered.try_recv().map_err(|_| anyhow::anyhow!("the call waited elsewhere than at the engine's hold"))?;
                false
            }
        }
    };

    *engine.hold_map_call.lock().unwrap() = None;
    *engine.hold_map_write.lock().unwrap() = None;

    match finished {
        true => trx.commit().await?.committed().map(|_| ())?,
        false => assert!(
            matches!(trx.commit().await, Err(MutationError::TransactionFailed)),
            "dropped at map call {step}, mid-write {mid_write}"
        ),
    }

    Ok(finished)
}

/// Every map await the case drops its call at: each map call in turn, until
/// the call finishes first, and then part way through its write.
async fn every_map_await(mut case: impl AsyncFnMut(usize, bool) -> anyhow::Result<bool>) -> anyhow::Result<()> {
    let mut step = 0;
    while !case(step, false).await? {
        step += 1;
    }

    assert!(step > 1, "the call made more than one map call");
    assert!(!case(0, true).await?, "the call was dropped part way through its write");
    Ok(())
}

/// An apply to two trees, one of which refuses its changes and is marked
/// invalid, dropped at each map await.
async fn an_apply_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit<G: DigestGroup>() -> anyhow::Result<()> {
    every_map_await(async |step, mid_write| {
        let engine = Arc::new(TestStorage::default());
        let trees = uncached_stores(&engine);
        let refusing: Tree<G> = created(&trees, &engine, "a refusing", several_levels!()).await?;
        let tree: Tree<G> = created(&trees, &engine, "b changing", several_levels!()).await?;

        let (mut refused, mut changes) = (refusing.transaction(), tree.transaction());
        refused.remove(item!(1000, 1000))?;
        changes.insert(item!(1000, 100))?;
        changes.remove(item!(3, 3))?;

        let changes = vec![TreeChange::Changes(refused), TreeChange::Changes(changes)];
        let finished = dropped_at(&engine, step, mid_write, async |trx| trees.apply(trx, changes).await).await?;

        if !finished {
            assert_holds(&tree, &several_levels!()).await?;
            assert_holds(&refusing, &several_levels!()).await?;
        }

        Ok(finished)
    })
    .await
}

/// A creation dropped at each map await.
async fn a_creation_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit<G: DigestGroup>() -> anyhow::Result<()> {
    every_map_await(async |step, mid_write| {
        let engine = Arc::new(TestStorage::default());
        let trees = uncached_stores(&engine);

        let creating = async |trx: &mut TestStorageTransaction<'_>| {
            trees.create::<_, Id, _, G, _>(trx, TreeId::new("tree"), by_number(), several_levels!()).await.map(drop)
        };
        let finished = dropped_at(&engine, step, mid_write, creating).await?;

        if !finished {
            assert!(matches!(cause(&opened::<G>(&trees).await), Some(Error::NoSuchTree(_))));
        }

        Ok(finished)
    })
    .await
}

/// A rebuild dropped at each map await, including part way through writing
/// its replacement rows, leaves the old tree intact.
async fn a_rebuild_dropped_at_any_map_await_preserves_the_old_tree<G: DigestGroup>() -> anyhow::Result<()> {
    every_map_await(async |step, mid_write| {
        let engine = Arc::new(TestStorage::default());
        let trees = uncached_stores(&engine);
        let old: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

        let rebuilding = async |trx: &mut TestStorageTransaction<'_>| {
            trees.rebuild::<_, Id, _, G, _>(trx, TreeId::new("tree"), by_number(), several_levels!()).await.map(drop)
        };
        let finished = dropped_at(&engine, step, mid_write, rebuilding).await?;

        if !finished {
            assert_holds(&old, &[item!(1, 1)]).await?;
            assert_holds(&opened::<G>(&trees).await?, &[item!(1, 1)]).await?;
        }

        Ok(finished)
    })
    .await
}

/// A drop dropped at each map await.
async fn a_drop_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit<G: DigestGroup>() -> anyhow::Result<()> {
    every_map_await(async |step, mid_write| {
        let engine = Arc::new(TestStorage::default());
        let trees = uncached_stores(&engine);
        let tree: Tree<G> = created(&trees, &engine, "tree", several_levels!()).await?;

        let id = TreeId::new("tree");
        let finished = dropped_at(&engine, step, mid_write, async |trx| trees.drop(trx, &id).await).await?;

        if !finished {
            assert_holds(&tree, &several_levels!()).await?;
        }

        Ok(finished)
    })
    .await
}

/// An apply dropped at its preparation's yield between hashing chunks,
/// before its first map call, leaves its transaction unable to commit: the
/// entity record buffered before it lands no more than the tree's changes.
async fn an_apply_dropped_while_it_prepares_leaves_its_transaction_unable_to_commit<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = uncached_stores(&engine);
    let tree: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut changes = tree.transaction();
    for item in more_than_a_chunk!() {
        changes.insert(item)?;
    }

    let (state, _) = entity!(1);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;

    engine.record_map_reads();
    {
        let mut applying = pin!(trees.apply(&mut trx, vec![TreeChange::Changes(changes)]));
        assert!(poll_once(applying.as_mut()).is_pending());
    }
    assert!(engine.recorded_map_reads().is_empty(), "the apply made no map call");

    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert!(matches!(engine.get_state(state.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert_holds(&tree, &[item!(1, 1)]).await
}

/// A creation dropped at a yield between the chunks of members it builds its
/// rows from, where it waits on no map call, leaves its transaction unable to
/// commit: the entity record buffered before it lands no more than the tree.
async fn a_creation_dropped_while_it_builds_its_rows_leaves_its_transaction_unable_to_commit<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = uncached_stores(&engine);

    let (state, _) = entity!(1);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;

    {
        let mut creating = pin!(trees.create::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), by_number(), more_than_a_chunk!()));
        assert!(poll_once(creating.as_mut()).is_pending());
    }

    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert!(matches!(engine.get_state(state.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert!(matches!(cause(&opened::<G>(&trees).await), Some(Error::NoSuchTree(_))));
    Ok(())
}

/// Cancellation while the rebuild hashes its replacement members preserves
/// the old tree and rolls back the entity record staged alongside it.
async fn a_rebuild_dropped_while_it_builds_its_rows_preserves_the_old_tree<G: DigestGroup>() -> anyhow::Result<()> {
    let engine = Arc::new(TestStorage::default());
    let trees = uncached_stores(&engine);
    let old: Tree<G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;
    let (state, _) = entity!(1);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;

    {
        let mut rebuilding = pin!(trees.rebuild::<_, Id, _, G, _>(&mut trx, TreeId::new("tree"), by_number(), more_than_a_chunk!()));
        assert!(poll_once(rebuilding.as_mut()).is_pending());
    }

    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert!(matches!(engine.get_state(state.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert_holds(&old, &[item!(1, 1)]).await?;
    assert_holds(&opened::<G>(&trees).await?, &[item!(1, 1)]).await
}

async fn opened<G: DigestGroup>(trees: &TestStores<TestStorage>) -> Result<Tree<G>, crate::error::RetrievalError> {
    trees.open(&TreeId::new("tree")).await
}

in_both_groups!(
    #[tokio::test]
    an_apply_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit,
    a_creation_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit,
    a_rebuild_dropped_at_any_map_await_preserves_the_old_tree,
    a_drop_dropped_at_any_map_await_leaves_its_transaction_unable_to_commit,
    an_apply_dropped_while_it_prepares_leaves_its_transaction_unable_to_commit,
    a_creation_dropped_while_it_builds_its_rows_leaves_its_transaction_unable_to_commit,
    a_rebuild_dropped_while_it_builds_its_rows_preserves_the_old_tree,
);
