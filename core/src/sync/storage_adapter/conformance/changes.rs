//! Trees created, changed and dropped in storage transactions, their rows
//! landing with each transaction's commit.

use std::pin::pin;

use ankurah_digest_tree::DigestGroup;
use ankurah_proto::Clock;
use futures::future::try_join_all;

use super::fixtures::{assert_holds, by_number, cause, commit, created, entity, item, poll_once, stores, Item, Tree};
use super::TestDatabase;
use crate::error::{MutationError, RetrievalError};
use crate::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};
use crate::sync::{Error, TreeChange, TreeId};

/// A tree created in a storage transaction exists once it commits: it opens
/// by its id, holding its members.
pub async fn a_created_tree_opens_by_its_id_holding_its_members<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let members = (1..=100).map(|n| item!(n, n as i64 % 7));

    let mut trx = engine.transaction();
    let creating = trees.create(&mut trx, TreeId::new("tree"), by_number(), members.clone()).await?;
    let unopened: Result<Tree<D::Engine, G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&unopened), Some(Error::NoSuchTree(_))), "a tree whose creation hasn't committed");

    trx.commit().await?.committed()?;
    let tree: Tree<D::Engine, G> = creating.landed();
    let opened: Tree<D::Engine, G> = trees.open(&TreeId::new("tree")).await?;

    assert_holds(&tree, &members.clone().collect::<Vec<_>>()).await?;
    assert_holds(&opened, &members.collect::<Vec<_>>()).await
}

/// A tree's changes, applied in a storage transaction, land when it commits:
/// until then the tree reads as it was.
pub async fn changes_land_when_their_storage_transaction_commits<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", (1..=100).map(|n| item!(n, n as i64))).await?;

    let mut changes = tree.transaction();
    changes.insert(item!(101, 0))?;
    changes.update(item!(2, 2), item!(2, 2, "relabelled"))?;
    changes.remove(item!(3, 3))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(changes)]).await?;
    assert_holds(&tree, &(1..=100).map(|n| item!(n, n as i64)).collect::<Vec<_>>()).await?;

    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    let mut after: Vec<_> = (1..=100).filter(|&n| n != 3).map(|n| item!(n, n as i64)).collect();
    after[1] = item!(2, 2, "relabelled");
    after.push(item!(101, 0));

    assert_holds(&tree, &after).await
}

/// A dropped tree goes when its storage transaction commits: it no longer
/// opens, its handles' reads fail as retired, and its id is free for a tree
/// created again.
pub async fn a_dropped_tree_goes_when_its_storage_transaction_commits<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let mut trx = engine.transaction();
    trees.drop(&mut trx, &TreeId::new("tree")).await?;
    assert_holds(&tree, &[item!(1, 1)]).await?;
    trx.commit().await?.committed()?;

    let opened: Result<Tree<D::Engine, G>, _> = trees.open(&TreeId::new("tree")).await;
    assert!(matches!(cause(&opened), Some(Error::NoSuchTree(_))));
    assert!(matches!(cause(&tree.view().count(..).await), Some(Error::Retired(_))));

    let again: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(2, 2)]).await?;
    assert_holds(&again, &[item!(2, 2)]).await
}

/// Several trees' changes land in one storage transaction with its entity
/// records, all together, and a transaction that conflicts on an entity
/// lands none of them: not the trees' rows, and not the records it holds for
/// other entities.
pub async fn several_trees_and_entity_records_land_in_one_storage_transaction_or_none_do<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let (first, second): (Tree<D::Engine, G>, Tree<D::Engine, G>) =
        (created(&trees, &engine, "first", [item!(1, 1)]).await?, created(&trees, &engine, "second", [item!(1, 1)]).await?);
    let (state, event) = entity!(1);

    let (mut one, mut other) = (first.transaction(), second.transaction());
    one.insert(item!(2, 2))?;
    other.remove(item!(1, 1))?;

    let mut trx = engine.transaction();
    trx.add_events(std::slice::from_ref(&event)).await?;
    trx.set_state(&Clock::default(), &state).await?;
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(other), TreeChange::Changes(one)]).await?;
    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    assert_eq!(engine.get_state(state.payload.entity_id).await?.payload, state.payload);
    assert_eq!(engine.get_events(vec![event.payload.id()]).await?.len(), 1);
    assert_holds(&first, &[item!(1, 1), item!(2, 2)]).await?;
    assert_holds(&second, &[]).await?;

    let (mut one, mut other) = (first.transaction(), second.transaction());
    one.insert(item!(3, 3))?;
    other.insert(item!(3, 3))?;
    let (fresh, fresh_event) = entity!(2);

    let mut stale = engine.transaction();
    stale.add_events(std::slice::from_ref(&fresh_event)).await?;
    stale.set_state(&Clock::default(), &state).await?;
    stale.set_state(&Clock::default(), &fresh).await?;
    let applied = trees.apply(&mut stale, vec![TreeChange::Changes(one), TreeChange::Changes(other)]).await?;
    assert!(matches!(stale.commit().await?, StorageCommitOutcome::Conflict { .. }), "the entity's head has moved");
    drop(applied);

    assert_eq!(engine.get_state(state.payload.entity_id).await?.payload, state.payload);
    assert!(matches!(engine.get_state(fresh.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert!(engine.get_events(vec![fresh_event.payload.id()]).await?.is_empty());
    assert_holds(&first, &[item!(1, 1), item!(2, 2)]).await?;
    assert_holds(&second, &[]).await
}

/// A storage transaction whose apply asks for a tree's lock while another
/// holds it waits until that one ends, and then applies its changes to the
/// tree as the other's commit left it.
pub async fn a_second_transaction_waits_for_the_first_to_end_and_sees_its_commit<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", (1..=100).map(|n| item!(n, n as i64))).await?;

    let (mut first_changes, mut second_changes) = (tree.transaction(), tree.transaction());
    first_changes.insert(item!(101, 50))?;
    second_changes.insert(item!(102, 50))?;
    second_changes.remove(item!(101, 50))?;

    let mut first = engine.transaction();
    let first_applied = trees.apply(&mut first, vec![TreeChange::Changes(first_changes)]).await?;

    let mut second = engine.transaction();
    let second_applied = {
        let mut waiting = pin!(trees.apply(&mut second, vec![TreeChange::Changes(second_changes)]));
        assert!(poll_once(waiting.as_mut()).is_pending(), "the second waits for the tree's lock");

        let committed = first.commit().await?.committed()?;
        first_applied.landed(&committed);

        waiting.await?
    };

    assert!(second_applied.refused().is_empty(), "the second found the member the first inserted");
    let committed = second.commit().await?.committed()?;
    second_applied.landed(&committed);

    let mut expected: Vec<_> = (1..=100).map(|n| item!(n, n as i64)).collect();
    expected.push(item!(102, 50));
    assert_holds(&tree, &expected).await
}

/// Four writers on one node commit to two trees at once, two to each, every
/// commit inserting eight members at its tree's rising edge, so that the
/// tree's buckets split while the other writers wait their turn at its lock:
/// every commit lands, and each tree holds what its writers put.
pub async fn several_writers_on_one_node_committing_to_two_trees_at_once_land_every_commit<G: DigestGroup, D: TestDatabase>(
    db: &D,
) -> anyhow::Result<()> {
    const WRITERS: u64 = 4;
    const ROUNDS: u64 = 16;

    let engine = db.engine().await?;
    let trees = stores(&engine);
    let both: [Tree<D::Engine, G>; 2] =
        [created(&trees, &engine, "evens", Vec::<Item>::new()).await?, created(&trees, &engine, "odds", Vec::<Item>::new()).await?];

    let writers = (0..WRITERS).map(|writer| {
        let (trees, engine, tree) = (&trees, &engine, &both[writer as usize % 2]);
        async move {
            for round in 0..ROUNDS {
                let inserting = (0..8).map(|n| (round * 8 + n) * WRITERS + writer);
                commit(trees, engine, tree, |changes| inserting.clone().try_for_each(|at| changes.insert(item!(at, at as i64)))).await?;
            }

            anyhow::Ok(())
        }
    });
    try_join_all(writers).await?;

    for (parity, tree) in (0..).zip(&both) {
        let put: Vec<Item> = (0..ROUNDS * 8 * WRITERS).filter(|at| at % 2 == parity).map(|at| item!(at, at as i64)).collect();
        assert_holds(tree, &put).await?;
    }

    Ok(())
}

/// An apply that fails leaves its storage transaction unable to commit, and
/// lands nothing of it: here, two changes for one tree.
pub async fn an_apply_that_fails_leaves_its_transaction_unable_to_commit<G: DigestGroup, D: TestDatabase>(db: &D) -> anyhow::Result<()> {
    let engine = db.engine().await?;
    let trees = stores(&engine);
    let tree: Tree<D::Engine, G> = created(&trees, &engine, "tree", [item!(1, 1)]).await?;

    let (mut first, mut second) = (tree.transaction(), tree.transaction());
    first.insert(item!(2, 2))?;
    second.insert(item!(3, 3))?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(first), TreeChange::Changes(second)]).await;
    assert!(matches!(cause(&applied), Some(Error::ChangedTwice(_))));

    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
    assert_holds(&tree, &[item!(1, 1)]).await
}
