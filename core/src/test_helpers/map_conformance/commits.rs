//! How a transaction's map writes land with its commit, or not at all: with
//! its entity records, dropped, poisoned, or written part way; and a
//! transaction that makes no map call, which commits as it would if engines
//! kept no maps.

use std::pin::pin;
use std::task::Poll;

use ankurah_proto::Clock;

use super::fixtures::{commit, committed, entity, entries, everything, found, get, lock, only, poll_once, put, ranged};
use crate::error::{MutationError, RetrievalError};
use crate::storage::{CommittedEntityWrite, StorageCommitOutcome, StorageEngine, StorageTransaction};

/// A transaction's map writes land with its entity records when it commits,
/// all together, and a commit that conflicts on an entity lands nothing: not
/// its map writes, and not the records it holds for other entities.
pub async fn map_writes_land_with_the_transactions_entity_records_or_not_at_all<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let (state, event) = entity!(1);
    let id = state.payload.entity_id;

    let mut trx = engine.transaction();
    trx.add_events(std::slice::from_ref(&event)).await?;
    trx.set_state(&Clock::default(), &state).await?;
    trx.map_write(vec![put!("m", "a", 1), put!("n", "b", 2)]).await?;
    trx.commit().await?.committed()?;

    assert_eq!(engine.get_state(id).await?.payload, state.payload);
    assert_eq!(engine.get_events(vec![event.payload.id()]).await?.len(), 1);
    assert_eq!(found(committed(engine, get!("m", ["a"])).await?), [Some(1)]);
    assert_eq!(found(committed(engine, get!("n", ["b"])).await?), [Some(2)]);

    let (other, other_event) = entity!(2);
    let mut stale = engine.transaction();
    stale.add_events(std::slice::from_ref(&other_event)).await?;
    stale.set_state(&Clock::default(), &state).await?;
    stale.set_state(&Clock::default(), &other).await?;
    stale.map_write(vec![put!("m", "a", 10), put!("n", "c", 3)]).await?;
    assert!(matches!(stale.commit().await?, StorageCommitOutcome::Conflict { .. }), "the entity's head has moved");

    assert_eq!(engine.get_state(id).await?.payload, state.payload);
    assert!(matches!(engine.get_state(other.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert!(engine.get_events(vec![other_event.payload.id()]).await?.is_empty());
    assert_eq!(found(committed(engine, get!("m", ["a"])).await?), [Some(1)]);
    assert_eq!(found(committed(engine, get!("n", ["b", "c"])).await?), [Some(2), None]);
    Ok(())
}

/// A transaction sets its entity states before its first map call: one set
/// after it is refused at once.
pub async fn a_state_set_after_a_map_call_is_refused<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let (state, _) = entity!(1);

    let mut trx = engine.transaction();
    trx.map_read(vec![get!("m", ["a"])]).await?;

    let set = trx.set_state(&Clock::default(), &state).await;
    assert!(matches!(set, Err(MutationError::InvalidUpdate(_))), "{set:?}");
    Ok(())
}

/// A transaction dropped uncommitted lands none of its map writes, and its
/// locks go with it: the next transaction takes them and commits.
pub async fn a_dropped_transaction_lands_none_of_its_map_writes<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1)]).await?;

    let mut trx = engine.transaction();
    trx.map_lock_and_read(&[lock!("l")], vec![]).await?;
    trx.map_write(vec![put!("m", "a", 2), put!("m", "b", 2)]).await?;
    drop(trx);

    assert_eq!(found(committed(engine, get!("m", ["a", "b"])).await?), [Some(1), None]);

    let mut next = engine.transaction();
    assert_eq!(found(only(next.map_lock_and_read(&[lock!("l")], vec![get!("m", ["a"])]).await?)), [Some(1)]);
    next.map_write(vec![put!("m", "a", 3)]).await?;
    next.commit().await?.committed()?;

    assert_eq!(found(committed(engine, get!("m", ["a"])).await?), [Some(3)]);
    Ok(())
}

/// A poisoned transaction fails every later call, of its entity records or
/// its maps, and its commit, and lands nothing: neither its entity records
/// nor its map writes.
pub async fn a_poisoned_transaction_fails_every_later_call_and_its_commit<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let (state, _) = entity!(1);
    let (other, event) = entity!(2);

    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;
    trx.map_write(vec![put!("m", "a", 1)]).await?;
    trx.poison();

    assert!(matches!(trx.add_events(&[event]).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.set_state(&Clock::default(), &other).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.map_read(vec![get!("m", ["a"])]).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.map_lock_and_read(&[lock!("l")], vec![]).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.map_write(vec![put!("m", "b", 2)]).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    assert!(matches!(engine.get_state(state.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries![]);
    Ok(())
}

/// A map write dropped before it returns leaves its transaction unable to
/// commit, and lands nothing of it. A write that returns before it can be
/// dropped has finished, and lands with the commit.
pub async fn a_map_write_dropped_part_way_leaves_the_transaction_unable_to_commit<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let mut trx = engine.transaction();

    let finished = {
        let mut writing = pin!(trx.map_write(vec![put!("m", "a", 1), put!("m", "b", 2)]));
        match poll_once(writing.as_mut()) {
            Poll::Ready(result) => {
                result?;
                true
            }

            Poll::Pending => false,
        }
    };

    if finished {
        trx.commit().await?.committed()?;
        assert_eq!(ranged(committed(engine, everything!("m")).await?), entries!["a" => 1, "b" => 2]);
    } else {
        assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));
        assert_eq!(ranged(committed(engine, everything!("m")).await?), entries![]);
    }

    Ok(())
}

/// A transaction that makes no map call takes no lock and commits as it
/// would if engines kept no maps: it lands its records, and does so after a
/// `delete_all` that came before its commit too.
pub async fn a_transaction_that_touches_no_map_commits_as_without_maps<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let (state, event) = entity!(1);
    let id = state.payload.entity_id;

    let mut trx = engine.transaction();
    trx.add_events(std::slice::from_ref(&event)).await?;
    trx.set_state(&Clock::default(), &state).await?;
    let result = trx.commit().await?.committed()?;

    assert_eq!(result.entities, [CommittedEntityWrite { entity_id: id, canonical_changed: true }]);
    assert_eq!(engine.get_state(id).await?.payload, state.payload);
    assert_eq!(
        engine.get_events(vec![event.payload.id()]).await?.into_iter().map(|event| event.payload).collect::<Vec<_>>(),
        [event.payload]
    );

    let (later, _) = entity!(2);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &later).await?;

    engine.delete_all().await?;
    trx.commit().await?.committed()?;

    assert_eq!(engine.get_state(later.payload.entity_id).await?.payload, later.payload);
    Ok(())
}
