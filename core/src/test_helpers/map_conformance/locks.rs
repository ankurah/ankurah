//! Lock names: a transaction waits for one another holds, and takes its
//! names once each, in ascending order.

use std::pin::pin;

use ankurah_proto::Clock;

use super::fixtures::{committed, entity, found, get, lock, only, out_of_order, poll_once, put};
use crate::storage::{StorageEngine, StorageTransaction};

/// A transaction asking for a lock another holds waits until that one ends,
/// and then reads what it committed. The two are one engine's: where the
/// holder is another process, as another tab is to IndexedDB, an engine may
/// fail the asker's commit instead, which its own tests show.
pub async fn a_lock_waits_for_the_transaction_holding_it_to_end<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let mut first = engine.transaction();
    first.map_lock_and_read(&[lock!("l")], vec![]).await?;

    let mut second = engine.transaction();
    let held = [lock!("l")];
    let mut waiting = pin!(second.map_lock_and_read(&held, vec![get!("m", ["a"])]));
    assert!(poll_once(waiting.as_mut()).is_pending(), "the second waits while the first holds the lock");

    first.map_write(vec![put!("m", "a", 1)]).await?;
    first.commit().await?.committed()?;

    assert_eq!(found(only(waiting.await?)), [Some(1)], "the second reads once the first has ended");
    Ok(())
}

/// On an engine that locks by name, a transaction asking for a lock under
/// another name than one held doesn't wait for the transaction holding it.
pub async fn a_lock_under_another_name_doesnt_wait_for_the_one_held<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let mut first = engine.transaction();
    first.map_lock_and_read(&[lock!("l")], vec![]).await?;

    let mut elsewhere = engine.transaction();
    elsewhere.map_lock_and_read(&[lock!("k")], vec![]).await?;
    Ok(())
}

/// A call that asks for a lock name taken before, or for one before a name
/// taken, fails, within one call and across calls. A call that fails takes
/// none of its names.
pub async fn a_lock_name_taken_twice_or_out_of_order_fails_the_call<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let mut trx = engine.transaction();

    let failure = out_of_order(trx.map_lock_and_read(&[lock!("b"), lock!("a")], vec![]).await);
    assert_eq!(failure, (lock!("a"), lock!("b")));

    let failure = out_of_order(trx.map_lock_and_read(&[lock!("a"), lock!("a")], vec![]).await);
    assert_eq!(failure, (lock!("a"), lock!("a")));

    trx.map_lock_and_read(&[lock!("b")], vec![]).await?;

    let failure = out_of_order(trx.map_lock_and_read(&[lock!("b")], vec![]).await);
    assert_eq!(failure, (lock!("b"), lock!("b")));

    let failure = out_of_order(trx.map_lock_and_read(&[lock!("a")], vec![]).await);
    assert_eq!(failure, (lock!("a"), lock!("b")));

    trx.map_lock_and_read(&[lock!("c")], vec![]).await?;
    Ok(())
}

/// On an engine that locks by name, a transaction that makes no map call
/// doesn't wait for an open map transaction: it commits while another holds
/// locks and map writes, landing its records and none of the other's writes.
pub async fn a_transaction_that_touches_no_map_doesnt_wait_for_an_open_map_transaction<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    let mut holder = engine.transaction();
    holder.map_lock_and_read(&[lock!("l")], vec![]).await?;
    holder.map_write(vec![put!("m", "a", 1)]).await?;

    let (state, _) = entity!(1);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;
    trx.commit().await?.committed()?;

    assert_eq!(engine.get_state(state.payload.entity_id).await?.payload, state.payload);
    assert_eq!(found(committed(engine, get!("m", ["a"])).await?), [None]);
    Ok(())
}
