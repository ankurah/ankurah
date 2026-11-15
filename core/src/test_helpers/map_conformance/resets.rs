//! `delete_all` under an open map transaction.

use std::pin::pin;
use std::task::Poll;

use super::fixtures::{commit, committed, entries, everything, lock, poll_once, put, ranged};
use crate::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};

/// `delete_all` empties every map, and lands nothing of a transaction that
/// made map calls before it: it waits for that transaction to end, or makes
/// its commit fail. Either way the maps end empty, and take writes again.
pub async fn delete_all_empties_every_map_and_lands_nothing_of_an_open_map_transaction<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1), put!("n", "b", 2)]).await?;

    let mut open = engine.transaction();
    open.map_lock_and_read(&[lock!("l")], vec![]).await?;
    open.map_write(vec![put!("m", "c", 3)]).await?;

    let mut deleting = pin!(engine.delete_all());
    let deleted_at_once = match poll_once(deleting.as_mut()) {
        Poll::Ready(result) => {
            result?;
            true
        }

        Poll::Pending => false,
    };

    match deleted_at_once {
        true => {
            let landed = matches!(open.commit().await, Ok(StorageCommitOutcome::Committed(_)));
            assert!(!landed, "a reset that completed while the transaction was open fails its commit");
        }

        // A reset still under way goes on beside the commit, as one in a
        // task of its own does.
        false => futures::future::join(open.commit(), deleting).await.1.map(drop)?,
    }

    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries![]);
    assert_eq!(ranged(committed(engine, everything!("n")).await?), entries![]);

    commit(engine, vec![put!("m", "d", 4)]).await?;
    assert_eq!(ranged(committed(engine, everything!("m")).await?), entries!["d" => 4]);
    Ok(())
}
