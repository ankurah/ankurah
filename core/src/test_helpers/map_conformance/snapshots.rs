//! Snapshots: one committed state of what transactions write under their
//! names, while a transaction under them commits.

use std::pin::pin;
use std::task::Poll;

use super::fixtures::{commit, committed, committed_under, found, get, lock, only, poll_once, put};
use crate::error::{MutationError, RetrievalError};
use crate::storage::{MapSnapshot, StorageEngine};

/// A snapshot sees one committed state of what transactions write under its
/// names, while a transaction under them commits: the entries before that
/// commit, never some of each, or a retryable failure on an engine that can't
/// hold that state. A snapshot begun once the commit has completed sees it.
pub async fn a_snapshot_sees_one_committed_state_while_a_transaction_commits<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1), put!("m", "b", 1)]).await?;

    let mut snapshot = engine.map_snapshot(&[lock!("l")]).await?;
    assert_eq!(found(only(snapshot.map_read(vec![get!("m", ["a"])]).await?)), [Some(1)]);

    let mut committing = pin!(committed_under(engine, lock!("l"), vec![put!("m", "a", 2), put!("m", "b", 2)]));
    let committed_at_once = match poll_once(committing.as_mut()) {
        Poll::Ready(result) => {
            result?;
            true
        }

        Poll::Pending => false,
    };

    match snapshot.map_read(vec![get!("m", ["b", "a"])]).await {
        Ok(rows) => assert_eq!(found(only(rows)), [Some(1), Some(1)], "one state throughout"),
        Err(RetrievalError::MutationError(failure)) if matches!(*failure, MutationError::WriteConflict) => {}
        Err(failure) => return Err(failure.into()),
    }

    drop(snapshot);
    if !committed_at_once {
        committing.await?;
    }

    assert_eq!(found(committed(engine, get!("m", ["a", "b"])).await?), [Some(2), Some(2)], "a snapshot after the commit");
    Ok(())
}

/// On an engine whose snapshots keep their state across commits, a snapshot
/// open while a transaction under its names commits goes on seeing the
/// entries before that commit, which completes without waiting for it.
pub async fn a_snapshot_keeps_its_state_across_a_commit_under_its_names<E: StorageEngine>(engine: &E) -> anyhow::Result<()> {
    commit(engine, vec![put!("m", "a", 1), put!("m", "b", 1)]).await?;

    let mut snapshot = engine.map_snapshot(&[lock!("l")]).await?;
    assert_eq!(found(only(snapshot.map_read(vec![get!("m", ["a"])]).await?)), [Some(1)]);

    committed_under(engine, lock!("l"), vec![put!("m", "a", 2), put!("m", "b", 2)]).await?;

    assert_eq!(found(only(snapshot.map_read(vec![get!("m", ["b", "a"])]).await?)), [Some(1), Some(1)], "the state it began at");
    assert_eq!(found(committed(engine, get!("m", ["a", "b"])).await?), [Some(2), Some(2)], "a snapshot after the commit");
    Ok(())
}
