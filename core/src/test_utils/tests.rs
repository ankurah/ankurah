//! The storage contract's map conformance suite, run on the test engine, and
//! what the test engine chooses where the contract leaves a choice.

use tokio::sync::oneshot;

use super::TestStorage;
use crate::error::MutationError;
use crate::storage::{LockName, MapId, MapRead, MapRows, MapWrite, StorageEngine, StorageTransaction};
use crate::test_helpers::map_conformance;

/// The suite's cases, those of an engine that locks by name and of one whose
/// snapshots keep their state among them: the test engine's locks under
/// different names keep apart, and a snapshot holds the committed maps it
/// began with.
mod suite {
    use super::{map_conformance, TestStorage};

    /// Runs each case on a fresh test engine.
    macro_rules! on_the_test_engine {
        ($($case:ident),* $(,)?) => {
            $(#[tokio::test]
            async fn $case() -> anyhow::Result<()> { map_conformance::$case(&TestStorage::default()).await })*
        };
    }

    map_conformance::every_case!(on_the_test_engine);
    map_conformance::every_case_for_engines_that_lock_by_name!(on_the_test_engine);
    map_conformance::every_case_for_engines_whose_snapshots_keep_their_state!(on_the_test_engine);
}

/// The test engine's writes never wait of themselves, so that the suite's
/// run of this case finds the write finished; here a write pauses part way,
/// where the case drops it.
#[tokio::test]
async fn a_map_write_paused_and_dropped_part_way_leaves_the_transaction_unable_to_commit() -> anyhow::Result<()> {
    let storage = TestStorage::default();
    let (entered, mut paused) = oneshot::channel();
    let (_release, held) = oneshot::channel();
    *storage.hold_map_write.lock().unwrap() = Some((entered, held));

    map_conformance::a_map_write_dropped_part_way_leaves_the_transaction_unable_to_commit(&storage).await?;

    assert!(paused.try_recv().is_ok(), "the write paused part way, where the case dropped it");
    Ok(())
}

#[tokio::test]
async fn a_map_write_that_fails_part_way_leaves_the_transaction_unable_to_commit() -> anyhow::Result<()> {
    let storage = TestStorage::default();
    let map = MapId::new("m");

    let mut trx = storage.transaction();
    storage.fail_next_map_write();
    let writes = vec![MapWrite::Put { map: map.clone(), key: b"a".to_vec(), value: vec![1] }, MapWrite::Clear { map: map.clone() }];
    assert!(matches!(trx.map_write(writes).await, Err(MutationError::General(_))));

    assert!(matches!(trx.map_read(vec![]).await, Err(MutationError::TransactionFailed)));
    assert!(matches!(trx.commit().await, Err(MutationError::TransactionFailed)));

    let everything = MapRead::Range {
        map,
        start: std::ops::Bound::Unbounded,
        end: std::ops::Bound::Unbounded,
        direction: crate::storage::ScanDirection::Forward,
        limit: usize::MAX,
    };
    let mut snapshot = storage.map_snapshot(&[]).await?;
    assert_eq!(crate::storage::MapSnapshot::map_read(&mut snapshot, vec![everything]).await?, [MapRows::Ranged(Vec::new())]);
    Ok(())
}

/// Where the contract lets `delete_all` wait for an open map transaction or
/// fail its commit, the test engine fails it, as a write conflict to retry,
/// and fails its later map calls too, so that none reads a state the reset
/// broke into. A call that fails so poisons its transaction, as every failed
/// map call does.
#[tokio::test]
async fn a_reset_fails_an_open_map_transactions_later_calls_and_commit_as_a_conflict() -> anyhow::Result<()> {
    let storage = TestStorage::default();

    let (mut committing, mut reading) = (storage.transaction(), storage.transaction());
    committing.map_lock_and_read(&[LockName::new("l")], vec![]).await?;
    reading.map_read(vec![]).await?;
    storage.delete_all().await?;

    assert!(matches!(committing.commit().await, Err(MutationError::WriteConflict)));
    assert!(matches!(reading.map_read(vec![]).await, Err(MutationError::WriteConflict)));
    assert!(matches!(reading.commit().await, Err(MutationError::TransactionFailed)));
    Ok(())
}
