//! A reset of the engine against the map transactions on the database: a
//! reset on one node waits for another node's open map transaction, and
//! removes all data that transaction landed; no map transaction begins until
//! the reset ends, whether or not any
//! map existed when it began; a reset waiting for its own node's map
//! transaction leaves the node's pool to that transaction's commit; one that
//! waits past its node's lock timeout fails with it; and a reset answers
//! whether it removed anything.

use std::time::Duration;

use ankurah::core::error::RetrievalError;
use ankurah::core::storage::{LockName, MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use ankurah::core::test_helpers::map_conformance::fixtures::{entity, HANG};
use ankurah::proto::Clock;
use ankurah_storage_postgres::{MapError, MapSettings};
use tokio::time::timeout;

use crate::database::{map_error, Database};

/// A put of the value 1 under `$key` in the map `$map`.
macro_rules! put {
    ($map:expr, $key:expr) => {
        MapWrite::Put { map: MapId::new($map), key: Vec::from($key), value: vec![1] }
    };
}

/// A read of `$key` in the map `$map`.
macro_rules! get {
    ($map:expr, $key:expr) => {
        MapRead::Get { map: MapId::new($map), keys: vec![Vec::from($key)] }
    };
}

/// A reset on one node waits at the database for another node's open map
/// transaction, whose commit then lands an entity record and a map's entry:
/// once the reset returns, neither is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_removes_what_the_map_transaction_it_waited_for_landed() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let (here, there) = (database.node().await?, database.node().await?);
    let (record, _) = entity!(1);

    let mut open = here.transaction();
    open.set_state(&Clock::default(), &record).await?;
    open.map_lock_and_read(&[LockName::new("l")], vec![]).await?;
    open.map_write(vec![put!("m", "a")]).await?;

    let resetting = tokio::spawn({
        let there = there.clone();
        async move { there.delete_all().await }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    open.commit().await?.committed()?;
    timeout(HANG, resetting).await???;

    let found = there.get_state(record.payload.entity_id).await;
    let entries = there.map_snapshot(&[]).await?.map_read(vec![get!("m", "a")]).await?;

    assert!(matches!(found, Err(RetrievalError::EntityNotFound(_))), "the record is gone: {found:?}");
    assert_eq!(entries, [MapRows::Got(vec![None])], "the map's entry is gone");
    Ok(())
}

/// A map transaction that begins on one node while a reset on another is
/// under way, on a database that holds an entity record and no map yet,
/// waits at the database for the reset to end, and then lands. The reset,
/// as it deletes entity records, waits for the test's writer-blocking table lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_map_transaction_begun_during_a_reset_waits_for_it_though_no_map_existed_when_it_began() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let (here, there) = (database.node().await?, database.node().await?);

    let mut first = here.transaction();
    first.set_state(&Clock::default(), &entity!(1).0).await?;
    first.commit().await?.committed()?;

    let holder = database.connection().await?;
    holder.batch_execute(r#"BEGIN; LOCK TABLE "_ankurah_entity" IN SHARE MODE"#).await?;
    let resetting = tokio::spawn({
        let there = there.clone();
        async move { there.delete_all().await }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    let writing = tokio::spawn({
        let here = here.clone();
        async move {
            let mut trx = here.transaction();
            trx.map_write(vec![put!("m", "a")]).await?;
            trx.commit().await?.committed()?;
            anyhow::Ok(())
        }
    });

    database.sessions_waiting_for_a_lock(2).await?;
    holder.batch_execute("ROLLBACK").await?;
    timeout(HANG, resetting).await???;
    timeout(HANG, writing).await???;

    let entries = there.map_snapshot(&[]).await?.map_read(vec![get!("m", "a")]).await?;
    assert_eq!(entries, [MapRows::Got(vec![Some(vec![1])])], "the map transaction landed, after the reset");
    Ok(())
}

/// A reset on a node whose pool holds one connection waits for the node's
/// own open map transaction, on a database that holds no record yet. The
/// transaction's commit lands without acquiring a second share of the reset
/// lock through that pool; the reset then removes what it landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_waiting_for_its_own_nodes_map_transaction_leaves_the_pool_to_that_transactions_commit() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let engine = database.node_with_a_pool_of_one().await?;

    let mut open = engine.transaction();
    open.set_state(&Clock::default(), &entity!(1).0).await?;
    open.map_lock_and_read(&[LockName::new("l")], vec![]).await?;

    let resetting = tokio::spawn({
        let engine = engine.clone();
        async move { engine.delete_all().await }
    });

    database.sessions_waiting_for_a_lock(1).await?;
    timeout(HANG, open.commit()).await??.committed()?;

    assert!(timeout(HANG, resetting).await???, "the reset removed what the commit landed");
    Ok(())
}

/// A reset that waits for another node's open map transaction past its
/// node's lock timeout, here PostgreSQL's smallest, fails with the maps' lock
/// timeout; once that transaction has ended, the node resets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_that_waits_past_its_nodes_lock_timeout_fails_with_it_and_succeeds_once_the_transaction_ends() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let holder = database.node().await?;
    let impatient = database.node_with(MapSettings { lock_timeout: Duration::from_millis(1), ..MapSettings::default() }).await?;

    let mut open = holder.transaction();
    open.map_lock_and_read(&[LockName::new("l")], vec![]).await?;
    let failed = impatient.delete_all().await;

    open.commit().await?.committed()?;
    impatient.delete_all().await?;

    assert!(matches!(failed.as_ref().map_err(map_error), Err(Some(MapError::LockTimeout(_)))), "{failed:?}");
    Ok(())
}

/// Once a reset has emptied the maps, whose table stays, the next reset finds
/// nothing to remove, and says so.
#[tokio::test]
async fn once_a_reset_has_emptied_the_maps_the_next_answers_that_it_removed_nothing() -> anyhow::Result<()> {
    let database = Database::start().await?;
    let engine = database.node().await?;

    let mut trx = engine.transaction();
    trx.map_write(vec![put!("m", "a")]).await?;
    trx.commit().await?.committed()?;

    assert!(engine.delete_all().await?, "the first reset removed the map's entry");
    assert!(!engine.delete_all().await?, "nothing was left to remove");
    Ok(())
}
