//! The storage contract's map conformance suite, run on SQLite, and SQLite's
//! own cases.
//!
//! SQLite lets one transaction at a time write to a database, so an open map
//! transaction makes every other writer wait, and SQLite runs none of the
//! cases of an engine that locks by name.

use std::sync::Arc;
use std::time::Duration;

use ankurah::core::error::RetrievalError;
use ankurah::core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use ankurah::core::test_helpers::map_conformance::{
    self,
    fixtures::{entity, HANG},
};
use ankurah::proto::Clock;
use ankurah_storage_sqlite::{SqliteConnectionManager, SqliteStorageEngine};
use tokio::sync::Notify;
use tokio::time::timeout;

/// Runs each case on an engine over a database file of its own.
macro_rules! on_a_fresh_database {
    ($($case:ident),* $(,)?) => {
        $(#[tokio::test]
        async fn $case() -> anyhow::Result<()> {
            let engine = SqliteStorageEngine::open_temporary().await?;
            map_conformance::$case(&engine).await
        })*
    };
}

map_conformance::every_case!(on_a_fresh_database);

// A snapshot is a read transaction over the write-ahead log, which keeps its
// state while later commits land.
map_conformance::every_case_for_engines_whose_snapshots_keep_their_state!(on_a_fresh_database);

/// A reset answers whether it deleted anything, the maps' entries among it:
/// once one has emptied the maps, the next finds nothing to delete.
#[tokio::test]
async fn a_reset_answers_whether_it_deleted_anything_the_maps_entries_among_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let engine = SqliteStorageEngine::open(directory.path().join("maps.sqlite")).await?;

    let mut trx = engine.transaction();
    trx.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: b"a".to_vec(), value: vec![1] }]).await?;
    trx.commit().await?.committed()?;

    assert!(engine.delete_all().await?, "the maps held an entry");
    assert!(!engine.delete_all().await?, "nothing was left");
    Ok(())
}

/// A reset waits for an open map transaction, which holds the database's
/// write lock, before it looks for what to delete: so it removes all the
/// transaction's commit landed, its entity's records with its map writes,
/// though the commit made the records' tables.
#[tokio::test]
async fn a_reset_waiting_for_an_open_map_transaction_removes_its_records_with_its_map_writes() -> anyhow::Result<()> {
    static WAITING: Notify = Notify::const_new();

    /// The reset's busy handler: it tells the test the reset waits, and tries
    /// again a moment later, for as long as it takes. The moment paces the
    /// tries, and decides nothing.
    fn waiting(_tries: i32) -> bool {
        WAITING.notify_one();
        std::thread::sleep(Duration::from_millis(1));
        true
    }

    let directory = tempfile::tempdir()?;
    // One connection, so the busy handler installed below is the reset's.
    let pool = bb8::Pool::builder().max_size(1).build(SqliteConnectionManager::file(directory.path().join("maps.sqlite"))).await?;
    let engine = Arc::new(SqliteStorageEngine::new(pool).await?);
    engine.pool().get().await?.with_connection(|connection| Ok(connection.busy_handler(Some(waiting))?)).await?;

    let (state, _) = entity!(1);
    let mut open = engine.transaction();
    open.set_state(&Clock::default(), &state).await?;
    open.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: b"a".to_vec(), value: vec![1] }]).await?;

    let resetting = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move { engine.delete_all().await }
    });

    timeout(HANG, WAITING.notified()).await?;
    open.commit().await?.committed()?;
    assert!(timeout(HANG, resetting).await???, "the reset deleted what the commit landed");

    let found = engine.get_state(state.payload.entity_id).await;
    assert!(matches!(found, Err(RetrievalError::EntityNotFound(_))), "the entity's records are gone: {found:?}");

    let read = MapRead::Get { map: MapId::new("m"), keys: vec![b"a".to_vec()] };
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, [MapRows::Got(vec![None])], "the map write is gone");
    Ok(())
}
