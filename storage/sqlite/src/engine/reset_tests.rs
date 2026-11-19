use std::sync::mpsc;

use ankurah_core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use ankurah_core::test_helpers::map_conformance::fixtures::{entity, HANG};
use ankurah_proto::Clock;
use futures_util::FutureExt;
use tokio::runtime::{Builder, Runtime};

use super::SqliteStorageEngine;
use crate::connection::SqliteConnectionManager;

/// Dispatch the commit only after its preparation call has completed, then
/// cancel it while the native transaction still waits for a blocking thread.
#[test]
fn reset_drains_cancelled_queued_commits_without_map_calls() -> anyhow::Result<()> {
    for states in [false, true] {
        let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
        let free = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
        free.block_on(async {
            let engine = engine().await?;
            let first = engine.pool.get().await?;
            let reserved = engine.pool.get().await?;
            let observed = first.clone();
            drop(first);

            let (state, event) = entity!(1);
            let id = state.payload.entity_id;
            let event_id = event.payload.id();
            let mut trx = engine.transaction();
            trx.add_events(&[event]).await?;
            if states {
                trx.set_state(&Clock::default(), &state).await?;
            }
            let preparation = block(&free).await?;
            let mut commit = Box::pin(trx.commit());
            assert!(commit.as_mut().now_or_never().is_none());
            preparation.send(())?;
            // The same connection's mutex puts this after the preparation
            // call. The commit future has not yet been polled to dispatch DML.
            observed.with_connection(|_| Ok(())).await?;

            let release = block(&occupied).await?;
            {
                let _runtime = occupied.enter();
                assert!(commit.as_mut().now_or_never().is_none());
            }
            drop(commit);

            // Keep the commit's pooled handle out of circulation: reset must
            // use another connection, not merely wait on its connection mutex.
            let _busy = engine.pool.get().await?;
            drop(reserved);
            let mut reset = Box::pin(engine.delete_all());
            assert!(reset.as_mut().now_or_never().is_none());
            // If reset had queued native work, this marker would run after it.
            tokio::task::spawn_blocking(|| ()).await?;
            let waited = reset.as_mut().now_or_never().is_none();
            release.send(())?;
            assert!(waited, "reset must drain the cancelled queued commit (states={states})");
            tokio::time::timeout(HANG, reset).await??;
            drop(_busy);
            assert!(engine.get_state(id).await.is_err());
            assert!(engine.get_events(vec![event_id]).await?.is_empty());
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}

#[test]
fn reset_drains_cancelled_map_commit_and_its_cleanup() -> anyhow::Result<()> {
    let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
    let free = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
    free.block_on(async {
        let engine = engine().await?;
        let map = MapId::new("reset");
        let mut trx = engine.transaction();
        trx.map_write(vec![MapWrite::Put { map: map.clone(), key: b"k".to_vec(), value: b"v".to_vec() }]).await?;

        let release = block(&occupied).await?;
        let mut commit = Box::pin(trx.commit());
        {
            let _runtime = occupied.enter();
            assert!(commit.as_mut().now_or_never().is_none());
        }
        drop(commit);
        let mut reset = Box::pin(engine.delete_all());
        assert!(reset.as_mut().now_or_never().is_none());
        release.send(())?;
        tokio::time::timeout(HANG, reset).await??;
        let read = MapRead::Get { map, keys: vec![b"k".to_vec()] };
        assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, vec![MapRows::Got(vec![None])]);
        Ok(())
    })
}

#[test]
fn reset_drains_cancelled_schema_preparation() -> anyhow::Result<()> {
    let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
    let free = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
    free.block_on(async {
        let engine = engine().await?;
        let writer = engine.pool.get().await?;
        // Reset has an idle second connection and needs no native pool validation.
        drop(engine.pool.get().await?);
        let release = block(&occupied).await?;
        let mut prepare = Box::pin(engine.ensure_shared_tables(&writer));
        {
            let _runtime = occupied.enter();
            assert!(prepare.as_mut().now_or_never().is_none());
        }
        drop(prepare);
        let mut reset = Box::pin(engine.delete_all());
        assert!(reset.as_mut().now_or_never().is_none());
        tokio::task::spawn_blocking(|| ()).await?;
        let waited = reset.as_mut().now_or_never().is_none();
        release.send(())?;
        assert!(waited, "schema creation is native work even without an entity commit");
        tokio::time::timeout(HANG, reset).await??;
        assert!(!writer.with_connection(|connection| super::table_exists(connection, super::ENTITY_TABLE)).await?);
        Ok(())
    })
}

async fn engine() -> anyhow::Result<SqliteStorageEngine> {
    let manager = SqliteConnectionManager::temporary(tempfile::tempdir()?);
    let pool = bb8::Pool::builder().max_size(2).test_on_check_out(false).build(manager).await?;
    Ok(SqliteStorageEngine::new(pool).await?)
}

async fn block(runtime: &Runtime) -> anyhow::Result<mpsc::Sender<()>> {
    let (release, blocked) = mpsc::channel();
    let (started, entered) = tokio::sync::oneshot::channel();
    runtime.spawn_blocking(move || {
        let _ = started.send(());
        blocked.recv_timeout(HANG).ok();
    });
    entered.await?;
    Ok(release)
}
