use std::num::NonZeroUsize;
use std::sync::mpsc;

use ankurah_core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StateScan, StorageEngine, StorageTransaction};
use ankurah_core::test_helpers::map_conformance::fixtures::{entity, HANG};
use ankurah_proto::{Clock, EntityId, ModelId};
use futures::FutureExt;
use tokio::runtime::Builder;

use super::SledStorageEngine;

#[test]
fn reset_drains_cancelled_queued_commits() -> anyhow::Result<()> {
    for (states, maps) in [(false, false), (true, false), (true, true)] {
        let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
        let free = Builder::new_current_thread().enable_all().build()?;
        free.block_on(async {
            let engine = SledStorageEngine::new_test()?;
            let (mut state, event) = entity!(1);
            let model = ModelId::EntityId(EntityId::from_bytes([7; 32]));
            state.payload.state.memberships.insert(model);
            let id = state.payload.entity_id;
            let event_id = event.payload.id();
            let map = MapId::new("reset");
            let mut trx = engine.transaction();
            trx.add_events(&[event]).await?;
            if states {
                trx.set_state(&Clock::default(), &state).await?;
            }
            if maps {
                trx.map_write(vec![MapWrite::Put { map: map.clone(), key: b"k".to_vec(), value: b"v".to_vec() }]).await?;
            }

            let (release, blocked) = mpsc::channel::<()>();
            let (started, entered) = tokio::sync::oneshot::channel();
            occupied.spawn_blocking(move || {
                let _ = started.send(());
                blocked.recv_timeout(HANG).ok();
            });
            entered.await?;
            let mut commit = Box::pin(trx.commit());
            {
                let _runtime = occupied.enter();
                assert!(commit.as_mut().now_or_never().is_none());
            }
            drop(commit);

            let mut reset = Box::pin(engine.delete_all());
            let waited = reset.as_mut().now_or_never().is_none();
            release.send(())?;
            assert!(waited, "reset must drain a dispatched commit after its caller drops (states={states}, maps={maps})");
            tokio::time::timeout(HANG, reset).await??;
            assert!(engine.get_state(id).await.is_err());
            assert!(engine.get_events(vec![event_id]).await?.is_empty());
            assert!(engine.list_models()?.is_empty());
            let read = MapRead::Get { map, keys: vec![b"k".to_vec()] };
            assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, vec![MapRows::Got(vec![None])]);
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}

#[test]
fn reset_drains_a_cancelled_queued_indexed_query() -> anyhow::Result<()> {
    let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
    let free = Builder::new_current_thread().enable_all().build()?;
    free.block_on(async {
        let engine = SledStorageEngine::new_test()?;
        let (mut state, _) = entity!(2);
        let model = ModelId::EntityId(EntityId::from_bytes([7; 32]));
        state.payload.state.memberships.insert(model);
        let mut trx = engine.transaction();
        trx.set_state(&Clock::default(), &state).await?;
        trx.commit().await?.committed()?;
        let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }.and_member_of(model);

        let (release, blocked) = mpsc::channel::<()>();
        let (started, entered) = tokio::sync::oneshot::channel();
        occupied.spawn_blocking(move || {
            let _ = started.send(());
            blocked.recv_timeout(HANG).ok();
        });
        entered.await?;
        let mut query = Box::pin(engine.fetch_states(&selection));
        {
            let _runtime = occupied.enter();
            assert!(query.as_mut().now_or_never().is_none());
        }
        drop(query);
        let mut reset = Box::pin(engine.delete_all());
        let waited = reset.as_mut().now_or_never().is_none();
        release.send(())?;
        assert!(waited, "an indexed query can create an index after its caller drops");
        tokio::time::timeout(HANG, reset).await??;
        assert!(engine.list_models()?.is_empty());
        assert!(engine.database.lock().unwrap().index_manager.indexes.read().unwrap().is_empty());
        Ok(())
    })
}

#[test]
fn repeated_resets_drain_cancelled_general_queries_and_snapshot_refills() -> anyhow::Result<()> {
    for refill in [false, true] {
        let occupied = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
        let free = Builder::new_current_thread().max_blocking_threads(1).enable_all().build()?;
        free.block_on(async {
            let engine = SledStorageEngine::new_test()?;
            let first = ModelId::EntityId(EntityId::from_bytes([7; 32]));
            let second = ModelId::EntityId(EntityId::from_bytes([8; 32]));
            // An OR over two memberships uses the general materialization join.
            let selection = ankql::ast::Selection {
                predicate: ankql::ast::Predicate::Or(
                    Box::new(ankql::ast::Predicate::MemberOf(first)),
                    Box::new(ankql::ast::Predicate::MemberOf(second)),
                ),
                order_by: None,
                limit: Some(2),
            };
            for generation in 0..2 {
                let (mut state, _) = entity!(generation + 3);
                state.payload.state.memberships.insert(first);
                let mut transaction = engine.transaction();
                transaction.set_state(&Clock::default(), &state).await?;
                transaction.commit().await?.committed()?;
                let mut snapshot = engine.state_snapshot(StateScan::SelectionChanges { selection: &selection, since: None }).await?;

                let (release, blocked) = mpsc::channel::<()>();
                let (started, entered) = tokio::sync::oneshot::channel();
                occupied.spawn_blocking(move || {
                    let _ = started.send(());
                    blocked.recv_timeout(HANG).ok();
                });
                entered.await?;
                let mut query = Box::pin(async {
                    if refill {
                        snapshot.select_after(None, NonZeroUsize::new(2).unwrap()).await
                    } else {
                        engine.fetch_states(&selection).await
                    }
                });
                if refill {
                    assert!(query.as_mut().now_or_never().is_none());
                    // Finish the refill's validation on this runtime's one
                    // blocking thread before dispatching its query elsewhere.
                    tokio::task::spawn_blocking(|| ()).await?;
                }
                {
                    let _runtime = occupied.enter();
                    assert!(query.as_mut().now_or_never().is_none());
                }
                drop(query);

                let mut reset = Box::pin(engine.delete_all());
                let waited = reset.as_mut().now_or_never().is_none();
                release.send(())?;
                assert!(waited, "reset must drain the native general query (refill={refill}, generation={generation})");
                tokio::time::timeout(HANG, reset).await??;
                assert!(engine.list_models()?.is_empty());
                // Keeping the pure snapshot did not hold up reset. A later
                // refill must reject its old database before opening any tree.
                assert!(snapshot.select_after(None, NonZeroUsize::new(2).unwrap()).await.is_err());
                assert!(engine.list_models()?.is_empty());
            }
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}
