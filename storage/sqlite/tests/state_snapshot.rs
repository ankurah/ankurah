use std::num::NonZeroUsize;

use ankql::ast::{Predicate, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StateScan, StorageEngine, StorageTransaction},
    test_helpers::map_conformance::fixtures::entity,
};
use ankurah_proto::{Attested, Clock, EntityState};
use ankurah_storage_sqlite::{SqliteConnectionManager, SqliteStorageEngine};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

async fn put(engine: &SqliteStorageEngine, previous: &Clock, state: &Attested<EntityState>) -> anyhow::Result<()> {
    let mut transaction = engine.transaction();
    transaction.set_state(previous, state).await?;
    transaction.commit().await?.committed()?;
    Ok(())
}

#[tokio::test]
async fn memory_pages_validate_the_counter_and_release_the_single_connection() -> anyhow::Result<()> {
    let engine = SqliteStorageEngine::open_in_memory().await?;
    let (state, _) = entity!(5);
    put(&engine, &Clock::default(), &state).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.len(), 1);
    put(&engine, &state.payload.state.head, &state).await?;
    assert!(matches!(snapshot.finish().await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    let snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
    engine.delete_all().await?;
    assert!(matches!(snapshot.finish().await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    let snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
    engine
        .pool()
        .get()
        .await?
        .with_connection(|connection| {
            connection.execute("UPDATE _ankurah_meta SET value = X'00' WHERE key = 'state_cursor'", [])?;
            Ok(())
        })
        .await?;
    assert!(matches!(snapshot.finish().await, Err(RetrievalError::StorageError(_))), "corrupt metadata is not a retryable boundary move");
    Ok(())
}

#[tokio::test]
async fn memory_operations_share_the_database_after_pool_connection_expiry() -> anyhow::Result<()> {
    use std::time::Duration;

    use ankurah_core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite};
    use ankurah_proto::{EntityId, ModelId};

    let pool = bb8::Pool::builder()
        .max_size(1)
        .idle_timeout(None)
        .max_lifetime(Duration::from_millis(100))
        .reaper_rate(Duration::from_millis(20))
        .build(SqliteConnectionManager::memory())
        .await?;
    let engine = SqliteStorageEngine::new(pool).await?;
    let (mut first, event) = entity!(1);
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    first.payload.state.memberships.insert(model);
    let map = MapId::new("memory");
    let read = MapRead::Get { map: map.clone(), keys: vec![b"key".to_vec()] };
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &first).await?;
    transaction.add_events(std::slice::from_ref(&event)).await?;
    transaction.map_write(vec![MapWrite::Put { map: map.clone(), key: b"key".to_vec(), value: vec![1] }]).await?;
    transaction.commit().await?.committed()?;
    let cursor = engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?;

    // Observe the pool retiring its handle; elapsed time alone does not prove
    // that the next checkout must obtain a replacement from the manager.
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.pool().state().statistics.connections_closed_max_lifetime == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    assert_eq!(engine.get_state(first.payload.entity_id).await?.payload, first.payload);
    assert_eq!(engine.get_events(vec![event.payload.id()]).await?, [event.clone()]);
    let membership = Selection { predicate: Predicate::MemberOf(model), order_by: None, limit: Some(10) };
    assert_eq!(engine.fetch_states(&membership).await?, [first.clone()]);
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read.clone()]).await?, [MapRows::Got(vec![Some(vec![1])])]);

    // A new model also exercises schema preparation after the pool replacement.
    let (mut second, _) = entity!(2);
    second.payload.state.memberships.insert(ModelId::EntityId(EntityId::from_bytes([102; 32])));
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &second).await?;
    transaction.map_write(vec![MapWrite::Put { map, key: b"key".to_vec(), value: vec![2] }]).await?;
    transaction.commit().await?.committed()?;
    assert_eq!(engine.get_state(second.payload.entity_id).await?.payload, second.payload);

    let mut changes = engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?;
    assert_eq!(changes.next_page(NonZeroUsize::new(10).unwrap()).await?, [second.clone()]);
    changes.finish().await?;
    let all = Selection { predicate: Predicate::True, order_by: None, limit: Some(10) };
    let mut selected = engine.state_snapshot(StateScan::Selection(&all)).await?;
    let rows = selected.next_page(NonZeroUsize::new(10).unwrap()).await?;
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&first) && rows.contains(&second));
    assert_eq!(selected.get_states(&[first.payload.entity_id, second.payload.entity_id]).await?, [Some(first.clone()), Some(second)]);
    selected.finish().await?;
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read.clone()]).await?, [MapRows::Got(vec![Some(vec![2])])]);

    assert!(engine.delete_all().await?);
    assert!(matches!(engine.get_state(first.payload.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert!(engine.get_events(vec![event.payload.id()]).await?.is_empty());
    assert!(engine.fetch_states(&all).await?.is_empty());
    let mut empty = engine.state_snapshot(StateScan::Changes(None)).await?;
    assert!(empty.next_page(NonZeroUsize::new(10).unwrap()).await?.is_empty());
    empty.finish().await?;
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, [MapRows::Got(vec![None])]);
    put(&engine, &Clock::default(), &first).await?;
    assert_eq!(engine.fetch_states(&membership).await?, [first]);
    Ok(())
}

#[tokio::test]
async fn wal_pages_point_reads_and_refill_share_a_snapshot_while_writes_continue() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let pool = bb8::Pool::builder().max_size(1).build(SqliteConnectionManager::file(directory.path().join("snapshot.sqlite"))).await?;
    let engine = SqliteStorageEngine::new(pool).await?;
    let (a, _) = entity!(1);
    let (b, _) = entity!(2);
    let (later, _) = entity!(3);
    put(&engine, &Clock::default(), &a).await?;
    put(&engine, &Clock::default(), &b).await?;
    let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(10) };
    let mut snapshot = engine.state_snapshot(StateScan::Selection(&selection)).await?;
    let first = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?;
    put(&engine, &Clock::default(), &later).await?;
    put(&engine, &a.payload.state.head, &a).await?;
    assert!(snapshot.get_states(&[later.payload.entity_id]).await?[0].is_none());
    let rest = snapshot.next_page(NonZeroUsize::new(10).unwrap()).await?;
    assert_eq!(first.len() + rest.len(), 2);
    let cursor = snapshot.finish().await?;
    let mut caught_up = engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?;
    let mut changed: Vec<_> =
        caught_up.next_page(NonZeroUsize::new(10).unwrap()).await?.into_iter().map(|state| state.payload.entity_id).collect();
    changed.sort();
    let mut expected = vec![a.payload.entity_id, later.payload.entity_id];
    expected.sort();
    assert_eq!(changed, expected);
    caught_up.finish().await?;
    Ok(())
}

#[tokio::test]
async fn event_only_writes_and_failed_resets_leave_continuation_intact() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("reset.sqlite");
    let pool = bb8::Pool::builder().max_size(1).build(SqliteConnectionManager::file(&path)).await?;
    let engine = SqliteStorageEngine::new(pool).await?;
    let (state, event) = entity!(4);
    put(&engine, &Clock::default(), &state).await?;
    let cursor = engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    let mut events = engine.transaction();
    events.add_events(&[event]).await?;
    events.commit().await?.committed()?;
    assert_eq!(engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?, cursor);

    // Refuse a late DROP, after the reset has already dropped the canonical
    // entity table. Its rollback must restore both state and continuation.
    engine
        .pool()
        .get()
        .await?
        .with_connection(|connection| {
            connection.authorizer(Some(|context: AuthContext<'_>| {
                if matches!(context.action, AuthAction::DropTable { table_name: "_ankurah_state_changes" }) {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }));
            Ok(())
        })
        .await?;
    assert!(engine.delete_all().await.is_err());
    engine
        .pool()
        .get()
        .await?
        .with_connection(|connection| {
            connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
            Ok(())
        })
        .await?;
    assert_eq!(engine.get_state(state.payload.entity_id).await?.payload.state.head, state.payload.state.head);
    assert_eq!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?.finish().await?, cursor);
    drop(engine);
    let engine = SqliteStorageEngine::open(&path).await?;
    assert_eq!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?.finish().await?, cursor);
    engine.delete_all().await?;
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
    Ok(())
}

#[path = "../../common/tests/support/state_selection.rs"]
mod selection_cases;

#[tokio::test]
async fn selection_snapshots_match_fetch_ordering_and_membership() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let engine = SqliteStorageEngine::open(directory.path().join("selection.sqlite")).await?;
    selection_cases::selection_matches_fetch(&engine).await?;
    let engine = SqliteStorageEngine::open_in_memory().await?;
    selection_cases::selection_matches_fetch(&engine).await?;
    selection_cases::selection_rejects_changed_boundary(&engine).await?;
    selection_cases::selection_tracks_updated_keys(&engine).await
}

#[tokio::test]
async fn a_new_materialization_during_selection_planning_invalidates_its_boundary() -> anyhow::Result<()> {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use ankql::ast::{OrderByItem, OrderDirection};
    use ankurah_core::{
        property::backend::{LWWBackend, PropertyBackend},
        value::Value,
    };
    use ankurah_proto::{EntityId, ModelId, PropertyId, State, StateBuffers};

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("planning.sqlite");
    let writer = SqliteStorageEngine::open(&path).await?;
    let property = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let state = |byte: u8, score: i64| -> anyhow::Result<_> {
        let backend = LWWBackend::new();
        backend.set(property, Some(Value::I64(score)));
        let event = ankurah_core::test_helpers::commit_backend(&backend, byte)?;
        Ok(Attested::opt(
            EntityState {
                entity_id: EntityId::from_bytes([byte; 32]),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: [ModelId::EntityId(EntityId::from_bytes([100 + byte; 32]))].into_iter().collect(),
                    head: Clock::singleton(&event),
                },
            },
            None,
        ))
    };
    let first = state(1, 1)?;
    let later = state(2, 10)?;
    put(&writer, &Clock::default(), &first).await?;
    let pool = bb8::Pool::builder().max_size(1).build(SqliteConnectionManager::file(&path)).await?;
    let reader = Arc::new(SqliteStorageEngine::new(pool).await?);
    let (entered, paused) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    reader
        .pool()
        .get()
        .await?
        .with_connection(move |connection| {
            let mut entered = Some(entered);
            connection.authorizer(Some(move |context: AuthContext<'_>| {
                // Model enumeration is complete when preparation resolves the
                // first materialization's table name. A new model can now escape
                // this already enumerated query plan.
                if matches!(
                    context.action,
                    AuthAction::Read { table_name: "_ankurah_sqlite_model_map", column_name: "materialization_table_name" }
                ) {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                        if resume.recv_timeout(Duration::from_secs(10)).is_err() {
                            return Authorization::Deny;
                        }
                    }
                }
                Authorization::Allow
            }));
            Ok(())
        })
        .await?;
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: property.into(), direction: OrderDirection::Desc }]),
        limit: Some(1),
    };
    let opening = tokio::spawn({
        let reader = reader.clone();
        let selection = selection.clone();
        async move { reader.state_snapshot(StateScan::Selection(&selection)).await }
    });
    tokio::time::timeout(Duration::from_secs(10), paused).await??;
    let written = put(&writer, &Clock::default(), &later).await;
    release.send(())?;
    written?;
    assert!(matches!(opening.await?, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    let mut snapshot = reader.state_snapshot(StateScan::Selection(&selection)).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?[0].payload.entity_id, later.payload.entity_id);
    snapshot.finish().await?;
    Ok(())
}
