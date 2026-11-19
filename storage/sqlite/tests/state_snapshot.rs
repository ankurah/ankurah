use std::num::NonZeroUsize;

use ankql::ast::{Predicate, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StorageEngine, StorageTransaction},
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
    let mut snapshot = engine.state_snapshot(None).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.len(), 1);
    put(&engine, &state.payload.state.head, &state).await?;
    assert!(matches!(snapshot.finish().await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    let snapshot = engine.state_snapshot(None).await?;
    engine.delete_all().await?;
    assert!(matches!(snapshot.finish().await, Err(RetrievalError::InvalidStateCursor)));
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
    let mut snapshot = engine.state_snapshot(None).await?;
    let first = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?;
    put(&engine, &Clock::default(), &later).await?;
    put(&engine, &a.payload.state.head, &a).await?;
    assert!(snapshot.get_states(&[later.payload.entity_id]).await?[0].is_none());
    let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(10) };
    let selected = snapshot.limited_states(&selection).await?;
    assert_eq!(selected.len(), 2);
    let rest = snapshot.next_page(NonZeroUsize::new(10).unwrap()).await?;
    assert_eq!(first.len() + rest.len(), 2);
    let cursor = snapshot.finish().await?;
    let mut caught_up = engine.state_snapshot(Some(&cursor)).await?;
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
    let cursor = engine.state_snapshot(None).await?.finish().await?;
    let mut events = engine.transaction();
    events.add_events(&[event]).await?;
    events.commit().await?.committed()?;
    assert_eq!(engine.state_snapshot(None).await?.finish().await?, cursor);

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
    assert_eq!(engine.state_snapshot(Some(&cursor)).await?.finish().await?, cursor);
    drop(engine);
    let engine = SqliteStorageEngine::open(&path).await?;
    assert_eq!(engine.state_snapshot(Some(&cursor)).await?.finish().await?, cursor);
    engine.delete_all().await?;
    assert!(matches!(engine.state_snapshot(Some(&cursor)).await, Err(RetrievalError::InvalidStateCursor)));
    Ok(())
}
