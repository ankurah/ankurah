use std::num::NonZeroUsize;

use ankql::ast::{Predicate, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StateScan, StorageCommitOutcome, StorageEngine, StorageTransaction},
    test_helpers::map_conformance::fixtures::entity,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState};
use ankurah_storage_sled::SledStorageEngine;

async fn put(engine: &SledStorageEngine, previous: &Clock, state: &Attested<EntityState>) -> anyhow::Result<()> {
    let mut transaction = engine.transaction();
    transaction.set_state(previous, state).await?;
    transaction.commit().await?.committed()?;
    Ok(())
}

#[tokio::test]
async fn continuation_coalesces_state_writes_and_ignores_events_and_conflicts() -> anyhow::Result<()> {
    let engine = SledStorageEngine::new_test()?;
    let (a, event) = entity!(1);
    let (b, _) = entity!(2);
    put(&engine, &Clock::default(), &a).await?;
    let before = engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?;

    let mut events = engine.transaction();
    events.add_events(&[event]).await?;
    events.commit().await?.committed()?;
    let mut conflict = engine.transaction();
    conflict.set_state(&Clock::default(), &a).await?;
    assert!(matches!(conflict.commit().await?, StorageCommitOutcome::Conflict { .. }));
    assert_eq!(engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?, before);

    // These writes have no new event; repeated changes retain one latest entry.
    put(&engine, &a.payload.state.head, &a).await?;
    put(&engine, &a.payload.state.head, &a).await?;
    put(&engine, &Clock::default(), &b).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(Some(&before))).await?;
    let mut found = Vec::new();
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?;
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        found.push(page[0].payload.entity_id);
    }
    found.sort();
    let mut expected = vec![a.payload.entity_id, b.payload.entity_id];
    expected.sort();
    assert_eq!(found, expected);
    let after = snapshot.finish().await?;
    let mut unchanged = engine.state_snapshot(StateScan::Changes(Some(&after))).await?;
    assert!(unchanged.next_page(NonZeroUsize::new(10).unwrap()).await?.is_empty());

    let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(1) };
    let mut selected = engine.state_snapshot(StateScan::Selection(&selection)).await?;
    assert_eq!(selected.next_page(NonZeroUsize::new(1).unwrap()).await?[0].payload.entity_id, expected[0]);
    selected.finish().await?;
    let ids = [b.payload.entity_id, EntityId::random(), a.payload.entity_id];
    let states = unchanged.get_states(&ids).await?;
    assert_eq!(states[0].as_ref().unwrap().payload.entity_id, ids[0]);
    assert!(states[1].is_none());
    assert_eq!(states[2].as_ref().unwrap().payload.entity_id, ids[2]);
    unchanged.finish().await?;
    Ok(())
}

#[tokio::test]
async fn a_write_between_pages_invalidates_the_snapshot_and_finish() -> anyhow::Result<()> {
    let engine = SledStorageEngine::new_test()?;
    let (a, _) = entity!(3);
    put(&engine, &Clock::default(), &a).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.len(), 1);
    put(&engine, &a.payload.state.head, &a).await?;
    for result in [snapshot.next_page(NonZeroUsize::new(1).unwrap()).await.map(|_| ()), snapshot.finish().await.map(|_| ())] {
        assert!(matches!(result, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    }
    Ok(())
}

#[tokio::test]
async fn cursors_survive_reopen_but_not_reset_or_another_store() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let engine = SledStorageEngine::with_path(directory.path().to_owned())?;
    let (a, _) = entity!(4);
    put(&engine, &Clock::default(), &a).await?;
    let cursor = engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    engine.database.lock().unwrap().db.flush()?;
    drop(engine);
    let engine = SledStorageEngine::with_path(directory.path().to_owned())?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?;
    assert!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.is_empty());
    snapshot.finish().await?;
    let other = SledStorageEngine::new_test()?;
    assert!(matches!(other.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
    let active = engine.state_snapshot(StateScan::Changes(None)).await?;
    engine.delete_all().await?;
    assert!(matches!(active.finish().await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
    Ok(())
}

#[path = "../../common/tests/support/state_selection.rs"]
mod selection_cases;

#[tokio::test]
async fn selection_snapshots_match_fetch_ordering_and_membership() -> anyhow::Result<()> {
    let engine = SledStorageEngine::new_test()?;
    selection_cases::selection_matches_fetch(&engine).await?;
    selection_cases::selection_rejects_changed_boundary(&engine).await?;
    selection_cases::selection_tracks_updated_keys(&engine).await
}
