use std::num::NonZeroUsize;

use ankql::ast::{Predicate, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StateScan, StorageEngine, StorageTransaction},
    test_helpers::map_conformance::fixtures::entity,
};
use ankurah_proto::{Attested, Clock, EntityState};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

async fn put(engine: &IndexedDBStorageEngine, previous: &Clock, state: &Attested<EntityState>) -> anyhow::Result<()> {
    let mut transaction = engine.transaction();
    transaction.set_state(previous, state).await?;
    transaction.commit().await?.committed()?;
    Ok(())
}

#[wasm_bindgen_test]
async fn continuation_coalesces_snapshot_writes_but_not_event_receipts() -> anyhow::Result<()> {
    let name = format!("state_cursor_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let (a, event) = entity!(1);
    let (b, _) = entity!(2);
    put(&engine, &Clock::default(), &a).await?;
    let before = engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    let mut events = engine.transaction();
    events.add_events(&[event]).await?;
    events.commit().await?.committed()?;
    assert_eq!(engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?, before);
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
    let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(1) };
    let mut selected = engine.state_snapshot(StateScan::Selection(&selection)).await?;
    assert_eq!(selected.next_page(NonZeroUsize::new(1).unwrap()).await?[0].payload.entity_id, expected[0]);
    selected.finish().await?;
    let cursor = snapshot.finish().await?;
    engine.db.close().await;
    drop(engine);
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(Some(&cursor))).await?;
    assert!(snapshot.next_page(NonZeroUsize::new(10).unwrap()).await?.is_empty());
    snapshot.finish().await?;
    let active = engine.state_snapshot(StateScan::Changes(None)).await?;
    engine.delete_all().await?;
    assert!(matches!(active.finish().await, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    assert!(matches!(engine.state_snapshot(StateScan::Changes(Some(&cursor))).await, Err(RetrievalError::InvalidStateCursor)));
    IndexedDBStorageEngine::cleanup(&name).await
}

#[wasm_bindgen_test]
async fn another_handle_cannot_change_a_paged_snapshot_without_invalidating_it() -> anyhow::Result<()> {
    let name = format!("state_cursor_race_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let writer = IndexedDBStorageEngine::open(&name).await?;
    let (state, _) = entity!(3);
    put(&engine, &Clock::default(), &state).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(None)).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.len(), 1);
    // Yield beyond the first native transaction's lifetime, then commit
    // through another handle as another tab would.
    gloo_timers::future::TimeoutFuture::new(1).await;
    put(&writer, &state.payload.state.head, &state).await?;
    for result in [snapshot.get_states(&[state.payload.entity_id]).await.map(|_| ()), snapshot.finish().await.map(|_| ())] {
        assert!(matches!(result, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    }
    IndexedDBStorageEngine::cleanup(&name).await
}

#[wasm_bindgen_test]
async fn state_positions_preserve_integers_above_javascript_number_precision() -> anyhow::Result<()> {
    use ankurah_storage_common::state::CounterCursor;
    use send_wrapper::SendWrapper;
    use wasm_bindgen::JsValue;

    let name = format!("state_cursor_integer_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let mut counter = CounterCursor::decode(&engine.state_snapshot(StateScan::Changes(None)).await?.finish().await?, b"indexeddb")?;
    counter.position = (1u64 << 53) + 7;
    let before = counter.encode(b"indexeddb");
    let connection = engine.db.get_connection().await;
    SendWrapper::new(async move {
        let transaction = connection
            .transaction_with_str_and_mode("_ankurah_meta", web_sys::IdbTransactionMode::Readwrite)
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
        transaction
            .object_store("_ankurah_meta")
            .map_err(|error| anyhow::anyhow!("{error:?}"))?
            .put_with_key(&js_sys::Uint8Array::from(counter.encode(b"indexeddb").as_bytes()), &JsValue::from_str("state_cursor"))
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
        let completion = js_sys::Promise::new(&mut |resolve, reject| {
            transaction.set_oncomplete(Some(&resolve));
            transaction.set_onabort(Some(&reject));
        });
        wasm_bindgen_futures::JsFuture::from(completion).await.map_err(|error| anyhow::anyhow!("{error:?}"))?;
        anyhow::Ok(())
    })
    .await?;
    let (state, _) = entity!(6);
    put(&engine, &Clock::default(), &state).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Changes(Some(&before))).await?;
    assert_eq!(snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?[0].payload.entity_id, state.payload.entity_id);
    let after = CounterCursor::decode(&snapshot.finish().await?, b"indexeddb")?;
    assert_eq!(after.position, counter.position + 1);
    IndexedDBStorageEngine::cleanup(&name).await
}

#[path = "../../common/tests/support/state_selection.rs"]
mod selection_cases;

#[wasm_bindgen_test]
async fn selection_snapshots_match_fetch_ordering_and_membership() -> anyhow::Result<()> {
    let name = format!("state_selection_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    selection_cases::selection_matches_fetch(&engine).await?;
    selection_cases::selection_rejects_changed_boundary(&engine).await?;
    selection_cases::selection_tracks_updated_keys(&engine).await?;
    IndexedDBStorageEngine::cleanup(&name).await
}
