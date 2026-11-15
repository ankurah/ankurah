//! A reset of the records and the maps together: dropped with its requests
//! still queued, it lands nothing.

use std::pin::pin;

use ankurah::core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use ankurah::core::test_helpers::map_conformance::fixtures::{entity, poll_once};
use ankurah::proto::Clock;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::wasm_bindgen_test;

/// A reset dropped with its requests still queued is aborted, and nothing of
/// it lands: the records and the maps stay as they were.
#[wasm_bindgen_test]
async fn a_reset_dropped_with_its_requests_still_queued_lands_nothing() -> anyhow::Result<()> {
    let name = format!("map_resets_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;

    let (state, _) = entity!(1);
    let mut trx = engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;
    trx.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: b"a".to_vec(), value: vec![1] }]).await?;
    trx.commit().await?.committed()?;

    {
        let mut reset = pin!(engine.delete_all());
        assert!(poll_once(reset.as_mut()).is_pending(), "the reset waits for its transaction to complete");
    }

    assert_eq!(engine.get_state(state.payload.entity_id).await?.payload, state.payload);
    let read = MapRead::Get { map: MapId::new("m"), keys: vec![b"a".to_vec()] };
    assert_eq!(engine.map_snapshot(&[]).await?.map_read(vec![read]).await?, [MapRows::Got(vec![Some(vec![1])])]);

    IndexedDBStorageEngine::cleanup(&name).await
}
