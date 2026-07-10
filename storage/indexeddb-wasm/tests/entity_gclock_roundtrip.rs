//! Runtime round-trip of the per-tip head generations through real
//! IndexedDB (GClock pin (iii), D2 M4, plan REV 5 section K home 3): the
//! write path stores the annotation under the entity record's
//! __generations key as [generation, eventIdBase64] pairs and the read
//! path (get_state) reconstitutes it exactly, including a multi-tip head
//! and the u32::MAX saturation sentinel, so rehydration never reads
//! events. Compilation alone cannot prove this; the suite runs headless
//! in Chrome (wasm-pack test --headless --chrome).

mod common;

use ankurah::proto::{self, Attested};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
use std::collections::BTreeMap;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
pub async fn entity_head_generations_survive_indexeddb_roundtrip() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;
    let collection = ctx.collection(&"gclock_roundtrip".into()).await?;

    let entity_id = proto::EntityId::new();
    let (e1, e2) = (proto::EventId::from_bytes([1; 32]), proto::EventId::from_bytes([2; 32]));
    let head = proto::Clock::from(vec![e1.clone(), e2.clone()]);
    let head_generations = proto::GClock::new(vec![(7, e1.clone()), (u32::MAX, e2.clone())]);
    let state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::new()),
        head: head.clone(),
        head_generations: head_generations.clone(),
    };
    let attested = Attested::opt(proto::EntityState { entity_id, collection: "gclock_roundtrip".into(), state }, None);

    collection.set_state(attested).await?;

    let read = collection.get_state(entity_id).await?;
    assert_eq!(read.payload.state.head, head, "the head round-trips (precondition)");
    assert_eq!(
        read.payload.state.head_generations, head_generations,
        "GClock pin (iii): the per-tip head generations must survive set_state + get_state rehydration, u32::MAX included"
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
