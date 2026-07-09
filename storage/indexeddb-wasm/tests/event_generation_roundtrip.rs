//! Runtime round-trip of the event generation through real IndexedDB
//! (M1 interim review finding F2, approved as a ruling 2026-07-09): the
//! write path stores the generation as a JS number and the read path
//! recovers it exactly, including the u32::MAX saturation sentinel
//! (266-C.iv, the eligibility disable signal), with has_event probed both
//! ways. Compilation alone cannot prove this; the suite runs headless in
//! Chrome (wasm-pack test --headless --chrome).

mod common;

use ankurah::proto::{self, Attested};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
use std::collections::BTreeMap;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
pub async fn event_generation_survives_indexeddb_roundtrip() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;
    let collection = ctx.collection(&"gen_roundtrip".into()).await?;

    let entity_id = proto::EntityId::new();
    let genesis = proto::Event {
        collection: "gen_roundtrip".into(),
        entity_id,
        operations: proto::OperationSet(BTreeMap::new()),
        parent: proto::Clock::default(),
        generation: 1,
    };
    // A saturated stamp: u32::MAX rides through JsValue::from(u32) and
    // as_f64 (exact through 2^53) and must come back bit-identical.
    let saturated = proto::Event {
        collection: "gen_roundtrip".into(),
        entity_id,
        operations: proto::OperationSet(BTreeMap::new()),
        parent: proto::Clock::from(vec![genesis.id()]),
        generation: u32::MAX,
    };

    collection.add_event(&Attested::opt(genesis.clone(), None)).await?;
    collection.add_event(&Attested::opt(saturated.clone(), None)).await?;

    let fetched = collection.get_events(vec![genesis.id(), saturated.id()]).await?;
    assert_eq!(fetched.len(), 2, "both events round-trip");
    let fetch = |id: &proto::EventId| fetched.iter().find(|e| e.payload.id() == *id).expect("event round-trips").payload.clone();

    let genesis_back = fetch(&genesis.id());
    assert_eq!(genesis_back.generation, 1, "genesis generation survives");
    let saturated_back = fetch(&saturated.id());
    assert_eq!(saturated_back.generation, u32::MAX, "the u32::MAX saturation sentinel survives exactly");

    // The generation is inside the hashed content: recomputing the id from
    // the read-back payload proves no field drifted in storage.
    assert_eq!(genesis_back.id(), genesis.id(), "read-back genesis recomputes to the same id");
    assert_eq!(saturated_back.id(), saturated.id(), "read-back saturated event recomputes to the same id");

    // has_event both ways: present ids answer true, an absent id false.
    assert!(collection.has_event(&genesis.id()).await?, "has_event finds the stored genesis");
    assert!(collection.has_event(&saturated.id()).await?, "has_event finds the stored saturated event");
    assert!(!collection.has_event(&proto::EventId::from_bytes([0x5A; 32])).await?, "has_event answers false for an absent id");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
