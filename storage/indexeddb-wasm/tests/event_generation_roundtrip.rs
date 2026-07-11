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
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Await one IdbRequest's success (returning its result) through a Promise
/// bridge; the crate's own cb_future util is private to it.
async fn await_request(req: &web_sys::IdbRequest) -> JsValue {
    use wasm_bindgen::closure::Closure;
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let ok_req = req.clone();
        let ok = Closure::once(move |_e: web_sys::Event| {
            let _ = resolve.call1(&JsValue::NULL, &ok_req.result().unwrap_or(JsValue::UNDEFINED));
        });
        let err = Closure::once(move |_e: web_sys::Event| {
            let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("idb request error"));
        });
        req.set_onsuccess(Some(ok.as_ref().unchecked_ref()));
        req.set_onerror(Some(err.as_ref().unchecked_ref()));
        ok.forget();
        err.forget();
    });
    wasm_bindgen_futures::JsFuture::from(promise).await.expect("idb request succeeds")
}

/// Overwrite the stored __generation of one event row IN PLACE through a
/// raw second IndexedDB connection, bypassing the engine's write path
/// entirely: the doctored-storage seam for the read-path pins below.
async fn doctor_stored_generation(db_name: &str, event_key: &str, generation: JsValue) {
    let open_req = web_sys::window()
        .expect("browser test has a window")
        .indexed_db()
        .expect("indexedDB accessible")
        .expect("indexedDB present")
        .open(db_name)
        .expect("open request");
    let db: web_sys::IdbDatabase = await_request(open_req.unchecked_ref()).await.dyn_into().expect("open yields a database");

    let txn = db.transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite).expect("events transaction");
    let store = txn.object_store("events").expect("events store");
    let row = await_request(&store.get(&event_key.into()).expect("get request")).await;
    assert!(!row.is_undefined() && !row.is_null(), "precondition: the honest row is stored under {event_key}");
    js_sys::Reflect::set(&row, &JsValue::from_str("__generation"), &generation).expect("doctor the generation field");
    await_request(&store.put_with_key(&row, &event_key.into()).expect("put request")).await;
    db.close();
}

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

/// M6 close-out (routed from the M4 review, adversarial finding 6a's
/// sibling): the EVENT-column generation read (read_generation in
/// storage/indexeddb-wasm/src/collection.rs, serving get_events and
/// dump_entity_events) must fail LOUDLY with the typed
/// DecodeError::InvalidGeneration on any stored number that is not exactly
/// a u32, matching the checked standard the M4 remediation set for the
/// entity-record GClock decode (proto/src/wasm.rs). The coercing cast it
/// replaces silently mapped NaN and negatives to 0, truncated fractions,
/// and clamped overflow to u32::MAX. The corruption is planted through a
/// raw second IndexedDB connection, so the engine's write path (which only
/// ever produces exact u32 numbers) is not in the loop: this is the
/// stored-corruption read seam.
#[wasm_bindgen_test]
pub async fn doctored_stored_event_generation_fails_loudly_on_read() -> Result<(), anyhow::Error> {
    use ankurah::core::error::RetrievalError;

    let (ctx, db_name) = setup_context().await?;
    let collection = ctx.collection(&"gen_doctor".into()).await?;

    let entity_id = proto::EntityId::new();
    let genesis = proto::Event {
        collection: "gen_doctor".into(),
        entity_id,
        operations: proto::OperationSet(BTreeMap::new()),
        parent: proto::Clock::default(),
        generation: 1,
    };
    collection.add_event(&Attested::opt(genesis.clone(), None)).await?;
    let key = genesis.id().to_base64();

    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 1.5, u32::MAX as f64 + 1.0] {
        doctor_stored_generation(&db_name, &key, JsValue::from_f64(bad)).await;

        let got = collection.get_events(vec![genesis.id()]).await;
        assert!(
            matches!(got, Err(RetrievalError::DecodeError(proto::DecodeError::InvalidGeneration(_)))),
            "get_events over a stored generation of {bad} must fail with the typed InvalidGeneration error, got {got:?}"
        );
        let dumped = collection.dump_entity_events(entity_id).await;
        assert!(
            matches!(dumped, Err(RetrievalError::DecodeError(proto::DecodeError::InvalidGeneration(_)))),
            "dump_entity_events over a stored generation of {bad} must fail with the typed InvalidGeneration error, got {dumped:?}"
        );
    }

    // A non-number is refused typed as well (missing/garbage field class).
    doctor_stored_generation(&db_name, &key, JsValue::from_str("seven")).await;
    let got = collection.get_events(vec![genesis.id()]).await;
    assert!(got.is_err(), "get_events over a non-number stored generation must fail loudly, got {got:?}");

    // Restore the honest value: the row decodes again, exact (the happy
    // path is lossless and must not be collateral damage of the checks).
    doctor_stored_generation(&db_name, &key, JsValue::from_f64(1.0)).await;
    let events = collection.get_events(vec![genesis.id()]).await?;
    assert_eq!(events.len(), 1, "the restored row decodes");
    assert_eq!(events[0].payload.generation, 1, "the restored generation reads back exact");
    assert_eq!(events[0].payload.id(), genesis.id(), "the restored payload recomputes to the original id");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
