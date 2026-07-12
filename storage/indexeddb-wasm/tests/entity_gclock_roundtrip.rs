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
use ankurah::{core::property::PropertyResolver, core::storage::StorageEngine};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

struct GClockModelResolver {
    model: proto::EntityId,
}

impl PropertyResolver for GClockModelResolver {
    fn resolve(&self, _collection: &str, _name: &str) -> Option<proto::EntityId> { None }
    fn name_for(&self, _id: &proto::EntityId) -> Option<String> { None }
    fn model_id_for(&self, collection: &str) -> Option<proto::EntityId> { (collection == "gclock_roundtrip").then_some(self.model) }
}

#[wasm_bindgen_test]
pub async fn entity_head_generations_survive_indexeddb_roundtrip() -> Result<(), anyhow::Error> {
    setup();
    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&db_name).await?;
    let model = proto::EntityId::from_bytes([0xEE; 16]);
    let resolver: Arc<dyn PropertyResolver> = Arc::new(GClockModelResolver { model });
    engine.set_property_resolver(Arc::downgrade(&resolver));
    let collection = engine.collection(&"gclock_roundtrip".into()).await?;

    let entity_id = proto::EntityId::new();
    let (e1, e2) = (proto::EventId::from_bytes([1; 32]), proto::EventId::from_bytes([2; 32]));
    let head = proto::Clock::from(vec![e1.clone(), e2.clone()]);
    let head_generations = proto::GClock::new(vec![(7, e1.clone()), (u32::MAX, e2.clone())]);
    let state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::new()),
        head: head.clone(),
        head_generations: head_generations.clone(),
    };
    let attested = Attested::opt(proto::EntityState { entity_id, model, state }, None);

    collection.set_state(attested).await?;

    let read = collection.get_state(entity_id).await?;
    assert_eq!(read.payload.model, model, "the bare engine restores the model envelope through its resolver");
    assert_eq!(read.payload.state.head, head, "the head round-trips (precondition)");
    assert_eq!(
        read.payload.state.head_generations, head_generations,
        "GClock pin (iii): the per-tip head generations must survive set_state + get_state rehydration, u32::MAX included"
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// M4 remediation item 3 (adversarial review finding 6a): the indexeddb
/// generation decode (TryFrom<JsValue> for GClock, proto/src/wasm.rs, the
/// read path for the entity record's __generations pairs) must fail LOUDLY
/// on any stored number that is not exactly a u32, matching the
/// range-checked discipline of the other engine homes (postgres try_into
/// with typed errors, sqlite strict typing). The saturating cast it
/// replaces coerced NaN and negatives to 0, truncated fractions, and
/// clamped overflow to u32::MAX, all silently.
#[wasm_bindgen_test]
pub fn generation_decode_rejects_numbers_that_are_not_exactly_u32() {
    use js_sys::Array;
    use wasm_bindgen::JsValue;

    let id = proto::EventId::from_bytes([1; 32]);
    let encode = |generation: JsValue| -> JsValue {
        let pair = Array::new();
        pair.push(&generation);
        pair.push(&JsValue::from_str(&id.to_base64()));
        let entries = Array::new();
        entries.push(&pair);
        entries.into()
    };

    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 1.5, u32::MAX as f64 + 1.0] {
        let result = proto::GClock::try_from(encode(JsValue::from_f64(bad)));
        assert!(
            matches!(result, Err(proto::DecodeError::InvalidGeneration(_))),
            "a stored generation of {bad} must fail the decode with the typed error, got {result:?}"
        );
    }

    // The exact u32 boundaries still decode (a u32 is exactly representable
    // as an f64, so the happy path is lossless).
    for good in [0u32, 1, u32::MAX] {
        let decoded = proto::GClock::try_from(encode(JsValue::from_f64(good as f64)))
            .unwrap_or_else(|e| panic!("an exact u32 ({good}) must decode, got {e}"));
        assert_eq!(decoded, proto::GClock::new(vec![(good, id.clone())]), "the decoded entry carries the exact stored value");
    }
}
