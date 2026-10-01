#[path = "../../common/tests/support/membership.rs"]
mod cases;

use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn joined_memberships() -> anyhow::Result<()> {
    let name = format!("joined_memberships_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check(&engine).await?;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_memberships() -> anyhow::Result<()> {
    let name = format!("indexed_memberships_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check_indexed(&engine).await?;
    let db = engine.db.get_connection().await;
    let transaction = db.transaction_with_str("materializations").unwrap();
    let indexes = transaction.object_store("materializations").unwrap().index_names();
    assert_eq!(indexes.length(), 2, "one property index plus the built-in membership/id index; not a full scan");
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn membership_rejection_does_not_read_entity_state() -> anyhow::Result<()> {
    use ankql::ast::{OrderByItem, OrderDirection, Predicate, Selection};
    use ankurah_core::storage::StorageEngine;
    use ankurah_proto::{EntityId, ModelId, PropertyId};
    use wasm_bindgen::JsValue;

    let name = format!("membership_before_state_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check_indexed(&engine).await?;
    let id = |byte| EntityId::from_bytes([byte; 32]);
    let [a, b] = [101, 102].map(|byte| ModelId::EntityId(id(byte)));
    let property = PropertyId::EntityId(id(201));

    // Keep the indexed projection and memberships intact, but make an excluded state unreadable.
    let db = engine.db.get_connection().await;
    let transaction = db.transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readwrite).unwrap();
    let complete = js_sys::Promise::new(&mut |resolve, reject| {
        transaction.set_oncomplete(Some(&resolve));
        transaction.set_onerror(Some(&reject));
        transaction.set_onabort(Some(&reject));
    });
    transaction.object_store("entities").unwrap().put_with_key(&js_sys::Object::new(), &JsValue::from_str(&id(1).to_base64())).unwrap();
    wasm_bindgen_futures::JsFuture::from(complete).await.unwrap();

    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: property.into(), direction: OrderDirection::Asc }]),
        limit: Some(1),
    }
    .and_member_of(b)
    .and_member_of(a);
    let states = engine.fetch_states(&selection).await?;
    assert_eq!(states.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![id(2)]);
    assert!(engine.get_state(id(1)).await.is_err(), "the excluded state really is unreadable");
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
