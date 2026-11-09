//! A store records this engine's `STORE_FORMAT_VERSION` when it is created and is checked on every
//! open; a store recording another format refuses to open with a `StoreFormatError`.

mod common;
use ankurah_core::{error::RetrievalError, storage::StoreFormatError};
use ankurah_storage_indexeddb_wasm::{database::STORE_FORMAT_VERSION, IndexedDBStorageEngine};
use common::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

/// Rewrite the format record of the store `engine` holds open.
async fn record_format(engine: &IndexedDBStorageEngine, record: JsValue) {
    let db = engine.db.get_connection().await;
    let transaction = db.transaction_with_str_and_mode("_ankurah_meta", web_sys::IdbTransactionMode::Readwrite).unwrap();
    let complete = js_sys::Promise::new(&mut |resolve, reject| {
        transaction.set_oncomplete(Some(&resolve));
        transaction.set_onerror(Some(&reject));
        transaction.set_onabort(Some(&reject));
    });
    transaction.object_store("_ankurah_meta").unwrap().put_with_key(&record, &JsValue::from_str("store_format_version")).unwrap();
    wasm_bindgen_futures::JsFuture::from(complete).await.unwrap();
}

/// The refusal opening the store `name` meets, as the typed store format error.
async fn refusal(name: &str) -> StoreFormatError {
    let error = IndexedDBStorageEngine::open(name).await.err().expect("the store is refused");
    match error.downcast_ref::<RetrievalError>() {
        Some(RetrievalError::StorageError(source)) => source.downcast_ref::<StoreFormatError>().cloned(),
        _ => None,
    }
    .unwrap_or_else(|| panic!("not a store format refusal: {error}"))
}

#[wasm_bindgen_test]
async fn a_store_of_another_format_is_refused_naming_both_versions() -> Result<(), anyhow::Error> {
    setup();
    let name = format!("store_format_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    record_format(&engine, JsValue::from(STORE_FORMAT_VERSION + 1)).await;
    engine.db.close().await;
    assert_eq!(refusal(&name).await, StoreFormatError::Mismatch { found: STORE_FORMAT_VERSION + 1, expected: STORE_FORMAT_VERSION });

    let engine = IndexedDBStorageEngine::open(&format!("{name}_unreadable")).await?;
    record_format(&engine, JsValue::from_str("not a version")).await;
    engine.db.close().await;
    assert!(matches!(refusal(&format!("{name}_unreadable")).await, StoreFormatError::UnreadableRecord { .. }));

    IndexedDBStorageEngine::cleanup(&name).await?;
    IndexedDBStorageEngine::cleanup(&format!("{name}_unreadable")).await?;
    Ok(())
}
