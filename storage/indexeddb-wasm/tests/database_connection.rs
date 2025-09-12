mod common;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;

use wasm_bindgen_test::*;

#[wasm_bindgen_test]
pub async fn test_open_database() {
    setup();

    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&db_name).await.expect("Failed to open database");
    assert_eq!(engine.name(), db_name, "Database name mismatch");
    let engine2 = IndexedDBStorageEngine::open(&db_name).await.expect("Failed to reopen database");
    assert_eq!(engine2.name(), db_name, "Database name mismatch");

    drop(engine);
    drop(engine2);

    IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
}
