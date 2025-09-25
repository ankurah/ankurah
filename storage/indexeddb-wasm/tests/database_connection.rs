mod common;
use ankurah_core::error::RetrievalError;
use ankurah_core::indexing::{IndexKeyPart, KeySpec};
use ankurah_core::value::ValueType;
use ankurah_storage_indexeddb_wasm::database::Database;
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

#[wasm_bindgen_test]
pub async fn test_duplicate_index_creation_error_handling() -> Result<(), anyhow::Error> {
    setup();

    let db_name = format!("test_db_duplicate_index_{}", ulid::Ulid::new());
    let index_spec = KeySpec::new(vec![IndexKeyPart::asc("test_field", ValueType::String)]);

    // First, create the database and establish a baseline version
    let db = Database::open(&db_name).await.expect("Failed to open database first time");
    let initial_version = db.get_connection().await.version() as u32;
    db.close().await;

    // First call - should succeed and create the index
    let db =
        ankurah_storage_indexeddb_wasm::database::Connection::open_with_index(&db_name, initial_version + 1, index_spec.clone()).await?;
    db.close();

    // Second call - should trigger the duplicate index error
    let result = ankurah_storage_indexeddb_wasm::database::Connection::open_with_index(&db_name, initial_version + 2, index_spec.clone())
        .await
        .expect_err("Expected error for duplicate index creation, but operation succeeded");

    match result {
        RetrievalError::Anyhow(e) => {
            assert_eq!(
                e.to_string(),
                "create index - ConstraintError: Failed to execute 'createIndex' on 'IDBObjectStore': An index with the specified name already exists."
            );
        }
        _ => panic!("Incorrect error type {:?}", result),
    }

    Ok(())
}
