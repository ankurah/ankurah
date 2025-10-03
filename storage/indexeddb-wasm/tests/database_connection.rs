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
pub async fn test_multi_connection_versionchange_reconnect() {
    setup();

    let db_name = format!("test_db_multi_conn_{}", ulid::Ulid::new());

    // Open two logical connections via engine wrappers
    let engine1 = IndexedDBStorageEngine::open(&db_name).await.expect("open engine1");
    let engine2 = IndexedDBStorageEngine::open(&db_name).await.expect("open engine2");

    let db1 = Database::open(&db_name).await.expect("open db1");
    let version_before = db1.get_connection().await.version() as u32;

    // Trigger an upgrade via open_with_index on a new index
    let index_spec = KeySpec::new(vec![IndexKeyPart::asc("multi_conn_field", ValueType::String)]);
    ankurah_storage_indexeddb_wasm::database::Connection::open_with_index(&db_name, version_before + 1, index_spec)
        .await
        .expect("upgrade with index");

    // Other connections should have received versionchange and closed; lazy reconnect should yield newer version
    let db2 = Database::open(&db_name).await.expect("open db2 for check");
    let v1 = db1.get_connection().await.version() as u32;
    let v2 = db2.get_connection().await.version() as u32;
    assert!(v1 >= version_before + 1, "db1 should reopen to at least upgraded version");
    assert!(v2 >= version_before + 1, "db2 should open at upgraded version");

    drop(engine1);
    drop(engine2);
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
