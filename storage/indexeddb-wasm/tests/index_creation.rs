mod common;
use ankurah_storage_common::{IndexKeyPart, KeySpec};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;

use wasm_bindgen_test::*;

#[wasm_bindgen_test]
pub async fn test_index_creation_and_reconnection() -> Result<(), anyhow::Error> {
    setup();

    let db_name = format!("test_index_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;

    // Get the Database instance from the storage engine to avoid conflicts
    let db = &storage_engine.db;

    // Test that we can get a connection initially
    let initial_version = db.get_connection().await.version();
    tracing::info!("Initial database version: {}", initial_version);

    // Create an index spec for testing using the new common IndexSpec
    let index_spec = KeySpec::new(vec![IndexKeyPart::asc("__collection"), IndexKeyPart::asc("name")]);
    tracing::info!("Creating index: {}", index_spec.name_with("", "__"));

    // Test index creation (this should trigger reconnection)
    db.assure_index_exists(&index_spec).await?;

    // Verify we can still get a connection after index creation
    let post_index_version = db.get_connection().await.version();
    tracing::info!("Post-index database version: {}", post_index_version);

    // Version should have been incremented
    assert!(post_index_version > initial_version, "Database version should have increased after index creation");

    // Verify we can create a transaction on the new connection and access the index
    let transaction =
        db.get_connection().await.transaction_with_str("entities").map_err(|e| anyhow::anyhow!("Failed to create transaction: {:?}", e))?;
    let store = transaction.object_store("entities").map_err(|e| anyhow::anyhow!("Failed to get object store: {:?}", e))?;

    // Verify the index exists by trying to access it
    let index_result = store.index(&index_spec.name_with("", "__"));
    assert!(index_result.is_ok(), "Index should exist after creation: {:?}", index_result.err());

    tracing::info!("Index creation and reconnection test passed!");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
