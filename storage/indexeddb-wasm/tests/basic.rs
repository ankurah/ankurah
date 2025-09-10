#![allow(unused)]

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Mutable, Node, PermissiveAgent};
use ankurah_proto::{AttestationSet, CollectionId, StateBuffers};
use ankurah_storage_indexeddb_wasm::{
    database::Database,
    indexes::{ConstraintValue, IndexDirection, IndexSpec, RangeConstraint},
    IndexedDBStorageEngine,
};
use serde::{Deserialize, Serialize};
use tracing_subscriber::layer::SubscriberExt;
use tracing_wasm::{ConsoleConfig, WASMLayerConfigBuilder};
use wasm_bindgen_test::*;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

wasm_bindgen_test_configure!(run_in_browser);

fn setup() -> () {
    console_error_panic_hook::set_once();

    std::panic::set_hook(Box::new(|info| {
        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<dyn Any>",
            },
        };
        let location = info.location().unwrap();
        web_sys::console::error_2(&format!("panic at {}:{}", location.file(), location.line()).into(), &msg.into());
    }));

    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::registry::Registry::default().with(tracing_wasm::WASMLayer::new(
            WASMLayerConfigBuilder::new()
                .set_report_logs_in_timings(true)
                .set_console_config(ConsoleConfig::ReportWithoutConsoleColor)
                .set_max_level(tracing::Level::INFO)
                .build(),
        )),
    );
}

#[wasm_bindgen_test]
async fn test_open_database() {
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
async fn test_range_constraint_conversion() -> Result<(), anyhow::Error> {
    setup();

    let collection_id = CollectionId::from("test_collection");

    // Test CollectionOnly constraint
    let constraint = RangeConstraint::CollectionOnly;
    let key_range =
        constraint.to_idb_key_range(&collection_id).map_err(|e| anyhow::anyhow!("CollectionOnly constraint failed: {:?}", e))?;
    tracing::info!("CollectionOnly constraint created successfully");

    // Test Exact constraint
    let constraint = RangeConstraint::Exact(ConstraintValue::String("test_value".to_string()));
    let key_range = constraint.to_idb_key_range(&collection_id).map_err(|e| anyhow::anyhow!("Exact constraint failed: {:?}", e))?;
    tracing::info!("Exact constraint created successfully");

    // Test StartFrom constraint
    let constraint = RangeConstraint::StartFrom(ConstraintValue::Integer(42));
    let key_range = constraint.to_idb_key_range(&collection_id).map_err(|e| anyhow::anyhow!("StartFrom constraint failed: {:?}", e))?;
    tracing::info!("StartFrom constraint created successfully");

    // Test EndAt constraint
    let constraint = RangeConstraint::EndAt(ConstraintValue::Float(3.14));
    let key_range = constraint.to_idb_key_range(&collection_id).map_err(|e| anyhow::anyhow!("EndAt constraint failed: {:?}", e))?;
    tracing::info!("EndAt constraint created successfully");

    tracing::info!("All range constraint conversions passed!");
    Ok(())
}

#[wasm_bindgen_test]
async fn test_cursor_direction() -> Result<(), anyhow::Error> {
    setup();

    use ankurah_storage_indexeddb_wasm::indexes::IndexDirection;
    use ankurah_storage_indexeddb_wasm::to_idb_cursor_direction;

    // Test direction conversion
    let asc_direction = to_idb_cursor_direction(IndexDirection::Asc);
    let desc_direction = to_idb_cursor_direction(IndexDirection::Desc);

    // Verify the directions are different
    tracing::info!("Ascending direction: {:?}", asc_direction);
    tracing::info!("Descending direction: {:?}", desc_direction);

    // The actual values should be Next and Prev
    assert_ne!(asc_direction as u32, desc_direction as u32);

    tracing::info!("Cursor direction conversion test passed!");
    Ok(())
}

#[wasm_bindgen_test]
async fn test_index_creation_and_reconnection() -> Result<(), anyhow::Error> {
    setup();

    let db_name = format!("test_index_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;

    // Get the Database instance from the storage engine to avoid conflicts
    let db = &storage_engine.db;

    // Test that we can get a connection initially
    let initial_version = db.get_connection().await.version();
    tracing::info!("Initial database version: {}", initial_version);

    // Create an index spec for testing
    let index_spec = IndexSpec::new("name".to_string(), IndexDirection::Asc);
    tracing::info!("Creating index: {}", index_spec.name());

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
    let index_result = store.index(index_spec.name());
    assert!(index_result.is_ok(), "Index should exist after creation: {:?}", index_result.err());

    tracing::info!("Index creation and reconnection test passed!");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn test_indexeddb_basic_workflow() -> Result<(), anyhow::Error> {
    setup();
    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    // Initialize the node's system catalog
    node.system.create().await?;

    // Get context after system is ready
    let context = node.context_async(c).await;

    // Create a simple test - just verify IndexedDB storage works
    {
        let trx = context.begin();
        let _album = trx.create(&Album { name: "Walking on a Dream".to_owned(), year: "2008".to_owned() }).await?;
        trx.commit().await?;
    }

    // Verify we can query the album
    let albums = context.fetch::<AlbumView>("name = 'Walking on a Dream'").await?;
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].name().unwrap(), "Walking on a Dream");
    assert_eq!(albums[0].year().unwrap(), "2008");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
    Ok(())
}

#[wasm_bindgen_test]
async fn test_indexeddb_order_by_and_index_creation() -> Result<(), anyhow::Error> {
    setup();

    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();

        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?;

        trx.commit().await?;
    }

    // Test ORDER BY name (should create an index and return sorted results)
    let albums: Vec<AlbumView> = ctx.fetch("year >= '2000' ORDER BY name").await?;
    let names: Vec<String> = albums.iter().map(|a| a.name().unwrap()).collect();
    assert_eq!(names, vec!["Ask That God", "Ice on the Dune", "Two Vines", "Walking on a Dream"]);

    // Test ORDER BY year (should create another index)
    // then should do a range query beween 2010 and Infinity (forward scan because the Order by is ASC)
    let albums: Vec<AlbumView> = ctx.fetch("year >= '2010' ORDER BY year").await?;
    let years: Vec<String> = albums.iter().map(|a| a.year().unwrap()).collect();
    assert_eq!(years, vec!["2013", "2016", "2024"]);

    // Test LIMIT
    let albums: Vec<AlbumView> = ctx.fetch("year >= '2000' ORDER BY name LIMIT 2").await?;
    assert_eq!(albums.len(), 2);
    let names: Vec<String> = albums.iter().map(|a| a.name().unwrap()).collect();
    assert_eq!(names, vec!["Ask That God", "Ice on the Dune"]);

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
