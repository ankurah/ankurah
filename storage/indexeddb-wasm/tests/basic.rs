#![allow(unused)]

use std::collections::BTreeMap;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Mutable, Node, PermissiveAgent};
use ankurah_proto::{AttestationSet, StateBuffers};
use ankurah_storage_indexeddb_wasm::{
    database::Database,
    indexes::{IndexDirection, IndexSpec},
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

async fn setup() -> anyhow::Result<()> {
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

    // tracing::debug!("Test setup complete");
    Ok(())
}

#[wasm_bindgen_test]
async fn test_open_database() {
    setup().await.expect("Failed to setup test");

    let db_name = format!("test_db_{}", ulid::Ulid::new());
    tracing::info!("Starting test_open_database");
    let result = IndexedDBStorageEngine::open(&db_name).await;
    tracing::info!("Open result: {:?}", result.is_ok());

    assert!(result.is_ok(), "Failed to open database: {:?}", result.err());

    let engine = result.unwrap();
    tracing::info!("Successfully opened database: {}", engine.name());
    assert_eq!(engine.name(), db_name, "Database name mismatch");

    // Test reopening existing database
    tracing::info!("Attempting to reopen database 'test_db'");
    let result2 = IndexedDBStorageEngine::open(&db_name).await;

    assert!(result2.is_ok(), "Failed to reopen database: {:?}", result2.err());
    tracing::info!("Test completed successfully");

    // Drop both engine instances
    drop(engine);
    if let Ok(engine2) = result2 {
        drop(engine2);
    }
    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
}

#[wasm_bindgen_test]
async fn test_index_creation_and_reconnection() -> Result<(), anyhow::Error> {
    setup().await.expect("Failed to setup test");

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

// #[wasm_bindgen_test]
// async fn test_indexeddb_basic_workflow() -> Result<(), anyhow::Error> {
//     setup().await.expect("Failed to setup test");
//     let db_name = format!("test_db_{}", ulid::Ulid::new());
//     let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
//     let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

//     // Initialize the node's system catalog
//     node.system.create().await?;

//     // Get context after system is ready
//     let context = node.context_async(c).await;

//     // Create a simple test - just verify IndexedDB storage works
//     {
//         let trx = context.begin();
//         let _album = trx.create(&Album { name: "Walking on a Dream".to_owned(), year: "2008".to_owned() }).await?;
//         trx.commit().await?;
//     }

//     // Verify we can query the album
//     let albums = context.fetch::<AlbumView>("name = 'Walking on a Dream'").await?;
//     assert_eq!(albums.len(), 1);
//     assert_eq!(albums[0].name().unwrap(), "Walking on a Dream");
//     assert_eq!(albums[0].year().unwrap(), "2008");

//     // Cleanup
//     IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
//     Ok(())
// }

// #[wasm_bindgen_test]
// async fn test_indexeddb_order_by_and_index_creation() -> Result<(), anyhow::Error> {
//     setup().await.expect("Failed to setup test");

//     let db_name = format!("test_db_{}", ulid::Ulid::new());
//     let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
//     let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
//     node.system.create().await?;
//     let ctx = node.context_async(c).await;

//     {
//         let trx = ctx.begin();

//         trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;
//         trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?;
//         trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?;
//         trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?;

//         trx.commit().await?;
//     }

//     // Test ORDER BY name (should create an index and return sorted results)
//     let albums: Vec<AlbumView> = ctx.fetch("year >= '2000' ORDER BY name").await?;
//     let names: Vec<String> = albums.iter().map(|a| a.name().unwrap()).collect();
//     assert_eq!(names, vec!["Ask That God", "Ice on the Dune", "Two Vines", "Walking on a Dream"]);

//     // Test ORDER BY year (should create another index)
//     let albums: Vec<AlbumView> = ctx.fetch("year >= '2010' ORDER BY year").await?;
//     let years: Vec<String> = albums.iter().map(|a| a.year().unwrap()).collect();
//     assert_eq!(years, vec!["2013", "2016", "2024"]);

//     // Test LIMIT
//     let albums: Vec<AlbumView> = ctx.fetch("year >= '2000' ORDER BY name LIMIT 2").await?;
//     assert_eq!(albums.len(), 2);
//     let names: Vec<String> = albums.iter().map(|a| a.name().unwrap()).collect();
//     assert_eq!(names, vec!["Ask That God", "Ice on the Dune"]);

//     // Cleanup
//     IndexedDBStorageEngine::cleanup(&db_name).await?;
//     Ok(())
// }
