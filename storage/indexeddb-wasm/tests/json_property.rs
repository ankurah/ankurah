//! IndexedDB JSON Property Tests
//!
//! These tests verify that the `Json` property type works correctly with IndexedDB storage,
//! including:
//! - Storing entities with Json properties
//! - Querying with JSON path syntax (e.g., `licensing.territory = 'US'`)
//!
//! JSON properties are stored as parsed JS objects (not raw bytes), enabling IndexedDB's
//! native nested property indexing via dot-notation keyPath (e.g., "licensing.territory").
//! This means JSON path queries can use index-backed lookups.

mod common;

use ankurah::property::Json;
use ankurah::{error::MutationError, policy::DEFAULT_CONTEXT, Context, Model, Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::setup;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// A model with a Json property for testing JSON queries.
/// The licensing field is stored as binary JSON data (Value::Json).
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Track {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub licensing: Json,
}

async fn setup_track_context() -> Result<(Context, String), anyhow::Error> {
    setup();
    let db_name = format!("test_json_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok((node.context_async(DEFAULT_CONTEXT).await, db_name))
}

async fn create_tracks(ctx: &Context, tracks: Vec<(&str, serde_json::Value)>) -> Result<(), MutationError> {
    let trx = ctx.begin();
    for (name, licensing) in tracks {
        trx.create(&Track { name: name.to_owned(), licensing: Json::new(licensing) }).await?;
    }
    trx.commit().await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_json_property_storage_and_simple_query() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_track_context().await?;

    // Create a track with JSON licensing data
    create_tracks(&ctx, vec![("Test Track", serde_json::json!({"territory": "US", "rights": "exclusive"}))]).await?;

    // Simple query (non-JSON) - should work
    let tracks: Vec<TrackView> = ctx.fetch("name = 'Test Track'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Test Track");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_json_path_query_string_equality() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_track_context().await?;

    // Create tracks with different licensing territories
    create_tracks(
        &ctx,
        vec![
            ("US Track", serde_json::json!({"territory": "US", "rights": "exclusive"})),
            ("UK Track", serde_json::json!({"territory": "UK", "rights": "non-exclusive"})),
        ],
    )
    .await?;

    // Query by JSON path
    let us_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'US'").await?;
    assert_eq!(us_tracks.len(), 1);
    assert_eq!(us_tracks[0].name().unwrap(), "US Track");

    // Query for UK
    let uk_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'UK'").await?;
    assert_eq!(uk_tracks.len(), 1);
    assert_eq!(uk_tracks[0].name().unwrap(), "UK Track");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_json_path_query_numeric_comparison() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_track_context().await?;

    // Create tracks with numeric JSON fields
    create_tracks(
        &ctx,
        vec![
            ("Popular Track", serde_json::json!({"territory": "US", "plays": 1000})),
            ("New Track", serde_json::json!({"territory": "US", "plays": 50})),
        ],
    )
    .await?;

    // Query with numeric comparison
    let popular: Vec<TrackView> = ctx.fetch("licensing.plays > 500").await?;
    assert_eq!(popular.len(), 1);
    assert_eq!(popular[0].name().unwrap(), "Popular Track");

    // Also verify equality works
    let exact: Vec<TrackView> = ctx.fetch("licensing.plays = 1000").await?;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].name().unwrap(), "Popular Track");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_json_path_nested_query() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_track_context().await?;

    // Create track with nested JSON
    create_tracks(
        &ctx,
        vec![(
            "Nested Track",
            serde_json::json!({
                "territory": "US",
                "rights": {
                    "holder": "Label",
                    "type": "exclusive"
                }
            }),
        )],
    )
    .await?;

    // Query nested path
    let label_tracks: Vec<TrackView> = ctx.fetch("licensing.rights.holder = 'Label'").await?;
    assert_eq!(label_tracks.len(), 1);
    assert_eq!(label_tracks[0].name().unwrap(), "Nested Track");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_json_path_combined_with_regular_field() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_track_context().await?;

    // Create multiple tracks
    create_tracks(
        &ctx,
        vec![
            ("US Track A", serde_json::json!({"territory": "US"})),
            ("US Track B", serde_json::json!({"territory": "US"})),
            ("UK Track", serde_json::json!({"territory": "UK"})),
        ],
    )
    .await?;

    // Query combining regular field and JSON path
    let results: Vec<TrackView> = ctx.fetch("name = 'US Track A' AND licensing.territory = 'US'").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name().unwrap(), "US Track A");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
