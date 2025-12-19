//! Sled JSON Property Integration Tests
//!
//! These tests verify that the `Json` property type works correctly with Sled storage,
//! including:
//! - Storing entities with Json properties
//! - Querying with JSON path syntax (e.g., `licensing.territory = 'US'`)
//!
//! Note: Sled uses table-scan + post-filtering for JSON path queries since it doesn't
//! have native JSON indexing. These tests verify the filtering logic works correctly.

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A model with a Json property for testing JSON queries
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

#[tokio::test]
async fn test_json_property_storage_and_simple_query() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create a track with JSON licensing data
    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Test Track".to_string(),
            licensing: Json::new(serde_json::json!({
                "territory": "US",
                "rights": "exclusive"
            })),
        })
        .await?;
        trx.commit().await?;
    }

    // Simple query - should work
    let tracks: Vec<TrackView> = ctx.fetch("name = 'Test Track'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Test Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_string_equality() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create tracks with different licensing territories
    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "US Track".to_string(),
            licensing: Json::new(serde_json::json!({"territory": "US", "rights": "exclusive"})),
        })
        .await?;
        trx.create(&Track {
            name: "UK Track".to_string(),
            licensing: Json::new(serde_json::json!({"territory": "UK", "rights": "non-exclusive"})),
        })
        .await?;
        trx.commit().await?;
    }

    // Query by JSON path
    let us_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'US'").await?;
    assert_eq!(us_tracks.len(), 1);
    assert_eq!(us_tracks[0].name().unwrap(), "US Track");

    // Query for UK
    let uk_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'UK'").await?;
    assert_eq!(uk_tracks.len(), 1);
    assert_eq!(uk_tracks[0].name().unwrap(), "UK Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_numeric_comparison() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create tracks with numeric JSON fields
    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Popular Track".to_string(),
            licensing: Json::new(serde_json::json!({"territory": "US", "plays": 1000})),
        })
        .await?;
        trx.create(&Track { name: "New Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "US", "plays": 50})) })
            .await?;
        trx.commit().await?;
    }

    // Query with numeric comparison
    let popular: Vec<TrackView> = ctx.fetch("licensing.plays > 500").await?;
    assert_eq!(popular.len(), 1);
    assert_eq!(popular[0].name().unwrap(), "Popular Track");

    // Also verify equality works
    let exact: Vec<TrackView> = ctx.fetch("licensing.plays = 1000").await?;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].name().unwrap(), "Popular Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_nested_query() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create track with nested JSON
    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Nested Track".to_string(),
            licensing: Json::new(serde_json::json!({
                "territory": "US",
                "rights": {
                    "holder": "Label",
                    "type": "exclusive"
                }
            })),
        })
        .await?;
        trx.commit().await?;
    }

    // Query nested path
    let label_tracks: Vec<TrackView> = ctx.fetch("licensing.rights.holder = 'Label'").await?;
    assert_eq!(label_tracks.len(), 1);
    assert_eq!(label_tracks[0].name().unwrap(), "Nested Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_combined_with_regular_field() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create multiple tracks
    {
        let trx = ctx.begin();
        trx.create(&Track { name: "US Track A".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "US Track B".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "UK Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "UK"})) }).await?;
        trx.commit().await?;
    }

    // Query combining regular field and JSON path
    let results: Vec<TrackView> = ctx.fetch("name = 'US Track A' AND licensing.territory = 'US'").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name().unwrap(), "US Track A");

    Ok(())
}
