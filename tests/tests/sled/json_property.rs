//! Sled JSON Property Tests
//!
//! These tests verify that JSON path queries work correctly with the Sled storage engine.
//! JSON queries in Sled are post-filtered in Rust (not pushed down to the storage layer),
//! so these tests verify the in-memory evaluation logic.

use super::common::*;
use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A model with a Json property for testing JSON path queries
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

async fn setup_track_context() -> Result<ankurah::Context> {
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await)
}

#[tokio::test]
async fn test_json_property_storage_and_simple_query() -> Result<()> {
    let ctx = setup_track_context().await?;

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

    // Simple query (non-JSON) - should work
    let tracks: Vec<TrackView> = ctx.fetch("name = 'Test Track'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Test Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_string_equality() -> Result<()> {
    let ctx = setup_track_context().await?;

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

    let uk_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'UK'").await?;
    assert_eq!(uk_tracks.len(), 1);
    assert_eq!(uk_tracks[0].name().unwrap(), "UK Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_numeric_comparison() -> Result<()> {
    let ctx = setup_track_context().await?;

    {
        let trx = ctx.begin();
        trx.create(&Track { name: "Cheap Track".to_string(), licensing: Json::new(serde_json::json!({"price": 5, "currency": "USD"})) })
            .await?;
        trx.create(&Track {
            name: "Expensive Track".to_string(),
            licensing: Json::new(serde_json::json!({"price": 100, "currency": "USD"})),
        })
        .await?;
        trx.commit().await?;
    }

    // Numeric comparison - should use proper numeric ordering (not lexicographic)
    let cheap: Vec<TrackView> = ctx.fetch("licensing.price < 50").await?;
    assert_eq!(cheap.len(), 1);
    assert_eq!(cheap[0].name().unwrap(), "Cheap Track");

    let expensive: Vec<TrackView> = ctx.fetch("licensing.price >= 50").await?;
    assert_eq!(expensive.len(), 1);
    assert_eq!(expensive[0].name().unwrap(), "Expensive Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_nested_query() -> Result<()> {
    let ctx = setup_track_context().await?;

    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Deep Track".to_string(),
            licensing: Json::new(serde_json::json!({
                "rights": {
                    "holder": "Label",
                    "type": "exclusive"
                }
            })),
        })
        .await?;
        trx.commit().await?;
    }

    // Nested JSON path query
    let tracks: Vec<TrackView> = ctx.fetch("licensing.rights.holder = 'Label'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Deep Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_combined_with_regular_field() -> Result<()> {
    let ctx = setup_track_context().await?;

    {
        let trx = ctx.begin();
        trx.create(&Track { name: "US Track A".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "US Track B".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "UK Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "UK"})) }).await?;
        trx.commit().await?;
    }

    // Combined query: regular field AND JSON path
    let tracks: Vec<TrackView> = ctx.fetch("name = 'US Track A' AND licensing.territory = 'US'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "US Track A");

    Ok(())
}
