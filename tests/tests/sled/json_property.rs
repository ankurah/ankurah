//! Sled JSON Property Tests
//!
//! These tests verify that JSON path queries work correctly with the Sled storage engine.
//! JSON path queries use index-backed lookups via `IndexKeyPart.sub_path` and
//! `Value::extract_at_path()` for key extraction during indexing.

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

/// Test that entities with missing JSON paths are correctly excluded from index
#[tokio::test]
async fn test_json_path_missing_field() -> Result<()> {
    let ctx = setup_track_context().await?;

    {
        let trx = ctx.begin();
        // Track with territory field
        trx.create(&Track { name: "Has Territory".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        // Track WITHOUT territory field (different JSON structure)
        trx.create(&Track { name: "No Territory".to_string(), licensing: Json::new(serde_json::json!({"other": "value"})) }).await?;
        trx.commit().await?;
    }

    // Query should only find the track that HAS the territory field
    let tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'US'").await?;
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Has Territory");

    Ok(())
}

/// Verify planner generates correct sub_path for JSON queries
#[test]
fn test_json_path_planner_generates_sub_path() {
    use ankurah_storage_common::planner::{Planner, PlannerConfig};
    use ankurah_storage_common::Plan;

    let planner = Planner::new(PlannerConfig::full_support());
    let selection = ankql::parser::parse_selection("licensing.territory = 'US'").expect("parse selection");
    let plans = planner.plan(&selection, "id");

    // Find the index plan
    let index_plan = plans.iter().find(|p| matches!(p, Plan::Index { .. }));
    assert!(index_plan.is_some(), "Should generate an index plan for JSON path query");

    if let Some(Plan::Index { index_spec, remaining_predicate, .. }) = index_plan {
        // Verify keypart has sub_path
        assert!(!index_spec.keyparts.is_empty(), "Should have at least one keypart");
        let keypart = &index_spec.keyparts[0];
        assert_eq!(keypart.column, "licensing", "Column should be 'licensing'");
        assert_eq!(keypart.sub_path, Some(vec!["territory".to_string()]), "sub_path should be ['territory']");
        assert_eq!(keypart.full_path(), "licensing.territory", "full_path should be 'licensing.territory'");

        // Verify full pushdown (remaining predicate should be True)
        assert!(
            matches!(remaining_predicate, ankql::ast::Predicate::True),
            "JSON path equality should be fully pushed down, got: {:?}",
            remaining_predicate
        );
    }
}
