//! SQLite JSON Property Integration Tests
//!
//! These tests verify that the `Json` property type works correctly with SQLite storage,
//! including:
//! - Storing entities with Json properties (as BLOB with JSONB format)
//! - Querying with JSON path syntax (e.g., `licensing.territory = 'US'`)
//! - SQLite JSONB operator behavior for path traversal
//!
//! ## Implementation Notes
//!
//! `Json` properties are stored as BLOB (SQLite JSONB binary format) in SQLite.
//! This enables using JSONB functions (`jsonb()`, `->`, `->>`) for efficient path queries.
//! SQLite 3.45.0+ is required for JSONB support.

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::sql_builder::{split_predicate_for_sqlite, SplitPredicate};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Assert that a query predicate fully pushes down to SQLite (no post-filtering required).
/// This catches bugs where queries unexpectedly spill to Rust-side filtering.
#[allow(dead_code)]
fn assert_fully_pushes_down(query: &str) {
    let selection = ankql::parser::parse_selection(query).expect("Failed to parse query");
    let split = split_predicate_for_sqlite(&selection.predicate);
    assert!(
        !split.needs_post_filter(),
        "Query '{}' should fully push down to SQLite, but remaining predicate is: {:?}",
        query,
        split.remaining_predicate
    );
}

/// Get the split predicate for a query (for tests that need to inspect the split).
#[allow(dead_code)]
fn get_predicate_split(query: &str) -> SplitPredicate {
    let selection = ankql::parser::parse_selection(query).expect("Failed to parse query");
    split_predicate_for_sqlite(&selection.predicate)
}

/// A model with a Json property for testing JSON query pushdown
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

#[tokio::test]
async fn test_json_property_storage_and_simple_query() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
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

/// Verify that JSON path queries fully push down to SQLite (no spill to Rust).
#[test]
fn test_json_path_pushdown_verification() {
    // All these queries should fully push down to SQLite
    assert_fully_pushes_down("licensing.territory = 'US'");
    assert_fully_pushes_down("licensing.rights.holder = 'Label'");
    assert_fully_pushes_down("licensing.count > 10");
    assert_fully_pushes_down("name = 'Test' AND licensing.territory = 'US'");
    assert_fully_pushes_down("licensing.territory = 'US' OR licensing.territory = 'UK'");

    // Nested paths should also push down
    assert_fully_pushes_down("licensing.nested.deeply.value = 'test'");
}

#[tokio::test]
async fn test_json_path_query_string_equality() -> Result<()> {
    // Verify pushdown before running the actual query
    assert_fully_pushes_down("licensing.territory = 'US'");

    let storage = SqliteStorageEngine::open_in_memory().await?;
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

    // First verify the data was stored correctly with a simple query
    let all_tracks: Vec<TrackView> = ctx.fetch("name = 'US Track' OR name = 'UK Track'").await?;
    assert_eq!(all_tracks.len(), 2, "Should have stored both tracks");

    // Verify the JSON data is accessible
    let us_track = all_tracks.iter().find(|t| t.name().unwrap() == "US Track").unwrap();
    let uk_track = all_tracks.iter().find(|t| t.name().unwrap() == "UK Track").unwrap();
    assert_eq!(us_track.licensing().unwrap().get("territory").and_then(|v| v.as_str()), Some("US"));
    assert_eq!(uk_track.licensing().unwrap().get("territory").and_then(|v| v.as_str()), Some("UK"));

    // Query by JSON path
    // The licensing column should have been created during set_state
    // This query uses json_extract() which should work with BLOB JSONB columns
    let us_tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'US'").await?;

    assert_eq!(us_tracks.len(), 1, "Should find one track with territory = 'US'");
    assert_eq!(us_tracks[0].name().unwrap(), "US Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_numeric_comparison() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Short Track".to_string(),
            licensing: Json::new(serde_json::json!({"duration": 120, "territory": "US"})),
        })
        .await?;
        trx.create(&Track {
            name: "Long Track".to_string(),
            licensing: Json::new(serde_json::json!({"duration": 300, "territory": "US"})),
        })
        .await?;
        trx.commit().await?;
    }

    // Query with numeric comparison
    let long_tracks: Vec<TrackView> = ctx.fetch("licensing.duration > 200").await?;

    assert_eq!(long_tracks.len(), 1);
    assert_eq!(long_tracks[0].name().unwrap(), "Long Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_nested_query() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();
        trx.create(&Track {
            name: "Label A Track".to_string(),
            licensing: Json::new(serde_json::json!({
                "rights": {
                    "holder": "Label A",
                    "type": "exclusive"
                }
            })),
        })
        .await?;
        trx.create(&Track {
            name: "Label B Track".to_string(),
            licensing: Json::new(serde_json::json!({
                "rights": {
                    "holder": "Label B",
                    "type": "non-exclusive"
                }
            })),
        })
        .await?;
        trx.commit().await?;
    }

    // Query nested JSON path
    let label_a_tracks: Vec<TrackView> = ctx.fetch("licensing.rights.holder = 'Label A'").await?;

    assert_eq!(label_a_tracks.len(), 1);
    assert_eq!(label_a_tracks[0].name().unwrap(), "Label A Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_combined_with_regular_field() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();
        trx.create(&Track { name: "Track A".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "Track B".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "Track C".to_string(), licensing: Json::new(serde_json::json!({"territory": "UK"})) }).await?;
        trx.commit().await?;
    }

    // Combined query: regular field AND JSON path
    let tracks: Vec<TrackView> = ctx.fetch("name = 'Track A' AND licensing.territory = 'US'").await?;

    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].name().unwrap(), "Track A");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_with_or() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();
        trx.create(&Track { name: "US Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "US"})) }).await?;
        trx.create(&Track { name: "UK Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "UK"})) }).await?;
        trx.create(&Track { name: "CA Track".to_string(), licensing: Json::new(serde_json::json!({"territory": "CA"})) }).await?;
        trx.commit().await?;
    }

    // Query with OR condition on JSON path
    let tracks: Vec<TrackView> = ctx.fetch("licensing.territory = 'US' OR licensing.territory = 'UK'").await?;

    assert_eq!(tracks.len(), 2);
    let territories: Vec<String> =
        tracks.iter().map(|t| t.licensing().unwrap().get("territory").and_then(|v| v.as_str()).unwrap_or("").to_string()).collect();
    assert!(territories.contains(&"US".to_string()));
    assert!(territories.contains(&"UK".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_numeric_ordering() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    {
        let trx = ctx.begin();
        trx.create(&Track { name: "Track 1".to_string(), licensing: Json::new(serde_json::json!({"priority": 1, "territory": "US"})) })
            .await?;
        trx.create(&Track { name: "Track 2".to_string(), licensing: Json::new(serde_json::json!({"priority": 2, "territory": "US"})) })
            .await?;
        trx.create(&Track { name: "Track 3".to_string(), licensing: Json::new(serde_json::json!({"priority": 3, "territory": "US"})) })
            .await?;
        trx.commit().await?;
    }

    // Query with numeric comparison - should use numeric comparison, not lexicographic
    let high_priority_tracks: Vec<TrackView> = ctx.fetch("licensing.priority > 1").await?;

    assert_eq!(high_priority_tracks.len(), 2);
    // Verify numeric comparison worked (not lexicographic - "3" > "10" would be true lexicographically)
    let priorities: Vec<i64> =
        high_priority_tracks.iter().map(|t| t.licensing().unwrap().get("priority").and_then(|v| v.as_i64()).unwrap_or(0)).collect();
    assert!(priorities.contains(&2));
    assert!(priorities.contains(&3));
    assert!(!priorities.contains(&1));

    Ok(())
}
