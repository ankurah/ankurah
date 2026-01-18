//! PostgreSQL JSON Property Integration Tests
//!
//! These tests verify that the `Json` property type works correctly with PostgreSQL storage,
//! including:
//! - Storing entities with Json properties (as native `jsonb` columns)
//! - Querying with JSON path syntax (e.g., `licensing.territory = 'US'`)
//! - PostgreSQL JSONB operator behavior for path traversal
//!
//! ## Implementation Notes
//!
//! `Json` properties are stored as PostgreSQL's native `jsonb` type (not `bytea`).
//! This enables using JSONB operators (`->`, `->>`) for efficient path queries.
//!
//! The `test_bytea_jsonb_operator_behavior` test is intentionally kept to document
//! PostgreSQL's behavior when JSONB operators are used on `bytea` columns - this was
//! the source of a subtle bug where the query would silently fail instead of erroring.

mod common;

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_postgres::sql_builder::{split_predicate_for_postgres, SplitPredicate};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Assert that a query predicate fully pushes down to PostgreSQL (no post-filtering required).
/// This catches bugs where queries unexpectedly spill to Rust-side filtering.
#[allow(dead_code)]
fn assert_fully_pushes_down(query: &str) {
    let selection = ankql::parser::parse_selection(query).expect("Failed to parse query");
    let split = split_predicate_for_postgres(&selection.predicate);
    assert!(
        !split.needs_post_filter(),
        "Query '{}' should fully push down to PostgreSQL, but remaining predicate is: {:?}",
        query,
        split.remaining_predicate
    );
}

/// Get the split predicate for a query (for tests that need to inspect the split).
#[allow(dead_code)]
fn get_predicate_split(query: &str) -> SplitPredicate {
    let selection = ankql::parser::parse_selection(query).expect("Failed to parse query");
    split_predicate_for_postgres(&selection.predicate)
}

/// A model with a Json property for testing JSON query pushdown
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Track {
    pub name: String,
    pub licensing: Json,
}

#[tokio::test]
async fn test_json_property_storage_and_simple_query() -> Result<()> {
    let (_container, storage) = common::create_postgres_container().await?;
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

/// Documents PostgreSQL's behavior when JSONB operators are used on `bytea` columns.
///
/// **Historical Context**: Before we added proper `jsonb` column support, `Json` properties
/// were stored as `bytea`. This caused a subtle bug:
///
/// 1. SQL builder generated JSONB operators: `WHERE "licensing"->'territory' = 'US'::jsonb`
/// 2. PostgreSQL errored: "operator does not exist: bytea -> unknown"
/// 3. But our tests showed 0 results, not an error!
///
/// The reason: `referenced_columns()` was returning the JSON path step (`territory`)
/// instead of the actual column name (`licensing`). PostgreSQL's schema check thought
/// `territory` didn't exist, so `assume_null()` transformed the predicate to `FALSE`,
/// returning 0 rows without ever hitting the actual operator error.
///
/// This test is kept as documentation to show that PostgreSQL **does** error on bytea->jsonb
/// operator mismatch - the silent failure was a different bug in our column resolution.
#[tokio::test]
async fn test_bytea_jsonb_operator_behavior() -> Result<()> {
    let (container, _storage) = common::create_postgres_container().await?;

    // Get a raw PostgreSQL connection to test native behavior
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let (client, connection) =
        tokio_postgres::connect(&format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"), tokio_postgres::NoTls)
            .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Create a table with bytea column (simulating old Json storage)
    client.execute("CREATE TABLE IF NOT EXISTS test_bytea (id SERIAL PRIMARY KEY, data BYTEA)", &[]).await?;

    // Insert JSON as raw bytes
    let json_bytes = serde_json::to_vec(&serde_json::json!({"territory": "US"})).unwrap();
    client.execute("INSERT INTO test_bytea (data) VALUES ($1)", &[&json_bytes]).await?;

    // IMPORTANT: Using JSONB operator (->) on bytea column ERRORS, it doesn't silently fail
    let result = client.query("SELECT data->'territory' FROM test_bytea", &[]).await;
    assert!(result.is_err(), "JSONB operator on bytea should error");
    // The exact error message may vary by Postgres version, but it should indicate a type/operator mismatch
    let err = result.unwrap_err();
    let err_debug = format!("{:?}", err);
    let err_display = err.to_string();
    assert!(
        err_debug.contains("operator does not exist") || err_debug.contains("type") || err_debug.contains("bytea"),
        "Expected type/operator error, got display='{}', debug='{}'",
        err_display,
        err_debug
    );

    // Verify the column is indeed bytea
    let col_info =
        client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_bytea'", &[]).await?;
    let data_type: String = col_info.iter().find(|r| r.get::<_, String>(0) == "data").unwrap().get(1);
    assert_eq!(data_type, "bytea");

    Ok(())
}

/// Verify that JSON path queries fully push down to PostgreSQL (no spill to Rust).
#[test]
fn test_json_path_pushdown_verification() {
    // All these queries should fully push down to PostgreSQL
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

    let (container, storage) = common::create_postgres_container().await?;
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

    // Debug: Check what type the licensing column actually is
    {
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432).await?;
        let (client, connection) = tokio_postgres::connect(
            &format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"),
            tokio_postgres::NoTls,
        )
        .await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let col_info =
            client.query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'track'", &[]).await?;
        println!("Track table columns:");
        for row in &col_info {
            let name: String = row.get(0);
            let dtype: String = row.get(1);
            println!("  {} : {}", name, dtype);
        }
    }

    // Query by JSON path - THIS WILL FAIL with current implementation
    // because "licensing" column is BYTEA but we generate JSONB operators
    println!("About to fetch with JSON path query...");
    let fetch_result = ctx.fetch::<TrackView>("licensing.territory = 'US'").await;
    println!("Fetch result: {:?}", fetch_result.as_ref().map(|v| v.len()).map_err(|e| e.to_string()));
    let us_tracks = fetch_result?;
    println!("Found {} tracks", us_tracks.len());

    assert_eq!(us_tracks.len(), 1);
    assert_eq!(us_tracks[0].name().unwrap(), "US Track");

    Ok(())
}

#[tokio::test]
async fn test_json_path_query_numeric_comparison() -> Result<()> {
    let (_container, storage) = common::create_postgres_container().await?;
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
    let (_container, storage) = common::create_postgres_container().await?;
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
    let (_container, storage) = common::create_postgres_container().await?;
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
