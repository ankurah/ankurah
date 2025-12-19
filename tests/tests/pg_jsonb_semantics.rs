//! PostgreSQL JSONB Comparison Semantics Tests
//!
//! These tests verify and document PostgreSQL's JSONB comparison behavior.
//! They run raw SQL against a real PostgreSQL instance to:
//! 1. Validate our understanding of JSONB comparison semantics
//! 2. Catch any changes in future PostgreSQL versions
//! 3. Document the behavior we're relying on for JSON query pushdown
//!
//! Key behaviors verified:
//! - Numeric JSONB comparisons are numeric (not lexicographic)
//! - String JSONB comparisons are lexicographic
//! - Cross-type comparisons return false (not error)
//! - Float/int comparisons work correctly within numeric family

#![cfg(feature = "postgres")]

mod pg_common;

use anyhow::Result;

/// Helper to run a SQL query that returns a boolean
async fn query_bool(container: &testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>, sql: &str) -> Result<bool> {
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;

    let (client, connection) =
        tokio_postgres::connect(&format!("host={host} port={port} user=postgres password=postgres dbname=ankurah"), tokio_postgres::NoTls)
            .await?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let row = client.query_one(sql, &[]).await?;
    let result: bool = row.get(0);
    Ok(result)
}

#[tokio::test]
async fn test_jsonb_numeric_comparison_is_numeric() -> Result<()> {
    // This test verifies that JSONB numeric comparison is numeric, NOT lexicographic.
    // If this were lexicographic, "9" > "10" would be true (because "9" > "1").
    // With proper numeric comparison, 9 > 10 is false.
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // 9 > 10 should be FALSE (numeric comparison)
    let result = query_bool(&container, "SELECT '9'::jsonb > '10'::jsonb").await?;
    assert!(!result, "JSONB numeric comparison should be numeric, not lexicographic: 9 > 10 should be false");

    // 9 < 10 should be TRUE
    let result = query_bool(&container, "SELECT '9'::jsonb < '10'::jsonb").await?;
    assert!(result, "9 < 10 should be true");

    // 100 > 9 should be TRUE
    let result = query_bool(&container, "SELECT '100'::jsonb > '9'::jsonb").await?;
    assert!(result, "100 > 9 should be true");

    Ok(())
}

#[tokio::test]
async fn test_jsonb_string_comparison_is_lexicographic() -> Result<()> {
    // JSONB string comparisons are lexicographic (as expected for strings)
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // "9" > "10" lexicographically (because '9' > '1')
    let result = query_bool(&container, r#"SELECT '"9"'::jsonb > '"10"'::jsonb"#).await?;
    assert!(result, "String '9' > '10' lexicographically");

    // "abc" < "abd"
    let result = query_bool(&container, r#"SELECT '"abc"'::jsonb < '"abd"'::jsonb"#).await?;
    assert!(result, "String 'abc' < 'abd'");

    Ok(())
}

#[tokio::test]
async fn test_jsonb_cross_type_comparison_returns_false() -> Result<()> {
    // Cross-type comparisons in JSONB return false (not error)
    // This is critical for our filter semantics where we want type mismatches to not match
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // Number 9 should NOT equal string "9"
    let result = query_bool(&container, r#"SELECT '9'::jsonb = '"9"'::jsonb"#).await?;
    assert!(!result, "Number 9 should not equal string '9'");

    // Number 9 should NOT equal boolean true
    let result = query_bool(&container, "SELECT '9'::jsonb = 'true'::jsonb").await?;
    assert!(!result, "Number 9 should not equal boolean true");

    // String "true" should NOT equal boolean true
    let result = query_bool(&container, r#"SELECT '"true"'::jsonb = 'true'::jsonb"#).await?;
    assert!(!result, "String 'true' should not equal boolean true");

    Ok(())
}

#[tokio::test]
async fn test_jsonb_float_int_comparison() -> Result<()> {
    // Float and int comparisons should work correctly within the numeric family
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // 9 should equal 9.0
    let result = query_bool(&container, "SELECT '9'::jsonb = '9.0'::jsonb").await?;
    assert!(result, "Integer 9 should equal float 9.0");

    // 9.5 > 9 should be true
    let result = query_bool(&container, "SELECT '9.5'::jsonb > '9'::jsonb").await?;
    assert!(result, "9.5 > 9 should be true");

    // 9 < 9.1 should be true
    let result = query_bool(&container, "SELECT '9'::jsonb < '9.1'::jsonb").await?;
    assert!(result, "9 < 9.1 should be true");

    Ok(())
}

#[tokio::test]
async fn test_jsonb_null_comparison() -> Result<()> {
    // JSONB null comparisons
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // null should equal null
    let result = query_bool(&container, "SELECT 'null'::jsonb = 'null'::jsonb").await?;
    assert!(result, "JSONB null should equal JSONB null");

    // null should not equal anything else
    let result = query_bool(&container, "SELECT 'null'::jsonb = '0'::jsonb").await?;
    assert!(!result, "JSONB null should not equal 0");

    let result = query_bool(&container, r#"SELECT 'null'::jsonb = '""'::jsonb"#).await?;
    assert!(!result, "JSONB null should not equal empty string");

    Ok(())
}

#[tokio::test]
async fn test_jsonb_path_extraction_with_comparison() -> Result<()> {
    // Test that our actual query pattern works correctly
    // This simulates: data->'count' > '10'::jsonb
    let (container, _storage) = pg_common::create_postgres_container().await?;

    // Create a test with actual JSONB column extraction
    let result = query_bool(&container, r#"SELECT ('{"count": 9}'::jsonb)->'count' > '10'::jsonb"#).await?;
    assert!(!result, "JSON count 9 > 10 should be false");

    let result = query_bool(&container, r#"SELECT ('{"count": 100}'::jsonb)->'count' > '10'::jsonb"#).await?;
    assert!(result, "JSON count 100 > 10 should be true");

    // String comparison in JSON
    let result = query_bool(&container, r#"SELECT ('{"status": "active"}'::jsonb)->'status' = '"active"'::jsonb"#).await?;
    assert!(result, "JSON status = 'active' should match");

    // Cross-type: JSON number should not match JSON string
    let result = query_bool(&container, r#"SELECT ('{"count": 9}'::jsonb)->'count' = '"9"'::jsonb"#).await?;
    assert!(!result, "JSON number 9 should not equal JSON string '9'");

    Ok(())
}

