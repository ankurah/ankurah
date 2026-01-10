//! SQLite JSON Comparison Semantics Tests
//!
//! These tests verify and document SQLite's json_extract() comparison behavior.
//! They run raw SQL against a real SQLite instance to:
//! 1. Validate our understanding of json_extract() semantics
//! 2. Catch any changes in future SQLite versions
//! 3. Document the behavior we're relying on for JSON query pushdown
//!
//! Key behaviors verified:
//! - Numeric comparisons via json_extract are numeric (not lexicographic)
//! - String comparisons are lexicographic
//! - Cross-type comparisons behavior
//! - Float/int comparisons work correctly within numeric family

use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;

/// Helper to run a SQL query that returns a boolean (0 or 1 in SQLite)
async fn query_bool(sql: &str) -> Result<bool> {
    let engine = SqliteStorageEngine::open_in_memory().await?;
    let conn = engine.pool().get().await.map_err(|e| anyhow::anyhow!("{}", e))?;

    let sql = sql.to_owned();
    let result = conn
        .with_connection(move |c| {
            let value: i32 = c.query_row(&sql, [], |row| row.get(0))?;
            Ok(value != 0)
        })
        .await?;

    Ok(result)
}

#[tokio::test]
async fn test_json_extract_numeric_comparison_is_numeric() -> Result<()> {
    // This test verifies that json_extract numeric comparison is numeric, NOT lexicographic.
    // If this were lexicographic, "9" > "10" would be true (because "9" > "1").
    // With proper numeric comparison, 9 > 10 is false.

    // 9 > 10 should be FALSE (numeric comparison)
    let result = query_bool("SELECT json_extract('{\"n\": 9}', '$.n') > json_extract('{\"n\": 10}', '$.n')").await?;
    assert!(!result, "json_extract numeric comparison should be numeric, not lexicographic: 9 > 10 should be false");

    // 9 < 10 should be TRUE
    let result = query_bool("SELECT json_extract('{\"n\": 9}', '$.n') < json_extract('{\"n\": 10}', '$.n')").await?;
    assert!(result, "9 < 10 should be true");

    // 100 > 9 should be TRUE
    let result = query_bool("SELECT json_extract('{\"n\": 100}', '$.n') > json_extract('{\"n\": 9}', '$.n')").await?;
    assert!(result, "100 > 9 should be true");

    Ok(())
}

#[tokio::test]
async fn test_json_extract_string_comparison_is_lexicographic() -> Result<()> {
    // json_extract string comparisons are lexicographic (as expected for strings)

    // "9" > "10" lexicographically (because '9' > '1')
    let result = query_bool(r#"SELECT json_extract('{"s": "9"}', '$.s') > json_extract('{"s": "10"}', '$.s')"#).await?;
    assert!(result, "String '9' > '10' lexicographically");

    // "abc" < "abd"
    let result = query_bool(r#"SELECT json_extract('{"s": "abc"}', '$.s') < json_extract('{"s": "abd"}', '$.s')"#).await?;
    assert!(result, "String 'abc' < 'abd'");

    Ok(())
}

#[tokio::test]
async fn test_json_extract_cross_type_comparison() -> Result<()> {
    // Cross-type comparisons in SQLite json_extract
    // SQLite's behavior differs from PostgreSQL JSONB here - it does type coercion

    // Number 9 compared to string "9" - SQLite may coerce
    let result = query_bool(r#"SELECT json_extract('{"n": 9}', '$.n') = json_extract('{"s": "9"}', '$.s')"#).await?;
    // Document actual behavior (SQLite coerces, so this may be true)
    println!("Number 9 = String '9': {}", result);

    // Number 9 compared to boolean true
    let result = query_bool(r#"SELECT json_extract('{"n": 9}', '$.n') = json_extract('{"b": true}', '$.b')"#).await?;
    assert!(!result, "Number 9 should not equal boolean true");

    Ok(())
}

#[tokio::test]
async fn test_json_extract_float_int_comparison() -> Result<()> {
    // Float and int comparisons should work correctly within the numeric family

    // 9 should equal 9.0
    let result = query_bool("SELECT json_extract('{\"n\": 9}', '$.n') = json_extract('{\"n\": 9.0}', '$.n')").await?;
    assert!(result, "Integer 9 should equal float 9.0");

    // 9.5 > 9 should be true
    let result = query_bool("SELECT json_extract('{\"n\": 9.5}', '$.n') > json_extract('{\"n\": 9}', '$.n')").await?;
    assert!(result, "9.5 > 9 should be true");

    // 9 < 9.1 should be true
    let result = query_bool("SELECT json_extract('{\"n\": 9}', '$.n') < json_extract('{\"n\": 9.1}', '$.n')").await?;
    assert!(result, "9 < 9.1 should be true");

    Ok(())
}

#[tokio::test]
async fn test_json_extract_null_comparison() -> Result<()> {
    // SQLite JSON null comparisons
    // Note: json_extract returns SQL NULL for JSON null, which has SQL NULL semantics

    // JSON null extracted becomes SQL NULL, and NULL = NULL is NULL (falsy)
    let result = query_bool("SELECT json_extract('{\"n\": null}', '$.n') IS NULL").await?;
    assert!(result, "json_extract of JSON null should be SQL NULL");

    // null should not equal 0
    let result = query_bool("SELECT COALESCE(json_extract('{\"n\": null}', '$.n') = 0, 0)").await?;
    assert!(!result, "JSON null should not equal 0");

    Ok(())
}

#[tokio::test]
async fn test_json_extract_path_with_comparison() -> Result<()> {
    // Test that our actual query pattern works correctly
    // This simulates: json_extract(data, '$.count') > 10

    // JSON count 9 > 10 should be false
    let result = query_bool(r#"SELECT json_extract('{"count": 9}', '$.count') > 10"#).await?;
    assert!(!result, "JSON count 9 > 10 should be false");

    // JSON count 100 > 10 should be true
    let result = query_bool(r#"SELECT json_extract('{"count": 100}', '$.count') > 10"#).await?;
    assert!(result, "JSON count 100 > 10 should be true");

    // String comparison
    let result = query_bool(r#"SELECT json_extract('{"status": "active"}', '$.status') = 'active'"#).await?;
    assert!(result, "JSON status = 'active' should match");

    // Nested path extraction
    let result = query_bool(r#"SELECT json_extract('{"user": {"name": "alice"}}', '$.user.name') = 'alice'"#).await?;
    assert!(result, "Nested JSON path extraction should work");

    Ok(())
}
