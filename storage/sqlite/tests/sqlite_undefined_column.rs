//! Test for undefined column handling in SQLite queries
//!
//! Tests that queries referencing columns that don't exist yet are handled gracefully
//! by treating missing columns as NULL via schema-based filtering.
//!
//! These tests mirror pg_undefined_column.rs to ensure SQLite has parity with Postgres.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Model with status and created fields to test missing column handling
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Task {
    pub name: String,
    pub status: String,
    pub created: String,
}

/// Test that queries handle undefined columns in WHERE clause
#[tokio::test]
async fn test_undefined_column_in_where() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Query for tasks with status filter - but no tasks exist yet, so the column doesn't exist
    // This should return empty results, not error
    let results = ctx.fetch::<TaskView>("status = 'active'").await?;
    assert!(results.is_empty(), "Expected empty results for query on non-existent column");

    Ok(())
}

/// Test that queries handle undefined columns in ORDER BY clause
#[tokio::test]
async fn test_undefined_column_in_order_by() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Query with ORDER BY on a column that doesn't exist yet
    let results = ctx.fetch::<TaskView>("name = 'nonexistent' ORDER BY created DESC").await?;
    assert!(results.is_empty(), "Expected empty results for query with ORDER BY on non-existent column");

    Ok(())
}

/// Test WHERE on one missing column, ORDER BY on another (both handled upfront)
#[tokio::test]
async fn test_undefined_columns_where_and_order_by() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Both "status" (WHERE) and "created" (ORDER BY) don't exist
    // Schema-based filtering treats both as NULL upfront (no retry needed)
    let results = ctx.fetch::<TaskView>("status = 'pending' OR status = 'active' ORDER BY created DESC").await?;
    assert!(results.is_empty(), "Expected empty results");

    Ok(())
}

/// Test that after writing data, subsequent queries work
#[tokio::test]
async fn test_columns_exist_after_write() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // First, create a task - this should create the columns
    let trx = ctx.begin();
    let _task = trx.create(&Task { name: "Test task".to_owned(), status: "pending".to_owned(), created: "2024-01-01".to_owned() }).await?;
    trx.commit().await?;

    // Now the query should work
    let results = ctx.fetch::<TaskView>("status = 'pending' ORDER BY created DESC").await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name().unwrap(), "Test task");

    Ok(())
}

/// Test cache refresh: query before columns exist, write (creates columns), query again
/// This validates that the schema cache gets refreshed when columns are added.
#[tokio::test]
async fn test_cache_refresh_after_column_creation() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Query before any data - columns don't exist, should get empty results
    let results = ctx.fetch::<TaskView>("status = 'pending'").await?;
    assert!(results.is_empty(), "Expected empty before write");

    // Write creates the columns
    let trx = ctx.begin();
    trx.create(&Task { name: "Task 1".to_owned(), status: "pending".to_owned(), created: "2024-01-01".to_owned() }).await?;
    trx.commit().await?;

    // Query again - cache should refresh and find the column now exists
    let results = ctx.fetch::<TaskView>("status = 'pending'").await?;
    assert_eq!(results.len(), 1, "Should find the task after column was created");

    Ok(())
}
