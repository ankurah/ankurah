//! Core tests for DESC inequality bounds in composite indexes.
//!
//! This test suite covers the bug fix in PR #212 where the query planner was incorrectly
//! checking if the FIRST column of an index was DESC, instead of checking if the
//! INEQUALITY column was DESC.
//!
//! ## The Bug (PR #212)
//!
//! For a composite index like `[room ASC, deleted ASC, timestamp DESC]`:
//! - Old code checked: `key_spec.keyparts.first().direction.is_desc()` -> FALSE (room is ASC)
//! - Fixed code checks: `key_spec.keyparts.get(eq_prefix_len).direction.is_desc()` -> TRUE
//!
//! ## Test Organization
//!
//! 1. **Equality Prefix Variations** - Tests with eq_prefix_len = 0, 1, 2, 3
//! 2. **Strict Operators** - The < and > operators (unique tests not covered elsewhere)
//! 3. **Range Queries** - Combined inequalities with all inclusivity combinations
//! 4. **Boundary Conditions** - Exact boundary values, empty results, duplicates
//! 5. **ORDER BY Variations** - ASC sanity check, multi-column, baseline

mod common;

use crate::common::*;
use ankurah::{Model, Ref};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Test Models
// ============================================================================

/// Message model with composite index: [room ASC, deleted ASC, timestamp DESC]
/// This matches the real-world pagination scenario from the React Native app.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct TestMessage {
    pub room: Ref<TestRoom>,
    pub text: String,
    pub timestamp: i64,
    #[active_type(LWW)]
    pub deleted: bool,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct TestRoom {
    pub name: String,
}

/// Simple model for testing with no equality prefix (eq_prefix_len = 0)
/// Index: [timestamp DESC]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct SimpleEvent {
    pub timestamp: i64,
    pub data: String,
}

/// Model for testing single equality prefix (eq_prefix_len = 1)
/// Index: [category ASC, score DESC]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct ScoredItem {
    pub category: String,
    pub score: i64,
    pub name: String,
}

/// Model for testing three equality columns (eq_prefix_len = 3)
/// Index: [org ASC, team ASC, project ASC, priority DESC]
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Task {
    pub org: String,
    pub team: String,
    pub project: String,
    pub priority: i64,
    pub title: String,
}

// ============================================================================
// Test Constants
// ============================================================================

const TIMESTAMP_BASE: i64 = 1700000000000;
const TIMESTAMP_STEP: i64 = 1000;

// ============================================================================
// Helper Functions
// ============================================================================

async fn setup() -> Result<Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context(DEFAULT_CONTEXT)?)
}

async fn create_messages(ctx: &Context, room_id: EntityId, count: i64) -> Result<Vec<i64>> {
    let trx = ctx.begin();
    let mut timestamps = Vec::new();
    for i in 0..count {
        let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
        timestamps.push(ts);
        trx.create(&TestMessage { room: room_id.into(), text: format!("Message #{:03}", i), timestamp: ts, deleted: false }).await?;
    }
    trx.commit().await?;
    Ok(timestamps)
}

async fn create_room(ctx: &Context, name: &str) -> Result<EntityId> {
    let trx = ctx.begin();
    let room = trx.create(&TestRoom { name: name.to_string() }).await?;
    let id = room.id();
    trx.commit().await?;
    Ok(id)
}

fn get_timestamps(results: &[TestMessageView]) -> Vec<i64> { results.iter().map(|m| m.timestamp().unwrap()).collect() }

fn assert_desc_order(timestamps: &[i64], context: &str) {
    assert!(timestamps.windows(2).all(|w| w[0] >= w[1]), "{}: Expected DESC order, got {:?}", context, timestamps);
}

fn assert_asc_order(timestamps: &[i64], context: &str) {
    assert!(timestamps.windows(2).all(|w| w[0] <= w[1]), "{}: Expected ASC order, got {:?}", context, timestamps);
}

// ============================================================================
// SECTION 1: Equality Prefix Variations (eq_prefix_len = 0, 1, 2, 3)
// ============================================================================

/// eq_prefix_len = 0: DESC inequality with NO equality prefix
/// Index: [timestamp DESC]
/// Tests handle_desc_inequality when the inequality column IS the first column.
#[tokio::test]
async fn test_desc_inequality_no_equality_prefix() -> Result<()> {
    let ctx = setup().await?;

    {
        let trx = ctx.begin();
        for i in 0..10i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&SimpleEvent { timestamp: ts, data: format!("Event {}", i) }).await?;
        }
        trx.commit().await?;
    }

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;
    let ts_max = TIMESTAMP_BASE + 9 * TIMESTAMP_STEP;

    // Test <=
    let q = format!("timestamp <= {} ORDER BY timestamp DESC", ts_mid);
    let results: Vec<SimpleEventView> = ctx.fetch(q.as_str()).await?;
    let timestamps: Vec<i64> = results.iter().map(|e| e.timestamp().unwrap()).collect();

    assert_eq!(results.len(), 6, "timestamp <= mid should return 6 events (0-5)");
    assert!(timestamps.windows(2).all(|w| w[0] >= w[1]), "Should be DESC order");
    assert_eq!(timestamps[0], ts_mid, "First result should be the boundary value");

    // Test >=
    let q = format!("timestamp >= {} ORDER BY timestamp DESC", ts_mid);
    let results: Vec<SimpleEventView> = ctx.fetch(q.as_str()).await?;
    let timestamps: Vec<i64> = results.iter().map(|e| e.timestamp().unwrap()).collect();

    assert_eq!(results.len(), 5, "timestamp >= mid should return 5 events (5-9)");
    assert_eq!(timestamps[0], ts_max, "First result should be newest");
    assert_eq!(*timestamps.last().unwrap(), ts_mid, "Last result should be the boundary");

    // Test <
    let q = format!("timestamp < {} ORDER BY timestamp DESC", ts_mid);
    let results: Vec<SimpleEventView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 5, "timestamp < mid should return 5 events (0-4)");

    // Test >
    let q = format!("timestamp > {} ORDER BY timestamp DESC", ts_mid);
    let results: Vec<SimpleEventView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 4, "timestamp > mid should return 4 events (6-9)");

    Ok(())
}

/// eq_prefix_len = 1: Single equality prefix + DESC inequality
/// Index: [category ASC, score DESC]
#[tokio::test]
async fn test_desc_inequality_single_equality_prefix() -> Result<()> {
    let ctx = setup().await?;

    {
        let trx = ctx.begin();
        for i in 0..10i64 {
            trx.create(&ScoredItem { category: "A".to_string(), score: i * 10, name: format!("Item A-{}", i) }).await?;
            trx.create(&ScoredItem { category: "B".to_string(), score: i * 10, name: format!("Item B-{}", i) }).await?;
        }
        trx.commit().await?;
    }

    let score_mid = 50;

    // Test: category = 'A' AND score <= 50 ORDER BY score DESC
    let q = format!("category = 'A' AND score <= {} ORDER BY score DESC", score_mid);
    let results: Vec<ScoredItemView> = ctx.fetch(q.as_str()).await?;
    let scores: Vec<i64> = results.iter().map(|s| s.score().unwrap()).collect();

    assert_eq!(results.len(), 6, "Should get 6 items with score <= 50");
    assert_eq!(scores, vec![50, 40, 30, 20, 10, 0], "Should be in DESC order");

    for item in &results {
        assert_eq!(item.category().unwrap(), "A", "All results should be category A");
    }

    // Test: category = 'B' AND score >= 50 ORDER BY score DESC
    let q = format!("category = 'B' AND score >= {} ORDER BY score DESC", score_mid);
    let results: Vec<ScoredItemView> = ctx.fetch(q.as_str()).await?;
    let scores: Vec<i64> = results.iter().map(|s| s.score().unwrap()).collect();

    assert_eq!(results.len(), 5, "Should get 5 items with score >= 50");
    assert_eq!(scores, vec![90, 80, 70, 60, 50], "Should be in DESC order");

    Ok(())
}

/// eq_prefix_len = 2: Two equality columns + DESC inequality (THE BUG SCENARIO)
/// Index: [room ASC, deleted ASC, timestamp DESC]
///
/// This is THE canonical PR #212 bug scenario - tests that we check the inequality
/// column's direction (timestamp at index 2) not the first column (room at index 0).
#[tokio::test]
async fn test_desc_inequality_two_equality_prefix() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_mid = timestamps[5];
    let ts_max = timestamps[9];

    // Test <=  (upper bound only in handle_desc_inequality)
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 6, "Should get 6 messages (0-5)");
    assert_desc_order(&result_ts, "timestamp <= mid");
    assert_eq!(result_ts[0], ts_mid, "First should be the boundary");
    assert_eq!(*result_ts.last().unwrap(), timestamps[0], "Last should be oldest");

    // Test >= (lower bound only in handle_desc_inequality)
    let q = format!("room = '{}' AND deleted = false AND timestamp >= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get 5 messages (5-9)");
    assert_eq!(result_ts[0], ts_max, "First should be newest");
    assert_eq!(*result_ts.last().unwrap(), ts_mid, "Last should be boundary");

    Ok(())
}

/// eq_prefix_len = 3: Three equality columns + DESC inequality
/// Index: [org ASC, team ASC, project ASC, priority DESC]
#[tokio::test]
async fn test_desc_inequality_three_equality_prefix() -> Result<()> {
    let ctx = setup().await?;

    {
        let trx = ctx.begin();
        for i in 0..10i64 {
            trx.create(&Task {
                org: "Acme".to_string(),
                team: "Engineering".to_string(),
                project: "Backend".to_string(),
                priority: i * 10,
                title: format!("Task {}", i),
            })
            .await?;
        }
        trx.commit().await?;
    }

    let priority_mid = 50;

    let q =
        format!("org = 'Acme' AND team = 'Engineering' AND project = 'Backend' AND priority <= {} ORDER BY priority DESC", priority_mid);
    let results: Vec<TaskView> = ctx.fetch(q.as_str()).await?;
    let priorities: Vec<i64> = results.iter().map(|t| t.priority().unwrap()).collect();

    assert_eq!(results.len(), 6, "Should get 6 tasks with priority <= 50");
    assert_eq!(priorities, vec![50, 40, 30, 20, 10, 0], "Should be in DESC order");

    Ok(())
}

// ============================================================================
// SECTION 2: Strict Inequality Operators (< and >)
// These are the ONLY tests of < and > with eq_prefix_len > 0
// ============================================================================

/// Test < operator with equality prefix
/// Exercises handle_desc_inequality branch: (false, true) with upper_open=true
#[tokio::test]
async fn test_operator_less_than_desc() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_5 = timestamps[5];

    let q = format!("room = '{}' AND deleted = false AND timestamp < {} ORDER BY timestamp DESC", room_id.to_base64(), ts_5);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 5, "< should exclude boundary (messages 0-4)");
    assert!(!result_ts.contains(&ts_5), "Should NOT contain boundary value");
    assert_eq!(result_ts[0], timestamps[4], "First should be one below boundary");

    Ok(())
}

/// Test > operator with equality prefix
/// Exercises handle_desc_inequality branch: (true, false) with lower_open=true
#[tokio::test]
async fn test_operator_greater_than_desc() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_5 = timestamps[5];

    let q = format!("room = '{}' AND deleted = false AND timestamp > {} ORDER BY timestamp DESC", room_id.to_base64(), ts_5);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 4, "> should exclude boundary (messages 6-9)");
    assert!(!result_ts.contains(&ts_5), "Should NOT contain boundary value");
    assert_eq!(*result_ts.last().unwrap(), timestamps[6], "Last should be one above boundary");

    Ok(())
}

// ============================================================================
// SECTION 3: Range Queries (All Inclusivity Combinations)
// Each tests a unique sub-case of handle_desc_inequality's (true, true) branch
// ============================================================================

/// Range query: >= AND <= (both inclusive)
#[tokio::test]
async fn test_range_inclusive_inclusive() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_3 = timestamps[3];
    let ts_7 = timestamps[7];

    let q = format!(
        "room = '{}' AND deleted = false AND timestamp >= {} AND timestamp <= {} ORDER BY timestamp DESC",
        room_id.to_base64(),
        ts_3,
        ts_7
    );
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get messages 3,4,5,6,7");
    assert_eq!(result_ts, vec![ts_7, timestamps[6], timestamps[5], timestamps[4], ts_3]);
    assert!(result_ts.contains(&ts_3), "Should include lower bound");
    assert!(result_ts.contains(&ts_7), "Should include upper bound");

    Ok(())
}

/// Range query: > AND < (both exclusive)
#[tokio::test]
async fn test_range_exclusive_exclusive() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_3 = timestamps[3];
    let ts_7 = timestamps[7];

    let q = format!(
        "room = '{}' AND deleted = false AND timestamp > {} AND timestamp < {} ORDER BY timestamp DESC",
        room_id.to_base64(),
        ts_3,
        ts_7
    );
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 3, "Should get messages 4,5,6 only");
    assert!(!result_ts.contains(&ts_3), "Should NOT include lower bound");
    assert!(!result_ts.contains(&ts_7), "Should NOT include upper bound");
    assert_eq!(result_ts, vec![timestamps[6], timestamps[5], timestamps[4]]);

    Ok(())
}

/// Range query: >= AND < (lower inclusive, upper exclusive)
#[tokio::test]
async fn test_range_inclusive_exclusive() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_3 = timestamps[3];
    let ts_7 = timestamps[7];

    let q = format!(
        "room = '{}' AND deleted = false AND timestamp >= {} AND timestamp < {} ORDER BY timestamp DESC",
        room_id.to_base64(),
        ts_3,
        ts_7
    );
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 4, "Should get messages 3,4,5,6");
    assert!(result_ts.contains(&ts_3), "Should include lower bound");
    assert!(!result_ts.contains(&ts_7), "Should NOT include upper bound");

    Ok(())
}

/// Range query: > AND <= (lower exclusive, upper inclusive)
#[tokio::test]
async fn test_range_exclusive_inclusive() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_3 = timestamps[3];
    let ts_7 = timestamps[7];

    let q = format!(
        "room = '{}' AND deleted = false AND timestamp > {} AND timestamp <= {} ORDER BY timestamp DESC",
        room_id.to_base64(),
        ts_3,
        ts_7
    );
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 4, "Should get messages 4,5,6,7");
    assert!(!result_ts.contains(&ts_3), "Should NOT include lower bound");
    assert!(result_ts.contains(&ts_7), "Should include upper bound");

    Ok(())
}

// ============================================================================
// SECTION 4: Boundary Conditions & Edge Cases
// ============================================================================

/// Empty result set: query matches nothing
#[tokio::test]
async fn test_empty_result_set() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let _timestamps = create_messages(&ctx, room_id, 10).await?;

    let q =
        format!("room = '{}' AND deleted = false AND timestamp < {} ORDER BY timestamp DESC", room_id.to_base64(), TIMESTAMP_BASE - 1000);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;

    assert_eq!(results.len(), 0, "Should return empty result set");

    Ok(())
}

/// Single result: exactly one match
#[tokio::test]
async fn test_single_result() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_5 = timestamps[5];
    let q = format!(
        "room = '{}' AND deleted = false AND timestamp >= {} AND timestamp <= {} ORDER BY timestamp DESC",
        room_id.to_base64(),
        ts_5,
        ts_5
    );
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;

    assert_eq!(results.len(), 1, "Should return exactly one result");
    assert_eq!(results[0].timestamp().unwrap(), ts_5);

    Ok(())
}

/// Duplicate timestamps: multiple records with same timestamp
#[tokio::test]
async fn test_duplicate_timestamps() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;

    let same_ts = TIMESTAMP_BASE + 5000;
    {
        let trx = ctx.begin();
        for i in 0..5 {
            trx.create(&TestMessage { room: room_id.into(), text: format!("Duplicate {}", i), timestamp: same_ts, deleted: false }).await?;
        }
        trx.commit().await?;
    }

    // Query <= same_ts should get all 5
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", room_id.to_base64(), same_ts);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;

    assert_eq!(results.len(), 5, "Should return all 5 duplicates");
    for r in &results {
        assert_eq!(r.timestamp().unwrap(), same_ts);
    }

    // Query < same_ts should get none
    let q = format!("room = '{}' AND deleted = false AND timestamp < {} ORDER BY timestamp DESC", room_id.to_base64(), same_ts);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 0, "Should exclude all duplicates with <");

    Ok(())
}

/// Boundary at minimum value - tests end of DESC-encoded key space
/// (minimum logical value -> maximum encoded bytes)
#[tokio::test]
async fn test_boundary_at_minimum() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_min = timestamps[0];

    // <= min should get only the first message
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_min);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].timestamp().unwrap(), ts_min);

    // < min should get nothing
    let q = format!("room = '{}' AND deleted = false AND timestamp < {} ORDER BY timestamp DESC", room_id.to_base64(), ts_min);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 0);

    // >= min should get all
    let q = format!("room = '{}' AND deleted = false AND timestamp >= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_min);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 10);

    Ok(())
}

/// Boundary at maximum value - tests start of DESC-encoded key space
/// (maximum logical value -> minimum encoded bytes)
#[tokio::test]
async fn test_boundary_at_maximum() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_max = timestamps[9];

    // >= max should get only the last message
    let q = format!("room = '{}' AND deleted = false AND timestamp >= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_max);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].timestamp().unwrap(), ts_max);

    // > max should get nothing
    let q = format!("room = '{}' AND deleted = false AND timestamp > {} ORDER BY timestamp DESC", room_id.to_base64(), ts_max);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 0);

    // <= max should get all
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", room_id.to_base64(), ts_max);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    assert_eq!(results.len(), 10);

    Ok(())
}

// ============================================================================
// SECTION 5: ORDER BY Variations
// ============================================================================

/// ASC ordering with inequalities (ensure we didn't break ASC)
#[tokio::test]
async fn test_asc_ordering_not_broken() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_mid = timestamps[5];

    // ASC with <=
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp ASC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 6);
    assert_asc_order(&result_ts, "ASC with <=");
    assert_eq!(result_ts[0], timestamps[0], "First should be oldest");
    assert_eq!(*result_ts.last().unwrap(), ts_mid, "Last should be boundary");

    // ASC with >=
    let q = format!("room = '{}' AND deleted = false AND timestamp >= {} ORDER BY timestamp ASC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 5);
    assert_asc_order(&result_ts, "ASC with >=");
    assert_eq!(result_ts[0], ts_mid, "First should be boundary");
    assert_eq!(*result_ts.last().unwrap(), timestamps[9], "Last should be newest");

    Ok(())
}

/// Multi-column ORDER BY: verify adding secondary sort columns doesn't break primary DESC sort
#[tokio::test]
async fn test_multi_column_order_by() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let ts_mid = timestamps[5];

    // Mixed directions: timestamp DESC, text ASC
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC, text ASC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 6);
    assert_desc_order(&result_ts, "Primary sort should be DESC (mixed)");

    // Both DESC: timestamp DESC, text DESC
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC, text DESC", room_id.to_base64(), ts_mid);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 6);
    assert_desc_order(&result_ts, "Primary sort should be DESC (both)");

    Ok(())
}

/// No inequality, just ORDER BY DESC (baseline sanity check)
#[tokio::test]
async fn test_no_inequality_just_order_by() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 10).await?;

    let q = format!("room = '{}' AND deleted = false ORDER BY timestamp DESC", room_id.to_base64());
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;
    let result_ts = get_timestamps(&results);

    assert_eq!(results.len(), 10);
    assert_desc_order(&result_ts, "Should be DESC order");
    assert_eq!(result_ts[0], timestamps[9], "First should be newest");
    assert_eq!(*result_ts.last().unwrap(), timestamps[0], "Last should be oldest");

    Ok(())
}

// ============================================================================
// SECTION 6: Regression Guard
// ============================================================================

/// Explicit regression test for PR #212 bug - "all records match" scenario
///
/// This tests the specific failure mode where the bug caused the query to return
/// only 1 record instead of all matching records. The boundary is at the maximum
/// timestamp, so ALL records should match `timestamp <= max`.
#[tokio::test]
async fn test_regression_pr212_desc_inequality_with_asc_prefix() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;
    let timestamps = create_messages(&ctx, room_id, 50).await?; // Reduced from 100

    let newest_ts = timestamps[49];

    // THE BUG: This query returned only 1 record instead of all 50 because the planner
    // checked if `room` (first column, ASC) was DESC instead of `timestamp` (inequality column, DESC)
    let q = format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", room_id.to_base64(), newest_ts);
    let results: Vec<TestMessageView> = ctx.fetch(q.as_str()).await?;

    assert_eq!(results.len(), 50, "PR #212 regression: timestamp <= newest should return ALL messages, not 1");

    let result_ts = get_timestamps(&results);
    assert_desc_order(&result_ts, "Results should be in DESC order");
    assert_eq!(result_ts[0], newest_ts, "First should be newest");
    assert_eq!(*result_ts.last().unwrap(), timestamps[0], "Last should be oldest");

    Ok(())
}
