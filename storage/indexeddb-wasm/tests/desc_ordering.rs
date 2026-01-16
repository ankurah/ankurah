//! DESC ordering tests for IndexedDB.
//!
//! Unlike Sled, IndexedDB doesn't support DESC indexes natively. Instead:
//! - DESC ordering is achieved via `ScanDirection::Reverse` cursor
//! - Multi-column mixed directions use `order_by_spill` for in-memory sorting
//!
//! These tests verify that inequality predicates work correctly with DESC ordering
//! when using the Reverse scan direction approach.

mod common;

use ankurah::Model;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

// ============================================================================
// Test Models
// ============================================================================

/// Simple event for testing basic DESC ordering with inequality
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct LogEvent {
    pub category: String,
    pub timestamp: i64,
    pub level: String,
}

/// Model for testing multi-column equality prefix with DESC ordering
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Message {
    pub room: String,
    #[active_type(LWW)]
    pub deleted: bool,
    pub timestamp: i64,
    pub text: String,
}

// ============================================================================
// Test Constants
// ============================================================================

const TIMESTAMP_BASE: i64 = 1700000000000;
const TIMESTAMP_STEP: i64 = 1000;

// ============================================================================
// Helper Functions
// ============================================================================

fn log_timestamps(events: &[LogEventView]) -> Vec<i64> { events.iter().map(|e| e.timestamp().unwrap()).collect() }

fn msg_timestamps(messages: &[MessageView]) -> Vec<i64> { messages.iter().map(|m| m.timestamp().unwrap()).collect() }

fn assert_desc_order(timestamps: &[i64], context: &str) {
    assert!(timestamps.windows(2).all(|w| w[0] >= w[1]), "{}: Expected DESC order, got {:?}", context, timestamps);
}

fn assert_asc_order(timestamps: &[i64], context: &str) {
    assert!(timestamps.windows(2).all(|w| w[0] <= w[1]), "{}: Expected ASC order, got {:?}", context, timestamps);
}

async fn create_log_events(ctx: &ankurah::Context, events: Vec<(&str, i64, &str)>) -> Result<(), anyhow::Error> {
    let trx = ctx.begin();
    for (category, timestamp, level) in events {
        trx.create(&LogEvent { category: category.to_string(), timestamp, level: level.to_string() }).await?;
    }
    trx.commit().await?;
    Ok(())
}

async fn create_messages(ctx: &ankurah::Context, messages: Vec<(&str, bool, i64, &str)>) -> Result<(), anyhow::Error> {
    let trx = ctx.begin();
    for (room, deleted, timestamp, text) in messages {
        trx.create(&Message { room: room.to_string(), deleted, timestamp, text: text.to_string() }).await?;
    }
    trx.commit().await?;
    Ok(())
}

// ============================================================================
// SECTION 1: Basic DESC Ordering with Inequality (No Equality Prefix)
// ============================================================================

/// No equality prefix: DESC ordering with inequality on timestamp
/// Tests Reverse scan direction
#[wasm_bindgen_test]
pub async fn test_desc_inequality_no_equality_prefix() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> =
        (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), if i % 2 == 0 { "INFO" } else { "ERROR" })).collect();
    create_log_events(&ctx, events).await?;

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;
    let ts_max = TIMESTAMP_BASE + 9 * TIMESTAMP_STEP;

    // Test <=
    let q = format!("timestamp <= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 6, "timestamp <= mid should return 6 events (0-5)");
    assert_desc_order(&timestamps, "Should be DESC order");
    assert_eq!(timestamps[0], ts_mid, "First should be the boundary");

    // Test >=
    let q = format!("timestamp >= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 5, "timestamp >= mid should return 5 events (5-9)");
    assert_eq!(timestamps[0], ts_max, "First should be newest");
    assert_eq!(*timestamps.last().unwrap(), ts_mid, "Last should be boundary");

    // Test <
    let q = format!("timestamp < {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    assert_eq!(results.len(), 5, "timestamp < mid should return 5 events (0-4)");

    // Test >
    let q = format!("timestamp > {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    assert_eq!(results.len(), 4, "timestamp > mid should return 4 events (6-9)");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 2: Single Equality Prefix + DESC Inequality
// ============================================================================

/// Single equality prefix with DESC inequality
/// Tests: category = X AND timestamp <= Y ORDER BY timestamp DESC
#[wasm_bindgen_test]
pub async fn test_desc_inequality_single_equality_prefix() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create events in two categories
    let mut events = Vec::new();
    for i in 0..10i64 {
        events.push(("cat_a", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO"));
        events.push(("cat_b", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "ERROR"));
    }
    create_log_events(&ctx, events).await?;

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;

    // Test: category = 'cat_a' AND timestamp <= mid ORDER BY timestamp DESC
    let q = format!("category = 'cat_a' AND timestamp <= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 6, "Should get 6 events from cat_a with timestamp <= mid");
    assert_desc_order(&timestamps, "Should be DESC order");
    assert_eq!(timestamps[0], ts_mid, "First should be the boundary");

    // Verify all results are from cat_a
    for event in &results {
        assert_eq!(event.category().unwrap(), "cat_a", "All should be from cat_a");
    }

    // Test: category = 'cat_b' AND timestamp >= mid ORDER BY timestamp DESC
    let q = format!("category = 'cat_b' AND timestamp >= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get 5 events from cat_b with timestamp >= mid");
    assert_desc_order(&timestamps, "Should be DESC order");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 3: Two Equality Columns + DESC Inequality (Chat Message Pattern)
// ============================================================================

/// Two equality columns with DESC inequality (mimics chat message pagination)
/// Tests: room = X AND deleted = false AND timestamp <= Y ORDER BY timestamp DESC
#[wasm_bindgen_test]
pub async fn test_desc_inequality_two_equality_prefix_lte() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create messages in a room
    let mut messages = Vec::new();
    for i in 0..10i64 {
        messages.push(("room_1", false, TIMESTAMP_BASE + (i * TIMESTAMP_STEP), format!("Message {}", i).leak() as &str));
    }
    // Add some deleted messages that should be filtered out
    messages.push(("room_1", true, TIMESTAMP_BASE + 5 * TIMESTAMP_STEP, "Deleted"));
    // Add messages in another room
    messages.push(("room_2", false, TIMESTAMP_BASE + 5 * TIMESTAMP_STEP, "Other room"));
    create_messages(&ctx, messages).await?;

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;

    // Test <=
    let q = format!("room = 'room_1' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<MessageView>(q.as_str()).await?;
    let timestamps = msg_timestamps(&results);

    assert_eq!(results.len(), 6, "Should get 6 non-deleted messages with timestamp <= mid");
    assert_desc_order(&timestamps, "Should be DESC order");
    assert_eq!(timestamps[0], ts_mid, "First should be the boundary");

    // Verify all results are from room_1 and not deleted
    for msg in &results {
        assert_eq!(msg.room().unwrap(), "room_1");
        assert!(!msg.deleted().unwrap());
    }

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Two equality columns with DESC inequality using >=
/// Tests: room = X AND deleted = false AND timestamp >= Y ORDER BY timestamp DESC
///
/// This was a known bug where >= with two equality prefix columns and DESC ordering
/// didn't correctly set up the IndexedDB scan bounds. The fix is in plan_bounds_to_idb_range()
/// which now caps the upper bound at the equality prefix boundary for Reverse scans.
#[wasm_bindgen_test]
pub async fn test_desc_inequality_two_equality_prefix_gte() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create messages in a room
    let mut messages = Vec::new();
    for i in 0..10i64 {
        messages.push(("room_1", false, TIMESTAMP_BASE + (i * TIMESTAMP_STEP), format!("Message {}", i).leak() as &str));
    }
    // Add some deleted messages that should be filtered out
    messages.push(("room_1", true, TIMESTAMP_BASE + 5 * TIMESTAMP_STEP, "Deleted"));
    // Add messages in another room
    messages.push(("room_2", false, TIMESTAMP_BASE + 5 * TIMESTAMP_STEP, "Other room"));
    create_messages(&ctx, messages).await?;

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;
    let ts_max = TIMESTAMP_BASE + 9 * TIMESTAMP_STEP;

    // Test >=
    let q = format!("room = 'room_1' AND deleted = false AND timestamp >= {} ORDER BY timestamp DESC", ts_mid);
    let results = ctx.fetch::<MessageView>(q.as_str()).await?;
    let timestamps = msg_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get 5 messages with timestamp >= mid");
    assert_eq!(timestamps[0], ts_max, "First should be newest");
    assert_eq!(*timestamps.last().unwrap(), ts_mid, "Last should be boundary");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 4: Range Queries with DESC Ordering
// ============================================================================

/// Range query with both bounds: timestamp >= A AND timestamp <= B
#[wasm_bindgen_test]
pub async fn test_range_inclusive_inclusive_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    let ts_3 = TIMESTAMP_BASE + 3 * TIMESTAMP_STEP;
    let ts_7 = TIMESTAMP_BASE + 7 * TIMESTAMP_STEP;

    let q = format!("timestamp >= {} AND timestamp <= {} ORDER BY timestamp DESC", ts_3, ts_7);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get events 3,4,5,6,7");
    assert!(timestamps.contains(&ts_3), "Should include lower bound");
    assert!(timestamps.contains(&ts_7), "Should include upper bound");
    assert_eq!(timestamps[0], ts_7, "First should be upper bound");
    assert_eq!(*timestamps.last().unwrap(), ts_3, "Last should be lower bound");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Range query with exclusive bounds: timestamp > A AND timestamp < B
#[wasm_bindgen_test]
pub async fn test_range_exclusive_exclusive_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    let ts_3 = TIMESTAMP_BASE + 3 * TIMESTAMP_STEP;
    let ts_7 = TIMESTAMP_BASE + 7 * TIMESTAMP_STEP;

    let q = format!("timestamp > {} AND timestamp < {} ORDER BY timestamp DESC", ts_3, ts_7);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 3, "Should get events 4,5,6 only");
    assert!(!timestamps.contains(&ts_3), "Should NOT include lower bound");
    assert!(!timestamps.contains(&ts_7), "Should NOT include upper bound");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 5: LIMIT with DESC Ordering
// ============================================================================

/// LIMIT with DESC ordering and inequality
#[wasm_bindgen_test]
pub async fn test_limit_with_desc_inequality() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..20).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    let ts_mid = TIMESTAMP_BASE + 15 * TIMESTAMP_STEP;

    // Get 5 most recent events with timestamp <= mid
    let q = format!("timestamp <= {} ORDER BY timestamp DESC LIMIT 5", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 5, "Should get exactly 5 events");
    assert_desc_order(&timestamps, "Should be DESC order");
    assert_eq!(timestamps[0], ts_mid, "First should be the boundary");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// LIMIT with equality prefix and DESC
#[wasm_bindgen_test]
pub async fn test_limit_with_equality_prefix_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let mut messages = Vec::new();
    for i in 0..50i64 {
        messages.push(("room_1", false, TIMESTAMP_BASE + (i * TIMESTAMP_STEP), format!("Msg {}", i).leak() as &str));
    }
    create_messages(&ctx, messages).await?;

    let ts_boundary = TIMESTAMP_BASE + 40 * TIMESTAMP_STEP;

    // Typical chat pagination: get 20 most recent messages before boundary
    let q = format!("room = 'room_1' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC LIMIT 20", ts_boundary);
    let results = ctx.fetch::<MessageView>(q.as_str()).await?;
    let timestamps = msg_timestamps(&results);

    assert_eq!(results.len(), 20, "Should get exactly 20 messages");
    assert_desc_order(&timestamps, "Should be DESC order");
    assert_eq!(timestamps[0], ts_boundary, "First should be the boundary");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 6: Edge Cases
// ============================================================================

/// Empty result set with DESC ordering
#[wasm_bindgen_test]
pub async fn test_empty_result_set_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    // Query for timestamps before all data
    let q = format!("timestamp < {} ORDER BY timestamp DESC", TIMESTAMP_BASE - 1000);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    assert!(results.is_empty(), "Should return empty result set");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Single result with DESC ordering
#[wasm_bindgen_test]
pub async fn test_single_result_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    let ts_5 = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;

    // Query for exact match
    let q = format!("timestamp >= {} AND timestamp <= {} ORDER BY timestamp DESC", ts_5, ts_5);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;

    assert_eq!(results.len(), 1, "Should return exactly one result");
    assert_eq!(results[0].timestamp().unwrap(), ts_5);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Duplicate timestamps with DESC ordering
#[wasm_bindgen_test]
pub async fn test_duplicate_timestamps_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let same_ts = TIMESTAMP_BASE + 5000;
    let events: Vec<(&str, i64, &str)> = (0..5).map(|i| ("logs", same_ts, if i % 2 == 0 { "INFO" } else { "ERROR" })).collect();
    create_log_events(&ctx, events).await?;

    // Query <= same_ts should get all 5
    let q = format!("timestamp <= {} ORDER BY timestamp DESC", same_ts);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    assert_eq!(results.len(), 5, "Should return all 5 duplicates");

    // Query < same_ts should get none
    let q = format!("timestamp < {} ORDER BY timestamp DESC", same_ts);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    assert_eq!(results.len(), 0, "Should exclude all duplicates");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 7: ASC Ordering Sanity Check
// ============================================================================

/// ASC ordering still works correctly (baseline)
#[wasm_bindgen_test]
pub async fn test_asc_ordering_with_inequality() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let events: Vec<(&str, i64, &str)> = (0..10).map(|i| ("logs", TIMESTAMP_BASE + (i * TIMESTAMP_STEP), "INFO")).collect();
    create_log_events(&ctx, events).await?;

    let ts_mid = TIMESTAMP_BASE + 5 * TIMESTAMP_STEP;

    // ASC with <=
    let q = format!("timestamp <= {} ORDER BY timestamp ASC", ts_mid);
    let results = ctx.fetch::<LogEventView>(q.as_str()).await?;
    let timestamps = log_timestamps(&results);

    assert_eq!(results.len(), 6);
    assert_asc_order(&timestamps, "Should be ASC order");
    assert_eq!(timestamps[0], TIMESTAMP_BASE, "First should be oldest");
    assert_eq!(*timestamps.last().unwrap(), ts_mid, "Last should be boundary");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// SECTION 8: Pagination Pattern (Real-World Scenario)
// ============================================================================

/// Full pagination pattern: get newest N, then load more before cursor
#[wasm_bindgen_test]
pub async fn test_pagination_pattern_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create 100 messages
    let mut messages = Vec::new();
    for i in 0..100i64 {
        messages.push(("room_1", false, TIMESTAMP_BASE + (i * TIMESTAMP_STEP), format!("Msg {}", i).leak() as &str));
    }
    create_messages(&ctx, messages).await?;

    // Initial load: get 33 newest messages
    let initial = ctx.fetch::<MessageView>("room = 'room_1' AND deleted = false ORDER BY timestamp DESC LIMIT 33").await?;
    assert_eq!(initial.len(), 33);
    let initial_ts = msg_timestamps(&initial);
    assert_desc_order(&initial_ts, "Initial load");

    let newest_ts = initial_ts[0];
    let oldest_in_page = *initial_ts.last().unwrap();

    assert_eq!(newest_ts, TIMESTAMP_BASE + 99 * TIMESTAMP_STEP, "Newest should be message 99");
    assert_eq!(oldest_in_page, TIMESTAMP_BASE + 67 * TIMESTAMP_STEP, "Oldest in first page should be message 67");

    // Pagination: expand window to get 54 total messages <= newest
    let q = format!("room = 'room_1' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC LIMIT 54", newest_ts);
    let expanded = ctx.fetch::<MessageView>(q.as_str()).await?;

    assert_eq!(expanded.len(), 54, "Expanded query should return 54 messages");
    let expanded_ts = msg_timestamps(&expanded);
    assert_desc_order(&expanded_ts, "Expanded load");
    assert_eq!(expanded_ts[0], newest_ts, "Should still start at newest");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
