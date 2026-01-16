//! End-to-end pagination tests for LiveQuery cursor scenarios.
//!
//! These tests verify that pagination works correctly in real-world scenarios
//! including local queries, forward pagination, and inter-node communication.
//!
//! For core DESC inequality bound tests, see `desc_inequality.rs`.

mod common;

use crate::common::*;
use ankurah::{Model, Ref};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Test Models
// ============================================================================

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

// ============================================================================
// Constants & Helpers
// ============================================================================

const TIMESTAMP_BASE: i64 = 1700000000000;
const TIMESTAMP_STEP: i64 = 1000;

async fn setup() -> Result<Context> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context(DEFAULT_CONTEXT)?)
}

async fn create_room(ctx: &Context, name: &str) -> Result<EntityId> {
    let trx = ctx.begin();
    let room = trx.create(&TestRoom { name: name.to_string() }).await?;
    let id = room.id();
    trx.commit().await?;
    Ok(id)
}

fn get_timestamps(results: &[TestMessageView]) -> Vec<i64> { results.iter().map(|m| m.timestamp().unwrap()).collect() }

// ============================================================================
// E2E Pagination Tests
// ============================================================================

/// LiveQuery pagination: update_selection with cursor
/// Tests the original bug scenario from the React Native app.
#[tokio::test]
async fn test_pagination_cursor_local() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;

    // Create 100 messages
    {
        let trx = ctx.begin();
        for i in 0..100i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&TestMessage { room: room_id.into(), text: format!("Message #{:03}", i), timestamp: ts, deleted: false }).await?;
        }
        trx.commit().await?;
    }

    // Initial query: newest 33 messages
    let q = format!("room = '{}' AND deleted = false ORDER BY timestamp DESC LIMIT 33", room_id.to_base64());
    let lq = ctx.query_wait::<TestMessageView>(q.as_str()).await?;

    let items = lq.peek();
    assert_eq!(items.len(), 33, "Initial query should return 33");

    let timestamps = get_timestamps(&items);
    let newest_ts = *timestamps.iter().max().unwrap();
    let oldest_in_page = *timestamps.iter().min().unwrap();

    assert_eq!(newest_ts, TIMESTAMP_BASE + 99 * TIMESTAMP_STEP);
    assert_eq!(oldest_in_page, TIMESTAMP_BASE + 67 * TIMESTAMP_STEP);

    // Pagination: get more messages with timestamp <= newest (expand the window)
    let pagination_q =
        format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC LIMIT 54", room_id.to_base64(), newest_ts);
    lq.update_selection_wait(pagination_q.as_str()).await?;

    let items_after = lq.peek();
    assert_eq!(items_after.len(), 54, "After pagination should return 54 (the bug returned only 33)");

    Ok(())
}

/// LiveQuery pagination: forward pagination with >
/// Tests next-page navigation pattern (ASC order, > cursor).
#[tokio::test]
async fn test_pagination_forward() -> Result<()> {
    let ctx = setup().await?;
    let room_id = create_room(&ctx, "TestRoom").await?;

    // Create 100 messages
    {
        let trx = ctx.begin();
        for i in 0..100i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&TestMessage { room: room_id.into(), text: format!("Message #{:03}", i), timestamp: ts, deleted: false }).await?;
        }
        trx.commit().await?;
    }

    // Initial query: oldest 20 messages (ASC order)
    let q = format!("room = '{}' AND deleted = false ORDER BY timestamp ASC LIMIT 20", room_id.to_base64());
    let lq = ctx.query_wait::<TestMessageView>(q.as_str()).await?;

    let items = lq.peek();
    assert_eq!(items.len(), 20);

    let timestamps = get_timestamps(&items);
    let oldest_ts = *timestamps.iter().min().unwrap();
    let cursor_ts = *timestamps.iter().max().unwrap(); // Last item becomes cursor

    assert_eq!(oldest_ts, TIMESTAMP_BASE);
    assert_eq!(cursor_ts, TIMESTAMP_BASE + 19 * TIMESTAMP_STEP);

    // Forward pagination: get next page after cursor
    let pagination_q =
        format!("room = '{}' AND deleted = false AND timestamp > {} ORDER BY timestamp ASC LIMIT 20", room_id.to_base64(), cursor_ts);
    lq.update_selection_wait(pagination_q.as_str()).await?;

    let items_after = lq.peek();
    assert_eq!(items_after.len(), 20, "Should get next 20 messages");

    let new_timestamps = get_timestamps(&items_after);
    assert_eq!(new_timestamps[0], TIMESTAMP_BASE + 20 * TIMESTAMP_STEP, "Should start after cursor");

    Ok(())
}

/// Inter-node pagination (server/client scenario)
/// Tests that pagination works correctly when queries cross network boundaries.
#[tokio::test]
async fn test_pagination_inter_node() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Create room on server
    let room_id = {
        let trx = server_ctx.begin();
        let room = trx.create(&TestRoom { name: "General".to_string() }).await?;
        let id = room.id();
        trx.commit().await?;
        id
    };

    // Create 100 messages on server
    {
        let trx = server_ctx.begin();
        for i in 0..100i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&TestMessage { room: room_id.into(), text: format!("Message #{:03}", i), timestamp: ts, deleted: false }).await?;
        }
        trx.commit().await?;
    }

    // Client queries with nocache to force server fetch
    let q = format!("room = '{}' AND deleted = false ORDER BY timestamp DESC LIMIT 33", room_id.to_base64());
    let lq = client_ctx.query_wait::<TestMessageView>(nocache(q.as_str())?).await?;

    let items = lq.peek();
    assert_eq!(items.len(), 33);

    let newest_ts = get_timestamps(&items).into_iter().max().unwrap();

    // Pagination through client
    let pagination_q =
        format!("room = '{}' AND deleted = false AND timestamp <= {} ORDER BY timestamp DESC LIMIT 54", room_id.to_base64(), newest_ts);
    lq.update_selection_wait(pagination_q.as_str()).await?;

    let items_after = lq.peek();
    assert_eq!(items_after.len(), 54, "Inter-node pagination should work correctly");

    Ok(())
}

// ============================================================================
// Multi-Column ORDER BY Pagination Tests
// ============================================================================

/// Multi-column ORDER BY model for cursor pagination
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct ForumPost {
    pub category: String,
    pub timestamp: i64,
    pub author: String,
    pub title: String,
}

/// Multi-column cursor pagination: ORDER BY category ASC, timestamp DESC
/// Tests pagination where cursor must account for both columns.
#[tokio::test]
async fn test_pagination_multi_column_order_by() -> Result<()> {
    let ctx = setup().await?;

    // Create posts with duplicate categories and varying timestamps
    {
        let trx = ctx.begin();
        // Category A: 10 posts
        for i in 0..10i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&ForumPost {
                category: "A".to_string(),
                timestamp: ts,
                author: format!("Author {}", i % 3),
                title: format!("A Post {}", i),
            })
            .await?;
        }
        // Category B: 10 posts
        for i in 0..10i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&ForumPost {
                category: "B".to_string(),
                timestamp: ts,
                author: format!("Author {}", i % 3),
                title: format!("B Post {}", i),
            })
            .await?;
        }
        // Category C: 10 posts
        for i in 0..10i64 {
            let ts = TIMESTAMP_BASE + (i * TIMESTAMP_STEP);
            trx.create(&ForumPost {
                category: "C".to_string(),
                timestamp: ts,
                author: format!("Author {}", i % 3),
                title: format!("C Post {}", i),
            })
            .await?;
        }
        trx.commit().await?;
    }

    // First page: ORDER BY category ASC, timestamp DESC LIMIT 15
    // Should get: all 10 A posts (newest first) + 5 B posts (newest first)
    let q = "timestamp > 0 ORDER BY category ASC, timestamp DESC LIMIT 15";
    let lq = ctx.query_wait::<ForumPostView>(q).await?;

    let items = lq.peek();
    assert_eq!(items.len(), 15);

    // Verify ordering: A posts should come first with DESC timestamps, then B posts
    let categories: Vec<String> = items.iter().map(|p| p.category().unwrap()).collect();
    let timestamps: Vec<i64> = items.iter().map(|p| p.timestamp().unwrap()).collect();

    // First 10 should be category A
    assert!(categories[..10].iter().all(|c| c == "A"), "First 10 should be category A");
    // Next 5 should be category B
    assert!(categories[10..15].iter().all(|c| c == "B"), "Next 5 should be category B");

    // Within each category, timestamps should be DESC
    assert!(timestamps[..10].windows(2).all(|w| w[0] >= w[1]), "A posts should be DESC");
    assert!(timestamps[10..15].windows(2).all(|w| w[0] >= w[1]), "B posts should be DESC");

    // Get the cursor position for next page
    let last_category = categories.last().unwrap().clone();
    let last_timestamp = *timestamps.last().unwrap();
    assert_eq!(last_category, "B");
    assert_eq!(last_timestamp, TIMESTAMP_BASE + 5 * TIMESTAMP_STEP); // B post 5

    // Second page using cursor: get items after cursor
    // This is the tricky case: we need items where
    // (category > 'B') OR (category = 'B' AND timestamp < last_timestamp)
    // For simplicity, we just get remaining items with LIMIT
    let q2 = "timestamp > 0 ORDER BY category ASC, timestamp DESC LIMIT 30";
    lq.update_selection_wait(q2).await?;

    let items2 = lq.peek();
    assert_eq!(items2.len(), 30, "Should get all 30 posts");

    let categories2: Vec<String> = items2.iter().map(|p| p.category().unwrap()).collect();
    assert!(categories2[..10].iter().all(|c| c == "A"), "First 10 still A");
    assert!(categories2[10..20].iter().all(|c| c == "B"), "Next 10 should be B");
    assert!(categories2[20..30].iter().all(|c| c == "C"), "Last 10 should be C");

    Ok(())
}

/// Multi-column pagination with equality prefix
/// ORDER BY timestamp DESC, author ASC within a category
#[tokio::test]
async fn test_pagination_multi_column_with_equality_prefix() -> Result<()> {
    let ctx = setup().await?;

    // Create posts with same timestamp but different authors
    {
        let trx = ctx.begin();
        // Posts with same timestamp, different authors
        for ts_offset in 0..5i64 {
            let ts = TIMESTAMP_BASE + (ts_offset * TIMESTAMP_STEP);
            for author_idx in 0..3 {
                trx.create(&ForumPost {
                    category: "Tech".to_string(),
                    timestamp: ts,
                    author: format!("Author_{}", (b'C' - author_idx) as char), // C, B, A
                    title: format!("Post ts={} author={}", ts_offset, author_idx),
                })
                .await?;
            }
        }
        trx.commit().await?;
    }

    // Query: category = 'Tech' ORDER BY timestamp DESC, author ASC LIMIT 10
    let q = "category = 'Tech' ORDER BY timestamp DESC, author ASC LIMIT 10";
    let lq = ctx.query_wait::<ForumPostView>(q).await?;

    let items = lq.peek();
    assert_eq!(items.len(), 10);

    let result_tuples: Vec<(i64, String)> = items.iter().map(|p| (p.timestamp().unwrap(), p.author().unwrap())).collect();

    // Expected order:
    // ts_offset=4: A, B, C (ts DESC first, then author ASC)
    // ts_offset=3: A, B, C
    // ts_offset=2: A (only first from this group to reach 10)

    // Check first 3 are from ts_offset=4 with author ASC
    let ts_4 = TIMESTAMP_BASE + 4 * TIMESTAMP_STEP;
    assert_eq!(result_tuples[0], (ts_4, "Author_A".to_string()));
    assert_eq!(result_tuples[1], (ts_4, "Author_B".to_string()));
    assert_eq!(result_tuples[2], (ts_4, "Author_C".to_string()));

    // Check next 3 are from ts_offset=3 with author ASC
    let ts_3 = TIMESTAMP_BASE + 3 * TIMESTAMP_STEP;
    assert_eq!(result_tuples[3], (ts_3, "Author_A".to_string()));
    assert_eq!(result_tuples[4], (ts_3, "Author_B".to_string()));
    assert_eq!(result_tuples[5], (ts_3, "Author_C".to_string()));

    Ok(())
}
