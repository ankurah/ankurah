//! Tests for JSON path queries with LiveQuery/subscriptions.
//!
//! This tests that JSON path predicates (e.g., `context.task_id = ?`) work correctly
//! when used with LiveQuery subscriptions, not just fetch().
//!
//! Issue: JSON path queries work with ctx.fetch() but may fail with ctx.query() because
//! the reactor's update_query re-evaluates predicates against Entity::Filterable::value()
//! which must properly handle JSON path traversal.

use ankurah::property::Json;
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

mod common;
use common::TestWatcher;

/// A model with a Json property for testing JSON path queries with LiveQuery
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub content: String,
    pub context: Json,
}

#[tokio::test]
async fn test_json_path_livequery_initial_results() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    // Create messages with different task_id contexts
    let task_id_a = "task-aaa";
    let task_id_b = "task-bbb";

    {
        let trx = ctx.begin();
        trx.create(&Message { content: "Message 1 for task A".to_string(), context: Json::new(serde_json::json!({"task_id": task_id_a})) })
            .await?;
        trx.create(&Message { content: "Message 2 for task A".to_string(), context: Json::new(serde_json::json!({"task_id": task_id_a})) })
            .await?;
        trx.create(&Message { content: "Message for task B".to_string(), context: Json::new(serde_json::json!({"task_id": task_id_b})) })
            .await?;
        trx.commit().await?;
    }

    // Verify fetch works (baseline)
    let query_str = format!("context.task_id = '{}'", task_id_a);
    let fetched: Vec<MessageView> = ctx.fetch(query_str.as_str()).await?;
    assert_eq!(fetched.len(), 2, "fetch() should return 2 messages for task A");

    // Now test LiveQuery - this is where the bug manifests
    let query = ctx.query_wait::<MessageView>(query_str.as_str()).await?;

    use ankurah::signals::Peek;
    let items = query.peek();
    assert_eq!(items.len(), 2, "LiveQuery should return 2 messages for task A, got {}", items.len());

    Ok(())
}

#[tokio::test]
async fn test_json_path_livequery_with_new_entity() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let task_id = "task-xyz";

    // Set up LiveQuery before creating any entities
    let query_str = format!("context.task_id = '{}'", task_id);
    let query = ctx.query_wait::<MessageView>(query_str.as_str()).await?;

    use ankurah::signals::{Peek, Subscribe};
    let watcher = TestWatcher::changeset();
    let _handle = query.subscribe(&watcher);

    // Initially empty
    assert_eq!(query.peek().len(), 0, "LiveQuery should initially be empty");

    // Create a matching message
    {
        let trx = ctx.begin();
        trx.create(&Message { content: "New message".to_string(), context: Json::new(serde_json::json!({"task_id": task_id})) }).await?;
        trx.commit().await?;
    }

    // Wait for notification and check
    assert!(watcher.wait().await, "Should receive notification for new message");

    let items = query.peek();
    assert_eq!(items.len(), 1, "LiveQuery should now have 1 message");
    assert_eq!(items[0].content().unwrap(), "New message");

    Ok(())
}

#[tokio::test]
async fn test_json_path_livequery_with_nested_path() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    // Create message with nested JSON context
    {
        let trx = ctx.begin();
        trx.create(&Message {
            content: "Nested context message".to_string(),
            context: Json::new(serde_json::json!({
                "refs": {
                    "task_id": "nested-task",
                    "user_id": "user-123"
                }
            })),
        })
        .await?;
        trx.commit().await?;
    }

    // Query with nested path
    let query = ctx.query_wait::<MessageView>("context.refs.task_id = 'nested-task'").await?;

    use ankurah::signals::Peek;
    let items = query.peek();
    assert_eq!(items.len(), 1, "LiveQuery should find message with nested JSON path");
    assert_eq!(items[0].content().unwrap(), "Nested context message");

    Ok(())
}

/// Test that demonstrates the issue: entities returned from storage are re-evaluated
/// against the predicate in update_query, and this re-evaluation must handle JSON paths.
#[tokio::test]
async fn test_json_path_predicate_reevaluation() -> Result<()> {
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let task_id = "reevaluation-test";

    // Create entity first
    {
        let trx = ctx.begin();
        trx.create(&Message {
            content: "Test message".to_string(),
            context: Json::new(serde_json::json!({"task_id": task_id, "extra": "data"})),
        })
        .await?;
        trx.commit().await?;
    }

    // Now create LiveQuery - this tests the path where:
    // 1. Storage returns the entity (it matches the indexed query)
    // 2. update_query re-evaluates the predicate using Entity::Filterable::value()
    // 3. The predicate must correctly extract context.task_id from the Entity
    let query_str = format!("context.task_id = '{}'", task_id);
    let query = ctx.query_wait::<MessageView>(query_str.as_str()).await?;

    use ankurah::signals::Peek;
    let items = query.peek();

    // This is the key assertion - if this fails, the predicate re-evaluation is broken
    assert_eq!(
        items.len(),
        1,
        "LiveQuery should contain the entity after predicate re-evaluation. \
         If this is 0, the Entity::Filterable::value() is not correctly handling JSON path extraction."
    );

    Ok(())
}
