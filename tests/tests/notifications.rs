mod common;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test 8.1: One Notification Per Change
/// Subscribe to entity, make concurrent changes, verify exactly one notification per change
#[tokio::test]
async fn test_one_notification_per_change() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Create entity on server
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Client subscribes via query
    let watcher = TestWatcher::changeset();
    let query_str = format!("id = '{}'", album_id);
    let query = client_ctx.query_wait::<AlbumView>(nocache(query_str.as_str())?).await?;
    let _guard = query.subscribe(&watcher);

    // Server makes two concurrent changes
    let album = server_ctx.get::<AlbumView>(album_id).await?;
    let trx1 = server_ctx.begin();
    let trx2 = server_ctx.begin();

    album.edit(&trx1)?.name().replace("Name-B")?;
    album.edit(&trx2)?.year().replace("2025")?;

    trx1.commit().await?;
    trx2.commit().await?;

    // Should receive exactly two notifications (one for each commit)
    // Wait for both
    let changes = watcher.take(2).await?;
    assert_eq!(changes.len(), 2, "Should receive exactly 2 notifications");

    // Both should be Update type (not Add, since entity already existed when subscription started)
    for change in &changes {
        assert_eq!(change.len(), 1, "Each notification should have 1 change");
        assert_eq!(change[0].0, album_id);
        assert_eq!(change[0].1, ChangeKind::Update);
    }

    // Should be no additional notifications
    assert_eq!(watcher.quiesce().await, 0, "Should be no extra notifications");

    Ok(())
}

/// Test 8.2: Causal Notification Order
/// Sequential changes should notify in causal order
#[tokio::test]
async fn test_causal_notification_order() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Create entity on server
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Client subscribes
    let watcher = TestWatcher::changeset_with_event_ids();
    let query_str = format!("id = '{}'", album_id);
    let query = client_ctx.query_wait::<AlbumView>(nocache(query_str.as_str())?).await?;
    let _guard = query.subscribe(&watcher);

    // Make sequential changes: A -> B -> C
    // Wait for each notification before making the next change
    let names = ["Name-B", "Name-C", "Name-D"];
    let mut all_event_ids = Vec::new();

    for name in &names {
        let album = server_ctx.get::<AlbumView>(album_id).await?;
        let trx = server_ctx.begin();
        album.edit(&trx)?.name().replace(*name)?;
        trx.commit().await?;

        // Wait for this notification before proceeding
        let change = watcher.take_one().await;
        assert_eq!(change.len(), 1, "Each notification should have 1 change");
        assert_eq!(change[0].1, ChangeKind::Update);
        all_event_ids.extend(change[0].2.clone());
    }

    // Verify we received 3 distinct event IDs
    assert_eq!(all_event_ids.len(), 3, "Should have 3 event IDs");

    // Verify all event IDs are unique
    let mut unique_ids = all_event_ids.clone();
    unique_ids.sort();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), 3, "All event IDs should be unique");

    Ok(())
}

/// Test 8.3: Multi-Subscriber Consistency
/// Multiple subscribers should all see same changes
#[tokio::test]
async fn test_multi_subscriber_consistency() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;

    let client1 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let client2 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let client3 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn1 = LocalProcessConnection::new(&client1, &server).await?;
    let _conn2 = LocalProcessConnection::new(&client2, &server).await?;
    let _conn3 = LocalProcessConnection::new(&client3, &server).await?;

    client1.system.wait_system_ready().await;
    client2.system.wait_system_ready().await;
    client3.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client1_ctx = client1.context(DEFAULT_CONTEXT)?;
    let client2_ctx = client2.context(DEFAULT_CONTEXT)?;
    let client3_ctx = client3.context(DEFAULT_CONTEXT)?;

    // Create entity on server
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // All three clients subscribe
    let watcher1 = TestWatcher::changeset();
    let watcher2 = TestWatcher::changeset();
    let watcher3 = TestWatcher::changeset();

    let query_str = format!("id = '{}'", album_id);
    let query1 = client1_ctx.query_wait::<AlbumView>(nocache(query_str.as_str())?).await?;
    let query2 = client2_ctx.query_wait::<AlbumView>(nocache(query_str.as_str())?).await?;
    let query3 = client3_ctx.query_wait::<AlbumView>(nocache(query_str.as_str())?).await?;

    let _guard1 = query1.subscribe(&watcher1);
    let _guard2 = query2.subscribe(&watcher2);
    let _guard3 = query3.subscribe(&watcher3);

    // Server makes a change
    {
        let album = server_ctx.get::<AlbumView>(album_id).await?;
        let trx = server_ctx.begin();
        album.edit(&trx)?.name().replace("Updated Name")?;
        trx.commit().await?;
    }

    // All three should receive the notification
    let changes1 = watcher1.take_one().await;
    let changes2 = watcher2.take_one().await;
    let changes3 = watcher3.take_one().await;

    // All should have exactly one change for our entity
    assert_eq!(changes1.len(), 1);
    assert_eq!(changes2.len(), 1);
    assert_eq!(changes3.len(), 1);

    assert_eq!(changes1[0].0, album_id);
    assert_eq!(changes2[0].0, album_id);
    assert_eq!(changes3[0].0, album_id);

    // All should converge to same view
    let query = format!("id = '{}'", album_id);
    let results1 = client1_ctx.fetch::<AlbumView>(query.as_str()).await?;
    let results2 = client2_ctx.fetch::<AlbumView>(query.as_str()).await?;
    let results3 = client3_ctx.fetch::<AlbumView>(query.as_str()).await?;

    assert_eq!(results1[0].name().unwrap(), "Updated Name");
    assert_eq!(results2[0].name().unwrap(), "Updated Name");
    assert_eq!(results3[0].name().unwrap(), "Updated Name");

    Ok(())
}

/// Test: Subscription receives Add notification for new matching entity
#[tokio::test]
async fn test_subscription_add_notification() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(DEFAULT_CONTEXT)?;
    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Client subscribes to a query BEFORE any entities exist
    let watcher = TestWatcher::changeset();
    let query = client_ctx.query_wait::<AlbumView>(nocache("name = 'Target'")?).await?;
    let _guard = query.subscribe(&watcher);

    // Server creates a matching entity
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Target".to_owned(), year: "2025".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Should receive Add notification
    let change = watcher.take_one().await;
    assert_eq!(change.len(), 1);
    assert_eq!(change[0].0, album_id);
    assert_eq!(change[0].1, ChangeKind::Add);

    Ok(())
}
