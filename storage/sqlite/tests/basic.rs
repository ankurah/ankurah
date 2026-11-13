//! SQLite Storage Integration Tests
//!
//! These tests verify that the SQLite storage engine works correctly with entity mutations,
//! including:
//! - Creating entities
//! - Updating entities
//! - Querying entities
//! - State change detection

mod common;

use ankurah::signals::Subscribe;
use ankurah::{fetch, policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_storage_sqlite::SqliteStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView, TestWatcher};
use std::sync::Arc;

#[tokio::test]
async fn test_sqlite_create_and_query() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    // Create some albums
    let trx = ctx.begin();
    trx.create(&Album { name: "Album 1".to_string(), year: "2020".to_string() }).await?;
    trx.create(&Album { name: "Album 2".to_string(), year: "2021".to_string() }).await?;
    trx.create(&Album { name: "Album 3".to_string(), year: "2022".to_string() }).await?;
    trx.commit().await?;

    // Query albums
    let albums: Vec<AlbumView> = ctx.fetch("year > '2020'").await?;
    assert_eq!(albums.len(), 2);
    assert!(albums.iter().any(|a| a.name().unwrap() == "Album 2"));
    assert!(albums.iter().any(|a| a.name().unwrap() == "Album 3"));

    Ok(())
}

#[tokio::test]
async fn test_sqlite_update_entity() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    // Create an album
    let album: AlbumView = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Original Name".to_string(), year: "2020".to_string() }).await?.read()?;
        trx.commit().await?;
        album
    };

    // Update the album
    {
        let trx = ctx.begin();
        album.edit(&trx).unwrap().name()?.overwrite(0, 13, "Updated Name")?;
        trx.commit().await?;
    }

    // Verify the update
    let albums: Vec<AlbumView> = ctx.fetch("name = 'Updated Name'").await?;
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].name().unwrap(), "Updated Name");
    assert_eq!(albums[0].year().unwrap(), "2020");

    Ok(())
}

#[tokio::test]
async fn test_sqlite_state_change_detection() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    // Create an album
    let album: AlbumView = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Test Album".to_string(), year: "2020".to_string() }).await?.read()?;
        trx.commit().await?;
        album
    };

    // First update should return true (state changed)
    {
        let trx = ctx.begin();
        album.edit(&trx).unwrap().name()?.overwrite(0, 10, "Updated")?;
        trx.commit().await?;
    }

    // Verify the update was applied
    let albums: Vec<AlbumView> = ctx.fetch("name = 'Updated'").await?;
    assert_eq!(albums.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_sqlite_multiple_updates() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    // Create multiple albums
    let (album1, album2) = {
        let trx = ctx.begin();
        let album1 = trx.create(&Album { name: "Album 1".to_string(), year: "2020".to_string() }).await?.read()?;
        let album2 = trx.create(&Album { name: "Album 2".to_string(), year: "2021".to_string() }).await?.read()?;
        trx.commit().await?;
        (album1, album2)
    };

    // Update both albums
    {
        let trx = ctx.begin();
        album1.edit(&trx).unwrap().name()?.overwrite(0, 7, "Updated 1")?;
        album2.edit(&trx).unwrap().name()?.overwrite(0, 7, "Updated 2")?;
        trx.commit().await?;
    }

    // Verify both updates
    let albums1: Vec<AlbumView> = ctx.fetch("name = 'Updated 1'").await?;
    let albums2: Vec<AlbumView> = ctx.fetch("name = 'Updated 2'").await?;
    assert_eq!(albums1.len(), 1);
    assert_eq!(albums2.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_sqlite_query_with_subscription() -> Result<()> {
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let watcher = TestWatcher::changeset();
    let query: ankurah::LiveQuery<AlbumView> = ctx.query_wait::<AlbumView>("year > '2020'").await?;
    let _handle = query.subscribe(&watcher);

    // Create albums before subscription (these won't trigger subscription)
    {
        let trx = ctx.begin();
        trx.create(&Album { name: "Album 1".to_string(), year: "2021".to_string() }).await?;
        trx.create(&Album { name: "Album 2".to_string(), year: "2022".to_string() }).await?;
        trx.commit().await?;
    }

    // Wait a bit for any initial notifications
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let _initial_count = watcher.count();

    // Create another album that matches (should trigger subscription)
    {
        let trx = ctx.begin();
        trx.create(&Album { name: "Album 3".to_string(), year: "2023".to_string() }).await?;
        trx.commit().await?;
    }

    // Should receive notification for the new album
    use common::EntityId;
    let changes: Vec<(EntityId, ankurah::changes::ChangeKind)> = watcher.take_one().await;
    assert!(changes.len() >= 1, "Should receive at least one change notification");

    Ok(())
}

/// Fetch the range returned by `EntityId::base64_prefix_range` to find entities by a base64 id prefix.
#[tokio::test]
async fn test_sqlite_fetch_by_id_prefix() -> Result<()> {
    use common::EntityId;
    let storage = SqliteStorageEngine::open_in_memory().await?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await.unwrap();

    let mut ids = Vec::new();
    let trx = ctx.begin();
    for n in 0..16 {
        ids.push(trx.create(&Album { name: format!("Album {n}"), year: "2020".to_string() }).await?.id());
    }
    trx.commit().await?;
    ids.sort();

    for id in &ids {
        let prefix = &id.to_base64()[..1];
        let range = EntityId::base64_prefix_range(prefix)?;
        let albums: Vec<AlbumView> = fetch!(ctx, "id >= {} AND id <= {}", range.start(), range.end()).await?;
        let mut found: Vec<EntityId> = albums.iter().map(|album| album.id()).collect();
        found.sort();
        let expected: Vec<EntityId> = ids.iter().copied().filter(|other| other.to_base64().starts_with(prefix)).collect();
        assert_eq!(found, expected, "prefix {prefix}");
    }
    Ok(())
}
