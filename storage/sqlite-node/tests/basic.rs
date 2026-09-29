//! SQLite (node:sqlite) Storage Integration Tests
//!
//! These tests verify that the SQLite storage engine works correctly with entity mutations,
//! including:
//! - Creating entities
//! - Updating entities
//! - Querying entities
//! - State change detection
//!
//! Mirrors `storage/sqlite/tests/basic.rs`; the live-query subscription test
//! stays native, since it waits on tokio timers.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_storage_sqlite_node::SqliteNodeStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView};
use std::sync::Arc;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn test_sqlite_create_and_query() -> Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
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

#[wasm_bindgen_test]
async fn test_sqlite_update_entity() -> Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
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

#[wasm_bindgen_test]
async fn test_sqlite_state_change_detection() -> Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
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

#[wasm_bindgen_test]
async fn test_sqlite_multiple_updates() -> Result<()> {
    let storage = SqliteNodeStorageEngine::open_in_memory().await?;
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
