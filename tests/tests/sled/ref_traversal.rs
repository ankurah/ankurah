//! Tests for Ref<T> type and entity traversal
//!
//! Verifies that typed entity references work correctly for fetching related entities.

use ankurah::property::Ref;
use ankurah::{policy::DEFAULT_CONTEXT, EntityId, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Use unique names to avoid collision with common.rs models
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct RefTestArtist {
    pub name: String,
}

// Note: No #[active_type(LWW)] needed - Ref<T> defaults to LWW backend
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct RefTestAlbum {
    pub name: String,
    pub artist: Ref<RefTestArtist>, // LWW backend selected automatically
}

async fn setup_context() -> Result<ankurah::Context> {
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await)
}

#[tokio::test]
async fn test_ref_basic_creation() -> Result<()> {
    let ctx = setup_context().await?;

    // Create an artist
    let artist_id = {
        let trx = ctx.begin();
        let artist = trx.create(&RefTestArtist { name: "Radiohead".to_string() }).await?;
        let id = artist.id();
        trx.commit().await?;
        id
    };

    // Create an album referencing the artist
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&RefTestAlbum { name: "OK Computer".to_string(), artist: Ref::new(artist_id.clone()) }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Fetch the album and verify the ref is stored correctly
    let album: RefTestAlbumView = ctx.get(album_id).await?;
    assert_eq!(album.name().unwrap(), "OK Computer");
    assert_eq!(album.artist().unwrap().id(), artist_id);

    Ok(())
}

#[tokio::test]
async fn test_ref_get_traversal() -> Result<()> {
    let ctx = setup_context().await?;

    // Create an artist
    let artist_id = {
        let trx = ctx.begin();
        let artist = trx.create(&RefTestArtist { name: "Muse".to_string() }).await?;
        let id = artist.id();
        trx.commit().await?;
        id
    };

    // Create an album referencing the artist
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&RefTestAlbum { name: "Origin of Symmetry".to_string(), artist: Ref::new(artist_id.clone()) }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Fetch the album, then traverse the ref to get the artist
    let album: RefTestAlbumView = ctx.get(album_id).await?;
    let artist_ref = album.artist().unwrap();

    // This is the key test - calling .get() on the Ref to fetch the referenced entity
    let artist: RefTestArtistView = artist_ref.get(&ctx).await?;
    assert_eq!(artist.name().unwrap(), "Muse");

    Ok(())
}

#[tokio::test]
async fn test_ref_from_entity_id() -> Result<()> {
    let id = EntityId::new();

    // Test From<EntityId> conversion
    let artist_ref: Ref<RefTestArtist> = id.clone().into();
    assert_eq!(artist_ref.id(), id);

    // Test Into<EntityId> conversion
    let recovered_id: EntityId = artist_ref.into();
    assert_eq!(recovered_id, id);

    Ok(())
}

#[tokio::test]
async fn test_ref_serialization() -> Result<()> {
    let id = EntityId::new();
    let artist_ref: Ref<RefTestArtist> = Ref::new(id.clone());

    // Serialize
    let json = serde_json::to_string(&artist_ref)?;

    // Deserialize
    let recovered: Ref<RefTestArtist> = serde_json::from_str(&json)?;
    assert_eq!(recovered.id(), id);

    Ok(())
}
