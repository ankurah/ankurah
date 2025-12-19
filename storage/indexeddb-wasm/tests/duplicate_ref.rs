//! Test that multiple models can use the same Ref<T> type without symbol collision.
//!
//! This test verifies that the model-scoped wrapper generation works correctly
//! when Album and Track both have `artist: Ref<SharedArtist>` fields.

mod common;

use ankurah::property::Ref;
use ankurah::{policy::DEFAULT_CONTEXT, Model, Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::setup;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Shared Artist model referenced by both Album and Track
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct SharedArtist {
    pub name: String,
}

/// Album model with a Ref<SharedArtist> field
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct AlbumWithRef {
    pub name: String,
    pub artist: Ref<SharedArtist>,
}

/// Track model ALSO with a Ref<SharedArtist> field - same Ref<T> type!
/// This would cause symbol collision without model-scoped wrapper names.
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TrackWithRef {
    pub title: String,
    pub artist: Ref<SharedArtist>,
}

async fn setup_context() -> Result<(ankurah::Context, String), anyhow::Error> {
    setup();
    let db_name = format!("test_dup_ref_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok((node.context_async(DEFAULT_CONTEXT).await, db_name))
}

#[wasm_bindgen_test]
async fn test_duplicate_ref_type_no_collision() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create an artist
    let artist_id = {
        let trx = ctx.begin();
        let artist = trx.create(&SharedArtist { name: "Radiohead".to_string() }).await?;
        let id = artist.id();
        trx.commit().await?;
        id
    };

    // Create an album referencing the artist
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&AlbumWithRef { name: "OK Computer".to_string(), artist: Ref::new(artist_id.clone()) }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Create a track ALSO referencing the same artist
    let track_id = {
        let trx = ctx.begin();
        let track = trx.create(&TrackWithRef { title: "Paranoid Android".to_string(), artist: Ref::new(artist_id.clone()) }).await?;
        let id = track.id();
        trx.commit().await?;
        id
    };

    // Verify both work - fetch and check the refs
    let album: AlbumWithRefView = ctx.get(album_id).await?;
    assert_eq!(album.name()?, "OK Computer");
    assert_eq!(album.artist()?.id(), artist_id);

    let track: TrackWithRefView = ctx.get(track_id).await?;
    assert_eq!(track.title()?, "Paranoid Android");
    assert_eq!(track.artist()?.id(), artist_id);

    // Test traversal works for both
    let artist_from_album: SharedArtistView = album.artist()?.get(&ctx).await?;
    assert_eq!(artist_from_album.name()?, "Radiohead");

    let artist_from_track: SharedArtistView = track.artist()?.get(&ctx).await?;
    assert_eq!(artist_from_track.name()?, "Radiohead");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
