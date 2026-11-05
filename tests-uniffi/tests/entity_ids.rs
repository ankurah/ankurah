//! The bindings carry every entity id a model holds as an object, never as a string: a plain
//! `EntityId` as the EntityId object and a `Ref<T>` as T's Ref object, into a model through the
//! Input record and the Mutable's field wrappers, and out of it through the View's getters.

mod common;

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use ankurah::{EntityId, Ref};
use ankurah_tests_uniffi::{Album, AlbumInput, AlbumOps, AlbumView, ArtistInput, ArtistOps, ArtistRef};
use anyhow::Result;
use common::context;

#[tokio::test]
async fn create_takes_id_objects_and_view_returns_them() -> Result<()> {
    let ctx = context().await?;
    let artist = ArtistOps::new().create_one(&ctx, ArtistInput { name: "Radiohead".into() }).await?;
    let label = EntityId::random();
    let distributor = EntityId::random();

    let trx = ctx.begin();
    let input = AlbumInput {
        name: "OK Computer".into(),
        artist: Arc::new(ArtistRef::from_view(&artist)),
        producer: None,
        label: Arc::new(label),
        distributor: Some(Arc::new(distributor)),
    };
    let album_id = AlbumOps::new().create(&trx, input).await?.uniffi_id();
    trx.commit().await?;

    let album = ctx.get::<AlbumView>(album_id).await?;
    assert_eq!(album.uniffi_artist()?.id(), artist.id());
    assert!(album.uniffi_producer()?.is_none());
    assert_eq!(*album.uniffi_label()?, label);
    assert_eq!(album.uniffi_distributor()?.as_deref(), Some(&distributor));
    // The model holds the ids themselves
    assert_eq!(album.artist()?.id(), artist.id());
    assert_eq!(album.label()?, label);
    assert_eq!(album.distributor()?, Some(distributor));
    Ok(())
}

#[tokio::test]
async fn mutable_wrappers_set_and_get_id_objects() -> Result<()> {
    let ctx = context().await?;
    let artist = ArtistOps::new().create_one(&ctx, ArtistInput { name: "Radiohead".into() }).await?;
    let producer = ArtistOps::new().create_one(&ctx, ArtistInput { name: "Nigel Godrich".into() }).await?;
    let input = AlbumInput {
        name: "OK Computer".into(),
        artist: Arc::new(ArtistRef::from_view(&artist)),
        producer: None,
        label: Arc::new(EntityId::random()),
        distributor: Some(Arc::new(EntityId::random())),
    };
    let album = AlbumOps::new().create_one(&ctx, input).await?;
    let label = EntityId::random();

    let trx = ctx.begin();
    let album_mut = album.uniffi_edit(&trx)?;
    album_mut.uniffi_producer()?.set(Some(Arc::new(ArtistRef::from_view(&producer))))?;
    album_mut.uniffi_label()?.set(Arc::new(label))?;
    album_mut.uniffi_distributor()?.set(None)?;
    assert_eq!(album_mut.uniffi_producer()?.get()?.map(|producer| producer.id()), Some(producer.id()));
    assert_eq!(*album_mut.uniffi_label()?.get()?, label);
    assert_eq!(album_mut.uniffi_distributor()?.get()?, None);
    trx.commit().await?;

    assert_eq!(album.uniffi_producer()?.map(|producer| producer.id()), Some(producer.id()));
    assert_eq!(*album.uniffi_label()?, label);
    assert_eq!(album.uniffi_distributor()?, None);
    Ok(())
}

#[test]
fn input_record_gives_the_model_its_ids() {
    let (artist, producer, label) = (EntityId::random(), EntityId::random(), EntityId::random());
    let album = Album::from(AlbumInput {
        name: "OK Computer".into(),
        artist: Arc::new(ArtistRef::from_id(&artist)),
        producer: Some(Arc::new(ArtistRef::from_id(&producer))),
        label: Arc::new(label),
        distributor: None,
    });
    assert_eq!(album.artist.id(), artist);
    assert_eq!(album.producer.map(|producer| producer.id()), Some(producer));
    assert_eq!(album.label, label);
    assert_eq!(album.distributor, None);
}

#[test]
fn refs_are_equal_and_hash_alike_by_id() {
    let hash = |artist_ref: &ArtistRef| {
        let mut hasher = DefaultHasher::new();
        artist_ref.hash(&mut hasher);
        hasher.finish()
    };
    let id = EntityId::random();
    let (from_id, from_ref) = (ArtistRef::from_id(&id), ArtistRef::from(Ref::new(id)));
    assert!(from_id == from_ref);
    assert_eq!(hash(&from_id), hash(&from_ref));
    assert!(from_id != ArtistRef::from_id(&EntityId::random()));
}
