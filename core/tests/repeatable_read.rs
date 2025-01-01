use ankurah_core::property::value::YrsString;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    // DECISION: The model always contains projected types, which will initally be just the native types.
    // We will leave the door open to backend specific projected newtypes in the future, but we will not have the active value types in the model.
    // Implication: we will still need to have a native type to active type lookup in the Model macro for now.
    // We have the option of adding override attributes to switch backends in the future.
    // We will initially only use Model structs for initial construction of the record (or a property group thereof) but we may later consider
    // using them for per-property group retrieval binding, but preferably only via an immutable borrow.
    #[active_value(YrsString)]
    pub name: String,
}

#[tokio::test]
async fn repeatable_read() -> Result<()> {
    let client = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    let id;
    {
        let trx = client.begin();
        let album_rw = trx
            .create(&Album {
                name: "I love cats".into(),
            })
            .await;
        assert_eq!(album_rw.name().value(), Some("I love cats".to_string()));
        id = album_rw.id();

        println!("{:?}", album_rw.record_state());

        trx.commit().await?;
    }

    println!("_____________");
    println!("REFETCHING");
    println!("_____________");
    // TODO: implement ScopedRecord.read() -> Record
    let album_ro: AlbumRecord = client.get_record(id).await?;

    println!("_____________");
    println!("name: {:?}", album_ro.name());
    println!("_____________");

    let trx2 = client.begin();
    let album_rw2 = album_ro.edit(&trx2).await?;

    let trx3 = client.begin();
    let album_rw3 = album_ro.edit(&trx3).await?;

    // tx2 cats -> tofu
    album_rw2.name().delete(7, 4);
    album_rw2.name().insert(7, "tofu");
    assert_eq!(album_rw2.name().value(), Some("I love tofu".to_string()));

    // tx3 love -> devour
    album_rw3.name().delete(2, 4);
    album_rw3.name().insert(2, "devour");
    // a modest proposal
    assert_eq!(album_rw3.name().value(), Some("I devour cats".to_string()));

    // trx2 and 3 are uncommited, so the value should not be updated
    assert_eq!(album_ro.name(), "I love cats");
    trx2.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I love tofu");

    trx3.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I devour tofu");

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg_common;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_repeatable_read() -> Result<()> {
    let (_container, postgres) = pg_common::create_postgres_container().await?;
    let client = Arc::new(Node::new(Arc::new(postgres)));

    let id;
    {
        let trx = client.begin();
        let album_rw = trx
            .create(&Album {
                name: "I love cats".into(),
            })
            .await;
        assert_eq!(album_rw.name().value(), Some("I love cats".to_string()));
        id = album_rw.id();
        trx.commit().await?;
    }

    // TODO: implement ScopedRecord.read() -> Record
    let album_ro: AlbumRecord = client.get_record(id).await?;

    let trx2 = client.begin();
    let album_rw2 = album_ro.edit(&trx2).await?;

    let trx3 = client.begin();
    let album_rw3 = album_ro.edit(&trx3).await?;

    // tx2 cats -> tofu
    album_rw2.name().delete(7, 4);
    album_rw2.name().insert(7, "tofu");
    assert_eq!(album_rw2.name().value(), Some("I love tofu".to_string()));

    // tx3 love -> devour
    album_rw3.name().delete(2, 4);
    album_rw3.name().insert(2, "devour");
    // a modest proposal
    assert_eq!(album_rw3.name().value(), Some("I devour cats".to_string()));

    // trx2 and 3 are uncommited, so the value should not be updated
    assert_eq!(album_ro.name(), "I love cats");
    trx2.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I love tofu");

    trx3.commit().await?;

    // FAIL - the value must be updated now
    assert_eq!(album_ro.name(), "I devour tofu");

    Ok(())
}
