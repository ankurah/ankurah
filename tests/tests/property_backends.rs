
mod common;
use ankurah::{property::{value::{PNCounter, LWW}, YrsString}, Model, Mutable, Node};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use common::{Album, AlbumView};
use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[wasm_bindgen]
pub enum Visibility {
    Public,
    Unlisted,
    Private,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Video {
    #[active_type(YrsString)]
    pub title: String,
    #[active_type(LWW<Visibility>)]
    pub visibility: Visibility,
    #[active_type(PNCounter<i32>)]
    pub views: i32,
    #[active_type(LWW<Option<String>>)]
    pub attribution: Option<String>,
}

#[tokio::test]
async fn property_backends() -> Result<()> {
    let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()));

    let trx = client.begin();
    let cat_video = trx.create(&Video { title: "Cat video #2918".into(), visibility: Visibility::Public, views: 0, attribution: None }).await;
    let id = cat_video.id();
    //cat_video.views.add(2); // FIXME: applying twice for some reason
    cat_video.visibility.set(&Visibility::Unlisted)?;
    cat_video.title.insert(15, " (Very cute)");
    trx.commit().await?;

    let video = client.get_entity::<VideoView>(id).await?;
    //assert_eq!(video.views().unwrap(), 1);
    assert_eq!(video.visibility().unwrap(), Visibility::Unlisted);
    assert_eq!(video.title().unwrap(), "Cat video #2918 (Very cute)");

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg_common;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_basic_where_clause() -> Result<()> {
    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let client = Node::new_durable(Arc::new(storage_engine));

    {
        let trx = client.begin();

        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await;
        trx.create(&Album { name: "Death Magnetic".into(), year: "2008".into() }).await;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;

        trx.commit().await?;
    };

    // The next step is to make this work:
    let albums: ankurah::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    let albums: ankurah::ResultSet<AlbumView> = client.fetch("year = '2008'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string(), "Death Magnetic".to_string()]
    );

    let albums: ankurah::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream' AND year = '1800'").await?;

    assert_eq!(albums.items.iter().map(|active_entity| active_entity.name().unwrap()).count(), 0,);

    Ok(())
}
