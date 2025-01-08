mod common;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::Mutable, node::Node};
use anyhow::Result;

use common::{Album, AlbumView};
use std::sync::Arc;
#[tokio::test]
async fn basic_where_clause() -> Result<()> {
    let client = Arc::new(Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap())));

    let _id = {
        let trx = client.begin();
        let id = trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await.id();
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;
        trx.commit().await?;
        id
    };

    let albums: ankurah_core::resultset::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg_common;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_basic_where_clause() -> Result<()> {
    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let client = Arc::new(Node::new_durable(Arc::new(storage_engine)));

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
    let albums: ankurah_core::resultset::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    let albums: ankurah_core::resultset::ResultSet<AlbumView> = client.fetch("year = '2008'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string(), "Death Magnetic".to_string()]
    );

    let albums: ankurah_core::resultset::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream' AND year = '1800'").await?;

    assert_eq!(albums.items.iter().map(|active_entity| active_entity.name()).count(), 0,);

    Ok(())
}
