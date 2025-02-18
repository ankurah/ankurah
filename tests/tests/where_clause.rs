mod common;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;

use common::{Album, AlbumView};
use std::sync::Arc;
#[tokio::test]
async fn basic_where_clause() -> Result<()> {
    let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);

    let _id = {
        let trx = client.begin();
        let id = trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await.id();
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;
        trx.commit().await?;
        id
    };

    let albums: ankurah::ResultSet<AlbumView> = client.fetch("name = 'Walking on a Dream'").await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    Ok(())
}

#[tokio::test]
async fn test_where_clause_with_id() -> Result<()> {
    let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);

    let album_id = {
        let trx = client.begin();
        let id = trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await.id();
        trx.commit().await?;
        id
    };

    // Test querying by ID
    let albums: ankurah::ResultSet<AlbumView> = client.fetch(format!("id = '{}'", album_id).as_str()).await?;

    assert_eq!(
        albums.items.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
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
    let client = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new()).context(c);

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
