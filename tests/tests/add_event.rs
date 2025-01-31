
#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;
use ankurah::Node;

#[tokio::test]
async fn add_event_postgres() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage_engine));

    let trx = node.begin();
    let album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await;
    let album_id = album.id();

    trx.commit().await?;

    let trx1 = node.begin();
    let album1 = trx1.edit::<Album>(album_id).await?;
    album1.name.insert(0, "(o.");
    trx1.commit().await?;

    let trx2 = node.begin();
    let album2 = trx2.edit::<Album>(album_id).await?;
    album2.name.insert(3, "o) ");
    trx2.commit().await?;

    let albums = node.collection(&"album".into()).await;
    let events = albums.get_events(album_id).await?;
    assert_eq!(events.len(), 3);

    Ok(())
}
