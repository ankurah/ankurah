mod common;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use anyhow::Result;
use std::sync::Arc;

#[tokio::test]
async fn add_event_postgres() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = common::create_postgres_container().await?;

    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context(c)?;

    let trx = context.begin();
    let album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;
    let album_id = album.id();

    trx.commit().await?;

    let trx1 = context.begin();

    let album1 = trx1.get::<Album>(&album_id).await?;

    album1.name().insert(0, "(o.")?;
    trx1.commit().await?;

    let trx2 = context.begin();
    let album2 = trx2.get::<Album>(&album_id).await?;
    album2.name().insert(3, "o) ")?;
    trx2.commit().await?;

    let albums = context.collection(&"album".into()).await?;
    let events = albums.dump_entity_events(album_id).await?;
    assert_eq!(events.len(), 3);

    Ok(())
}
