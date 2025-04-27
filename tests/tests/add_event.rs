#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};

#[tokio::test]
async fn add_event_postgres() -> Result<()> {
    use common::*;

    eprintln!("MARK 0");
    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    eprintln!("MARK 0.1");
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new()).context(c);
    eprintln!("MARK 0.2");

    let trx = node.begin();
    eprintln!("MARK 0.3");
    let album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;
    eprintln!("MARK 0.4 {}", album.entity.head());
    let album_id = album.id();

    println!("MARK 1");
    trx.commit().await?;

    println!("MARK 1.1");
    let trx1 = node.begin();

    println!("MARK 1.2");
    let album1 = trx1.edit::<Album>(album_id).await?;
    println!("MARK 1.2.1 {}", album1.entity.head());

    album1.name.insert(0, "(o.")?;
    println!("MARK 1.3");
    trx1.commit().await?;

    println!("MARK 2");

    let trx2 = node.begin();
    let album2 = trx2.edit::<Album>(album_id).await?;
    album2.name.insert(3, "o) ")?;
    trx2.commit().await?;

    println!("MARK 3");
    let albums = node.collection(&"album".into()).await?;
    let events = albums.dump_entity_events(album_id).await?;
    assert_eq!(events.len(), 3);

    println!("MARK 4");
    Ok(())
}
