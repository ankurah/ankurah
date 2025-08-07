#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;
use ankurah::signals::Subscribe;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn test_postgres() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    // Initialize the node's system catalog
    node.system.create().await?;

    // Get context after system is ready
    let context = node.context_async(c).await;

    let trx = context.begin();
    let _album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;

    trx.commit().await?;

    Ok(())
}

#[tokio::test]
async fn test_sled() -> Result<()> {
    use common::*;

    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    // Initialize the node's system catalog
    node.system.create().await?;

    // Get context after system is ready
    let context = node.context_async(c).await;

    let album_id;
    {
        let trx = context.begin();
        let _album = trx.create(&Album { name: "The rest of the bowl".to_owned(), year: "2024".to_owned() }).await?;
        album_id = _album.id();
        trx.commit().await?;
    }

    // get a new copy just for clarity purposes. it shouldn't matter how you get the AlbumView, as long as it's
    // resident, so it receives updates made on the local node.
    // (remote updates are another story)
    let album = context.get::<AlbumView>(album_id).await?;

    let (w, check) = generic_watcher::<AlbumView>();
    let (w2, check2) = generic_watcher::<String>();

    // store the handles to keep the subscriptions alive
    let _h1 = album.subscribe(w);
    let _h2 = album.name().subscribe(w2); // TODO: YrsString<String> implement Subscribe<String>

    let trx2 = context.begin();
    let album_mut2 = album.edit(&trx2)?;

    album_mut2.name().delete(16, 1)?; // remove the "typo" b from bowl

    // we haven't committed the transaction yet - neither watcher should have received any changes
    assert_eq!(check(), vec![]);
    assert_eq!(check2(), Vec::<String>::new());

    // commit the transaction
    trx2.commit().await?;

    // now we should have one change since we performed a delete operation
    // TODO - implement PartialEq for Views
    assert_eq!(check(), vec![album]);
    assert_eq!(check2(), vec!["The rest of the owl".to_owned()]);

    Ok(())
}

// After this:
// 1. ensure that each ActiveValue (YrsString<T>, LWW<T>) keeps the AlbumView resident so it continues to receive updates made on the local node
// 2. ensure that doing so doesn't leak memory by confirming that the AlbumView is dropped immediately after the YrsString<T> or LWW<T> is dropped.
//    We will have to test this in the inter_node test because the only way to make edits on a single-node test is to keep the AlbumMut alive, which
// invalidates the test, because that will continue to update the
