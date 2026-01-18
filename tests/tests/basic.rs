mod common;
use crate::common::TestWatcher;
use ankurah::signals::{CallbackObserver, Subscribe};
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

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

    let view_watcher = TestWatcher::transform(|v: AlbumView| (v.clone(), v.name().unwrap(), v.year().unwrap()));
    let render_watcher = TestWatcher::new();

    // store the handles to keep the subscriptions alive
    let _h1 = album.subscribe(&view_watcher);
    let observer = {
        let album = album.clone();
        let render_watcher = render_watcher.clone();
        CallbackObserver::new(Arc::new(move || {
            // Access the view fields - this should cause the View to be tracked by CurrentObserver
            render_watcher.notify(format!("name: {}, year: {}", album.name().unwrap(), album.year().unwrap()));
        }))
    };
    observer.trigger();
    assert_eq!(render_watcher.take_one().await, "name: The rest of the bowl, year: 2024");

    let trx2 = context.begin();
    let album_mut2 = album.edit(&trx2)?;

    album_mut2.name().delete(16, 1)?; // remove the "typo" b from bowl

    // we haven't committed the transaction yet - neither watcher should have received any changes
    assert_eq!(view_watcher.quiesce().await, 0);
    assert_eq!(render_watcher.quiesce().await, 0);

    // commit the transaction
    trx2.commit().await?;

    // now we should have one change since we performed a delete operation
    assert_eq!(view_watcher.take_one().await, (album.clone(), "The rest of the owl".to_owned(), "2024".to_owned()));
    assert_eq!(render_watcher.take_one().await, "name: The rest of the owl, year: 2024");

    let trx3 = context.begin();
    let album_mut3 = album.edit(&trx3)?;
    album_mut3.year().replace("2025")?;
    trx3.commit().await?;

    assert_eq!(view_watcher.take_one().await, (album.clone(), "The rest of the owl".to_owned(), "2025".to_owned())); // AlbumView changed
    assert_eq!(render_watcher.take_one().await, "name: The rest of the owl, year: 2025");

    Ok(())
}
