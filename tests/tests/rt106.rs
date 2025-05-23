use ankurah::changes::ChangeKind;
use ankurah::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;
use tracing::info;

mod common;
use common::{Album, AlbumView};

#[tokio::test]
async fn rt106() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server (durable) and client (ephemeral)
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_storage = Arc::new(SledStorageEngine::new_test().unwrap());
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    // Create an album on the server
    let server_album = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Test Album".into(), year: "2020".into() }).await?;
        let album: AlbumView = album.read();
        trx.commit().await?;
        album
    };
    let album_id = server_album.id();

    let client_collection = client_storage.collection(&"album".into()).await?;
    assert_eq!(0, client_collection.dump_entity_events(album_id.clone()).await?.len()); // before subscribe

    // Subscribe on the client
    let (watcher, check) = common::watcher::<AlbumView, _, _>(|change| (change.entity().year().unwrap_or_default()));
    let handle = client_ctx.subscribe("name = 'Test Album'", watcher).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    assert_eq!(vec![vec!["2020"]], check());

    // actually zero events because we receive a state from ItemChange::Initial
    assert_eq!(0, client_collection.dump_entity_events(album_id.clone()).await?.len()); // after subscribe

    // Unsubscribe (drop the handle)
    println!("MARK test.unsubscribe (dropping handle)");
    drop(handle);

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Make two changes on the server while client is unsubscribed
    {
        let trx = server_ctx.begin();
        server_album.edit(&trx)?.year().overwrite(0, 4, "2021")?;
        trx.commit().await?;
    }
    {
        let trx = server_ctx.begin();
        server_album.edit(&trx)?.year().overwrite(0, 4, "2022")?;
        trx.commit().await?;
    }

    assert_eq!(0, client_collection.dump_entity_events(album_id.clone()).await?.len()); // after edits

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    println!("MARK test.resubscribe");
    // Resubscribe on the client
    let (watcher2, check2) = common::watcher::<AlbumView, String, _>(|change| change.entity().year().unwrap_or_default());
    // in the repro, it's failling here rather than on the arrival of the StateFragment
    let _handle2 = client_ctx.subscribe("name = 'Test Album'", watcher2).await.expect("failed to resubscribe");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // The client should receive the correct, up-to-date state (year = "2022") via the watcher
    assert_eq!(vec![vec!["2022".to_string()]], check2());

    // After resubscribe, the client should have retrieved the missing events during the lineage comparison
    assert_eq!(3, client_collection.dump_entity_events(album_id.clone()).await?.len()); // after resubscribe

    Ok(())
}
