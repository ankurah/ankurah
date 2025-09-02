use ankurah::changes::{ChangeKind, ChangeSet, ItemChange};
use ankurah::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

mod common;
use common::{Album, AlbumView};

use crate::common::TestWatcher;

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
    let client_query = client_ctx.query_wait::<AlbumView>("name = 'Test Album'").await?;

    //But the livequery should have the album
    use ankurah::signals::Peek;
    assert_eq!(client_query.peek().iter().map(|p| p.id()).collect::<Vec<_>>(), vec![album_id]);

    // actually zero events because we receive a state from ItemChange::Initial
    assert_eq!(0, client_collection.dump_entity_events(album_id.clone()).await?.len()); // after subscribe

    // Fully unsubscribe (drop the LiveQuery)
    drop(client_query);

    // wait for the unsubscribe to be propagated to the server (probably a better way to do this without the sleep)
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

    // Not sure what we're waiting for here exactly - for the update to NOT arrive?
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Resubscribe on the client
    // in the repro, it's failling here rather than on the arrival of the StateFragment
    let client_query2 = client_ctx.query_wait::<AlbumView>("name = 'Test Album'").await?;

    // Update: We don't need to subscribe to the livequery or wait for this test anymore, because the LiveQuery is already initialized
    // and we can just inspect the LiveQuery directly

    // The client should have the correct, up-to-date state (year = "2022") in the LiveQuery
    let albums = client_query2.peek();
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].year().unwrap_or_default(), "2022");

    // After resubscribe, the client should have retrieved the missing events during the lineage comparison
    assert_eq!(3, client_collection.dump_entity_events(album_id.clone()).await?.len()); // after resubscribe

    Ok(())
}
