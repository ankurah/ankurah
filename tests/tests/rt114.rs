use ankurah::changes::ChangeKind;
use ankurah::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

mod common;
use common::{Album, AlbumView};

#[tokio::test]
async fn rt114() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server (durable) and client (ephemeral)
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_storage = Arc::new(SledStorageEngine::new_test().unwrap());
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    // Create two albums on the server, both initially matching year >= 2020
    let (server_album1, server_album2) = {
        let trx = server_ctx.begin();
        let album1 = trx.create(&Album { name: "Test Album 1".into(), year: "2020".into() }).await?;
        let album1: AlbumView = album1.read();
        let album2 = trx.create(&Album { name: "Test Album 2".into(), year: "2091".into() }).await?;
        let album2: AlbumView = album2.read();
        trx.commit().await?;
        (album1, album2)
    };
    let album1_id = server_album1.id();
    let album2_id = server_album2.id();

    let client_collection = client_storage.collection(&"album".into()).await?;
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // before subscribe
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // before subscribe

    // Subscribe on the client with predicate year >= 2020
    let (watcher, check) = common::watcher::<AlbumView, _, _>(|change| (change.entity().year().unwrap_or_default()));
    let handle = client_ctx.subscribe("year >= '2020'", watcher).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Should get both albums initially
    assert_eq!(vec![vec!["2020", "2091"]], check());

    // actually zero events because we receive a state from ItemChange::Initial
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after subscribe
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after subscribe

    // Unsubscribe (drop the handle)
    println!("MARK test.unsubscribe (dropping handle)");
    drop(handle);

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Make changes on the server while client is unsubscribed
    // Album2: change to 2019 (no longer matches year >= 2020)
    {
        let trx = server_ctx.begin();
        server_album2.edit(&trx)?.year().overwrite(0, 4, "2019")?;
        trx.commit().await?;
    }

    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after edits
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after edits

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    println!("MARK test.resubscribe");
    // Resubscribe on the client
    let (watcher2, check2) = common::watcher::<AlbumView, String, _>(|change| change.entity().year().unwrap_or_default());
    // in the repro, it's failling here rather than on the arrival of the StateFragment
    let _handle2 = client_ctx.subscribe("year >= '2020'", watcher2).await.expect("failed to resubscribe");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // The client should receive only album1 with the correct state (year = "2020")
    // Album2 should not be returned since it no longer matches (year = "2019")
    assert_eq!(vec![vec!["2020"]], check2());

    // After resubscribe, the client should have retrieved the missing events during the lineage comparison
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after resubscribe - no events for album1 since it wasn't changed
    assert_eq!(1, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after resubscribe - one event for album2's year change

    Ok(())
}

#[tokio::test]
async fn rt114_b() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server (durable) and client (ephemeral)
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_storage = Arc::new(SledStorageEngine::new_test().unwrap());
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    // Create two albums on the server, both initially matching year >= 2020
    let (server_album1, server_album2) = {
        let trx = server_ctx.begin();
        let album1 = trx.create(&Album { name: "Test Album 1".into(), year: "2020".into() }).await?;
        let album1: AlbumView = album1.read();
        let album2 = trx.create(&Album { name: "Test Album 2".into(), year: "2020".into() }).await?;
        let album2: AlbumView = album2.read();
        trx.commit().await?;
        (album1, album2)
    };
    let album1_id = server_album1.id();
    let album2_id = server_album2.id();

    let client_collection = client_storage.collection(&"album".into()).await?;
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // before fetch
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // before fetch

    // Fetch on the client with predicate year >= 2020
    let initial_fetch = client_ctx.fetch::<AlbumView>("year >= '2020'").await?;
    let initial_years: Vec<String> = initial_fetch.items.iter().map(|album| album.year().unwrap_or_default()).collect();
    assert_eq!(vec!["2020", "2020"], initial_years);

    // actually zero events because we receive states directly
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after fetch
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after fetch

    // Make changes on the server while client has cached data
    // Album2: change to 2019 (no longer matches year >= 2020)
    {
        let trx = server_ctx.begin();
        server_album2.edit(&trx)?.year().overwrite(0, 4, "2019")?;
        trx.commit().await?;
    }

    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after edits
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after edits

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    println!("MARK test.refetch");
    // Fetch again on the client
    let refetch = client_ctx.fetch::<AlbumView>("year >= '2020'").await?;
    let refetch_years: Vec<String> = refetch.items.iter().map(|album| album.year().unwrap_or_default()).collect();

    // The client should receive only album1 with the correct state (year = "2020")
    // Album2 should not be returned since it no longer matches (year = "2019")
    assert_eq!(vec!["2020"], refetch_years);

    // After refetch, the client should have retrieved the missing events during the lineage comparison
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after refetch - no events for album1 since it wasn't changed
    assert_eq!(1, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after refetch - one event for album2's year change

    Ok(())
}
