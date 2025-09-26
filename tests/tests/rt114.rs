use ankurah::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT as c, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

mod common;
use crate::common::*;

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
        let album2 = trx.create(&Album { name: "Test Album 2".into(), year: "2020".into() }).await?;
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
    let client_query = client_ctx.query_wait::<AlbumView>(nocache("year >= '2020'")?).await?;
    use ankurah::signals::Peek;
    assert_eq!(client_query.peek().iter().map(|p| p.year().unwrap_or_default()).collect::<Vec<_>>(), vec!["2020", "2020"]);

    // actually zero events because we receive a state from ItemChange::Initial
    assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after subscribe
    assert_eq!(0, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after subscribe

    // Unsubscribe (drop the LiveQuery)
    drop(client_query);

    // wait for the unsubscribe to be propagated to the server (probably a better way to do this without the sleep)
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

    // LiveQuery refactor note: I don't think we really need this sleep, but I guess this helps differentiate between
    // the commit event not arriving (which is expected), vs merely taking some time to arrive unexpectedly
    // I'm going to leave it for now in the interest of leaving as much of the original RT in place as possible
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Resubscribe on the client
    // in the repro, it's failling here rather than on the arrival of the StateFragment
    let client_query = client_ctx.query_wait::<AlbumView>(nocache("year >= '2020'")?).await?;

    // The client should receive only album1 with the correct state (year = "2020")
    // Album2 should not be returned since it no longer matches (year = "2019")
    assert_eq!(client_query.peek().iter().map(|p| p.year().unwrap_or_default()).collect::<Vec<_>>(), vec!["2020"]);

    // TODO: try to determine why album2 has 0 events under interim fix for 114
    // my guess is that the Entity wasn't resident at the time, so entities.with_state() didn't traverse the event lineage
    // After resubscribe, the client should have retrieved the missing events during the lineage comparison
    // assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after resubscribe - no events for album1 since it wasn't changed
    // assert_eq!(1, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after resubscribe - one event for album2's year change

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
    let initial_fetch = client_ctx.fetch::<AlbumView>(nocache("year >= '2020'")?).await?;
    let initial_years: Vec<String> = initial_fetch.iter().map(|album| album.year().unwrap_or_default()).collect();
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

    // Fetch still needs sleeps for now unfortunately. We can probably find a better way to do this though
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Fetch again on the client
    let refetch = client_ctx.fetch::<AlbumView>(nocache("year >= '2020'")?).await?;
    let refetch_years: Vec<String> = refetch.iter().map(|album| album.year().unwrap_or_default()).collect();

    // The client should receive only album1 with the correct state (year = "2020")
    // Album2 should not be returned since it no longer matches (year = "2019")
    assert_eq!(vec!["2020"], refetch_years);

    // TODO: try to determine why album2 has 0 events under interim fix for 114
    // After refetch, the client should have retrieved the missing events during the lineage comparison
    // assert_eq!(0, client_collection.dump_entity_events(album1_id.clone()).await?.len()); // after refetch - no events for album1 since it wasn't changed
    // assert_eq!(1, client_collection.dump_entity_events(album2_id.clone()).await?.len()); // after refetch - one event for album2's year change

    Ok(())
}
