use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::WebsocketClient;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

mod common;
use common::{TestWatcher, *};

#[tokio::test]
async fn test_websocket_client_server_fetch() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Start websocket server
    let (server_node, server_url, server_task) = start_test_server().await?;

    info!("Server node ID: {}", server_node.id);

    // Get server context
    let server_ctx = server_node.context(c)?;

    // Create some data on the server
    {
        let trx = server_ctx.begin();
        trx.create(&Album { name: "Dark Side of the Moon".into(), year: "1973".into() }).await?;
        trx.create(&Album { name: "Wish You Were Here".into(), year: "1975".into() }).await?;
        trx.create(&Album { name: "Animals".into(), year: "1977".into() }).await?;
        trx.commit().await?;
    }

    // Create client node
    let client_storage = Arc::new(SledStorageEngine::new_test()?);
    let client_node = Node::new(client_storage, PermissiveAgent::new());

    info!("Client node ID: {}", client_node.id);

    // Create websocket client and connect
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    client.wait_connected().await?;

    info!("Client connected successfully");

    // Give time for initial data sync - fetch still needs sleeps for now unfortunately. We can probably find a better way to do this though
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Get client context
    let client_ctx = client_node.context(c)?;

    // Test fetching data through websocket connection
    let query = "name = 'Dark Side of the Moon'";
    let results = client_ctx.fetch::<AlbumView>(query).await?;
    assert_eq!(names(results), ["Dark Side of the Moon"]);

    // Test broader query
    let all_results = client_ctx.fetch("year > '1970'").await?;
    let mut all_names = names(all_results);
    all_names.sort();
    assert_eq!(all_names, ["Animals", "Dark Side of the Moon", "Wish You Were Here"]);

    info!("Fetch tests passed");

    // Clean shutdown
    client.shutdown().await?;
    server_task.abort();

    Ok(())
}

#[tokio::test]
async fn test_websocket_client_create_propagation() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Start websocket server
    let (server_node, server_url, server_task) = start_test_server().await?;

    // Create client
    let client_storage = Arc::new(SledStorageEngine::new_test()?);
    let client_node = Node::new(client_storage, PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    client.wait_connected().await?;

    // Wait for system synchronization
    client_node.system.wait_system_ready().await;

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    // Create entity on client
    {
        let trx = client_ctx.begin();
        trx.create(&Album { name: "The Wall".into(), year: "1979".into() }).await?;
        trx.commit().await?;
    }

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Verify it's queryable on server
    let query = "name = 'The Wall'";
    let server_results = server_ctx.fetch(query).await?;
    assert_eq!(names(server_results), ["The Wall"]);

    info!("Create propagation test passed");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_websocket_subscription_propagation() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Start websocket server
    let (server_node, server_url, server_task) = start_test_server().await?;

    // Create client
    let client_storage = Arc::new(SledStorageEngine::new_test()?);
    let client_node = Node::new(client_storage, PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    client.wait_connected().await?;

    // Wait for system synchronization
    client_node.system.wait_system_ready().await;

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    // Set up subscription watchers
    let server_watcher = TestWatcher::changeset();
    let client_watcher = TestWatcher::changeset();

    use ankurah::signals::Subscribe;
    // Create and initialize LiveQueries, then subscribe
    let _server_sub = server_ctx.query_wait::<AlbumView>("name = 'Abbey Road'").await?.subscribe(&server_watcher);
    let _client_sub = client_ctx.query_wait::<AlbumView>("name = 'Abbey Road'").await?.subscribe(&client_watcher);

    // No notifications because we waited for initialization before subscribing
    assert_eq!(server_watcher.drain(), vec![] as Vec<Vec<(EntityId, ChangeKind)>>);
    assert_eq!(client_watcher.drain(), vec![] as Vec<Vec<(EntityId, ChangeKind)>>);

    // Create matching entity on server
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Abbey Road".into(), year: "1969".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation and check changes
    assert_eq!(server_watcher.take_one().await, vec![(album_id, ChangeKind::Add)]);
    assert_eq!(client_watcher.take_one().await, vec![(album_id, ChangeKind::Add)]);

    info!("Subscription propagation test passed");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_websocket_bidirectional_subscription() -> Result<()> {
    // Add timeout to prevent test hanging
    tokio::time::timeout(Duration::from_secs(60), test_websocket_bidirectional_subscription_impl())
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out"))?
}

async fn test_websocket_bidirectional_subscription_impl() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Start websocket server
    let (server_node, server_url, server_task) = start_test_server().await?;

    // Create client
    let client_storage = Arc::new(SledStorageEngine::new_test()?);
    let client_node = Node::new(client_storage, PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    client.wait_connected().await?;

    // Wait for system synchronization
    client_node.system.wait_system_ready().await;

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    // Set up subscription watchers
    let server_watcher = TestWatcher::changeset();
    let client_watcher = TestWatcher::changeset();

    // Subscribe to pets with age > 5
    use ankurah::signals::Subscribe;
    let server_livequery = server_ctx.query_wait::<PetView>("age > 5").await?;
    let client_livequery = client_ctx.query_wait::<PetView>("age > 5").await?;

    let _server_sub = server_livequery.subscribe(&server_watcher);
    let _client_sub = client_livequery.subscribe(&client_watcher);

    // No notifications because we intentionally waited for the LiveQueries to be initialized before subscribing
    assert_eq!(server_watcher.drain(), vec![] as Vec<Vec<(EntityId, ChangeKind)>>);
    assert_eq!(client_watcher.drain(), vec![] as Vec<Vec<(EntityId, ChangeKind)>>);

    // Create pet on server
    let rex_id = {
        let trx = server_ctx.begin();
        let pet = trx.create(&Pet { name: "Rex".to_string(), age: "7".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation and check changes
    assert_expected_change(server_watcher.take_one_with_timeout(Duration::from_secs(30)).await, rex_id, &[rex_id]);
    assert_expected_change(client_watcher.take_one_with_timeout(Duration::from_secs(30)).await, rex_id, &[rex_id]);

    // Create pet on client
    let buddy_id = {
        let trx = client_ctx.begin();
        let pet = trx.create(&Pet { name: "Buddy".to_string(), age: "8".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation and check changes
    assert_expected_change(server_watcher.take_one_with_timeout(Duration::from_secs(30)).await, buddy_id, &[rex_id, buddy_id]);
    assert_expected_change(client_watcher.take_one_with_timeout(Duration::from_secs(30)).await, buddy_id, &[rex_id, buddy_id]);

    use ankurah::signals::Peek;
    let mut server_pets = server_livequery.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    let mut client_pets = client_livequery.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    let mut expected_pets = vec![rex_id, buddy_id];

    // The query has no ORDER BY, so its result order is unspecified.
    server_pets.sort_unstable();
    client_pets.sort_unstable();
    expected_pets.sort_unstable();

    assert_eq!(server_pets, expected_pets);
    assert_eq!(client_pets, expected_pets);

    // Ensure no additional unexpected changes (allow duplicate Add for already-seen ids)
    let extra_server = server_watcher.quiesce_drain().await;
    let extra_client = client_watcher.quiesce_drain().await;
    assert_no_unexpected_changes(extra_server, &expected_pets);
    assert_no_unexpected_changes(extra_client, &expected_pets);

    info!("Bidirectional subscription test passed");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

/// Helper to extract names from album query results
fn names(resultset: Vec<AlbumView>) -> Vec<String> { resultset.iter().map(|r| r.name().unwrap()).collect::<Vec<String>>() }

fn assert_no_unexpected_changes(changes: Vec<Vec<(EntityId, ChangeKind)>>, expected: &[EntityId]) {
    for batch in changes {
        for (entity_id, kind) in batch {
            assert!(expected.contains(&entity_id), "unexpected change for entity {}", entity_id);
            assert!(matches!(kind, ChangeKind::Add | ChangeKind::Update), "unexpected change kind {:?}", kind);
        }
    }
}

fn assert_expected_change(change: Vec<(EntityId, ChangeKind)>, expected: EntityId, allowed: &[EntityId]) {
    assert!(change.iter().any(|(entity_id, _)| *entity_id == expected), "expected entity {} not found in change set", expected);
    for (entity_id, kind) in change {
        assert!(allowed.contains(&entity_id), "unexpected change for entity {}", entity_id);
        assert!(matches!(kind, ChangeKind::Add | ChangeKind::Update), "unexpected change kind {:?}", kind);
    }
}
// start_test_server lives in common.rs (shared with protocol_version.rs)
