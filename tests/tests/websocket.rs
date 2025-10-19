use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::WebsocketClient;
use ankurah_websocket_server::WebsocketServer;
use anyhow::Result;
use std::sync::Arc;
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
    tokio::time::timeout(tokio::time::Duration::from_secs(10), test_websocket_bidirectional_subscription_impl())
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
    assert_eq!(server_watcher.take_one().await, vec![(rex_id, ChangeKind::Add)]);
    assert_eq!(client_watcher.take_one().await, vec![(rex_id, ChangeKind::Add)]);

    // Create pet on client
    let buddy_id = {
        let trx = client_ctx.begin();
        let pet = trx.create(&Pet { name: "Buddy".to_string(), age: "8".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation and check changes
    assert_eq!(server_watcher.take_one().await, vec![(buddy_id, ChangeKind::Add)]);
    assert_eq!(client_watcher.take_one().await, vec![(buddy_id, ChangeKind::Add)]);

    use ankurah::signals::Peek;
    let server_pets = server_livequery.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    let client_pets = client_livequery.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    let expected_pets = vec![rex_id, buddy_id];

    assert_eq!(server_pets, expected_pets);
    assert_eq!(client_pets, expected_pets);

    // Ensure no additional unexpected changes
    // Note: With proper DivergedSince handling, we may get 1 extra notification
    // when the client applies the server's event (which triggers a re-evaluation)
    assert!(server_watcher.quiesce().await <= 1, "Server should have at most 1 additional change");
    assert!(client_watcher.quiesce().await <= 1, "Client should have at most 1 additional change");

    info!("Bidirectional subscription test passed");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

/// Helper to extract names from album query results
fn names(resultset: Vec<AlbumView>) -> Vec<String> { resultset.iter().map(|r| r.name().unwrap()).collect::<Vec<String>>() }

/// Start a test websocket server and return the server node, URL, and task handle
async fn start_test_server() -> Result<(Node<SledStorageEngine, PermissiveAgent>, String, tokio::task::JoinHandle<()>)> {
    // Create and initialize server node
    let server_storage = Arc::new(SledStorageEngine::new_test()?);
    let server_node = Node::new_durable(server_storage, PermissiveAgent::new());
    server_node.system.create().await?;

    use rand::Rng;
    let mut rng = rand::thread_rng();

    // Retry logic for port conflicts
    const MAX_PORT_RETRIES: usize = 10;
    let mut last_error = None;

    for attempt in 0..MAX_PORT_RETRIES {
        let port: u16 = rng.gen_range(20000..=65000);
        let bind_addr = format!("127.0.0.1:{}", port);
        let server_url = format!("ws://127.0.0.1:{}", port);

        info!("Attempt {} - Starting websocket server on {}", attempt + 1, server_url);

        // Start server in background task
        let server_node_clone = server_node.clone();
        let bind_addr_clone = bind_addr.clone();

        let server_task = tokio::spawn(async move {
            let mut server = WebsocketServer::new(server_node_clone);
            if let Err(e) = server.run(&bind_addr_clone).await {
                tracing::warn!("Test server error on {}: {}", bind_addr_clone, e);
            }
        });

        // Wait briefly for the TcpListener::bind() call to complete
        // If port is in use, the server task will complete immediately with an error
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check if the server task is still running (successful bind) or completed (failed bind)
        if server_task.is_finished() {
            // Server task completed immediately, which means binding failed
            tracing::warn!("Failed to bind server on port {} (attempt {})", port, attempt + 1);
            last_error = Some(anyhow::anyhow!("Port {} binding failed", port));

            if attempt < MAX_PORT_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            continue;
        }

        // Server task is still running, which means TcpListener::bind() succeeded
        // The server is now listening and ready for connections
        info!("Successfully started websocket server on {} (attempt {})", server_url, attempt + 1);
        return Ok((server_node, server_url, server_task));
    }

    Err(anyhow::anyhow!("Failed to start test server after {} attempts. Last error: {:?}", MAX_PORT_RETRIES, last_error))
}
