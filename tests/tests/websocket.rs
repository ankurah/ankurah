use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::WebsocketClient;
use ankurah_websocket_server::WebsocketServer;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

mod common;
use common::*;

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

    // Give time for initial data sync
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Get client context
    let client_ctx = client_node.context(c)?;

    // Test fetching data through websocket connection
    let query = "name = 'Dark Side of the Moon'";
    let results = client_ctx.fetch(query).await?;
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

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    // Set up subscription watchers
    let (server_watcher, check_server) = changeset_watcher::<AlbumView>();
    let (client_watcher, check_client) = changeset_watcher::<AlbumView>();

    // Subscribe on both ends
    let _server_sub = server_ctx.subscribe("name = 'Abbey Road'", server_watcher).await?;
    let _client_sub = client_ctx.subscribe("name = 'Abbey Road'", client_watcher).await?;

    // Initial state should be empty
    assert_eq!(check_server(), vec![vec![]] as Vec<Vec<(EntityId, ChangeKind)>>);
    assert_eq!(check_client(), vec![vec![]] as Vec<Vec<(EntityId, ChangeKind)>>);

    // Create matching entity on server
    let album_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Abbey Road".into(), year: "1969".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Check that both server and client received the change
    let server_changes = check_server();
    let client_changes = check_client();

    // Both should see exactly 1 change with the correct entity ID
    assert_eq!(server_changes.len(), 1);
    assert_eq!(client_changes.len(), 1);
    assert_eq!(server_changes[0].len(), 1);
    assert_eq!(client_changes[0].len(), 1);

    // Check entity IDs match (ChangeKind may vary between Add/Initial)
    assert_eq!(server_changes[0][0].0, album_id);
    assert_eq!(client_changes[0][0].0, album_id);

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

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    // Set up subscription watchers
    let (server_watcher, check_server) = changeset_watcher::<PetView>();
    let (client_watcher, check_client) = changeset_watcher::<PetView>();

    // Subscribe to pets with age > 5
    let _server_sub = server_ctx.subscribe("age > 5", server_watcher).await?;
    let _client_sub = client_ctx.subscribe("age > 5", client_watcher).await?;

    // Create pet on server
    let server_pet_id = {
        let trx = server_ctx.begin();
        let pet = trx.create(&Pet { name: "Rex".to_string(), age: "7".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create pet on client
    let client_pet_id = {
        let trx = client_ctx.begin();
        let pet = trx.create(&Pet { name: "Buddy".to_string(), age: "8".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Both sides should see both pets
    let server_changes = check_server();
    let client_changes = check_client();

    info!("Server changes: {:?}", server_changes);
    info!("Client changes: {:?}", client_changes);

    // Both should see exactly 3 change events (empty initial + 2 pets)
    assert_eq!(server_changes.len(), 3);
    assert_eq!(client_changes.len(), 3);

    // Initial should be empty
    assert_eq!(server_changes[0], vec![]);
    assert_eq!(client_changes[0], vec![]);

    // Both should see both pets (event types may vary)
    let mut server_pets: Vec<_> = server_changes.iter().skip(1).flat_map(|v| v.iter().map(|(id, _)| *id)).collect();
    let mut client_pets: Vec<_> = client_changes.iter().skip(1).flat_map(|v| v.iter().map(|(id, _)| *id)).collect();

    server_pets.sort();
    client_pets.sort();

    let mut expected_pets = vec![server_pet_id, client_pet_id];
    expected_pets.sort();

    assert_eq!(server_pets, expected_pets);
    assert_eq!(client_pets, expected_pets);

    info!("Bidirectional subscription test passed");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

/// Helper to extract names from album query results
fn names(resultset: ankurah::ResultSet<AlbumView>) -> Vec<String> {
    resultset.items.iter().map(|r| r.name().unwrap()).collect::<Vec<String>>()
}

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
