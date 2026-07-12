use ankurah::{changes::ChangeSet, policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent};
use ankurah_connector_iroh::{IrohClient, IrohServer};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

/// Bind a localhost-only iroh endpoint: no relays, no address lookup services.
/// `presets::Minimal` sets only the mandatory crypto provider and leaves
/// relaying disabled, so tests never touch external infrastructure.
async fn bind_local_endpoint() -> Result<iroh::Endpoint> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal).clear_ip_transports().bind_addr("127.0.0.1:0")?.bind().await?;
    Ok(endpoint)
}

/// EndpointAddr for dialing `endpoint` by its direct socket addresses only.
fn direct_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
    let mut addr = iroh::EndpointAddr::new(endpoint.id());
    for socket in endpoint.bound_sockets() {
        addr = addr.with_ip_addr(socket);
    }
    addr
}

async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {}", what))
}

fn names(resultset: Vec<AlbumView>) -> Vec<String> { resultset.iter().map(|r| r.name().unwrap()).collect::<Vec<String>>() }

/// A durable node accepts, an ephemeral node dials; data created on the durable
/// node is fetchable from the ephemeral node over the iroh connection.
#[tokio::test]
async fn iroh_fetch() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Durable node with real storage on the accepting side
    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    // Create some data on the server
    let server_ctx = server_node.context(c)?;
    {
        let trx = server_ctx.begin();
        trx.create(&Album { name: "Dark Side of the Moon".into(), year: "1973".into() }).await?;
        trx.create(&Album { name: "Wish You Were Here".into(), year: "1975".into() }).await?;
        trx.create(&Album { name: "Animals".into(), year: "1977".into() }).await?;
        trx.commit().await?;
    }

    // Ephemeral node dials
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;
    assert_eq!(client.server_node_id(), Some(server_node.id));

    // Presence exchange completed: the client registered the durable server
    client_node.system.wait_system_ready().await;
    assert_eq!(client_node.get_durable_peers(), vec![server_node.id]);

    // Fetch data through the iroh connection
    let client_ctx = client_node.context(c)?;
    let results = client_ctx.fetch::<AlbumView>("name = 'Dark Side of the Moon'").await?;
    assert_eq!(names(results), ["Dark Side of the Moon"]);

    let all_results = client_ctx.fetch("year > '1970'").await?;
    let mut all_names = names(all_results);
    all_names.sort();
    assert_eq!(all_names, ["Animals", "Dark Side of the Moon", "Wish You Were Here"]);

    shutdown_client(client).await?;
    server.shutdown().await?;
    Ok(())
}

/// Shut down a client and close its endpoint (which the embedder owns, and the
/// tests here are the embedder).
async fn shutdown_client(client: IrohClient<SledStorageEngine, PermissiveAgent>) -> Result<()> {
    let endpoint = client.endpoint().clone();
    client.shutdown().await?;
    endpoint.close().await;
    Ok(())
}

/// Shutdown is sticky even if requested before the spawned connection loop gets
/// its first poll. A current-thread runtime makes that ordering deterministic.
#[tokio::test(flavor = "current_thread")]
async fn iroh_immediate_client_shutdown_completes() -> Result<()> {
    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node, bind_local_endpoint().await?);

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node, bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;

    tokio::time::timeout(Duration::from_secs(2), shutdown_client(client))
        .await
        .map_err(|_| anyhow::anyhow!("immediate client shutdown timed out"))??;
    server.shutdown().await?;
    Ok(())
}

/// Explicit client shutdown removes the client's own Node registration before
/// returning, rather than merely closing the remote transport.
#[tokio::test]
async fn iroh_client_shutdown_deregisters_local_peer() -> Result<()> {
    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;
    assert_eq!(client_node.get_durable_peers(), vec![server_node.id]);

    shutdown_client(client).await?;
    assert!(client_node.get_durable_peers().is_empty());

    server.shutdown().await?;
    Ok(())
}

/// Dropping the client handle aborts its task, so registration cleanup must be
/// owned by a cancellation-safe guard inside the connection future.
#[tokio::test]
async fn iroh_client_drop_deregisters_local_peer() -> Result<()> {
    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;
    assert_eq!(client_node.get_durable_peers(), vec![server_node.id]);

    let endpoint = client.endpoint().clone();
    drop(client);
    let node = client_node.clone();
    wait_until("dropped client to deregister its local peer", move || node.get_durable_peers().is_empty()).await?;
    endpoint.close().await;

    server.shutdown().await?;
    Ok(())
}

/// Router shutdown cancels protocol-handler futures. The server's Node must be
/// cleaned up even though normal code after the connection pump is not polled.
#[tokio::test]
async fn iroh_server_shutdown_deregisters_local_peer() -> Result<()> {
    let server_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    client_node.system.create().await?;
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;

    let node = server_node.clone();
    let client_id = client_node.id;
    wait_until("server to register durable client", move || node.get_durable_peers() == vec![client_id]).await?;

    server.shutdown().await?;
    assert!(server_node.get_durable_peers().is_empty());
    shutdown_client(client).await?;
    Ok(())
}

/// Dropping the Router aborts its protocol tasks rather than awaiting their
/// normal return path. The registration guard must still clean up the Node.
#[tokio::test]
async fn iroh_server_drop_deregisters_local_peer() -> Result<()> {
    let server_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    client_node.system.create().await?;
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;

    let node = server_node.clone();
    let client_id = client_node.id;
    wait_until("server to register durable client", move || node.get_durable_peers() == vec![client_id]).await?;

    drop(server);
    let node = server_node.clone();
    wait_until("dropped server to deregister its local peer", move || node.get_durable_peers().is_empty()).await?;
    shutdown_client(client).await?;
    Ok(())
}

/// The supported standalone topology is a durable hub with independently
/// connected ephemeral spokes. A write from one spoke reaches another through
/// a live query on the hub.
#[tokio::test]
async fn iroh_two_spokes_propagate_through_durable_hub() -> Result<()> {
    let hub_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    hub_node.system.create().await?;
    let hub = IrohServer::new(hub_node, bind_local_endpoint().await?);
    let hub_addr = direct_addr(hub.endpoint());

    let writer_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = IrohClient::new(writer_node.clone(), bind_local_endpoint().await?, hub_addr.clone()).await?;
    writer.wait_connected().await?;
    writer_node.system.wait_system_ready().await;

    let reader_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let reader = IrohClient::new(reader_node.clone(), bind_local_endpoint().await?, hub_addr).await?;
    reader.wait_connected().await?;
    reader_node.system.wait_system_ready().await;

    use ankurah::signals::Subscribe;
    let reader_ctx = reader_node.context(c)?;
    let reader_query = reader_ctx.query_wait::<AlbumView>("year > '2000'").await?;
    let (reader_tx, mut reader_rx) = tokio::sync::mpsc::unbounded_channel::<ChangeSet<AlbumView>>();
    let _reader_sub = reader_query.subscribe(reader_tx);

    let writer_ctx = writer_node.context(c)?;
    let album_id = {
        let trx = writer_ctx.begin();
        let album = trx.create(&Album { name: "In Rainbows".into(), year: "2007".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    let reader_change = tokio::time::timeout(Duration::from_secs(10), reader_rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("reader subscription closed"))?;
    assert_eq!(reader_change.changes.len(), 1);
    assert_eq!(reader_change.changes[0].entity().id(), album_id);

    shutdown_client(writer).await?;
    shutdown_client(reader).await?;
    hub.shutdown().await?;
    Ok(())
}

/// Changes propagate through subscriptions in both directions: an entity
/// committed on either node shows up in the other node's live query.
#[tokio::test]
async fn iroh_subscription_propagation() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;
    client_node.system.wait_system_ready().await;

    let server_ctx = server_node.context(c)?;
    let client_ctx = client_node.context(c)?;

    use ankurah::signals::Subscribe;
    let (server_tx, mut server_rx) = tokio::sync::mpsc::unbounded_channel::<ChangeSet<AlbumView>>();
    let (client_tx, mut client_rx) = tokio::sync::mpsc::unbounded_channel::<ChangeSet<AlbumView>>();
    let server_query = server_ctx.query_wait::<AlbumView>("year > '1960'").await?;
    let client_query = client_ctx.query_wait::<AlbumView>("year > '1960'").await?;
    let _server_sub = server_query.subscribe(server_tx);
    let _client_sub = client_query.subscribe(client_tx);

    // Create on the server; both nodes observe the change
    let abbey_road_id = {
        let trx = server_ctx.begin();
        let album = trx.create(&Album { name: "Abbey Road".into(), year: "1969".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };
    let server_change = tokio::time::timeout(Duration::from_secs(10), server_rx.recv()).await?.expect("server subscription closed");
    assert_eq!(server_change.changes.len(), 1);
    assert_eq!(server_change.changes[0].entity().id(), abbey_road_id);
    let client_change = tokio::time::timeout(Duration::from_secs(10), client_rx.recv()).await?.expect("client subscription closed");
    assert_eq!(client_change.changes.len(), 1);
    assert_eq!(client_change.changes[0].entity().id(), abbey_road_id);

    // Create on the client; both nodes observe the change
    let let_it_be_id = {
        let trx = client_ctx.begin();
        let album = trx.create(&Album { name: "Let It Be".into(), year: "1970".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };
    let server_change = tokio::time::timeout(Duration::from_secs(10), server_rx.recv()).await?.expect("server subscription closed");
    assert_eq!(server_change.changes.len(), 1);
    assert_eq!(server_change.changes[0].entity().id(), let_it_be_id);
    let client_change = tokio::time::timeout(Duration::from_secs(10), client_rx.recv()).await?.expect("client subscription closed");
    assert_eq!(client_change.changes.len(), 1);
    assert_eq!(client_change.changes[0].entity().id(), let_it_be_id);

    shutdown_client(client).await?;
    server.shutdown().await?;
    Ok(())
}

/// The dialer registers the acceptor on presence exchange and deregisters it
/// when the acceptor goes away.
#[tokio::test]
async fn iroh_dialer_deregisters_on_disconnect() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server_node.system.create().await?;
    let server = IrohServer::new(server_node.clone(), bind_local_endpoint().await?);

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = IrohClient::new(client_node.clone(), bind_local_endpoint().await?, direct_addr(server.endpoint())).await?;
    client.wait_connected().await?;
    assert_eq!(client_node.get_durable_peers(), vec![server_node.id]);

    // Take the server down; the client's pump exits and deregisters the peer
    // (and then starts its reconnect backoff loop, which we don't exercise here)
    server.shutdown().await?;
    let node = client_node.clone();
    wait_until("client to deregister the server", move || node.get_durable_peers().is_empty()).await?;

    shutdown_client(client).await?;
    Ok(())
}

/// The acceptor registers the dialer on presence exchange and deregisters it on
/// clean disconnect. The dialer is the durable node here so that registration is
/// observable on the accepting side via get_durable_peers.
#[tokio::test]
async fn iroh_acceptor_deregisters_on_disconnect() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    // Ephemeral node accepts
    let acceptor_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let acceptor = IrohServer::new(acceptor_node.clone(), bind_local_endpoint().await?);

    // Durable node dials
    let dialer_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    dialer_node.system.create().await?;
    let dialer = IrohClient::new(dialer_node.clone(), bind_local_endpoint().await?, direct_addr(acceptor.endpoint())).await?;
    dialer.wait_connected().await?;

    // The acceptor registers the durable dialer once it processes the dialer's presence
    let node = acceptor_node.clone();
    let dialer_id = dialer_node.id;
    wait_until("acceptor to register the dialer", move || node.get_durable_peers() == vec![dialer_id]).await?;
    acceptor_node.system.wait_system_ready().await;

    // Clean disconnect from the dialer side; the acceptor deregisters the peer
    shutdown_client(dialer).await?;
    let node = acceptor_node.clone();
    wait_until("acceptor to deregister the dialer", move || node.get_durable_peers().is_empty()).await?;

    acceptor.shutdown().await?;
    Ok(())
}
