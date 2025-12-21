use ankurah::{policy::DEFAULT_CONTEXT, LiveQuery, Node, PermissiveAgent};
use ankurah_doc_example_model::Album;
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::WebsocketClient;
use ankurah_websocket_server::WebsocketServer;
use anyhow::Result;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let storage_dir = dirs::home_dir().unwrap().join(".ankurah");
    // liaison id=server-example
    let storage = SledStorageEngine::with_path(storage_dir)?;
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());
    node.system.create().await?;

    let mut server = WebsocketServer::new(node);
    println!("Running server...");
    server.run("0.0.0.0:9797").await?;
    // liaison end

    Ok(())
}

// Storage backend examples
#[allow(dead_code)]
async fn sled_storage_example() -> anyhow::Result<()> {
    // liaison id=storage-sled
    let storage = SledStorageEngine::new()?;
    // liaison end
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());

    let _ = node;
    Ok(())
}

// Alternate storage backend example
#[allow(dead_code)]
async fn postgres_example() -> anyhow::Result<()> {
    use ankurah_storage_postgres::Postgres;

    let uri = "postgresql://localhost/mydb";
    // liaison id=storage-postgres
    let storage = Postgres::open(uri).await?;
    // liaison end
    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());

    let _ = node;
    Ok(())
}

// Rust client example
#[allow(dead_code)]
#[rustfmt::skip]
async fn rust_client_example() -> anyhow::Result<()> {
    // liaison id=rust-client-example
    let storage = SledStorageEngine::new_test()?;
    let node = Node::new(Arc::new(storage), PermissiveAgent::new());
    let _client = WebsocketClient::new(node.clone(), "ws://localhost:9797").await?;
    node.system.wait_system_ready().await;

    // Create album
    let ctx = node.context(ankurah::policy::DEFAULT_CONTEXT)?;
    let trx = ctx.begin();
    trx.create(&Album { name: "Parade".into(), artist: "Prince".into(), year: 1986 }).await?;
    trx.commit().await?;
    // liaison end

    Ok(())
}

// Example showing reactive query pattern
#[allow(dead_code)]
#[rustfmt::skip]
async fn query_example(node: &Node<SledStorageEngine, PermissiveAgent>) -> anyhow::Result<()> {
    use ankurah_doc_example_model::AlbumView;

    let ctx = node.context(DEFAULT_CONTEXT)?;

    #[allow(unused_variables)]
    // liaison id=livequery-rust
    let q: LiveQuery<AlbumView> = ctx.query("year > 1985")?;
    // liaison end
    
    // liaison id=signals-rust
    use ankurah::signals::Get;
    q.get(); // tracked by observer
             // liaison end

    Ok(())
}
