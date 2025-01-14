use ankurah::Node;
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_server::WebsocketServer;
use anyhow::Result;
use std::sync::Arc;
use tracing::Level;

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Initialize storage engine
    let storage = SledStorageEngine::with_homedir_folder(".ankurah_example")?;
    let node = Node::new_durable(Arc::new(storage));

    // Create and start the websocket server
    let server = WebsocketServer::new(node);
    server.run("0.0.0.0:9797").await?;

    Ok(())
}
