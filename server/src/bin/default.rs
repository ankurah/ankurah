use ankurah_core::storage::SledStorageEngine;
use anyhow::Result;
use tracing::Level;

use ankurah_server::Server;
#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Initialize storage engine
    let storage = SledStorageEngine::with_homedir_folder(".syncra")?;

    // Create and start the server
    let server = Server::builder()
        .bind_address("0.0.0.0:9797")
        .with_storage(storage)
        .build()?;

    server.run().await?;

    Ok(())
}
