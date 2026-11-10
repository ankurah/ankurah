//! Nodes created independently connect and exchange entities.

mod common;

use common::*;
use std::sync::Arc;

#[tokio::test]
async fn a_durable_and_an_ephemeral_node_created_independently_exchange_entities() -> anyhow::Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    let server_context = server.context_async(DEFAULT_CONTEXT).await?;
    let client_context = client.context_async(DEFAULT_CONTEXT).await?;

    let transaction = client_context.begin();
    let from_client = transaction.create(&Album { name: "minted by the client".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;
    let transaction = server_context.begin();
    let from_server = transaction.create(&Album { name: "minted by the server".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;

    assert_eq!(server_context.get::<AlbumView>(from_client).await?.name()?, "minted by the client");
    assert_eq!(client_context.get::<AlbumView>(from_server).await?.name()?, "minted by the server");
    Ok(())
}
