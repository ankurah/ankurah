use ankurah::{Node, PermissiveAgent};
use ankurah_signals::Wait;
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::{ConnectionState, WebsocketClient};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpListener, time::timeout};

#[tokio::test]
async fn shutdown_before_connection_task_starts() -> anyhow::Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(node, "ws://unused.invalid").await?;
    let state = client.state();
    assert_eq!(state.value(), ConnectionState::Disconnected);
    timeout(Duration::from_secs(1), client.shutdown()).await??;
    assert_eq!(state.value(), ConnectionState::Disconnected);
    Ok(())
}

#[tokio::test]
async fn shutdown_interrupts_websocket_handshake() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node, &format!("ws://{}", listener.local_addr()?)).await?;
        let state = client.state();
        let (mut socket, _) = listener.accept().await?;
        socket.read_exact(&mut [0; 1]).await?;
        timeout(Duration::from_secs(1), client.shutdown()).await??;
        assert_eq!(state.value(), ConnectionState::Disconnected);
        socket.read_to_end(&mut Vec::new()).await?;
        anyhow::Ok(())
    })
    .await?
}

#[tokio::test]
async fn shutdown_interrupts_retry_backoff() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node, &format!("ws://{}", listener.local_addr()?)).await?;
        let state = client.state();
        let (socket, _) = listener.accept().await?;
        drop(socket);
        state.wait_for(|state| matches!(state, ConnectionState::Error(_))).await;
        timeout(Duration::from_millis(500), client.shutdown()).await??;
        assert_eq!(state.value(), ConnectionState::Disconnected);
        anyhow::Ok(())
    })
    .await?
}
