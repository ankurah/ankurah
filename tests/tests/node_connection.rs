//! Native websocket reconnects authenticate peers and respect terminal reset.

mod common;

use ankurah::{
    core::{connector::PeerConnectionError, error::NodeHaltReason},
    signals::Wait,
};
use ankurah_websocket_client::{ConnectionError, ConnectionState, WebsocketClient};
use common::*;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<TcpStream>;

/// A signed server handshake for testing peer replacement at one address.
async fn advertise(socket: &mut Socket, server: &Node<SledStorageEngine, PermissiveAgent>) -> anyhow::Result<()> {
    let client_challenge = match socket.next().await.transpose()? {
        Some(Message::Binary(bytes)) => match bincode::deserialize(&bytes)? {
            proto::Message::Challenge(challenge) => challenge,
            other => anyhow::bail!("expected client Challenge, got {other:?}"),
        },
        other => anyhow::bail!("expected client Challenge frame, got {other:?}"),
    };
    socket.send(Message::Binary(bincode::serialize(&proto::Message::Challenge(proto::Challenge::random()))?.into())).await?;
    let presence = server.presence(proto::ConnectionSide::Accepting, &client_challenge).await?;
    socket.send(Message::Binary(bincode::serialize(&proto::Message::Presence(presence))?.into())).await?;
    Ok(())
}

async fn client_presence(socket: &mut Socket) -> anyhow::Result<proto::Presence> {
    match socket.next().await.transpose()? {
        Some(Message::Binary(bytes)) => match bincode::deserialize(&bytes)? {
            proto::Message::Presence(signed) => Ok(signed.presence),
            other => anyhow::bail!("expected client Presence, got {other:?}"),
        },
        other => anyhow::bail!("expected client Presence frame, got {other:?}"),
    }
}

#[tokio::test]
async fn reconnect_can_authenticate_an_independently_initialized_durable_peer() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let original = durable_sled_setup().await?;
        let replacement = durable_sled_setup().await?;
        assert_ne!(original.id(), replacement.id());
        let node = ephemeral_sled_setup().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &original).await?;
        client.wait_connected().await?;
        assert_eq!(Some(client_presence(&mut socket).await?.node_id()), node.id());
        let epoch = node.epoch();
        socket.close(None).await?;
        drop(socket);

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &replacement).await?;
        client_presence(&mut socket).await?;
        client.wait_connected().await?;
        assert!(!node.connections_of(replacement.wait_id().await?).is_empty());
        assert_eq!(node.epoch(), epoch);
        assert!(node.state().peek().halt_reason().is_none());
        client.shutdown().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn explicit_reset_stops_native_reconnects_permanently() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let node = ephemeral_sled_setup().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;
        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &server).await?;
        client.wait_connected().await?;
        client_presence(&mut socket).await?;

        node.reset().await?;
        let expected = ConnectionError::Peer(PeerConnectionError::NodeHalted(NodeHaltReason::Reset));
        let state = client.state();
        state.wait_value(ConnectionState::Error(expected.clone())).await;
        assert_eq!(client.wait_connected().await, Err(expected.clone()));
        assert!(!client.is_connected());
        assert!(node.get_durable_peers().is_empty());
        drop(socket);
        // The normal retry delay is two seconds; keep the listener alive across it.
        assert!(tokio::time::timeout(Duration::from_secs(3), listener.accept()).await.is_err());
        assert_eq!(state.peek(), ConnectionState::Error(expected));
        assert_eq!(node.state().peek(), NodeState::Halted(NodeHaltReason::Reset));
        client.shutdown().await?;
        Ok(())
    })
    .await?
}
