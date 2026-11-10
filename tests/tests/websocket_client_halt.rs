//! Native websocket reconnects must respect the node's terminal halt.

mod common;

use ankurah::{
    core::{
        connector::{Handshake, PeerConnectionError, PeerProvenance},
        error::NodeHaltReason,
        node::NodeKey,
    },
    signals::Wait,
};
use ankurah_websocket_client::{ConnectionError, ConnectionState, WebsocketClient};
use common::*;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<TcpStream>;

/// Play the half of the handshake a server holding only `server` would: open, read the client's
/// opening and presence, then present.
async fn advertise(socket: &mut Socket, server: &NodeKey) -> anyhow::Result<()> {
    let mut handshake = Handshake::new(server.node_id(), PeerProvenance::Inbound);
    socket.send(Message::Binary(proto::Message::Open(handshake.opening()).encode().into())).await?;
    match socket.next().await.transpose()? {
        Some(Message::Binary(bytes)) => handshake.read_opening(&bytes)?,
        other => anyhow::bail!("expected the client's opening frame, got {other:?}"),
    }
    match socket.next().await.transpose()? {
        Some(Message::Binary(bytes)) if matches!(proto::Message::decode(&bytes)?, proto::Message::Presence(_)) => {}
        other => anyhow::bail!("expected the client's presence, got {other:?}"),
    }
    let presence = server.sign_presence(handshake.presence_claims(server.node_id(), true)?);
    socket.send(Message::Binary(proto::Message::Presence(presence).encode().into())).await?;
    Ok(())
}

#[tokio::test]
async fn a_reset_stops_native_reconnects_permanently() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = NodeKey::generate();
        let node = ephemeral_sled_setup().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &server).await?;
        client.wait_connected().await?;

        node.reset().await?;
        let halt_reason = NodeHaltReason::Reset;
        let expected = ConnectionError::Peer(PeerConnectionError::NodeHalted(halt_reason.clone()));
        let state = client.state();
        state.wait_value(ConnectionState::Error(expected.clone())).await;
        assert_eq!(client.wait_connected().await, Err(expected.clone()));
        assert!(!client.is_connected());
        drop(socket);

        // A normal retry starts within two seconds. Keep the listener open across that window.
        assert!(
            tokio::time::timeout(Duration::from_secs(3), listener.accept()).await.is_err(),
            "a halted node must not open another connection"
        );
        assert_eq!(state.peek(), ConnectionState::Error(expected.clone()));
        assert_eq!(client.wait_connected().await, Err(expected));
        assert_eq!(node.state().peek(), NodeState::Halted(halt_reason));
        client.shutdown().await?;
        Ok(())
    })
    .await?
}
