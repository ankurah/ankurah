//! Native websocket reconnects must respect the node's system identity and terminal halt.

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

async fn advertise(socket: &mut Socket, server: &Node<SledStorageEngine, PermissiveAgent>) -> anyhow::Result<()> {
    let presence = proto::Presence {
        node_id: server.id,
        durable: server.durable,
        system_root: server.system.root(),
        protocol_version: proto::PROTOCOL_VERSION,
    };
    socket.send(Message::Binary(bincode::serialize(&proto::Message::Presence(presence))?.into())).await?;
    Ok(())
}

async fn client_presence(socket: &mut Socket) -> anyhow::Result<proto::Presence> {
    match socket.next().await.transpose()? {
        Some(Message::Binary(bytes)) => match bincode::deserialize(&bytes)? {
            proto::Message::Presence(presence) => Ok(presence),
            other => anyhow::bail!("expected client Presence, got {other:?}"),
        },
        other => anyhow::bail!("expected client Presence frame, got {other:?}"),
    }
}

#[tokio::test]
async fn different_system_halts_native_reconnects_without_replacement_enabled() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let original = durable_sled_setup().await?;
        let replacement = durable_sled_setup().await?;
        let node = ephemeral_sled_setup().await?;
        let root = original.system.root().expect("original root");
        let offered = replacement.system.root_id().expect("different root");
        assert_ne!(root.payload.entity_id, offered);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &original).await?;
        client.wait_connected().await?;
        assert_eq!(client_presence(&mut socket).await?.system_root, Some(root.clone()));
        let epoch = node.system.system_epoch();
        assert!(epoch.is_some());
        socket.close(None).await?;
        drop(socket);

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &replacement).await?;
        let reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: offered };
        let expected = ConnectionError::Peer(PeerConnectionError::NodeHalted(reason.clone()));
        let state = client.state();
        state.wait_value(ConnectionState::Error(expected.clone())).await;
        assert_eq!(client.wait_connected().await, Err(expected.clone()));
        assert!(!client.is_connected());
        assert_eq!(node.state().peek(), NodeState::Halted(reason));
        assert!(!node.system.is_system_ready());
        assert_eq!(node.system.root(), Some(root.clone()));
        assert_eq!(node.system.system_epoch(), epoch);
        drop(socket);

        assert!(tokio::time::timeout(Duration::from_secs(3), listener.accept()).await.is_err());
        assert_eq!(state.peek(), ConnectionState::Error(expected.clone()));
        assert_eq!(client.wait_connected().await, Err(expected));
        assert!(!client.is_connected());

        client.shutdown().await?;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn enabled_system_replacement_stops_native_reconnects_permanently() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let original = durable_sled_setup().await?;
        let replacement = durable_sled_setup().await?;
        let node = ephemeral_sled_setup().await?;
        let root = original.system.root().expect("original root");
        let proposed = replacement.system.root_id().expect("different root");
        assert_ne!(root.payload.entity_id, proposed);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &original).await?;
        client.wait_connected().await?;
        assert_eq!(client_presence(&mut socket).await?.system_root, Some(root.clone()));
        node.set_allow_system_replacement(true);
        socket.close(None).await?;
        drop(socket);

        let mut socket = accept_async(listener.accept().await?.0).await?;
        advertise(&mut socket, &replacement).await?;
        let halt_reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed };
        let expected = ConnectionError::Peer(PeerConnectionError::NodeHalted(halt_reason.clone()));
        let state = client.state();
        state.wait_value(ConnectionState::Error(expected.clone())).await;
        assert_eq!(node.state().peek(), NodeState::Halted(halt_reason.clone()));
        assert_eq!(client.wait_connected().await, Err(expected.clone()));
        assert!(!client.is_connected());
        assert!(node.get_durable_peers().is_empty());
        assert_eq!(node.system.wait_system_ready().await, Err(halt_reason.clone()));
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.system.root().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(node.system.system_epoch().is_none());
        drop(socket);

        node.set_allow_system_replacement(false);
        // A normal retry after the second connection starts in two seconds. Keep the listener open across that window.
        assert!(
            tokio::time::timeout(Duration::from_secs(3), listener.accept()).await.is_err(),
            "a halted node must not open another connection"
        );
        assert_eq!(state.peek(), ConnectionState::Error(expected.clone()));
        assert_eq!(client.wait_connected().await, Err(expected));
        assert_eq!(node.state().peek(), NodeState::Halted(halt_reason));
        assert!(node.system.root().is_none());
        assert!(node.system.system_epoch().is_none());
        client.shutdown().await?;
        Ok(())
    })
    .await?
}
