//! #294: protocol version in the Presence handshake.
//!
//! Covers the refusal arms: register_peer (all transports), the websocket
//! server against version-0 and mismatched handshakes, and the websocket
//! client against a mismatched server.

use ankurah::core::connector::{PeerSender, SendError};
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::WebsocketClient;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMessage;

mod common;
use common::{proto, start_test_server};

#[derive(Clone)]
struct NullSender(proto::EntityId);

#[async_trait::async_trait]
impl PeerSender for NullSender {
    fn send_message(&self, _message: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

fn presence(protocol_version: u32) -> proto::Presence {
    proto::Presence { node_id: proto::EntityId::new(), durable: false, system_root: None, protocol_version }
}

/// The core enforcement point: register_peer refuses incompatible versions
/// for every transport, and accepts the current one.
#[tokio::test]
async fn register_peer_refuses_incompatible_version() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    for bad_version in [0, proto::PROTOCOL_VERSION + 1] {
        let p = presence(bad_version);
        let peer_id = p.node_id;
        let err = node.register_peer(p, Box::new(NullSender(peer_id))).expect_err("must refuse");
        assert_eq!(err, proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: bad_version });
    }

    let p = presence(proto::PROTOCOL_VERSION);
    let peer_id = p.node_id;
    node.register_peer(p, Box::new(NullSender(peer_id))).expect("current version must be accepted");
    Ok(())
}

/// Drive a raw websocket against a real server and collect frames until it
/// closes; panics if the server leaves the connection open.
async fn exchange_until_close(server_url: &str, first_frame: Vec<u8>) -> Result<Vec<proto::Message>> {
    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (mut sink, mut stream) = ws.split();
    sink.send(WsMessage::Binary(first_frame.into())).await?;

    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(None) | Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) => return Ok(received),
            Ok(Some(Ok(WsMessage::Binary(data)))) => {
                if let Ok(message) = bincode::deserialize::<proto::Message>(&data) {
                    received.push(message);
                }
            }
            Ok(Some(Ok(_))) => continue,
            Err(_) => panic!("server did not close the connection; frames so far: {received:?}"),
        }
    }
}

/// A 0.9.x peer (no protocol_version in Presence) is detected and the
/// connection is closed instead of left dangling.
#[tokio::test]
async fn server_refuses_version0_handshake() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let (_server_node, server_url, server_task) = start_test_server().await?;

    // The 0.9 encoding is a strict prefix of the current one (pinned by a
    // proto unit test), so truncating the version field reproduces it.
    let mut old_shape = bincode::serialize(&proto::Message::Presence(presence(proto::PROTOCOL_VERSION)))?;
    old_shape.truncate(old_shape.len() - 4);

    exchange_until_close(&server_url, old_shape).await?;
    server_task.abort();
    Ok(())
}

/// A versioned peer with the wrong version gets an explicit PresenceRejected
/// before the close.
#[tokio::test]
async fn server_refuses_mismatched_version() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let (_server_node, server_url, server_task) = start_test_server().await?;

    let frame = bincode::serialize(&proto::Message::Presence(presence(999)))?;
    let received = exchange_until_close(&server_url, frame).await?;

    let rejection = received
        .iter()
        .find_map(|m| match m {
            proto::Message::PresenceRejected(r) => Some(r.clone()),
            _ => None,
        })
        .expect("server must send PresenceRejected before closing");
    assert_eq!(rejection, proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: 999 });

    server_task.abort();
    Ok(())
}

/// A legacy browser client waits for the server Presence before sending its
/// own. A durable versioned Presence is undecodable by that client, so it remains
/// silent; the server must still bound the incomplete handshake.
#[tokio::test(start_paused = true)]
async fn server_closes_silent_client_after_durable_presence() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (_sink, mut stream) = ws.split();

    let server_presence = loop {
        match stream.next().await {
            Some(Ok(WsMessage::Binary(data))) => match bincode::deserialize::<proto::Message>(&data)? {
                proto::Message::Presence(presence) => break presence,
                other => panic!("server sent {other:?} before its presence"),
            },
            Some(Ok(other)) => panic!("server sent {other:?} before its presence"),
            Some(Err(e)) => return Err(e.into()),
            None => panic!("server closed before sending its presence"),
        }
    };
    assert!(server_presence.durable, "test server must reproduce the durable Presence shape");
    assert!(server_presence.system_root.is_some(), "durable Presence must carry the versioned system-root state");

    // Send nothing. Advancing beyond the production deadline keeps this test
    // deterministic and avoids adding ten seconds to the native test suite.
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;

    // The production deadline has fired in virtual time. Resume the clock
    // before bounding the socket read: a timeout on a still-paused clock can
    // win immediately, before the OS close becomes readable, which makes this
    // assertion scheduler-dependent in the full parallel test run.
    tokio::time::resume();

    let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("server did not close the silent handshake after its deadline");
    match next {
        None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => {}
        Some(Ok(other)) => panic!("server left the silent handshake open and sent {other:?}"),
    }

    server_task.abort();
    Ok(())
}

/// Application traffic before Presence is refused: a server that skips the
/// handshake and leads with a PeerMessage must not reach the node, and the
/// client must drop the connection rather than continue.
#[tokio::test]
async fn client_refuses_peer_message_before_presence() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Fake server: leads with application traffic instead of Presence, then
    // watches whether the client tears the connection down.
    let fake_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
        let (mut sink, mut stream) = ws.split();

        let early = proto::Message::PeerMessage(proto::NodeMessage::UnsubscribeQuery {
            from: proto::EntityId::new(),
            query_id: proto::QueryId::new(),
        });
        sink.send(WsMessage::Binary(bincode::serialize(&early).unwrap().into())).await.expect("send");

        // The client must close on the pre-negotiation frame instead of
        // continuing the session. Ignore control frames; a Binary frame or a
        // quiet open connection both mean the client continued.
        loop {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(None) | Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) => break true,
                Ok(Some(Ok(WsMessage::Binary(data)))) => {
                    // The client's own Presence (and a possible rejection
                    // notice) are handshake chatter, not session traffic.
                    match bincode::deserialize::<proto::Message>(&data) {
                        Ok(proto::Message::Presence(_)) | Ok(proto::Message::PresenceRejected(_)) => continue,
                        other => {
                            eprintln!("client continued the session instead of closing: {other:?}");
                            break false;
                        }
                    }
                }
                Ok(Some(Ok(_))) => continue,
                Err(_) => {
                    eprintln!("client left the connection open (timeout)");
                    break false;
                }
            }
        }
    });

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &format!("ws://{}", addr)).await?;

    match tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await {
        Ok(Ok(())) => panic!("client treated a pre-negotiation server as connected"),
        Ok(Err(_)) | Err(_) => {}
    }

    assert!(fake_server.await?, "client kept the connection open after pre-negotiation application traffic");
    client.shutdown().await?;
    Ok(())
}

/// The client side of the same handshake: a server speaking a different
/// version is refused, told why, and never treated as connected.
#[tokio::test]
async fn client_refuses_mismatched_server_without_hot_loop() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Fake server: sends a doctored Presence, records whether the client
    // explains itself with a PresenceRejected, and keeps listening long
    // enough to catch an immediate reconnect.
    let fake_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
        let (mut sink, mut stream) = ws.split();

        let doctored = proto::Presence { node_id: proto::EntityId::new(), durable: true, system_root: None, protocol_version: 999 };
        sink.send(WsMessage::Binary(bincode::serialize(&proto::Message::Presence(doctored)).unwrap().into())).await.expect("send");

        let mut saw_rejection = None;
        while let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            match msg {
                WsMessage::Binary(data) => {
                    if let Ok(proto::Message::PresenceRejected(r)) = bincode::deserialize::<proto::Message>(&data) {
                        saw_rejection = Some(r);
                        break;
                    }
                }
                WsMessage::Close(_) => break,
                _ => {}
            }
        }

        // INITIAL_BACKOFF is one second. A second TCP accept inside this
        // shorter window, after the rejection has reached us, would mean a
        // permanent refusal is hot-looping.
        let retried_immediately = tokio::time::timeout(Duration::from_millis(500), listener.accept()).await.is_ok();
        (saw_rejection, retried_immediately)
    });

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &format!("ws://{}", addr)).await?;

    // The client must refuse the server, so it never reaches Connected;
    // a timeout or a surfaced connection error are both refusal.
    match tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await {
        Ok(Ok(())) => panic!("client connected to an incompatible server"),
        Ok(Err(_)) | Err(_) => {}
    }

    let (rejection, retried_immediately) = fake_server.await?;
    let rejection = rejection.expect("client must send PresenceRejected before closing");
    assert_eq!(rejection, proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: 999 });
    assert!(!retried_immediately, "client retried an incompatible server without backoff");

    client.shutdown().await?;
    Ok(())
}
