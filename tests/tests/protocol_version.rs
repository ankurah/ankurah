//! #294: protocol version in the Presence handshake.
//!
//! Covers the refusal arms: register_peer (all transports), the websocket
//! server against unreadable and mismatched handshakes, and the websocket
//! client against a mismatched server.

use ankurah::core::connector::{PeerConnectionError, PeerProvenance, PeerSender, SendError};
use ankurah::core::node::NodeKey;
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
struct NullSender(proto::NodeId);

#[async_trait::async_trait]
impl PeerSender for NullSender {
    fn send_message(&self, _message: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> proto::NodeId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// A presence advertising `protocol_version`, signed by a fresh key for some
/// other node's challenge. A version other than the current one breaks the
/// signature, which the version check never reaches.
fn presence(protocol_version: u32) -> proto::Presence {
    let mut presence = NodeKey::generate().sign_presence(false, proto::HandshakeChallenge::new(NodeKey::generate().node_id()));
    presence.protocol_version = protocol_version;
    presence
}

/// The core enforcement point: register_peer refuses incompatible versions
/// for every transport, and accepts the current one.
#[tokio::test]
async fn register_peer_refuses_incompatible_version() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let key = NodeKey::generate();

    for bad_version in [0, proto::PROTOCOL_VERSION + 1] {
        let challenge = node.issue_challenge();
        let mut p = key.sign_presence(false, challenge.challenge());
        p.protocol_version = bad_version;
        let err =
            node.register_peer(p, challenge, PeerProvenance::Inbound, Box::new(NullSender(key.node_id()))).await.expect_err("must refuse");
        assert_eq!(
            err,
            PeerConnectionError::Protocol(proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: bad_version })
        );
    }

    let challenge = node.issue_challenge();
    let p = key.sign_presence(false, challenge.challenge());
    node.register_peer(p, challenge, PeerProvenance::Inbound, Box::new(NullSender(key.node_id())))
        .await
        .expect("current version must be accepted");
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

/// An unreadable handshake closes the connection instead of leaving it dangling.
#[tokio::test]
async fn server_refuses_unreadable_handshake() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let (_server_node, server_url, server_task) = start_test_server().await?;

    let mut frame = bincode::serialize(&proto::Message::Presence(presence(proto::PROTOCOL_VERSION)))?;
    frame.pop();

    exchange_until_close(&server_url, frame).await?;
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

/// A client that cannot decode the server's opening challenge (a legacy
/// client expects a Presence first) remains silent; the server must still
/// bound the incomplete handshake.
#[tokio::test(start_paused = true)]
async fn server_closes_silent_client_after_its_challenge() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (_sink, mut stream) = ws.split();

    let challenge = loop {
        match stream.next().await {
            Some(Ok(WsMessage::Binary(data))) => match bincode::deserialize::<proto::Message>(&data)? {
                proto::Message::Challenge(challenge) => break challenge,
                other => panic!("server sent {other:?} before its challenge"),
            },
            Some(Ok(other)) => panic!("server sent {other:?} before its challenge"),
            Some(Err(e)) => return Err(e.into()),
            None => panic!("server closed before sending its challenge"),
        }
    };
    assert_eq!(challenge.issuer(), server_node.id, "the server opens with its own challenge");

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

/// The server answers one challenge per connection: a second challenge before
/// the client's presence closes the connection.
#[tokio::test]
async fn server_closes_a_connection_that_sends_a_second_challenge() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (mut sink, mut stream) = ws.split();
    let challenge = proto::Message::Challenge(proto::HandshakeChallenge::new(NodeKey::generate().node_id()));
    sink.send(WsMessage::Binary(bincode::serialize(&challenge)?.into())).await?;
    sink.send(WsMessage::Binary(bincode::serialize(&challenge)?.into())).await?;

    let mut presences = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(None) | Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(WsMessage::Binary(data)))) => {
                if let Ok(proto::Message::Presence(_)) = bincode::deserialize::<proto::Message>(&data) {
                    presences += 1;
                }
            }
            Ok(Some(Ok(_))) => continue,
            Err(_) => panic!("server left the connection open after a second challenge"),
        }
    }
    assert_eq!(presences, 1, "the server answers one challenge per connection");

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
            from: NodeKey::generate().node_id(),
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
                    // The client's own challenge and Presence (and a possible
                    // rejection notice) are handshake chatter, not session traffic.
                    match bincode::deserialize::<proto::Message>(&data) {
                        Ok(proto::Message::Challenge(_)) | Ok(proto::Message::Presence(_)) | Ok(proto::Message::PresenceRejected(_)) => {
                            continue
                        }
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

    // Fake server: opens with its challenge, then sends a doctored Presence,
    // records whether the client explains itself with a PresenceRejected, and
    // keeps listening long enough to catch an immediate reconnect.
    let fake_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
        let (mut sink, mut stream) = ws.split();

        let key = NodeKey::generate();
        let challenge = proto::Message::Challenge(proto::HandshakeChallenge::new(key.node_id()));
        sink.send(WsMessage::Binary(bincode::serialize(&challenge).unwrap().into())).await.expect("send");
        let doctored = presence(999);
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
