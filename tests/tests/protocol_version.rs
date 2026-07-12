//! #294: protocol version in the Presence handshake.
//!
//! Covers the refusal arms: register_peer (all transports), the websocket
//! server against version-0 and mismatched handshakes, and the websocket
//! client against a mismatched server.

use ankurah::core::connector::{PeerSender, SendError};
use ankurah::signals::Wait;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::{ConnectionState, WebsocketClient};
use anyhow::Result;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMessage;

mod common;
use common::{proto, start_test_server};

/// Literal protocol-v5 `Message::Presence` bytes, independent of the live
/// Rust shape. V5 used discriminant 0 and had no challenge.
const FROZEN_V5_PRESENCE_FRAME: [u8; 114] = {
    let mut frame = [0u8; 114];
    // 0..4: Message::Presence discriminant 0.
    let mut i = 4;
    while i < 36 {
        frame[i] = 0x25; // node_id
        i += 1;
    }
    // 36: durable=false; 37: system_root=None.
    frame[38] = 0x08;
    frame[39] = 0x07;
    frame[40] = 0x06;
    frame[41] = 0x05;
    frame[42] = 0x04;
    frame[43] = 0x03;
    frame[44] = 0x02;
    frame[45] = 0x01; // timestamp 0x0102030405060708, little-endian
    i = 46;
    while i < 110 {
        frame[i] = 0x5a; // signature
        i += 1;
    }
    frame[110] = 5; // protocol_version=5, little-endian u32
    frame
};

#[derive(Clone)]
struct NullSender(proto::NodeId);

#[async_trait::async_trait]
impl PeerSender for NullSender {
    fn send_message(&self, _message: proto::SignedPeerMessage) -> Result<(), SendError> { Ok(()) }
    fn recipient_node_id(&self) -> proto::NodeId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    fn close(&self) {}
}

fn peer_key() -> SigningKey { SigningKey::from_bytes(&[0x51; 32]) }

fn presence(protocol_version: u32, challenge: proto::HandshakeChallenge) -> proto::Presence {
    let key = peer_key();
    let claims = proto::PresenceClaims {
        node_id: (&key.verifying_key()).into(),
        durable: false,
        system_root: None,
        challenge,
        timestamp: 1_700_000_000_000,
        protocol_version,
    };
    let signature = key.sign(&proto::Presence::signable_bytes(&claims)).into();
    proto::Presence {
        node_id: claims.node_id,
        durable: claims.durable,
        system_root: None,
        challenge,
        timestamp: claims.timestamp,
        signature,
        protocol_version,
    }
}

fn signed_unsubscribe(key: &SigningKey, session: proto::HandshakeChallenge, sequence: u64, query_id: u64) -> proto::SignedPeerMessage {
    let from = proto::NodeId::from(key.verifying_key());
    let message = proto::NodeMessage::UnsubscribeQuery { from, query_id: proto::QueryId::test(query_id) };
    let signature = key.sign(&proto::SignedPeerMessage::signable_bytes(session, sequence, &message)).into();
    proto::SignedPeerMessage { session, sequence, message, signature }
}

/// The core enforcement point: register_peer refuses incompatible versions
/// for every transport, and accepts the current one.
#[tokio::test]
async fn register_peer_refuses_incompatible_version() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    for bad_version in [0, proto::PROTOCOL_VERSION + 1] {
        let handshake = node.begin_peer_handshake();
        let p = presence(bad_version, handshake.challenge());
        let peer_id = p.node_id;
        let outgoing_session = proto::HandshakeChallenge::new(peer_id, [bad_version as u8; 32]);
        let err = node.register_peer(p, handshake, outgoing_session, Box::new(NullSender(peer_id))).expect_err("must refuse");
        assert!(
            matches!(err, proto::PresenceRefusal::IncompatibleVersion(ref rejection)
                if *rejection == proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: bad_version }),
            "unexpected refusal: {err}"
        );
    }

    let handshake = node.begin_peer_handshake();
    let p = presence(proto::PROTOCOL_VERSION, handshake.challenge());
    let peer_id = p.node_id;
    let outgoing_session = proto::HandshakeChallenge::new(peer_id, [0x61; 32]);
    node.register_peer(p, handshake, outgoing_session, Box::new(NullSender(peer_id))).expect("current version must be accepted");
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
                if let Ok(message) = proto::decode_message(&data) {
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

    #[derive(Serialize)]
    struct LegacyPresence {
        node_id: [u8; 16],
        durable: bool,
        system_root: Option<()>,
    }
    // Presence was bincode enum variant zero in the legacy Message enum.
    // Append the exact 16-byte-node-id payload that the version-0 classifier
    // recognizes. The v6 server has already sent its challenge, but an old
    // peer cannot decode or answer it and is closed immediately.
    let mut old_shape = vec![0, 0, 0, 0];
    old_shape.extend(bincode::serialize(&LegacyPresence { node_id: [0x19; 16], durable: false, system_root: None })?);

    let received = exchange_until_close(&server_url, old_shape).await?;
    assert!(
        received.iter().any(|message| matches!(message, proto::Message::HandshakeChallenge(_))),
        "v6 server must lead with its challenge"
    );
    server_task.abort();
    Ok(())
}

/// The frozen v5 Presence cannot be decoded as v6's appended challenge, and
/// a live v6 server closes the old wire instead of parking the connection.
#[tokio::test]
async fn server_closes_frozen_v5_presence() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    #[derive(Serialize)]
    struct V5Presence {
        node_id: [u8; 32],
        durable: bool,
        system_root: Option<()>,
        timestamp: u64,
        signature: ([u8; 32], [u8; 32]),
        protocol_version: u32,
    }
    #[derive(Serialize)]
    enum V5Message {
        Presence(V5Presence),
    }
    let legacy = V5Message::Presence(V5Presence {
        node_id: [0x25; 32],
        durable: false,
        system_root: None,
        timestamp: 0x0102_0304_0506_0708,
        signature: ([0x5a; 32], [0x5a; 32]),
        protocol_version: 5,
    });
    assert_eq!(bincode::serialize(&legacy)?, FROZEN_V5_PRESENCE_FRAME, "fixture must remain the actual bincode-1.x v5 wire");
    assert!(proto::decode_message(&FROZEN_V5_PRESENCE_FRAME).is_err(), "v5 Presence must not decode as a complete v6 frame");

    let (_server_node, server_url, server_task) = start_test_server().await?;
    let received = exchange_until_close(&server_url, FROZEN_V5_PRESENCE_FRAME.to_vec()).await?;
    assert!(
        received.iter().any(|message| matches!(message, proto::Message::HandshakeChallenge(_))),
        "v6 server must issue its challenge before closing old wire"
    );
    server_task.abort();
    Ok(())
}

/// A versioned peer with the wrong version gets an explicit PresenceRejected
/// before the close.
#[tokio::test]
async fn server_refuses_mismatched_version() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let (server_node, server_url, server_task) = start_test_server().await?;

    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (mut sink, mut stream) = ws.split();

    // v6 is symmetric and challenge-first. Read the server's challenge, give
    // it our own challenge, then answer its challenge with a correctly signed
    // Presence that advertises the incompatible version.
    let server_challenge = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        Ok(Some(Ok(WsMessage::Binary(data)))) => match proto::decode_message(&data)? {
            proto::Message::HandshakeChallenge(challenge) => challenge,
            other => panic!("expected server challenge first, got {other:?}"),
        },
        other => panic!("server did not send its challenge first: {other:?}"),
    };
    assert_eq!(server_challenge.issuer(), server_node.id);

    let peer_id = proto::NodeId::from(peer_key().verifying_key());
    let peer_challenge = proto::HandshakeChallenge::new(peer_id, [0x62; 32]);
    sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::HandshakeChallenge(peer_challenge))?.into())).await?;
    sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::Presence(presence(999, server_challenge)))?.into())).await?;

    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(None) | Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(WsMessage::Binary(data)))) => {
                if let Ok(message) = proto::decode_message(&data) {
                    received.push(message);
                }
            }
            Ok(Some(Ok(_))) => continue,
            Err(_) => panic!("server did not close the connection; frames so far: {received:?}"),
        }
    }

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

/// The client side of the same handshake: a server speaking a different
/// version is refused, told why, and never treated as connected.
#[tokio::test]
async fn client_refuses_mismatched_server_without_hot_loop() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let expected_client_id = client_node.id;

    // Fake server: completes the symmetric challenge exchange, sends a
    // correctly challenge-bound but version-mismatched Presence, then records
    // whether the client explains itself before disconnecting and keeps
    // listening long enough to catch an immediate reconnect.
    let fake_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let ws = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
        let (mut sink, mut stream) = ws.split();

        let client_challenge = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(WsMessage::Binary(data)))) => match proto::decode_message(&data).expect("decode challenge") {
                proto::Message::HandshakeChallenge(challenge) => challenge,
                other => panic!("expected client challenge first, got {other:?}"),
            },
            other => panic!("client did not send its challenge first: {other:?}"),
        };
        assert_eq!(client_challenge.issuer(), expected_client_id);

        let fake_server_id = proto::NodeId::from(peer_key().verifying_key());
        let server_challenge = proto::HandshakeChallenge::new(fake_server_id, [0x63; 32]);
        sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::HandshakeChallenge(server_challenge)).unwrap().into()))
            .await
            .expect("send challenge");
        let doctored = presence(999, client_challenge);
        sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::Presence(doctored)).unwrap().into()))
            .await
            .expect("send presence");

        let mut saw_rejection = None;
        while let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            match msg {
                WsMessage::Binary(data) => {
                    if let Ok(proto::Message::PresenceRejected(r)) = proto::decode_message(&data) {
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

    let client = WebsocketClient::new(client_node.clone(), &format!("ws://{}", addr)).await?;

    // Refusal is a real connection error (and therefore enters backoff), not
    // a normal close that resets backoff and busy-retries.
    let refusal = tokio::time::timeout(Duration::from_secs(3), client.wait_connected())
        .await
        .expect("client must surface refusal without timing out")
        .expect_err("client connected to an incompatible server");
    assert!(refusal.to_string().contains("incompatible protocol version"), "unexpected refusal: {refusal}");

    let (rejection, retried_immediately) = fake_server.await?;
    let rejection = rejection.expect("client must send PresenceRejected before closing");
    assert_eq!(rejection, proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: 999 });
    assert!(!retried_immediately, "client retried an incompatible server without backoff");

    client.shutdown().await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum EstablishedClientFault {
    InvalidSignature,
    TrailingBytes,
    TextFrame,
    RemoteClose,
    AbruptEof,
}

/// Establish an authenticated client session against a raw server, inject a
/// protocol or transport fault, and prove the client reports an error and
/// observes reconnect backoff instead of treating it as a normal completion.
async fn assert_established_client_fault_enters_backoff(fault: EstablishedClientFault) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let expected_client_id = client_node.id;
    let (established_tx, established_rx) = tokio::sync::oneshot::channel();
    let (inject_tx, inject_rx) = tokio::sync::oneshot::channel();

    let fake_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept initial connection");
        let ws = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
        let (mut sink, mut stream) = ws.split();

        let client_challenge = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(WsMessage::Binary(data)))) => match proto::decode_message(&data).expect("decode client challenge") {
                proto::Message::HandshakeChallenge(challenge) => challenge,
                other => panic!("expected client challenge first, got {other:?}"),
            },
            other => panic!("client did not send challenge: {other:?}"),
        };
        assert_eq!(client_challenge.issuer(), expected_client_id);

        let server_key = peer_key();
        let server_id = proto::NodeId::from(server_key.verifying_key());
        let server_challenge = proto::HandshakeChallenge::new(server_id, [0x73; 32]);
        sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::HandshakeChallenge(server_challenge)).unwrap().into()))
            .await
            .expect("send server challenge");
        sink.send(WsMessage::Binary(
            proto::encode_message(&proto::Message::Presence(presence(proto::PROTOCOL_VERSION, client_challenge))).unwrap().into(),
        ))
        .await
        .expect("send server Presence");

        // Receiving the client's challenge-bound Presence proves both sides
        // completed the wire handshake before the test injects a bad frame.
        loop {
            match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
                Ok(Some(Ok(WsMessage::Binary(data)))) => {
                    if matches!(proto::decode_message(&data), Ok(proto::Message::Presence(_))) {
                        break;
                    }
                }
                other => panic!("client did not send Presence: {other:?}"),
            }
        }
        established_tx.send(()).expect("test still waiting for establishment");
        inject_rx.await.expect("test still waiting to inject fault");

        match fault {
            EstablishedClientFault::InvalidSignature => {
                let message = proto::NodeMessage::UnsubscribeQuery { from: server_id, query_id: proto::QueryId::test(0xBAD) };
                let rogue_key = SigningKey::from_bytes(&[0x74; 32]);
                let signature = rogue_key.sign(&proto::SignedPeerMessage::signable_bytes(client_challenge, 0, &message)).into();
                let frame = proto::SignedPeerMessage { session: client_challenge, sequence: 0, message, signature };
                sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::PeerMessage(frame)).unwrap().into()))
                    .await
                    .expect("send bad signed frame");
            }
            EstablishedClientFault::TrailingBytes => {
                // This is otherwise a valid established frame. A permissive
                // decoder would authenticate and dispatch it, so only exact
                // framing can make the test connection fail closed.
                let valid = signed_unsubscribe(&server_key, client_challenge, 0, 0xBAD1);
                let mut frame = proto::encode_message(&proto::Message::PeerMessage(valid)).unwrap();
                frame.push(0);
                sink.send(WsMessage::Binary(frame.into())).await.expect("send non-exact frame");
            }
            EstablishedClientFault::TextFrame => {
                sink.send(WsMessage::Text("not an Ankurah frame".into())).await.expect("send text frame");
            }
            EstablishedClientFault::RemoteClose => {
                sink.send(WsMessage::Close(None)).await.expect("send remote close");
            }
            EstablishedClientFault::AbruptEof => {
                // Dropping both halves without a websocket Close frame makes
                // the authenticated TCP stream end abruptly.
                drop(sink);
                drop(stream);
                return tokio::time::timeout(Duration::from_millis(500), listener.accept()).await.is_ok();
            }
        }

        // Let the client observe the violation and tear down the first socket.
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(message) = stream.next().await {
                if matches!(message, Ok(WsMessage::Close(_)) | Err(_)) {
                    break;
                }
            }
        })
        .await;

        // INITIAL_BACKOFF is one second. A new TCP connection in this shorter
        // window demonstrates a normal-close hot reconnect.
        tokio::time::timeout(Duration::from_millis(500), listener.accept()).await.is_ok()
    });

    let client = WebsocketClient::new(client_node, &format!("ws://{addr}")).await?;
    tokio::time::timeout(Duration::from_secs(3), established_rx).await??;
    tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await??;
    inject_tx.send(()).expect("fake server still waiting to inject fault");

    let error = tokio::time::timeout(
        Duration::from_secs(3),
        client.state().wait_for(|state| match state {
            ConnectionState::Error(error) => Some(error.clone()),
            _ => None,
        }),
    )
    .await
    .expect("client did not surface established connection fault");
    assert!(!error.to_string().is_empty());
    assert!(!fake_server.await?, "client hot-reconnected after established connection fault");

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn client_authenticated_signature_violation_enters_backoff() -> Result<()> {
    assert_established_client_fault_enters_backoff(EstablishedClientFault::InvalidSignature).await
}

#[tokio::test]
async fn client_established_non_exact_frame_enters_backoff() -> Result<()> {
    assert_established_client_fault_enters_backoff(EstablishedClientFault::TrailingBytes).await
}

#[tokio::test]
async fn client_established_text_frame_enters_backoff() -> Result<()> {
    assert_established_client_fault_enters_backoff(EstablishedClientFault::TextFrame).await
}

#[tokio::test]
async fn client_established_remote_close_enters_backoff() -> Result<()> {
    assert_established_client_fault_enters_backoff(EstablishedClientFault::RemoteClose).await
}

#[tokio::test]
async fn client_established_abrupt_eof_enters_backoff() -> Result<()> {
    assert_established_client_fault_enters_backoff(EstablishedClientFault::AbruptEof).await
}

#[tokio::test]
async fn client_shutdown_and_drop_deregister_authenticated_sessions() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;

    let explicit_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let explicit_client = WebsocketClient::new(explicit_node.clone(), &server_url).await?;
    tokio::time::timeout(Duration::from_secs(3), explicit_client.wait_connected()).await??;
    tokio::time::timeout(Duration::from_secs(3), explicit_node.system.wait_system_ready()).await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !explicit_node.get_durable_peers().contains(&server_node.id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server peer did not become ready");

    explicit_client.shutdown().await?;
    assert!(!explicit_node.get_durable_peers().contains(&server_node.id), "shutdown leaked its authenticated peer registration");

    let dropped_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let dropped_client = WebsocketClient::new(dropped_node.clone(), &server_url).await?;
    tokio::time::timeout(Duration::from_secs(3), dropped_client.wait_connected()).await??;
    tokio::time::timeout(Duration::from_secs(3), dropped_node.system.wait_system_ready()).await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dropped_node.get_durable_peers().contains(&server_node.id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server peer did not become ready");

    drop(dropped_client);
    tokio::time::timeout(Duration::from_secs(3), async {
        while dropped_node.get_durable_peers().contains(&server_node.id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping client leaked its authenticated peer registration");

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn client_hard_reset_closes_transport_without_rejoining_old_server() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await??;
    tokio::time::timeout(Duration::from_secs(3), client_node.system.wait_system_ready()).await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client_node.get_durable_peers().contains(&server_node.id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server peer did not become ready");

    client_node.system.hard_reset().await?;
    tokio::time::timeout(
        Duration::from_secs(3),
        client.state().wait_for(|state| matches!(state, ConnectionState::Disconnected).then_some(())),
    )
    .await
    .expect("reset-driven sender close did not terminate the websocket connector");

    // Waiting past the ordinary remote-close retry delay proves that a local
    // reset does not reconnect to and immediately rejoin the old founder.
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    assert!(!client.is_connected());
    assert!(!client_node.system.is_system_ready());
    assert!(client_node.system.root().is_none());
    assert!(client_node.get_durable_peers().is_empty());

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn server_hard_reset_physically_closes_remote_transport() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &server_url).await?;
    tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await??;
    tokio::time::timeout(Duration::from_secs(3), client_node.system.wait_system_ready()).await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client_node.get_durable_peers().contains(&server_node.id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server peer did not become ready");

    server_node.system.hard_reset().await?;
    tokio::time::timeout(Duration::from_secs(3), client.state().wait_for(|state| matches!(state, ConnectionState::Error(_)).then_some(())))
        .await
        .expect("server reset did not physically close the client's websocket");
    assert!(!client.is_connected());
    assert!(client_node.get_durable_peers().is_empty(), "remote reset left the old founder routable");

    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn server_closes_non_exact_frame_after_establishment() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let (ws, _) = tokio_tungstenite::connect_async(format!("{server_url}/ws")).await?;
    let (mut sink, mut stream) = ws.split();

    let server_challenge = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        Ok(Some(Ok(WsMessage::Binary(data)))) => match proto::decode_message(&data)? {
            proto::Message::HandshakeChallenge(challenge) => challenge,
            other => panic!("expected server challenge first, got {other:?}"),
        },
        other => panic!("server did not send challenge: {other:?}"),
    };
    assert_eq!(server_challenge.issuer(), server_node.id);

    let client_key = peer_key();
    let client_id = proto::NodeId::from(client_key.verifying_key());
    let client_challenge = proto::HandshakeChallenge::new(client_id, [0x75; 32]);
    sink.send(WsMessage::Binary(proto::encode_message(&proto::Message::HandshakeChallenge(client_challenge))?.into())).await?;

    // The server answers our challenge before it authenticates our Presence.
    loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(WsMessage::Binary(data)))) if matches!(proto::decode_message(&data), Ok(proto::Message::Presence(_))) => break,
            Ok(Some(Ok(_))) => continue,
            other => panic!("server did not answer challenge with Presence: {other:?}"),
        }
    }
    sink.send(WsMessage::Binary(
        proto::encode_message(&proto::Message::Presence(presence(proto::PROTOCOL_VERSION, server_challenge)))?.into(),
    ))
    .await?;

    // Websocket messages are processed sequentially; this follows the valid
    // Presence and therefore reaches the established decode path.
    let valid = signed_unsubscribe(&client_key, server_challenge, 0, 0xBAD2);
    let mut malformed = proto::encode_message(&proto::Message::PeerMessage(valid))?;
    malformed.push(0);
    sink.send(WsMessage::Binary(malformed.into())).await?;

    loop {
        match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
            Ok(None) | Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(_))) => continue,
            Err(_) => panic!("server did not close after non-exact established frame"),
        }
    }
    server_task.abort();
    Ok(())
}
