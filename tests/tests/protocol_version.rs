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
