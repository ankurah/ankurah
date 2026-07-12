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
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMessage;

mod common;
use common::{proto, start_test_server};

/// Last protocol epoch before Phase 2 added generation to EventId hashing,
/// EventFragment, and State head metadata.
const PRE_D2_PROTOCOL_VERSION: u32 = 3;

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

/// Phase 2 changes both wire and persisted identity. A peer from the prior
/// epoch must be rejected at the common register_peer enforcement point.
#[tokio::test]
async fn register_peer_refuses_pre_d2_protocol_version() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let p = presence(PRE_D2_PROTOCOL_VERSION);
    let peer_id = p.node_id;

    let err = node.register_peer(p, Box::new(NullSender(peer_id))).expect_err("pre-D2 protocol must be refused");
    assert_eq!(err, proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: PRE_D2_PROTOCOL_VERSION });
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

/// Run a server that presents the same rejected handshake on every accepted
/// socket. The client must surface the refusal and leave the next attempt behind
/// its one-second initial backoff rather than spinning on a clean `Break`.
async fn assert_client_refusal_is_backed_off(
    server_frame: Vec<u8>,
    expected_rejection: Option<proto::PresenceRejection>,
    error_fragment: &str,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let accepts = Arc::new(AtomicUsize::new(0));
    let observed_rejection = Arc::new(Mutex::new(None));

    let server_accepts = accepts.clone();
    let server_rejection = observed_rejection.clone();
    let fake_server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            server_accepts.fetch_add(1, Ordering::SeqCst);
            let frame = server_frame.clone();
            let observed = server_rejection.clone();
            tokio::spawn(async move {
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else { return };
                let (mut sink, mut stream) = ws.split();
                if sink.send(WsMessage::Binary(frame.into())).await.is_err() {
                    return;
                }

                loop {
                    match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                        Ok(Some(Ok(WsMessage::Binary(data)))) => {
                            if let Ok(proto::Message::PresenceRejected(rejection)) = bincode::deserialize::<proto::Message>(&data) {
                                *observed.lock().unwrap() = Some(rejection);
                            }
                        }
                        Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
                        Ok(Some(Ok(_))) => {}
                    }
                }
            });
        }
    });

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node.clone(), &format!("ws://{}", addr)).await?;

    let error = tokio::time::timeout(Duration::from_secs(2), client.wait_connected())
        .await
        .expect("a rejected handshake must surface an error, not wait forever")
        .expect_err("client connected to a rejected server handshake");
    assert!(error.to_string().contains(error_fragment), "unexpected connection error: {error}");

    // INITIAL_BACKOFF is one second. Once the error is visible, a 250 ms
    // observation window must contain no second accept.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(accepts.load(Ordering::SeqCst), 1, "rejected handshakes must not reconnect without backoff");

    assert_eq!(*observed_rejection.lock().unwrap(), expected_rejection);

    client.shutdown().await?;
    fake_server.abort();
    Ok(())
}

/// A versioned server speaking a different epoch is refused, told why, and
/// retried only after backoff.
#[tokio::test]
async fn client_refuses_mismatched_server_with_backoff() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let presence = proto::Presence { node_id: proto::EntityId::new(), durable: true, system_root: None, protocol_version: 999 };
    let frame = bincode::serialize(&proto::Message::Presence(presence))?;
    let rejection = proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: 999 };

    assert_client_refusal_is_backed_off(frame, Some(rejection), "incompatible protocol version").await
}

/// A pre-versioning server Presence cannot carry an explicit epoch, but its
/// undecodable handshake must still surface an error and enter backoff.
#[tokio::test]
async fn client_refuses_version0_server_with_backoff() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).try_init();
    let mut frame = bincode::serialize(&proto::Message::Presence(presence(proto::PROTOCOL_VERSION)))?;
    frame.truncate(frame.len() - 4);

    assert_client_refusal_is_backed_off(frame, None, "pre-versioning").await
}

/// A clean close before presence is still a failed connection attempt. It
/// must surface Error and enter the same backoff as handshake refusals.
#[tokio::test]
async fn client_clean_close_is_backed_off() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let accepts = Arc::new(AtomicUsize::new(0));
    let server_accepts = accepts.clone();
    let fake_server = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            server_accepts.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else { return };
                let _ = ws.close(None).await;
            });
        }
    });

    let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(client_node, &format!("ws://{}", addr)).await?;
    let error = tokio::time::timeout(Duration::from_secs(2), client.wait_connected())
        .await
        .expect("clean close must surface an error")
        .expect_err("clean close cannot establish the connection");
    assert!(error.to_string().contains("closed"), "unexpected connection error: {error}");

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(accepts.load(Ordering::SeqCst), 1, "clean closes must not reconnect without backoff");

    client.shutdown().await?;
    fake_server.abort();
    Ok(())
}

/// A legacy browser client waits for the server Presence before sending its
/// own. A durable v3 Presence is undecodable by that client, so it remains
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
    assert!(server_presence.system_root.is_some(), "durable Presence must carry the v3 system-root state");

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
