//! The dialing side: a real client driven from a raw server.

use super::common::proto;
use super::*;
use ankurah::core::connector::PeerConnectionError;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::{ConnectionError, WebsocketClient};
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpStream;

/// A server played frame by frame against a real client: it reads the client's opening, then
/// sends `server_opening`, the bytes of its own.
async fn accept_client(listener: &tokio::net::TcpListener, server_opening: Vec<u8>) -> (WebSocketStream<TcpStream>, proto::Opening) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut socket = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
    let Some(proto::Message::Open(client_opening)) = receive(&mut socket).await else { panic!("the client opened otherwise") };
    socket.send(WsMessage::Binary(server_opening.into())).await.expect("send");
    (socket, client_opening)
}

/// The presence a real client sends on reading a server's opening, before the server presents.
async fn client_presence(socket: &mut WebSocketStream<TcpStream>) -> proto::Presence {
    let Some(proto::Message::Presence(presence)) = receive(socket).await else { panic!("the client did not present") };
    presence
}

/// The bytes of an opening from `server` offering `versions`.
fn opening_from(server: &NodeKey, versions: proto::ProtocolVersions) -> (proto::Opening, Vec<u8>) {
    let opening = proto::Opening { versions, challenge: proto::HandshakeChallenge::new(server.node_id()) };
    (opening, proto::Message::Open(opening).encode())
}

/// A real client connecting to a raw server on a fresh port.
async fn client_of_raw_server(
) -> Result<(tokio::net::TcpListener, Node<SledStorageEngine, PermissiveAgent>, WebsocketClient<SledStorageEngine, PermissiveAgent>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(node.clone(), &format!("ws://{}", listener.local_addr()?)).await?;
    Ok((listener, node, client))
}

#[tokio::test]
async fn the_client_refuses_a_server_presence_that_does_not_repeat_the_offers_and_the_chosen_version() -> Result<()> {
    let (listener, _node, client) = client_of_raw_server().await?;
    let server = NodeKey::generate();
    let (server_opening, server_frame) = opening_from(&server, SPOKEN);
    let (mut socket, client_opening) = accept_client(&listener, server_frame).await;
    assert_eq!(client_presence(&mut socket).await.claims.challenge, server_opening.challenge);
    let mut claims = claims_answering(&server, &client_opening, SPOKEN, true);
    claims.negotiation.chosen = 2;
    socket.send(frame(&proto::Message::Presence(server.sign_presence(claims)))).await?;

    let outcome = tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await?;
    assert_eq!(outcome, Err(ConnectionError::Peer(PeerConnectionError::OfferMismatch(server.node_id()))));
    client.shutdown().await?;
    Ok(())
}

/// The client connects to a server that names features and announces what it has no plugin for:
/// it answers a request sent after the server's presence.
#[tokio::test]
async fn the_client_ignores_unknown_features_and_announcements() -> Result<()> {
    let (listener, node, client) = client_of_raw_server().await?;
    let server = NodeKey::generate();
    let (server_opening, server_frame) = opening_from(&server, SPOKEN);
    let (mut socket, client_opening) = accept_client(&listener, server_frame).await;
    assert_eq!(client_presence(&mut socket).await.claims.challenge, server_opening.challenge);
    let mut claims = claims_answering(&server, &client_opening, SPOKEN, true);
    claims.features = vec![7, u32::MAX];
    claims.announcements.insert("a plugin the client does not have".into(), vec![1, 2, 3]);
    socket.send(frame(&proto::Message::Presence(server.sign_presence(claims)))).await?;
    tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await??;

    assert!(request_answered(&mut socket, server.node_id(), node.id).await.is_some(), "the client answers a request");
    assert!(client.is_connected());
    client.shutdown().await?;
    Ok(())
}

/// A message of a kind the client's version does not define closes the connection, whether it
/// arrives in place of the server's presence or once the client is connected.
#[tokio::test]
async fn the_client_closes_a_connection_whose_server_sends_a_message_of_a_kind_it_does_not_know() -> Result<()> {
    for unknown in UNKNOWN_KINDS {
        let server = NodeKey::generate();
        let (_, server_frame) = opening_from(&server, SPOKEN);
        let (listener, _node, client) = client_of_raw_server().await?;
        let (mut socket, _) = accept_client(&listener, server_frame.clone()).await;
        client_presence(&mut socket).await;
        socket.send(WsMessage::Binary(unknown.to_vec().into())).await?;
        assert!(receive(&mut socket).await.is_none(), "the client closes a handshake that sent {unknown:?}");
        client.shutdown().await?;

        let (listener, node, client) = client_of_raw_server().await?;
        let (mut socket, client_opening) = accept_client(&listener, server_frame).await;
        client_presence(&mut socket).await;
        let claims = claims_answering(&server, &client_opening, SPOKEN, true);
        socket.send(frame(&proto::Message::Presence(server.sign_presence(claims)))).await?;
        tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await??;
        socket.send(WsMessage::Binary(unknown.to_vec().into())).await?;
        assert!(request_answered(&mut socket, server.node_id(), node.id).await.is_none(), "{unknown:?}");
        assert!(!client.is_connected());
        client.shutdown().await?;
    }
    Ok(())
}

/// Application traffic before the server's presence is refused: nothing it says reaches the node,
/// and the client drops the connection rather than continue.
#[tokio::test]
async fn the_client_refuses_a_peer_message_before_the_servers_presence() -> Result<()> {
    let (listener, _node, client) = client_of_raw_server().await?;
    let server = NodeKey::generate();
    let (_, server_frame) = opening_from(&server, SPOKEN);
    let (mut socket, _) = accept_client(&listener, server_frame).await;
    client_presence(&mut socket).await;
    let early =
        proto::Message::PeerMessage(proto::NodeMessage::UnsubscribeQuery { from: server.node_id(), query_id: proto::QueryId::new() });
    socket.send(frame(&early)).await?;

    match tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await {
        Ok(Ok(())) => panic!("client treated a server that skipped its presence as connected"),
        Ok(Err(_)) | Err(_) => {}
    }
    assert!(receive(&mut socket).await.is_none(), "the client closes the connection");
    client.shutdown().await?;
    Ok(())
}

/// The client side of a refusal: a server speaking no version the client speaks is answered with
/// the client's versions, never treated as connected, and not dialed again before the backoff. A
/// version-1 reader rejects the bytes after the server's challenge, so the refusal shows the client
/// read no further than the versions.
#[tokio::test]
async fn the_client_refuses_a_server_sharing_no_version_without_a_hot_loop() -> Result<()> {
    let (listener, _node, client) = client_of_raw_server().await?;
    let (_, mut server_frame) = opening_from(&NodeKey::generate(), versions(2, 3));
    server_frame.extend_from_slice(b"bytes a version-1 reader rejects");
    let (mut socket, _) = accept_client(&listener, server_frame).await;

    let outcome = tokio::time::timeout(Duration::from_secs(3), client.wait_connected()).await?;
    assert_eq!(outcome, Err(ConnectionError::Peer(PeerConnectionError::NoCommonVersion { ours: SPOKEN, theirs: versions(2, 3) })));
    assert!(matches!(receive(&mut socket).await, Some(proto::Message::NoCommonVersion(spoken)) if spoken == SPOKEN));
    assert!(receive(&mut socket).await.is_none(), "then the client closes");

    // INITIAL_BACKOFF is one second. A second TCP accept inside this shorter window would mean a
    // permanent refusal is hot-looping.
    assert!(tokio::time::timeout(Duration::from_millis(500), listener.accept()).await.is_err(), "the client retried without backoff");
    client.shutdown().await?;
    Ok(())
}
