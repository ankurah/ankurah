//! The connection handshake through the websocket connectors: each side opens with the versions
//! it speaks and its challenge, answers the other's opening with a signed presence, and refuses
//! what it cannot take; once connected, it ignores a message of a kind it does not know. Each test
//! drives a real server from a raw socket, or a real client from a raw server.

use ankurah::core::connector::PeerConnectionError;
use ankurah::core::node::NodeKey;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_websocket_client::{ConnectionError, WebsocketClient};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream};

mod common;
use common::{proto, start_test_server};

fn versions(lowest: u32, highest: u32) -> proto::ProtocolVersions { proto::ProtocolVersions { lowest, highest } }

const SPOKEN: proto::ProtocolVersions = proto::ProtocolVersions::SPOKEN;

/// The frame bytes of `message`.
fn frame(message: &proto::Message) -> WsMessage { WsMessage::Binary(message.encode().into()) }

/// What a peer holding only `key` claims answering `opening`, after offering `offered`.
fn claims_answering(key: &NodeKey, opening: &proto::Opening, offered: proto::ProtocolVersions, durable: bool) -> proto::PresenceClaims {
    proto::PresenceClaims {
        node_id: key.node_id(),
        durable,
        challenge: opening.challenge,
        timestamp: 0,
        negotiation: proto::VersionNegotiation {
            offered,
            peer_offered: opening.versions,
            chosen: offered.highest_shared(opening.versions).expect("the two offers share a version"),
        },
        features: vec![],
        announcements: BTreeMap::new(),
    }
}

/// The next frame `socket` receives, decoded; `None` once the other side closes. Panics after ten seconds.
async fn receive<S>(socket: &mut WebSocketStream<S>) -> Option<proto::Message>
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), socket.next()).await.expect("the other side neither sent nor closed") {
            None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => return None,
            Some(Ok(WsMessage::Binary(data))) => return Some(proto::Message::decode(&data).expect("a frame this build reads")),
            Some(Ok(_)) => continue,
        }
    }
}

/// Send `peer` a request from `from` over `socket`, and read frames until its response arrives;
/// `None` if the connection closes first. A connected node sends requests of its own meanwhile.
async fn request_answered<S>(socket: &mut WebSocketStream<S>, from: proto::NodeId, peer: proto::NodeId) -> Option<proto::NodeResponse>
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
    let body = proto::CarriedBody::encode(&proto::NodeRequestBody::Get { ids: vec![] });
    let request = proto::NodeRequest { id: proto::RequestId::new(), to: peer, from, body };
    let request_id = request.id.clone();
    socket.send(frame(&proto::Message::PeerMessage(proto::NodeMessage::Request { auth: vec![Default::default()], request }))).await.ok()?;
    loop {
        if let proto::Message::PeerMessage(proto::NodeMessage::Response(response)) = receive(socket).await? {
            if response.request_id == request_id {
                return Some(response);
            }
        }
    }
}

/// Wait up to five seconds for `condition`.
async fn eventually(condition: impl Fn() -> bool) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

/// A client holding only a key, speaking the handshake frame by frame against a real server.
struct RawClient {
    key: NodeKey,
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    offered: proto::ProtocolVersions,
    server_opening: proto::Opening,
    server_presence: proto::Presence,
}

impl RawClient {
    /// Connect to `server_url`, open offering `offered`, and read the server's opening and presence.
    async fn open(server_url: &str, offered: proto::ProtocolVersions) -> Result<Self> {
        let key = NodeKey::generate();
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("{server_url}/ws")).await?;
        let opening = proto::Opening { versions: offered, challenge: proto::HandshakeChallenge::new(key.node_id()) };
        socket.send(frame(&proto::Message::Open(opening))).await?;
        let Some(proto::Message::Open(server_opening)) = receive(&mut socket).await else { anyhow::bail!("the server opened otherwise") };
        let Some(proto::Message::Presence(server_presence)) = receive(&mut socket).await else {
            anyhow::bail!("the server answered the opening otherwise")
        };
        anyhow::ensure!(server_presence.claims.challenge == opening.challenge, "the server's presence answers our challenge");
        Ok(Self { key, socket, offered, server_opening, server_presence })
    }

    /// Answer the server with a presence making what `change` makes of the claims an honest client makes.
    async fn present(&mut self, change: impl FnOnce(&mut proto::PresenceClaims)) -> Result<()> {
        let mut claims = claims_answering(&self.key, &self.server_opening, self.offered, false);
        change(&mut claims);
        self.socket.send(frame(&proto::Message::Presence(self.key.sign_presence(claims)))).await?;
        Ok(())
    }
}

#[tokio::test]
async fn a_wider_opening_settles_on_the_version_the_server_speaks() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, versions(1, 3)).await?;
    assert_eq!(client.server_opening.versions, SPOKEN);
    assert!(client.server_presence.signature_is_valid());
    assert_eq!(
        client.server_presence.claims.negotiation,
        proto::VersionNegotiation { offered: SPOKEN, peer_offered: versions(1, 3), chosen: 1 }
    );

    client.present(|_| {}).await?;
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_some()).await, "the server registers the client");
    server_task.abort();
    Ok(())
}

/// A version-1 reader rejects the bytes after this opening's challenge, so the refusal shows the
/// server read no further than the versions.
#[tokio::test]
async fn an_opening_sharing_no_version_is_refused_with_the_servers_versions_before_the_rest_is_read() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("{server_url}/ws")).await?;
    let opening = proto::Opening { versions: versions(2, 3), challenge: proto::HandshakeChallenge::new(NodeKey::generate().node_id()) };
    let mut bytes = proto::Message::Open(opening).encode();
    bytes.extend_from_slice(b"bytes a version-1 reader rejects");
    socket.send(WsMessage::Binary(bytes.into())).await?;

    assert!(matches!(receive(&mut socket).await, Some(proto::Message::Open(_))), "the server opens first");
    assert!(
        matches!(receive(&mut socket).await, Some(proto::Message::NoCommonVersion(spoken)) if spoken == SPOKEN),
        "the server answers with the versions it speaks"
    );
    assert!(receive(&mut socket).await.is_none(), "then closes");
    server_task.abort();
    Ok(())
}

/// A party between the two sides can change the openings, which nothing signs; the client then
/// signs the offers it saw, which differ from the ones the server sent or the version it chose.
#[tokio::test]
async fn the_server_refuses_a_presence_that_does_not_repeat_the_offers_and_the_chosen_version() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let alterations: [fn(&mut proto::VersionNegotiation); 2] =
        [|negotiation| negotiation.peer_offered = versions(1, 3), |negotiation| negotiation.chosen = 2];
    for alter in alterations {
        let mut client = RawClient::open(&server_url, SPOKEN).await?;
        client.present(|claims| alter(&mut claims.negotiation)).await?;
        assert!(receive(&mut client.socket).await.is_none(), "the server closes the connection");
        assert_eq!(server_node.granted_trust(&client.key.node_id()), None);
    }
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn the_server_ignores_unknown_feature_numbers_and_announcement_names() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client
        .present(|claims| {
            claims.features = vec![7, u32::MAX];
            claims.announcements.insert("a plugin the server does not have".into(), vec![1, 2, 3]);
        })
        .await?;
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_some()).await, "the server registers the client");
    server_task.abort();
    Ok(())
}

/// A later build may send a message, or a peer message, of a kind this build does not know.
#[tokio::test]
async fn the_server_ignores_a_message_of_an_unknown_kind_and_keeps_the_connection() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.present(|_| {}).await?;
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_some()).await);

    // Variant 99 of `Message`, then variant 99 of `NodeMessage` inside a peer message.
    client.socket.send(WsMessage::Binary(vec![99, 0, 0, 0].into())).await?;
    client.socket.send(WsMessage::Binary(vec![3, 0, 0, 0, 99, 0, 0, 0].into())).await?;
    assert!(request_answered(&mut client.socket, client.key.node_id(), server_node.id).await.is_some(), "the server answers a request");
    server_task.abort();
    Ok(())
}

/// The request's frame decodes around a body of a kind the server does not know, so the server
/// can address its answer.
#[tokio::test]
async fn the_server_answers_a_request_whose_body_it_cannot_read_with_unsupported() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.present(|_| {}).await?;
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_some()).await);

    // Variant 99 of the request body, carried as its bytes.
    let body = bincode::deserialize(&bincode::serialize(&vec![99u8, 0, 0, 0])?)?;
    let request = proto::NodeRequest { id: proto::RequestId::new(), to: server_node.id, from: client.key.node_id(), body };
    let request_id = request.id.clone();
    client
        .socket
        .send(frame(&proto::Message::PeerMessage(proto::NodeMessage::Request { auth: vec![Default::default()], request })))
        .await?;
    let response = loop {
        match receive(&mut client.socket).await {
            Some(proto::Message::PeerMessage(proto::NodeMessage::Response(response))) if response.request_id == request_id => {
                break response
            }
            Some(_) => continue,
            None => panic!("the server closed instead of answering"),
        }
    };
    assert!(matches!(response.body.decode(), Ok(proto::NodeResponseBody::Unsupported)), "{:?}", response.body);
    server_task.abort();
    Ok(())
}

/// An unreadable opening closes the connection instead of leaving it dangling.
#[tokio::test]
async fn the_server_closes_a_connection_whose_opening_it_cannot_read() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("{server_url}/ws")).await?;
    let opening = proto::Opening { versions: SPOKEN, challenge: proto::HandshakeChallenge::new(NodeKey::generate().node_id()) };
    let mut bytes = proto::Message::Open(opening).encode();
    bytes.pop();
    socket.send(WsMessage::Binary(bytes.into())).await?;

    assert!(matches!(receive(&mut socket).await, Some(proto::Message::Open(_))));
    assert!(receive(&mut socket).await.is_none(), "the server closes without answering");
    server_task.abort();
    Ok(())
}

/// A client that cannot read the server's opening (a legacy client expects a Presence first)
/// stays silent; the server must still bound the incomplete handshake.
#[tokio::test(start_paused = true)]
async fn the_server_closes_a_silent_client_after_its_opening() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let (ws, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let (_sink, mut stream) = ws.split();

    let opening = loop {
        match stream.next().await {
            Some(Ok(WsMessage::Binary(data))) => match proto::Message::decode(&data)? {
                proto::Message::Open(opening) => break opening,
                other => panic!("server sent {other:?} before its opening"),
            },
            Some(Ok(other)) => panic!("server sent {other:?} before its opening"),
            Some(Err(e)) => return Err(e.into()),
            None => panic!("server closed before sending its opening"),
        }
    };
    assert_eq!(opening.challenge.issuer(), server_node.id, "the server opens with its own challenge");

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

/// The server answers one opening per connection: a second opening before the client's presence
/// closes the connection.
#[tokio::test]
async fn the_server_closes_a_connection_that_sends_a_second_opening() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("{}/ws", server_url)).await?;
    let opening = proto::Opening { versions: SPOKEN, challenge: proto::HandshakeChallenge::new(NodeKey::generate().node_id()) };
    socket.send(frame(&proto::Message::Open(opening))).await?;
    socket.send(frame(&proto::Message::Open(opening))).await?;

    let mut presences = 0;
    while let Some(message) = receive(&mut socket).await {
        if let proto::Message::Presence(_) = message {
            presences += 1;
        }
    }
    assert_eq!(presences, 1, "the server answers one opening per connection");

    server_task.abort();
    Ok(())
}

/// A server played frame by frame against a real client: it reads the client's opening, then
/// sends `server_opening`, the bytes of its own.
async fn accept_client(listener: &tokio::net::TcpListener, server_opening: Vec<u8>) -> (WebSocketStream<TcpStream>, proto::Opening) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut socket = tokio_tungstenite::accept_async(stream).await.expect("ws accept");
    let Some(proto::Message::Open(client_opening)) = receive(&mut socket).await else { panic!("the client opened otherwise") };
    socket.send(WsMessage::Binary(server_opening.into())).await.expect("send");
    (socket, client_opening)
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
    let mut claims = claims_answering(&server, &client_opening, SPOKEN, true);
    claims.negotiation.chosen = 2;
    socket.send(frame(&proto::Message::Presence(server.sign_presence(claims)))).await?;

    let outcome = tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await?;
    assert_eq!(outcome, Err(ConnectionError::Peer(PeerConnectionError::OfferMismatch(server.node_id()))));
    client.shutdown().await?;
    Ok(())
}

/// The client connects to a server that names features and announces what it has no plugin for,
/// and stays connected through messages of kinds it does not know: it answers a request sent after them.
#[tokio::test]
async fn the_client_ignores_unknown_features_announcements_and_message_kinds() -> Result<()> {
    let (listener, node, client) = client_of_raw_server().await?;
    let server = NodeKey::generate();
    let (server_opening, server_frame) = opening_from(&server, SPOKEN);
    let (mut socket, client_opening) = accept_client(&listener, server_frame).await;
    let mut claims = claims_answering(&server, &client_opening, SPOKEN, true);
    claims.features = vec![7, u32::MAX];
    claims.announcements.insert("a plugin the client does not have".into(), vec![1, 2, 3]);
    socket.send(frame(&proto::Message::Presence(server.sign_presence(claims)))).await?;
    tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await??;
    let Some(proto::Message::Presence(client_presence)) = receive(&mut socket).await else { panic!("the client did not present") };
    assert_eq!(client_presence.claims.challenge, server_opening.challenge);

    socket.send(WsMessage::Binary(vec![99, 0, 0, 0].into())).await?;
    socket.send(WsMessage::Binary(vec![3, 0, 0, 0, 99, 0, 0, 0].into())).await?;
    assert!(request_answered(&mut socket, server.node_id(), node.id).await.is_some(), "the client answers a request");
    assert!(client.is_connected());
    client.shutdown().await?;
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
