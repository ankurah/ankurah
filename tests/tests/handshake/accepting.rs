//! The accepting side: a real server driven from a raw socket.

use super::common::{proto, serve, start_test_server};
use super::*;
use ankurah::core::connector::PeerConnectionError;
use ankurah::core::node::{AnnouncementFailure, NodePlugin, PeerRefusal, PeerRegistration};
use ankurah::core::storage::StorageEngine;
use ankurah::policy::PolicyAgent;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;

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
    opening: proto::Opening,
    server_opening: proto::Opening,
}

impl RawClient {
    /// Connect to `server_url`, open offering `offered`, and read the server's opening.
    async fn open(server_url: &str, offered: proto::ProtocolVersions) -> Result<Self> {
        let key = NodeKey::generate();
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("{server_url}/ws")).await?;
        let opening = proto::Opening { versions: offered, challenge: proto::HandshakeChallenge::new(key.node_id()) };
        socket.send(frame(&proto::Message::Open(opening))).await?;
        let Some(proto::Message::Open(server_opening)) = receive(&mut socket).await else { anyhow::bail!("the server opened otherwise") };
        Ok(Self { key, socket, opening, server_opening })
    }

    /// Present to the server with what `change` makes of the claims an honest client makes.
    async fn present(&mut self, change: impl FnOnce(&mut proto::PresenceClaims)) -> Result<()> {
        let mut claims = claims_answering(&self.key, &self.server_opening, self.opening.versions, false);
        change(&mut claims);
        self.socket.send(frame(&proto::Message::Presence(self.key.sign_presence(claims)))).await?;
        Ok(())
    }

    /// Present honestly and read the server's presence answering this client's.
    async fn connect(&mut self) -> Result<proto::Presence> {
        self.present(|_| {}).await?;
        let Some(proto::Message::Presence(server_presence)) = receive(&mut self.socket).await else {
            anyhow::bail!("the server answered the presence otherwise")
        };
        anyhow::ensure!(server_presence.claims.challenge == self.opening.challenge, "the server's presence answers our challenge");
        Ok(server_presence)
    }
}

#[tokio::test]
async fn a_wider_opening_settles_on_the_version_the_server_speaks() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, versions(1, 3)).await?;
    assert_eq!(client.server_opening.versions, SPOKEN);
    let server_presence = client.connect().await?;
    assert!(server_presence.signature_is_valid());
    assert_eq!(server_presence.claims.negotiation, proto::VersionNegotiation { offered: SPOKEN, peer_offered: versions(1, 3), chosen: 1 });
    assert!(server_node.granted_trust(&client.key.node_id()).is_some(), "the server registered the client");
    server_task.abort();
    Ok(())
}

/// A version-1 reader rejects the bytes after this opening's challenge, so the answer shows the
/// server read no further than the versions.
#[tokio::test]
async fn an_opening_sharing_no_version_is_answered_with_the_servers_versions_before_the_rest_is_read() -> Result<()> {
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
/// signs the offers it saw, which differ from the ones the server sent or the version it chose,
/// and the server answers with its refusal in place of its presence.
#[tokio::test]
async fn the_server_refuses_a_presence_that_does_not_repeat_the_offers_and_the_chosen_version() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let alterations: [fn(&mut proto::VersionNegotiation); 2] =
        [|negotiation| negotiation.peer_offered = versions(1, 3), |negotiation| negotiation.chosen = 2];
    for alter in alterations {
        let mut client = RawClient::open(&server_url, SPOKEN).await?;
        client.present(|claims| alter(&mut claims.negotiation)).await?;
        let refusal = PeerConnectionError::OfferMismatch(client.key.node_id()).to_string();
        let answer = receive(&mut client.socket).await;
        assert!(matches!(answer, Some(proto::Message::Refused(ref reason)) if *reason == refusal), "the server refuses: {answer:?}");
        assert!(receive(&mut client.socket).await.is_none(), "then closes");
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
    assert!(matches!(receive(&mut client.socket).await, Some(proto::Message::Presence(_))), "the server presents");
    assert!(server_node.granted_trust(&client.key.node_id()).is_some(), "the server registers the client");
    server_task.abort();
    Ok(())
}

/// A message of a kind the server's version does not define closes the connection, whether it
/// arrives in place of the client's presence or once the client is connected.
#[tokio::test]
async fn the_server_closes_a_connection_that_sends_a_message_of_a_kind_it_does_not_know() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    for unknown in UNKNOWN_KINDS {
        let mut client = RawClient::open(&server_url, SPOKEN).await?;
        client.socket.send(WsMessage::Binary(unknown.to_vec().into())).await?;
        assert!(receive(&mut client.socket).await.is_none(), "the server closes a handshake that sent {unknown:?}");

        let mut client = RawClient::open(&server_url, SPOKEN).await?;
        client.connect().await?;
        client.socket.send(WsMessage::Binary(unknown.to_vec().into())).await?;
        assert!(request_answered(&mut client.socket, client.key.node_id(), server_node.id).await.is_none(), "{unknown:?}");
    }
    server_task.abort();
    Ok(())
}

/// The request's frame decodes around a body of a kind the server does not know, so the server
/// can address its answer.
#[tokio::test]
async fn the_server_answers_a_request_whose_body_it_cannot_read_with_unsupported() -> Result<()> {
    let (server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.connect().await?;

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

/// A client that cannot read the server's opening (a client of the earlier protocol, which
/// expects the server's presence first) stays silent; the server must still bound the incomplete
/// handshake.
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

/// A plugin whose announcement never comes.
struct SilentAnnouncer;

#[async_trait::async_trait]
impl<SE, PA> NodePlugin<SE, PA> for SilentAnnouncer
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { "silent-announcer" }

    async fn announce(&self, _: &Node<SE, PA>) -> Result<Option<Vec<u8>>, AnnouncementFailure> { std::future::pending().await }
}

/// The server's own presence, its plugins' announcements included, is bound by the handshake
/// deadline: a server whose plugin never announces closes the connection of the client it
/// registered, and removes that registration.
#[tokio::test]
async fn the_server_closes_a_connection_whose_answer_is_not_ready_within_the_handshake_deadline() -> Result<()> {
    let server_node =
        Node::builder(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).durable().plug(SilentAnnouncer).build()?;
    let (server_url, server_task) = serve(server_node.clone()).await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.present(|_| {}).await?;
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_some()).await, "the server registers the client");

    // The server is now waiting on its plugin's announcement. Run the clock past the deadline
    // without waiting for it; the clock stays real until here, so no earlier deadline fires early.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    match tokio::time::timeout(Duration::from_secs(1), client.socket.next()).await.expect("the server left the connection open") {
        None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => {}
        Some(Ok(other)) => panic!("the server sent {other:?} instead of closing"),
    }
    assert!(eventually(|| server_node.granted_trust(&client.key.node_id()).is_none()).await, "the server removes the registration");
    server_task.abort();
    Ok(())
}

/// A plugin that never decides on a peer's registration, and records that its hook began and
/// that it was dropped.
struct UndecidedRegistrar {
    began: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

/// Sets its flag when dropped.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
}

#[async_trait::async_trait]
impl<SE, PA> NodePlugin<SE, PA> for UndecidedRegistrar
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { "undecided-registrar" }

    async fn register_peer(&self, _: &Node<SE, PA>, _: &PeerRegistration<'_>) -> Result<(), PeerRefusal> {
        let _dropped = SetOnDrop(self.dropped.clone());
        self.began.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
}

/// Registering the dialer, every plugin's hook included, is bound by the handshake deadline: a
/// server whose plugin never decides on the client drops the hook, registers nothing and closes
/// the connection.
#[tokio::test]
async fn the_server_closes_a_connection_whose_registration_is_not_decided_within_the_handshake_deadline() -> Result<()> {
    let (began, dropped) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let registrar = UndecidedRegistrar { began: began.clone(), dropped: dropped.clone() };
    let server_node = Node::builder(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).durable().plug(registrar).build()?;
    let (server_url, server_task) = serve(server_node.clone()).await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.present(|_| {}).await?;
    assert!(eventually(|| began.load(Ordering::SeqCst)).await, "the server asks its plugin about the client");

    // The clock stays real until the hook has begun, so no earlier deadline fires early.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    match tokio::time::timeout(Duration::from_secs(1), client.socket.next()).await.expect("the server left the connection open") {
        None | Some(Ok(WsMessage::Close(_))) | Some(Err(_)) => {}
        Some(Ok(other)) => panic!("the server sent {other:?} instead of closing"),
    }
    assert!(dropped.load(Ordering::SeqCst), "the server dropped the plugin's hook");
    assert_eq!(server_node.granted_trust(&client.key.node_id()), None, "the server registered nothing");
    server_task.abort();
    Ok(())
}

/// The server reads one opening per connection: a second opening before the client's presence
/// closes the connection.
#[tokio::test]
async fn the_server_closes_a_connection_that_sends_a_second_opening() -> Result<()> {
    let (_server_node, server_url, server_task) = start_test_server().await?;
    let mut client = RawClient::open(&server_url, SPOKEN).await?;
    client.socket.send(frame(&proto::Message::Open(client.opening))).await?;
    client.present(|_| {}).await?;
    assert!(receive(&mut client.socket).await.is_none(), "the server closes without presenting");
    server_task.abort();
    Ok(())
}

/// A stranger connects to one server, reads its challenge, and puts that challenge in its opening
/// to a second server. Were the second to answer with its presence, the stranger could present it
/// at the first and be registered there as the second.
#[tokio::test]
async fn a_stranger_relaying_one_servers_challenge_to_another_receives_no_presence_from_it() -> Result<()> {
    let (target, target_url, target_task) = start_test_server().await?;
    let (relayed, relayed_url, relayed_task) = start_test_server().await?;
    let stranger = NodeKey::generate();

    let (mut at_target, _) = tokio_tungstenite::connect_async(format!("{target_url}/ws")).await?;
    let Some(proto::Message::Open(target_opening)) = receive(&mut at_target).await else { panic!("the target opened otherwise") };
    let (mut at_relayed, _) = tokio_tungstenite::connect_async(format!("{relayed_url}/ws")).await?;
    let Some(proto::Message::Open(relayed_opening)) = receive(&mut at_relayed).await else { panic!("the second server opened otherwise") };
    at_relayed.send(frame(&proto::Message::Open(proto::Opening { versions: SPOKEN, challenge: target_opening.challenge }))).await?;
    let own = stranger.sign_presence(claims_answering(&stranger, &relayed_opening, SPOKEN, false));
    at_relayed.send(frame(&proto::Message::Presence(own))).await?;
    let mut relayed_presence = None;
    while let Some(message) = receive(&mut at_relayed).await {
        if let proto::Message::Presence(presence) = message {
            relayed_presence = Some(presence);
            break;
        }
    }

    if let Some(presence) = relayed_presence.clone() {
        at_target.send(frame(&proto::Message::Open(proto::Opening { versions: SPOKEN, challenge: relayed_opening.challenge }))).await?;
        at_target.send(frame(&proto::Message::Presence(presence))).await?;
        eventually(|| target.granted_trust(&relayed.id).is_some()).await;
    }
    assert_eq!(target.granted_trust(&relayed.id), None, "the target registered the stranger's connection as the second server");
    assert!(relayed_presence.is_none(), "the second server answered a challenge the stranger relayed");
    target_task.abort();
    relayed_task.abort();
    Ok(())
}
