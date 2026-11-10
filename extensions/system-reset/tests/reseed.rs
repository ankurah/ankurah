//! A node meets a durable peer through each real connector, or one holding only a key, and its
//! System reset plugin decides from the seeding the peer announces whether the node registers the
//! peer.

use std::{sync::Arc, time::Duration};

use ankurah::{
    core::{
        connector::{PeerConnectionError, PeerProvenance, PeerSender, SendError},
        error::NodeHaltReason,
        node::{NodeKey, NodePlugin, PeerRefusal},
        storage::StorageEngine,
        test_helpers::keyed_peer_claims,
    },
    policy::{PeerTrust, DEFAULT_CONTEXT},
    proto, EntityId, Model, Node, NodeState, PermissiveAgent,
};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use ankurah_system_reset::{OnReseed, SystemReset, PLUGIN_NAME};
use ankurah_websocket_client::{ConnectionError, WebsocketClient};
use ankurah_websocket_server::WebsocketServer;
use serde::{Deserialize, Serialize};

/// The application's System model: one entity per seeding of its data.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct System {
    pub seeding: String,
}

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

#[derive(Clone, Copy, Debug)]
enum Transport {
    LocalProcess,
    Websocket,
}

const TRANSPORTS: [Transport; 2] = [Transport::LocalProcess, Transport::Websocket];

/// A client's open connection to a server, through whichever transport; dropping it closes it.
struct Connection {
    _open: Box<dyn std::any::Any>,
}

/// A node serving websocket clients, which a local-process connection reaches directly.
struct Server {
    node: TestNode,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) { self.task.abort(); }
}

/// A durable node with the plugin that serves websocket clients; `seeding` names the System
/// entity it creates, and a server given none holds no System entity.
async fn server(seeding: Option<&str>) -> anyhow::Result<(Server, Option<EntityId>)> {
    let plugin = SystemReset::<System, _>::new(OnReseed::Refuse, DEFAULT_CONTEXT);
    let node = Node::builder(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).durable().plug(plugin).build()?;
    let system = create_system(&node, seeding).await?;
    Ok((serve(node).await?, system))
}

/// Create a System entity on `node` for `seeding`, if any.
async fn create_system(node: &TestNode, seeding: Option<&str>) -> anyhow::Result<Option<EntityId>> {
    let Some(seeding) = seeding else { return Ok(None) };
    let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
    let id = transaction.create(&System { seeding: seeding.into() }).await?.id();
    transaction.commit().await?;
    Ok(Some(id))
}

/// Serve websocket clients from `node` on a port the system assigns. The serving task holds the
/// listener, bound before the URL is returned, so no other listener can hold that port.
async fn serve(node: TestNode) -> anyhow::Result<Server> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = axum::Router::new().route("/ws", axum::routing::get(WebsocketServer::new(node.clone()).route_handler()));
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await {
            tracing::warn!("Test server on {address} stopped: {error}");
        }
    });
    Ok(Server { node, url: format!("ws://{address}"), task })
}

/// An ephemeral node on `engine` carrying the plugin, as an application builds its client.
fn client(engine: &Arc<SledStorageEngine>, on_reseed: OnReseed) -> anyhow::Result<TestNode> {
    Ok(Node::builder(engine.clone(), PermissiveAgent::new()).plug(SystemReset::<System, _>::new(on_reseed, DEFAULT_CONTEXT)).build()?)
}

/// Connect `client` to `server` through `transport`, or return why the connection was refused.
async fn connect(transport: Transport, client: &TestNode, server: &Server) -> Result<Connection, PeerConnectionError> {
    match transport {
        Transport::LocalProcess => match LocalProcessConnection::new(client, &server.node).await {
            Ok(connection) => Ok(Connection { _open: Box::new(connection) }),
            Err(error) => Err(error.downcast::<PeerConnectionError>().expect("the local connection was refused by a node")),
        },
        Transport::Websocket => {
            let connection = WebsocketClient::new(client.clone(), &server.url).await.expect("the websocket client starts");
            match tokio::time::timeout(Duration::from_secs(10), connection.wait_connected())
                .await
                .expect("the client connects or is refused")
            {
                Ok(()) => Ok(Connection { _open: Box::new(connection) }),
                Err(ConnectionError::Peer(error)) => {
                    connection.shutdown().await.expect("the websocket client shuts down");
                    Err(error)
                }
                Err(ConnectionError::RefusedByServer(reason)) => panic!("the server refused the client: {reason}"),
                Err(error) => panic!("the websocket connection failed: {error}"),
            }
        }
    }
}

/// Wait until `engine` holds the entity `id`.
async fn wait_until_held(engine: &SledStorageEngine, id: EntityId) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.get_state(id).await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the node comes to hold the entity");
}

/// Wait until `engine` no longer holds the entity `id`.
async fn wait_until_wiped(engine: &SledStorageEngine, id: EntityId) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while engine.get_state(id).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the store comes to hold no such entity");
}

/// Wait until neither `client` nor `server` holds a registration of the other. A dialer's plugin
/// decides after the accepting side has registered it, and the accepting side removes that
/// registration when the refused connection ends.
async fn wait_until_unregistered(client: &TestNode, server: &TestNode) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while client.granted_trust(&server.id).is_some() || server.granted_trust(&client.id).is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("each side removes its registration of the other");
}

/// Close `connection` and wait until `node` is gone, so nothing of it writes to its store again.
async fn close(node: TestNode, connection: Option<Connection>) {
    drop(connection);
    let weak = node.weak();
    drop(node);
    tokio::time::timeout(Duration::from_secs(10), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the node is dropped");
}

/// The store a client leaves behind after its first connection to `server`: it holds the server's
/// System entity `system`.
async fn store_of_a_client_of(transport: Transport, server: &Server, system: EntityId) -> anyhow::Result<Arc<SledStorageEngine>> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let client = client(&engine, OnReseed::Reset)?;
    let connection = connect(transport, &client, server).await?;
    wait_until_held(&engine, system).await;
    close(client, Some(connection)).await;
    Ok(engine)
}

#[tokio::test]
async fn a_client_on_its_first_connection_registers_and_comes_to_hold_the_system_entity() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let (server, system) = server(Some("first")).await?;
        let system = system.unwrap();
        let engine = Arc::new(SledStorageEngine::new_test()?);
        assert!(engine.get_state(system).await.is_err());
        let client = client(&engine, OnReseed::Reset)?;
        let _connection = connect(transport, &client, &server).await?;
        wait_until_held(&engine, system).await;
        assert_eq!(client.state().value(), NodeState::Running, "{transport:?}");
    }
    Ok(())
}

/// The websocket client stops when its node halts, before the plugin's refusal reaches it, so it
/// reports the halt; the local-process connector reports the refusal.
#[tokio::test]
async fn a_client_meeting_a_server_seeded_again_resets_and_refuses_and_a_fresh_node_holds_the_new_seeding() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let (first, first_system) = server(Some("first")).await?;
        let first_system = first_system.unwrap();
        let engine = store_of_a_client_of(transport, &first, first_system).await?;
        let (reseeded, second_system) = server(Some("second")).await?;
        let second_system = second_system.unwrap();

        let returning = client(&engine, OnReseed::Reset)?;
        let refused = connect(transport, &returning, &reseeded).await.err().expect("the returning client refuses the reseeded server");
        let expected = match transport {
            Transport::LocalProcess => PeerConnectionError::Refused(PeerRefusal {
                plugin: PLUGIN_NAME,
                reason: "the peer holds another seeding of the System model; this node reset".into(),
            }),
            Transport::Websocket => PeerConnectionError::NodeHalted(NodeHaltReason::Reset),
        };
        assert_eq!(refused, expected, "{transport:?}");
        assert_eq!(returning.state().value(), NodeState::Halted(NodeHaltReason::Reset));
        wait_until_unregistered(&returning, &reseeded.node).await;

        // The plugin's own reset wipes the store; the host's call only waits for that wipe, then
        // the host builds a fresh node on the store.
        wait_until_wiped(&engine, first_system).await;
        returning.reset().await?;
        close(returning, None).await;
        let fresh = client(&engine, OnReseed::Reset)?;
        let _connection = connect(transport, &fresh, &reseeded).await?;
        wait_until_held(&engine, second_system).await;
    }
    Ok(())
}

#[tokio::test]
async fn a_client_meeting_its_own_seeding_again_registers_and_keeps_its_data() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let (server, system) = server(Some("first")).await?;
        let system = system.unwrap();
        let engine = store_of_a_client_of(transport, &server, system).await?;

        let returning = client(&engine, OnReseed::Reset)?;
        let _connection = connect(transport, &returning, &server).await?;
        returning.wait_ready().await?;
        assert!(engine.get_state(system).await.is_ok(), "{transport:?}");
        assert_eq!(server.node.granted_trust(&returning.id).map(|trust| trust.durable), Some(false));
    }
    Ok(())
}

#[tokio::test]
async fn a_client_meeting_a_server_that_announces_no_seeding_registers_and_keeps_its_data() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let (first, system) = server(Some("first")).await?;
        let system = system.unwrap();
        let engine = store_of_a_client_of(transport, &first, system).await?;

        let (holding_none, _) = server(None).await?;
        let without_the_plugin = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        create_system(&without_the_plugin, Some("unannounced")).await?;
        for peer in [holding_none, serve(without_the_plugin).await?] {
            let returning = client(&engine, OnReseed::Reset)?;
            let _connection = connect(transport, &returning, &peer).await?;
            returning.wait_ready().await?;
            assert!(engine.get_state(system).await.is_ok(), "{transport:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_durable_node_configured_only_to_refuse_refuses_another_seeding_and_keeps_its_data() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let refuser = Node::builder(engine.clone(), PermissiveAgent::new())
            .durable()
            .plug(SystemReset::<System, _>::new(OnReseed::Refuse, DEFAULT_CONTEXT))
            .build()?;
        let own = create_system(&refuser, Some("its own")).await?.unwrap();
        let (other, _) = server(Some("another")).await?;

        let refused = connect(transport, &refuser, &other).await.err().expect("the node refuses a peer of another seeding");
        let refusal = PeerRefusal { plugin: PLUGIN_NAME, reason: "the peer holds another seeding of the System model".into() };
        assert_eq!(refused, PeerConnectionError::Refused(refusal), "{transport:?}");
        assert_eq!(refuser.state().value(), NodeState::Running);
        wait_until_unregistered(&refuser, &other.node).await;
        assert!(engine.get_state(own).await.is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn a_peer_not_trusted_as_durable_speaks_for_no_seeding() -> anyhow::Result<()> {
    for transport in TRANSPORTS {
        let (first, first_system) = server(Some("first")).await?;
        let (second, second_system) = server(Some("second")).await?;
        let first_store = store_of_a_client_of(transport, &first, first_system.unwrap()).await?;
        let second_store = store_of_a_client_of(transport, &second, second_system.unwrap()).await?;

        let dialing = client(&first_store, OnReseed::Reset)?;
        let ephemeral_peer = serve(client(&second_store, OnReseed::Reset)?).await?;
        let _connection = connect(transport, &dialing, &ephemeral_peer).await?;
        assert_eq!(dialing.granted_trust(&ephemeral_peer.node.id).map(|trust| trust.durable), Some(false), "{transport:?}");
        assert!(dialing.state().value().halt_reason().is_none());
        assert!(ephemeral_peer.node.state().value().halt_reason().is_none());
    }
    Ok(())
}

/// A connection that drops every frame, for a peer with no node behind it.
#[derive(Clone)]
struct Discard;

impl PeerSender for Discard {
    fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// An announcement the plugin cannot read, as a later build of a plugin under the same name might
/// make, is no verdict: the durable peer registers and the node keeps its data.
#[tokio::test]
async fn a_durable_peer_whose_announcement_cannot_be_read_registers_and_the_node_keeps_its_data() -> anyhow::Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::builder(engine.clone(), PermissiveAgent::new())
        .durable()
        .plug(SystemReset::<System, _>::new(OnReseed::Reset, DEFAULT_CONTEXT))
        .build()?;
    let own = create_system(&node, Some("its own")).await?.unwrap();

    let key = NodeKey::generate();
    let (mut handshake, mut claims) = keyed_peer_claims(node.id, PeerProvenance::Configured, &key, true);
    claims.announcements.insert(PLUGIN_NAME.into(), vec![7; 33]);
    node.register_peer(key.sign_presence(claims), &mut handshake, Box::new(Discard)).await?;
    assert_eq!(node.granted_trust(&key.node_id()), Some(PeerTrust { durable: true }));
    assert_eq!(node.state().value(), NodeState::Running);
    assert!(engine.get_state(own).await.is_ok());
    Ok(())
}

/// A node halted while a handshake is in flight cannot read its System entities, and its
/// announcement fails with the reason rather than announcing none.
#[tokio::test]
async fn a_node_that_cannot_read_its_system_entities_fails_its_announcement() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.reset().await?;
    let plugin = SystemReset::<System, _>::new(OnReseed::Refuse, DEFAULT_CONTEXT);
    let failure = plugin.announce(&node).await.expect_err("a halted node reads none of its entities");
    assert_eq!(failure.plugin, PLUGIN_NAME);
    assert!(failure.reason.starts_with("reading this node's System entities failed"), "{}", failure.reason);
    Ok(())
}
