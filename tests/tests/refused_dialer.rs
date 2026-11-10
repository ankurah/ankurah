//! A server that will not register a dialer tells it why: the websocket client reports the
//! refusal with the server's reason, never counts itself connected, and waits longer before each
//! dial that follows. A handshake that does not finish leaves nothing registered on either side.

mod common;

use ankurah::core::connector::{PeerConnectionError, PeerProvenance};
use ankurah::core::node::{NodePlugin, PeerRefusal, PeerRegistration};
use ankurah::core::storage::StorageEngine;
use ankurah::policy::PolicyAgent;
use ankurah::signals::Subscribe;
use ankurah_websocket_client::{ConnectionError, ConnectionState, WebsocketClient};
use common::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

const PLUGIN: &str = "refuse-inbound";
const REASON: &str = "this server takes no inbound peers";

/// Refuses every peer that connected to its node, and records when each registration was asked about.
struct RefuseInbound {
    asked: Arc<Mutex<Vec<Instant>>>,
}

#[async_trait::async_trait]
impl<SE, PA> NodePlugin<SE, PA> for RefuseInbound
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { PLUGIN }

    async fn register_peer(&self, _: &Node<SE, PA>, peer: &PeerRegistration<'_>) -> Result<(), PeerRefusal> {
        self.asked.lock().unwrap().push(Instant::now());
        match peer.provenance {
            PeerProvenance::Inbound => Err(PeerRefusal { plugin: PLUGIN, reason: REASON.into() }),
            PeerProvenance::Configured => Ok(()),
        }
    }
}

#[tokio::test]
async fn a_client_the_server_refuses_reports_why_never_connects_and_waits_longer_before_each_dial() -> anyhow::Result<()> {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let server = Node::builder(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new())
        .durable()
        .plug(RefuseInbound { asked: asked.clone() })
        .build()?;
    let (url, server_task) = serve(server.clone()).await?;
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = WebsocketClient::new(node.clone(), &url).await?;
    let connected = Arc::new(AtomicUsize::new(0));
    let _listener = client.state().subscribe({
        let connected = connected.clone();
        move |state: ConnectionState| {
            if matches!(state, ConnectionState::Connected { .. }) {
                connected.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let reason = PeerConnectionError::Refused(PeerRefusal { plugin: PLUGIN, reason: REASON.into() }).to_string();
    let outcome = tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await?;
    assert_eq!(outcome, Err(ConnectionError::RefusedByServer(reason.clone())));
    assert!(matches!(client.state().value(), ConnectionState::RefusedByServer { reason: refused, .. } if refused == reason));

    tokio::time::timeout(Duration::from_secs(10), async {
        while asked.lock().unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let asked = asked.lock().unwrap().clone();
    let (first, second) = (asked[1] - asked[0], asked[2] - asked[1]);
    assert!(first >= Duration::from_millis(900), "the client dialed again after {first:?}");
    assert!(second >= first + Duration::from_millis(500), "the wait did not grow: {first:?}, then {second:?}");

    assert!(matches!(client.state().value(), ConnectionState::RefusedByServer { .. }), "a refusal stays visible while the client retries");
    assert_eq!(connected.load(Ordering::Relaxed), 0, "the client never counted itself connected");
    assert!(!client.is_connected());
    assert_eq!(node.granted_trust(&server.id), None, "the client holds no registration of the server");
    assert_eq!(server.granted_trust(&node.id), None, "the server holds no registration of the client");
    client.shutdown().await?;
    server_task.abort();
    Ok(())
}

/// Asked about a peer, never decides.
struct NeverDecides;

#[async_trait::async_trait]
impl<SE, PA> NodePlugin<SE, PA> for NeverDecides
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn name(&self) -> &'static str { "never-decides" }

    async fn register_peer(&self, _: &Node<SE, PA>, _: &PeerRegistration<'_>) -> Result<(), PeerRefusal> { std::future::pending().await }
}

/// A local-process handshake dropped after the accepting node registered the dialer, while the
/// dialer's plugin is still deciding about the accepting node, leaves nothing registered on
/// either side.
#[tokio::test]
async fn a_local_process_handshake_dropped_before_the_dialer_registers_leaves_nothing_registered() -> anyhow::Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let client = Node::builder(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).plug(NeverDecides).build()?;
    let connecting = tokio::spawn({
        let (client, server) = (client.clone(), server.clone());
        async move { LocalProcessConnection::new(&client, &server).await.map(|_| ()) }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.granted_trust(&client.id).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    connecting.abort();
    assert!(connecting.await.unwrap_err().is_cancelled());
    assert_eq!(server.granted_trust(&client.id), None, "the accepting node removes the dialer it registered");
    assert_eq!(client.granted_trust(&server.id), None);
    Ok(())
}
