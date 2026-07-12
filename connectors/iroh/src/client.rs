use ankurah_core::{policy::PolicyAgent, storage::StorageEngine, Node};
use ankurah_proto as proto;
use ankurah_signals::{Mut, Read, Wait};
use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{select, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Connection state for the iroh client
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting { endpoint_id: EndpointId },
    Connected { endpoint_id: EndpointId, server_presence: proto::Presence },
    Error(ConnectionError),
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Connecting { .. } => write!(f, "Connecting"),
            ConnectionState::Connected { .. } => write!(f, "Connected"),
            ConnectionState::Error(_) => write!(f, "Error"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConnectionError {
    #[error("General connection error: {0}")]
    General(String),
}

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

struct Inner<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node: Node<SE, PA>,
    endpoint: Endpoint,
    server_addr: EndpointAddr,
    connection_state: Mut<ConnectionState>,
    connected: AtomicBool,
    shutdown: CancellationToken,
    registrations: Arc<crate::connection::RegistrationRegistry>,
}

/// Dialer side of the iroh connector.
///
/// Connects the given `Node` to a remote Ankurah peer identified by an
/// [`EndpointAddr`] (endpoint id plus direct addresses and/or relay URL),
/// using an already-bound [`iroh::Endpoint`]. Relay and address lookup
/// configuration are the embedder's responsibility when building the endpoint.
///
/// Mirrors the websocket client: it opens a connection, sends its own
/// `Presence` first, registers the remote peer upon receiving the server's
/// `Presence`, pumps messages until the connection drops, and then reconnects
/// with exponential backoff.
pub struct IrohClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    inner: Arc<Inner<SE, PA>>,
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl<SE, PA> IrohClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Create a new iroh client and start connecting to the server address.
    pub async fn new(node: Node<SE, PA>, endpoint: Endpoint, server_addr: EndpointAddr) -> Result<Self> {
        info!("Creating iroh client for endpoint {}", server_addr.id);

        let inner = Arc::new(Inner {
            node,
            endpoint,
            server_addr,
            connection_state: Mut::new(ConnectionState::Disconnected),
            connected: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            registrations: Arc::new(crate::connection::RegistrationRegistry::default()),
        });

        let task = tokio::spawn(Self::run_connection_loop(inner.clone()));
        Ok(Self { inner, task: std::sync::Mutex::new(Some(task)) })
    }

    /// The underlying iroh endpoint.
    pub fn endpoint(&self) -> &Endpoint { &self.inner.endpoint }

    /// Get the connection state as a reactive signal
    pub fn state(&self) -> Read<ConnectionState> { self.inner.connection_state.read() }

    /// Check if currently connected to the server
    pub fn is_connected(&self) -> bool { self.inner.connected.load(Ordering::Acquire) }

    /// Gracefully stop connecting. Does not close the endpoint, which remains
    /// owned by the embedder.
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down iroh client");

        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            self.inner.shutdown.cancel();

            match task.await {
                Ok(()) => info!("iroh client shutdown completed"),
                Err(e) => tracing::warn!("Connection task join error during shutdown: {}", e),
            }
        } else {
            info!("iroh client already shut down");
        }
        Ok(())
    }

    /// Wait for the client to establish a connection to the server (signal-based)
    pub async fn wait_connected(&self) -> Result<(), ConnectionError> {
        self.state()
            .wait_for(|state| match state {
                ConnectionState::Connected { .. } => Some(Ok(())),
                ConnectionState::Error(e) => Some(Err(e.clone())),
                _ => None, // Continue waiting for Connecting/Disconnected states
            })
            .await
    }

    /// Get the Ankurah node ID of the connected server (if connected)
    pub fn server_node_id(&self) -> Option<proto::EntityId> {
        use ankurah_signals::Get;
        match self.state().get() {
            ConnectionState::Connected { server_presence, .. } => Some(server_presence.node_id),
            _ => None,
        }
    }

    /// Main connection loop with automatic reconnection
    async fn run_connection_loop(inner: Arc<Inner<SE, PA>>) {
        let mut backoff = INITIAL_BACKOFF;
        info!("Starting iroh connection loop to endpoint {}", inner.server_addr.id);

        loop {
            if inner.shutdown.is_cancelled() {
                break;
            }

            select! {
                _ = inner.shutdown.cancelled() => {
                    info!("iroh connection shutting down");
                    break;
                }
                result = Self::connect_once(&inner) => {
                    match result {
                        Ok(()) => {
                            info!("Connection to endpoint {} completed normally", inner.server_addr.id);
                            backoff = INITIAL_BACKOFF;
                            if inner.shutdown.is_cancelled() {
                                info!("Shutdown requested, stopping reconnection attempts");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Connection to endpoint {} failed: {}", inner.server_addr.id, e);
                            inner.connection_state.set(ConnectionState::Error(ConnectionError::General(e.to_string())));
                            inner.connected.store(false, Ordering::Release);

                            info!("Retrying connection in {:?}", backoff);
                            select! {
                                _ = inner.shutdown.cancelled() => break,
                                _ = sleep(backoff) => {}
                            }
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }
        }

        inner.connection_state.set(ConnectionState::Disconnected);
        inner.connected.store(false, Ordering::Release);
    }

    /// Attempt a single connection
    async fn connect_once(inner: &Arc<Inner<SE, PA>>) -> Result<()> {
        info!("Attempting to connect to endpoint {}", inner.server_addr.id);
        inner.connection_state.set(ConnectionState::Connecting { endpoint_id: inner.server_addr.id });

        let connection = inner.endpoint.connect(inner.server_addr.clone(), crate::ALPN).await?;
        debug!("iroh connection established with endpoint {}", inner.server_addr.id);

        // The dialer opens the bidirectional stream; our Presence is the first frame
        // on it (sent inside run_connection), which is also what makes the stream
        // visible to the acceptor.
        let (send_stream, recv_stream) = connection.open_bi().await?;

        let result = crate::connection::run_connection(
            &inner.node,
            &inner.registrations,
            send_stream,
            recv_stream,
            Some(&inner.shutdown),
            |server_presence| {
                info!("Received server presence: {}", server_presence.node_id);
                inner
                    .connection_state
                    .set(ConnectionState::Connected { endpoint_id: inner.server_addr.id, server_presence: server_presence.clone() });
                inner.connected.store(true, Ordering::Release);
            },
        )
        .await;

        inner.connected.store(false, Ordering::Release);
        connection.close(0u32.into(), b"");
        result
    }
}

impl<SE, PA> Drop for IrohClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            debug!("iroh client dropped, requesting shutdown");
            self.inner.shutdown.cancel();
            self.inner.connection_state.set(ConnectionState::Disconnected);
            self.inner.connected.store(false, Ordering::Release);
            task.abort();
        }
    }
}
