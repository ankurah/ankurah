use crate::sender::WebsocketPeerSender;
use ankurah_core::{
    connector::{Handshake, PeerConnectionError, PeerProvenance},
    node::ConnectionRegistration,
    policy::PolicyAgent,
    storage::StorageEngine,
    Node, NodeState,
};
use ankurah_proto as proto;
use ankurah_signals::{Calculated, Get, Mut, Peek, Read, Wait};
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use strum::Display;
use thiserror::Error;
use tokio::{select, task::JoinHandle, time::sleep};
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    Connector,
};
use tracing::{debug, error, info, warn};

/// Builder for configuring and creating a WebSocket client
pub struct WebsocketClientBuilder<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node: Node<SE, PA>,
    server_url: String,
    config: Option<WebSocketConfig>,
    disable_nagle: bool,
    connector: Option<Connector>,
}

impl<SE, PA> WebsocketClientBuilder<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Set WebSocket protocol configuration
    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Disable Nagle's algorithm (enable TCP_NODELAY)
    pub fn disable_nagle(mut self) -> Self {
        self.disable_nagle = true;
        self
    }

    /// Use a custom TLS connector
    pub fn connector(mut self, connector: Connector) -> Self {
        self.connector = Some(connector);
        self
    }

    /// Skip TLS certificate verification (insecure, use only for development)
    pub fn insecure(mut self) -> Self {
        let tls =
            native_tls::TlsConnector::builder().danger_accept_invalid_certs(true).build().expect("Failed to build insecure TLS connector");
        self.connector = Some(Connector::NativeTls(tls));
        self
    }

    /// Build and start the WebSocket client
    pub async fn build(self) -> anyhow::Result<WebsocketClient<SE, PA>> {
        WebsocketClient::create(self.node, &self.server_url, self.config, self.disable_nagle, self.connector).await
    }
}

/// Connection state for the websocket client
#[derive(Debug, Clone, PartialEq, Display)]
pub enum ConnectionState {
    Disconnected,
    #[strum(serialize = "Connecting")]
    Connecting {
        url: String,
    },
    #[strum(serialize = "Connected")]
    Connected {
        url: String,
        server_presence: proto::Presence,
    },
    /// The server refused this node, for the reason it gave; the client dials again after its backoff.
    #[strum(serialize = "RefusedByServer")]
    RefusedByServer {
        url: String,
        reason: String,
    },
    #[strum(serialize = "Error")]
    Error(ConnectionError),
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConnectionError {
    #[error("{0}")]
    Peer(#[from] PeerConnectionError),
    /// The server refused this node, for the reason it gave.
    #[error("the server refused this node: {0}")]
    RefusedByServer(String),
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
    server_url: String,
    config: Option<WebSocketConfig>,
    disable_nagle: bool,
    connector: Option<Connector>,
    connection_state: Mut<ConnectionState>,
    connected: AtomicBool,
    shutdown_requested: Mut<bool>,
    run: Calculated<bool>,
}

/// A WebSocket client for connecting Ankurah nodes
pub struct WebsocketClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    inner: Arc<Inner<SE, PA>>,
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl<SE, PA> WebsocketClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Create a new WebSocket client and start connecting to the server
    pub async fn new(node: Node<SE, PA>, server_url: &str) -> anyhow::Result<Self> {
        Self::create(node, server_url, None, false, None).await
    }

    /// Create a builder for configuring the WebSocket client
    pub fn builder(node: Node<SE, PA>, server_url: &str) -> WebsocketClientBuilder<SE, PA> {
        WebsocketClientBuilder { node, server_url: server_url.to_string(), config: None, disable_nagle: false, connector: None }
    }

    async fn create(
        node: Node<SE, PA>,
        server_url: &str,
        config: Option<WebSocketConfig>,
        disable_nagle: bool,
        connector: Option<Connector>,
    ) -> anyhow::Result<Self> {
        let ws_url = Self::normalize_url(server_url);
        info!("Creating WebSocket client for {}", ws_url);

        let node_state = node.state();
        let shutdown_requested = Mut::new(false);
        let shutdown = shutdown_requested.read();
        let inner = Arc::new(Inner {
            node,
            server_url: ws_url,
            config,
            disable_nagle,
            connector,
            connection_state: Mut::new(ConnectionState::Disconnected),
            connected: AtomicBool::new(false),
            shutdown_requested,
            run: Calculated::new(move || !shutdown.get() && !matches!(node_state.get(), NodeState::Halted(_))),
        });

        let task = tokio::spawn(Self::run_connection_loop(inner.clone()));
        Ok(Self { inner, task: std::sync::Mutex::new(Some(task)) })
    }

    fn normalize_url(url: &str) -> String {
        match url {
            u if u.starts_with("ws://") || u.starts_with("wss://") => format!("{}/ws", u),
            u if u.starts_with("http://") => format!("ws://{}/ws", &u[7..]),
            u if u.starts_with("https://") => format!("wss://{}/ws", &u[8..]),
            u => format!("wss://{}/ws", u),
        }
    }

    /// Get the connection state as a reactive signal
    pub fn state(&self) -> Read<ConnectionState> { self.inner.connection_state.read() }

    /// Check if currently connected to the server
    pub fn is_connected(&self) -> bool { self.inner.connected.load(Ordering::Acquire) }

    /// Gracefully shutdown the WebSocket connection
    pub async fn shutdown(self) -> anyhow::Result<()> {
        info!("Shutting down WebSocket client");

        if let Some(task) = self.task.lock().unwrap().take() {
            self.inner.shutdown_requested.set(true);

            match task.await {
                Ok(()) => info!("WebSocket client shutdown completed"),
                Err(e) => warn!("Connection task join error during shutdown: {}", e),
            }
        } else {
            info!("WebSocket client already shut down");
        }
        Ok(())
    }

    /// Wait for the client to establish a connection to the server (signal-based), or to fail
    /// to: a refusal by the server is [`ConnectionError::RefusedByServer`] with its reason.
    pub async fn wait_connected(&self) -> Result<(), ConnectionError> {
        self.state()
            .wait_for(|state| match state {
                ConnectionState::Connected { .. } => Some(Ok(())),
                ConnectionState::RefusedByServer { reason, .. } => Some(Err(ConnectionError::RefusedByServer(reason.clone()))),
                ConnectionState::Error(e) => Some(Err(e.clone())),
                _ => None, // Continue waiting for Connecting/Disconnected states
            })
            .await
    }

    /// Get the node ID of the connected server (if connected)
    pub fn server_node_id(&self) -> Option<proto::NodeId> {
        match self.state().get() {
            ConnectionState::Connected { server_presence, .. } => Some(server_presence.claims.node_id),
            _ => None,
        }
    }

    /// Main connection loop with automatic reconnection
    async fn run_connection_loop(inner: Arc<Inner<SE, PA>>) {
        let mut backoff = INITIAL_BACKOFF;
        info!("Starting websocket connection loop to {}", inner.server_url);

        while inner.run.peek() {
            let result = Self::connect_once(&inner).await;
            if !inner.run.peek() {
                break;
            }
            let state = match result {
                // The server was registered before the connection closed, so the next dial need not wait long.
                Ok(()) => {
                    backoff = INITIAL_BACKOFF;
                    ConnectionState::Error(ConnectionError::General("Connection closed".into()))
                }
                Err(error) => match error.downcast::<ConnectionError>() {
                    Ok(ConnectionError::RefusedByServer(reason)) => {
                        ConnectionState::RefusedByServer { url: inner.server_url.clone(), reason }
                    }
                    Ok(error) => ConnectionState::Error(error),
                    Err(error) => ConnectionState::Error(
                        error
                            .downcast_ref::<PeerConnectionError>()
                            .cloned()
                            .map(ConnectionError::Peer)
                            .unwrap_or_else(|| ConnectionError::General(error.to_string())),
                    ),
                },
            };
            error!("Connection to {} failed: {:?}", inner.server_url, state);
            inner.connection_state.set(state);

            select! {
                _ = inner.run.wait_value(false) => break,
                _ = sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }

        inner.connected.store(false, Ordering::Release);
        let state = match inner.node.state().value() {
            NodeState::Halted(reason) if !inner.shutdown_requested.value() => ConnectionState::Error(ConnectionError::Peer(reason.into())),
            _ => ConnectionState::Disconnected,
        };
        inner.connection_state.set(state);
    }

    /// Attempt a single connection
    async fn connect_once(inner: &Arc<Inner<SE, PA>>) -> Result<()> {
        info!("Attempting to connect to {}", inner.server_url);
        // Retain the last connection error or refusal throughout retries, until admission succeeds.
        if !matches!(inner.connection_state.value(), ConnectionState::Error(_) | ConnectionState::RefusedByServer { .. }) {
            inner.connection_state.set(ConnectionState::Connecting { url: inner.server_url.clone() });
        }

        // Taken when the server registers; given back to deregister it once the connection ends.
        let mut registration: Option<ConnectionRegistration> = None;
        let mut outgoing_rx: Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>> = None;
        let handshake = inner.node.begin_handshake(PeerProvenance::Configured);
        let opening = proto::Message::Open(handshake.opening());
        // Taken when the server's presence registers it.
        let mut handshake = Some(handshake);
        let connection = async {
            let request = inner.server_url.as_str().into_client_request()?;
            let (ws_stream, _) = connect_async_tls_with_config(request, inner.config, inner.disable_nagle, inner.connector.clone()).await?;
            info!("WebSocket handshake completed with {}", inner.server_url);

            let (mut sink, mut stream) = ws_stream.split();
            debug!("Starting connection handling");
            sink.send(Message::Binary(opening.encode().into())).await?;

            loop {
                select! {
                    msg = async {
                        match &mut outgoing_rx {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if Self::handle_outgoing_message(&mut sink, msg).await.is_err() {
                            break;
                        }
                    }
                    msg = stream.next() => {
                        match Self::handle_incoming_message(inner, msg, &mut handshake, &mut registration, &mut outgoing_rx, &mut sink).await? {
                            MessageResult::Continue => continue,
                            MessageResult::Break => break,
                        }
                    }
                }
            }
            Ok(())
        };
        let result = select! {
            biased;
            _ = inner.run.wait_value(false) => Ok(()),
            result = connection => result,
        };

        // Cleanup
        inner.connected.store(false, Ordering::Release);
        let registered = registration.is_some();
        if let Some(registration) = registration {
            inner.node.deregister_peer(registration);
            debug!("Deregistered peer {}", registration.peer());
        }
        match result {
            Ok(()) if !registered => Err(anyhow!("server {} closed the connection before presenting", inner.server_url)),
            result => result,
        }
    }

    async fn handle_outgoing_message(
        sink: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        msg: Option<proto::NodeMessage>,
    ) -> Result<()> {
        if let Some(node_message) = msg {
            sink.send(Message::Binary(proto::Message::PeerMessage(node_message).encode().into())).await?;
        }
        Ok(())
    }

    async fn handle_incoming_message(
        inner: &Arc<Inner<SE, PA>>,
        msg: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        handshake: &mut Option<Handshake>,
        registration: &mut Option<ConnectionRegistration>,
        outgoing_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>>,
        sink: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) -> Result<MessageResult> {
        match msg {
            Some(Ok(Message::Binary(data))) => {
                // The server's first frame is its opening, which this node answers with its presence.
                if let Some(handshake) = handshake.as_mut().filter(|handshake| !handshake.opened()) {
                    return match handshake.read_opening(&data) {
                        Ok(()) => {
                            let presence = inner.node.presence(handshake).await?;
                            sink.send(Message::Binary(proto::Message::Presence(presence).encode().into())).await?;
                            Ok(MessageResult::Continue)
                        }
                        Err(refusal) => {
                            error!("Refusing server {}: {}", inner.server_url, refusal);
                            if let PeerConnectionError::NoCommonVersion { ours, .. } = refusal {
                                let no_common_version = proto::Message::NoCommonVersion(ours).encode();
                                // Best effort: the connection closes either way.
                                let _ = sink.send(Message::Binary(no_common_version.into())).await;
                            }
                            Err(refusal.into())
                        }
                    };
                }
                Self::handle_server_message(inner, &data, handshake, registration, outgoing_rx).await
            }
            Some(Ok(Message::Close(_))) => {
                info!("WebSocket connection closed by server");
                Ok(MessageResult::Break)
            }
            Some(Ok(Message::Ping(data))) => {
                debug!("Received ping, sending pong");
                if let Err(e) = sink.send(Message::Pong(data)).await {
                    warn!("Failed to send pong: {}", e);
                    return Err(e.into());
                }
                Ok(MessageResult::Continue)
            }
            Some(Ok(Message::Pong(_))) => {
                debug!("Received pong");
                Ok(MessageResult::Continue)
            }
            Some(Ok(Message::Text(text))) => {
                debug!("Received unexpected text message: {}", text);
                Ok(MessageResult::Continue)
            }
            Some(Ok(_)) => {
                debug!("Received other message type");
                Ok(MessageResult::Continue)
            }
            Some(Err(e)) => {
                error!("WebSocket error: {}", e);
                Err(e.into())
            }
            None => {
                info!("WebSocket stream closed");
                Ok(MessageResult::Break)
            }
        }
    }

    /// Handle a frame after the server's opening: its presence, then peer messages.
    async fn handle_server_message(
        inner: &Arc<Inner<SE, PA>>,
        data: &[u8],
        handshake: &mut Option<Handshake>,
        registration: &mut Option<ConnectionRegistration>,
        outgoing_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>>,
    ) -> Result<MessageResult> {
        match proto::Message::decode(data) {
            Ok(proto::Message::Open(_)) => {
                warn!("Ignoring repeated opening from {}", inner.server_url);
                Ok(MessageResult::Continue)
            }
            Ok(proto::Message::NoCommonVersion(theirs)) => {
                Err(PeerConnectionError::NoCommonVersion { ours: proto::ProtocolVersions::SPOKEN, theirs }.into())
            }
            Ok(proto::Message::Refused(reason)) => {
                error!("Server {} refused this node: {}", inner.server_url, reason);
                Err(ConnectionError::RefusedByServer(reason).into())
            }
            Ok(proto::Message::Presence(server_presence)) => {
                match Self::handle_server_presence(inner, server_presence, handshake, registration, outgoing_rx).await {
                    Ok(()) => Ok(MessageResult::Continue),
                    Err(rejection) => {
                        error!("Refusing server {}: {}", inner.server_url, rejection);
                        Err(rejection)
                    }
                }
            }
            Ok(proto::Message::PeerMessage(node_msg)) => {
                let Some(registration) = *registration else {
                    // Application traffic before a compatible Presence:
                    // negotiation has not admitted this server, so nothing
                    // it says may reach the node.
                    return Err(anyhow!("server {} sent application traffic before presence negotiation completed", inner.server_url));
                };
                Self::handle_peer_message(inner, registration, node_msg).await;
                Ok(MessageResult::Continue)
            }
            // A message of a kind this node's version does not define closes the connection, and so
            // does a handshake frame it cannot read, since that handshake will never establish.
            Err(unreadable) if registration.is_none() || matches!(unreadable, proto::UnreadableFrame::UnknownKind { .. }) => {
                error!("Closing the connection to {}: the server sent {unreadable}", inner.server_url);
                Err(unreadable.into())
            }
            Err(unreadable) => {
                warn!("Dropping a frame from {}: {unreadable}", inner.server_url);
                Ok(MessageResult::Continue)
            }
        }
    }

    /// Register the server whose presence answers this node's, and only then report connected.
    async fn handle_server_presence(
        inner: &Arc<Inner<SE, PA>>,
        server_presence: proto::Presence,
        handshake: &mut Option<Handshake>,
        registration: &mut Option<ConnectionRegistration>,
        outgoing_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>>,
    ) -> Result<()> {
        info!("Received server presence: {}", server_presence.claims.node_id);

        let Some(mut handshake) = handshake.take() else {
            warn!("Ignoring duplicate server presence");
            return Ok(());
        };
        let (sender, rx) = WebsocketPeerSender::new(server_presence.claims.node_id);
        *registration = Some(inner.node.register_peer(server_presence.clone(), &mut handshake, Box::new(sender)).await?);
        *outgoing_rx = Some(rx);

        inner.connected.store(true, Ordering::Release);
        inner.connection_state.set(ConnectionState::Connected { url: inner.server_url.to_string(), server_presence });
        info!("Successfully connected to server {}", inner.server_url);
        Ok(())
    }

    async fn handle_peer_message(inner: &Arc<Inner<SE, PA>>, registration: ConnectionRegistration, node_msg: proto::NodeMessage) {
        debug!("Received peer message");
        let node = inner.node.clone();
        tokio::spawn(async move {
            if let Err(e) = node.handle_message(registration, node_msg).await {
                warn!("Error handling peer message: {}", e);
            }
        });
    }
}

#[derive(Debug)]
enum MessageResult {
    Continue,
    Break,
}

impl<SE, PA> Drop for WebsocketClient<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            debug!("WebSocket client dropped, requesting shutdown");
            self.inner.shutdown_requested.set(true);
            task.abort();
        }
    }
}
