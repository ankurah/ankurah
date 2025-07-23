use crate::sender::WebsocketPeerSender;
use ankurah_core::{connector::PeerSender, policy::PolicyAgent, storage::StorageEngine, Node};
use ankurah_proto as proto;
use ankurah_signals::{Get, Mut, Read};
use anyhow::Result;
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
use tokio::{select, sync::Notify, task::JoinHandle, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

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
    #[strum(serialize = "Error")]
    Error(ConnectionError),
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
    server_url: String,
    connection_state: Mut<ConnectionState>,
    connected: AtomicBool,
    shutdown: Notify,
    shutdown_requested: AtomicBool,
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
        let ws_url = Self::normalize_url(server_url);
        info!("Creating WebSocket client for {}", ws_url);

        let inner = Arc::new(Inner {
            node,
            server_url: ws_url,
            connection_state: Mut::new(ConnectionState::Disconnected),
            connected: AtomicBool::new(false),
            shutdown: Notify::new(),
            shutdown_requested: AtomicBool::new(false),
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
            self.inner.shutdown_requested.store(true, Ordering::Release);
            self.inner.shutdown.notify_waiters();

            match task.await {
                Ok(()) => info!("WebSocket client shutdown completed"),
                Err(e) => warn!("Connection task join error during shutdown: {}", e),
            }
        } else {
            info!("WebSocket client already shut down");
        }
        Ok(())
    }

    /// Wait for the client to establish a connection to the server (signal-based)
    pub async fn wait_connected(&self) -> Result<(), ConnectionError> {
        // Wait for either Connected or Error state, returning appropriate Result
        self.state()
            .wait_for(|state| match state {
                ConnectionState::Connected { .. } => Some(Ok(())),
                ConnectionState::Error(e) => Some(Err(e.clone())),
                _ => None, // Continue waiting for Connecting/Disconnected states
            })
            .await
    }

    /// Get the node ID of the connected server (if connected)
    pub fn server_node_id(&self) -> Option<proto::EntityId> {
        match self.state().get() {
            ConnectionState::Connected { server_presence, .. } => Some(server_presence.node_id),
            _ => None,
        }
    }

    /// Main connection loop with automatic reconnection
    async fn run_connection_loop(inner: Arc<Inner<SE, PA>>) {
        let mut backoff = INITIAL_BACKOFF;
        info!("Starting websocket connection loop to {}", inner.server_url);

        loop {
            select! {
                _ = inner.shutdown.notified() => {
                    info!("Websocket connection shutting down");
                    break;
                }
                result = Self::connect_once(&inner) => {
                    match result {
                        Ok(()) => {
                            info!("Connection to {} completed normally", inner.server_url);
                            backoff = INITIAL_BACKOFF;
                            if inner.shutdown_requested.load(Ordering::Acquire) {
                                info!("Shutdown requested, stopping reconnection attempts");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Connection to {} failed: {}", inner.server_url, e);
                            inner.connection_state.set(ConnectionState::Error(ConnectionError::General(e.to_string())));
                            inner.connected.store(false, Ordering::Release);

                            info!("Retrying connection in {:?}", backoff);
                            select! {
                                _ = inner.shutdown.notified() => break,
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
        info!("Attempting to connect to {}", inner.server_url);
        inner.connection_state.set(ConnectionState::Connecting { url: inner.server_url.clone() });

        let (ws_stream, _) = connect_async(inner.server_url.as_str()).await?;
        info!("WebSocket handshake completed with {}", inner.server_url);

        let (mut sink, mut stream) = ws_stream.split();
        debug!("Starting connection handling");

        // Send our presence immediately
        let presence = proto::Message::Presence(proto::Presence {
            node_id: inner.node.id,
            durable: inner.node.durable,
            system_root: inner.node.system.root(),
        });

        sink.send(Message::Binary(bincode::serialize(&presence)?.into())).await?;
        debug!("Sent client presence");

        let mut peer_sender: Option<WebsocketPeerSender> = None;
        let mut outgoing_rx: Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>> = None;

        loop {
            select! {
                _ = inner.shutdown.notified() => {
                    debug!("Connection received shutdown signal");
                    break;
                }
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
                    match Self::handle_incoming_message(inner, msg, &mut peer_sender, &mut outgoing_rx, &mut sink).await? {
                        MessageResult::Continue => continue,
                        MessageResult::Break => break,
                    }
                }
            }
        }

        // Cleanup
        inner.connected.store(false, Ordering::Release);
        if let Some(sender) = peer_sender {
            inner.node.deregister_peer(sender.recipient_node_id());
            debug!("Deregistered peer {}", sender.recipient_node_id());
        }
        Ok(())
    }

    async fn handle_outgoing_message(
        sink: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        msg: Option<proto::NodeMessage>,
    ) -> Result<()> {
        if let Some(node_message) = msg {
            let proto_message = proto::Message::PeerMessage(node_message);
            match bincode::serialize(&proto_message) {
                Ok(data) => {
                    sink.send(Message::Binary(data.into())).await?;
                }
                Err(e) => error!("Failed to serialize outgoing message: {}", e),
            }
        }
        Ok(())
    }

    async fn handle_incoming_message(
        inner: &Arc<Inner<SE, PA>>,
        msg: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        peer_sender: &mut Option<WebsocketPeerSender>,
        outgoing_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>>,
        sink: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) -> Result<MessageResult> {
        match msg {
            Some(Ok(Message::Binary(data))) => match bincode::deserialize(&data) {
                Ok(proto::Message::Presence(server_presence)) => {
                    Self::handle_server_presence(inner, server_presence, peer_sender, outgoing_rx).await;
                    Ok(MessageResult::Continue)
                }
                Ok(proto::Message::PeerMessage(node_msg)) => {
                    Self::handle_peer_message(inner, node_msg).await;
                    Ok(MessageResult::Continue)
                }
                Err(e) => {
                    warn!("Failed to deserialize message: {}", e);
                    Ok(MessageResult::Continue)
                }
            },
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

    async fn handle_server_presence(
        inner: &Arc<Inner<SE, PA>>,
        server_presence: proto::Presence,
        peer_sender: &mut Option<WebsocketPeerSender>,
        outgoing_rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<proto::NodeMessage>>,
    ) {
        info!("Received server presence: {}", server_presence.node_id);

        let (sender, rx) = WebsocketPeerSender::new(server_presence.node_id);
        inner.node.register_peer(server_presence.clone(), Box::new(sender.clone()));

        *outgoing_rx = Some(rx);
        *peer_sender = Some(sender);

        inner.connection_state.set(ConnectionState::Connected { url: inner.server_url.to_string(), server_presence });
        inner.connected.store(true, Ordering::Release);
        info!("Successfully connected to server {}", inner.server_url);
    }

    async fn handle_peer_message(inner: &Arc<Inner<SE, PA>>, node_msg: proto::NodeMessage) {
        debug!("Received peer message");
        let node = inner.node.clone();
        tokio::spawn(async move {
            if let Err(e) = node.handle_message(node_msg).await {
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
            self.inner.shutdown_requested.store(true, Ordering::Release);
            self.inner.shutdown.notify_waiters();
            task.abort();
        }
    }
}
