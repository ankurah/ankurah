use ankurah_proto as proto;
use async_trait::async_trait;

use crate::{policy::PolicyAgent, storage::StorageEngine, Node, NodeState};

/// A peer rejected before its connection becomes available to the node.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PeerConnectionError {
    #[error("{0}")]
    Protocol(#[from] proto::PresenceRejection),
    #[error("peer belongs to a different system (current {current}, offered {offered})")]
    SystemMismatch { current: proto::EntityId, offered: proto::EntityId },
    #[error("durable peer did not advertise a system")]
    MissingSystem,
    #[error("invalid system: {0}")]
    InvalidSystem(String),
    #[error("failed to wipe local storage for system replacement: {0}")]
    SystemReset(String),
    #[error("node halted: {0}")]
    NodeHalted(#[from] crate::error::NodeHaltReason),
    #[error("peer did not prove that it holds the key of its presence")]
    UnprovenKey,
}

/// A number a node assigns to one of its own open connections, so peers with the same node id keep separate connections.
/// The node never sends this number to another node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "connection {}", self.0) }
}

// TODO redesign this such that:
// - the sender and receiver are disconnected at the same time

#[async_trait]
pub trait PeerSender: Send + Sync {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError>;
    fn cloned(&self) -> Box<dyn PeerSender>;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Send timeout")]
    Timeout,
    #[error("Other error: {0}")]
    Other(#[source] std::sync::Arc<anyhow::Error>),
    #[error("Unknown error")]
    Unknown,
}

impl From<anyhow::Error> for SendError {
    fn from(error: anyhow::Error) -> Self { Self::Other(std::sync::Arc::new(error)) }
}

#[async_trait]
pub trait NodeComms: Send + Sync {
    /// The node's id, or `None` while the node is unkeyed.
    fn id(&self) -> Option<proto::NodeId>;
    /// A signed presence for the peer's `challenge`, using this node's `side` of the connection.
    /// The node waits for its keypair, and for system storage to load so its presence includes any stored system root.
    async fn presence(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
    ) -> Result<proto::SignedPresence, crate::error::NodeHaltReason>;
    async fn register_peer(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
        presence: proto::SignedPresence,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionId, PeerConnectionError>;
    fn state(&self) -> ankurah_signals::Read<NodeState>;
    fn deregister_peer(&self, connection: ConnectionId);
    async fn handle_message(&self, connection: ConnectionId, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> Option<proto::NodeId> { self.0.id() }
    async fn presence(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
    ) -> Result<proto::SignedPresence, crate::error::NodeHaltReason> {
        self.presence(side, challenge).await
    }
    async fn register_peer(
        &self,
        side: proto::ConnectionSide,
        challenge: &proto::Challenge,
        presence: proto::SignedPresence,
        sender: Box<dyn PeerSender>,
    ) -> Result<ConnectionId, PeerConnectionError> {
        self.register_peer(side, challenge, presence, sender).await
    }
    fn state(&self) -> ankurah_signals::Read<NodeState> { self.state() }
    fn deregister_peer(&self, connection: ConnectionId) {
        //
        self.deregister_peer(connection);
    }
    async fn handle_message(&self, connection: ConnectionId, message: proto::NodeMessage) -> anyhow::Result<()> {
        //
        self.handle_message(connection, message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
