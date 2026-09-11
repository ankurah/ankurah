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
}

// TODO redesign this such that:
// - the sender and receiver are disconnected at the same time
// - a connection id or dyn Ord/Eq/Hash is used to identify the connection for deregistration
//   so that we can have multiple connections to the same node without things getting mixed up

#[async_trait]
pub trait PeerSender: Send + Sync {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError>;
    /// The node ID of the recipient of this message
    fn recipient_node_id(&self) -> proto::EntityId;
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
    fn id(&self) -> proto::EntityId;
    /// Build our handshake identity after local system storage loads; an unadopted node has no root.
    async fn presence(&self) -> Result<proto::Presence, crate::error::NodeHaltReason>;
    async fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) -> Result<(), PeerConnectionError>;
    fn state(&self) -> ankurah_signals::Read<NodeState>;
    fn deregister_peer(&self, node_id: proto::EntityId);
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> proto::EntityId { self.id }
    async fn presence(&self) -> Result<proto::Presence, crate::error::NodeHaltReason> { self.presence().await }
    async fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) -> Result<(), PeerConnectionError> {
        self.register_peer(presence, sender).await
    }
    fn state(&self) -> ankurah_signals::Read<NodeState> { self.state() }
    fn deregister_peer(&self, node_id: proto::EntityId) {
        //
        self.deregister_peer(node_id);
    }
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        //
        self.handle_message(message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
