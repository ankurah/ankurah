use crate::connector::PeerSender;
use crate::proto;
use async_trait::async_trait;
/// A trait representing the connector-specific functionality of a Node
/// This allows connectors to interact with nodes without needing to know about
/// PolicyAgent and Context generics
#[async_trait]
pub trait NodeConnector: Send + Sync {
    /// Get the node's ID
    fn id(&self) -> proto::NodeId;

    /// Whether this node is durable (persists data)
    fn durable(&self) -> bool;

    /// Register a new peer connection
    fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>);

    /// Deregister a peer connection
    fn deregister_peer(&self, node_id: proto::NodeId);

    /// Handle an incoming message from a peer
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()>;
}
