use ankurah_proto as proto;
use async_trait::async_trait;

use crate::{node::NodeInner, policy::PolicyAgent, storage::StorageEngine, Node};

// TODO redesign this such that:
// - the sender and receiver are disconnected at the same time
// - a connection id or dyn Ord/Eq/Hash is used to identify the connection for deregistration
//   so that we can have multiple connections to the same node without things getting mixed up

#[async_trait]
pub trait PeerSender: Send + Sync {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError>;
    /// The node ID of the recipient of this message
    fn recipient_node_id(&self) -> proto::NodeId;
    fn cloned(&self) -> Box<dyn PeerSender>;
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Send timeout")]
    Timeout,
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
    #[error("Unknown error")]
    Unknown,
}

#[async_trait]
pub trait NodeComms: Send + Sync {
    fn id(&self) -> proto::NodeId;
    fn durable(&self) -> bool;
    fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>);
    fn deregister_peer(&self, node_id: proto::NodeId);
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<SE: StorageEngine + 'static, PA: PolicyAgent + Send + Sync + 'static> NodeComms for Node<SE, PA> {
    fn id(&self) -> proto::NodeId { self.id.clone() }
    fn durable(&self) -> bool { self.durable }
    fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        //
        NodeInner::register_peer(&self, presence, sender);
    }
    fn deregister_peer(&self, node_id: proto::NodeId) {
        //
        NodeInner::deregister_peer(&self, node_id);
    }
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()> {
        //
        NodeInner::handle_message(&self, message).await
    }
    fn cloned(&self) -> Box<dyn NodeComms> { Box::new(self.clone()) }
}
