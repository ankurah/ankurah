use std::ops::Deref;
use std::sync::{Arc, Weak};

use crate::connector::PeerSender;
use crate::proto;
use async_trait::async_trait;
/// A trait representing the connector-specific functionality of a Node
/// This allows connectors to interact with nodes without needing to know about
/// PolicyAgent and Context generics
#[async_trait]
pub trait NodeConnector: Send + Sync + 'static {
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

    // fn cloned(&self) -> Box<dyn NodeConnector>;
}

#[derive(Clone)]
pub struct NodeHandle(pub(crate) Arc<dyn NodeConnector>);

#[derive(Clone)]
pub struct WeakNodeHandle(Weak<dyn NodeConnector>);

impl Into<NodeHandle> for Arc<dyn NodeConnector> {
    fn into(self) -> NodeHandle { NodeHandle(self) }
}

impl NodeHandle {
    pub fn weak(&self) -> WeakNodeHandle { WeakNodeHandle(Arc::downgrade(&self.0)) }
}

impl WeakNodeHandle {
    pub fn upgrade(&self) -> Option<NodeHandle> { self.0.upgrade().map(NodeHandle) }
}

impl Deref for NodeHandle {
    type Target = Arc<dyn NodeConnector>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
