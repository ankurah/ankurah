use ankurah_proto::{self as proto, Attested, EntityState};
use async_trait::async_trait;

use crate::{datagetter::DataGetter, policy::PolicyAgent, storage::StorageEngine, Node};

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
    fn id(&self) -> proto::EntityId;
    fn durable(&self) -> bool;
    fn system_root(&self) -> Option<Attested<EntityState>>;
    fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>);
    fn deregister_peer(&self, node_id: proto::EntityId);
    async fn handle_message(&self, message: proto::NodeMessage) -> anyhow::Result<()>;
    fn cloned(&self) -> Box<dyn NodeComms>;
}

#[async_trait]
impl<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        DG: DataGetter<PA::ContextData> + Send + Sync + 'static,
    > NodeComms for Node<SE, PA, DG>
{
    fn id(&self) -> proto::EntityId { self.id }
    fn durable(&self) -> bool { self.durable }
    fn system_root(&self) -> Option<Attested<EntityState>> { self.system.root() }
    fn register_peer(&self, presence: proto::Presence, sender: Box<dyn PeerSender>) {
        //
        self.register_peer(presence, sender);
    }
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
