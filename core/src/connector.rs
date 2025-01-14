use ankurah_proto as proto;
use async_trait::async_trait;

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
