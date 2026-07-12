use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// PeerSender implementation for iroh connections.
///
/// Queues outbound `NodeMessage`s on an unbounded channel; the connection pump
/// drains the channel and writes each message as a length-delimited bincode
/// frame on the connection's bidirectional QUIC stream.
#[derive(Clone)]
pub struct IrohPeerSender {
    tx: mpsc::UnboundedSender<proto::NodeMessage>,
    recipient_node_id: proto::EntityId,
}

impl IrohPeerSender {
    pub(crate) fn new(recipient_node_id: proto::EntityId) -> (Self, mpsc::UnboundedReceiver<proto::NodeMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx, recipient_node_id }, rx)
    }
}

#[async_trait]
impl PeerSender for IrohPeerSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        debug!("Queuing message for peer {}", self.recipient_node_id);

        self.tx.send(message).map_err(|_| {
            warn!("Failed to send message to peer {} - channel closed", self.recipient_node_id);
            SendError::ConnectionClosed
        })
    }

    fn recipient_node_id(&self) -> proto::EntityId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
