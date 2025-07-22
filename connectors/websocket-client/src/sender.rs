use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// PeerSender implementation for websocket connections
#[derive(Clone)]
pub struct WebsocketPeerSender {
    tx: mpsc::UnboundedSender<proto::NodeMessage>,
    recipient_node_id: proto::EntityId,
}

impl WebsocketPeerSender {
    pub fn new(recipient_node_id: proto::EntityId) -> (Self, mpsc::UnboundedReceiver<proto::NodeMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx, recipient_node_id }, rx)
    }
}

#[async_trait]
impl PeerSender for WebsocketPeerSender {
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
