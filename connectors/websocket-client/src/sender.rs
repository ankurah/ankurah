use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

/// PeerSender implementation for websocket connections
#[derive(Clone)]
pub struct WebsocketPeerSender {
    tx: mpsc::UnboundedSender<proto::SignedPeerMessage>,
    close_tx: watch::Sender<bool>,
    recipient_node_id: proto::NodeId,
}

impl WebsocketPeerSender {
    pub fn new(recipient_node_id: proto::NodeId) -> (Self, mpsc::UnboundedReceiver<proto::SignedPeerMessage>, watch::Receiver<bool>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (close_tx, close_rx) = watch::channel(false);
        (Self { tx, close_tx, recipient_node_id }, rx, close_rx)
    }
}

#[async_trait]
impl PeerSender for WebsocketPeerSender {
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError> {
        debug!("Queuing message for peer {}", self.recipient_node_id);

        if *self.close_tx.borrow() {
            return Err(SendError::ConnectionClosed);
        }

        self.tx.send(message).map_err(|_| {
            warn!("Failed to send message to peer {} - channel closed", self.recipient_node_id);
            SendError::ConnectionClosed
        })
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }

    fn close(&self) {
        self.close_tx.send_if_modified(|closed| {
            if *closed {
                false
            } else {
                *closed = true;
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_is_shared_and_idempotent() {
        let (sender, _outgoing, mut close_rx) = WebsocketPeerSender::new(proto::NodeId::from_bytes([0x81; 32]));
        let clone = sender.clone();
        sender.close();
        clone.close();

        close_rx.changed().await.expect("sender remains alive");
        assert!(*close_rx.borrow_and_update());
    }
}
