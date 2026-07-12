use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const OUTGOING_QUEUE_CAPACITY: usize = 32;
const OUTGOING_BYTE_CAPACITY: usize = crate::connection::MAX_FRAME_LENGTH;

/// PeerSender implementation for iroh connections.
///
/// Serializes and queues outbound signed peer frames on a bounded channel. The
/// connection pump drains the channel and writes each frame as a
/// length-delimited frame on the connection's bidirectional QUIC stream.
#[derive(Clone)]
pub struct IrohPeerSender {
    tx: mpsc::Sender<OutgoingFrame>,
    recipient_node_id: proto::NodeId,
    control: Arc<ConnectionControl>,
}

pub(crate) struct OutgoingFrame {
    pub(crate) data: Vec<u8>,
    // Retain the byte-budget permit until the connection pump finishes writing
    // the frame (or drops it while tearing down).
    pub(crate) _permit: OwnedSemaphorePermit,
}

pub(crate) struct ConnectionControl {
    stop: CancellationToken,
    /// Dialers use their client shutdown token here so a core-requested close
    /// is terminal and cannot reconnect to the old founder. Acceptors leave
    /// this unset because closing one peer must not stop the whole server.
    terminal_shutdown: Option<CancellationToken>,
    byte_budget: Arc<Semaphore>,
}

impl ConnectionControl {
    fn new(terminal_shutdown: Option<CancellationToken>) -> Arc<Self> {
        Arc::new(Self { stop: CancellationToken::new(), terminal_shutdown, byte_budget: Arc::new(Semaphore::new(OUTGOING_BYTE_CAPACITY)) })
    }

    /// Ask the connection pump to stop. Its cleanup guard performs any current
    /// Node deregistration through the connector's registration registry.
    pub(crate) fn stop_connection(&self) { self.stop.cancel() }

    fn close_from_core(&self) {
        if let Some(shutdown) = &self.terminal_shutdown {
            shutdown.cancel();
        }
        self.stop_connection();
    }

    pub(crate) async fn stopped(&self) { self.stop.cancelled().await }

    #[cfg(test)]
    pub(crate) fn is_stopped(&self) -> bool { self.stop.is_cancelled() }
}

impl IrohPeerSender {
    pub(crate) fn new(recipient_node_id: proto::NodeId) -> (Self, mpsc::Receiver<OutgoingFrame>, Arc<ConnectionControl>) {
        Self::new_with_terminal_shutdown(recipient_node_id, None)
    }

    pub(crate) fn new_with_terminal_shutdown(
        recipient_node_id: proto::NodeId,
        terminal_shutdown: Option<CancellationToken>,
    ) -> (Self, mpsc::Receiver<OutgoingFrame>, Arc<ConnectionControl>) {
        let (tx, rx) = mpsc::channel(OUTGOING_QUEUE_CAPACITY);
        let control = ConnectionControl::new(terminal_shutdown);
        (Self { tx, recipient_node_id, control: control.clone() }, rx, control)
    }
}

#[async_trait]
impl PeerSender for IrohPeerSender {
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError> {
        debug!("Queuing message for peer {}", self.recipient_node_id);

        let data = proto::encode_message(&proto::Message::PeerMessage(message))
            .map_err(|e| SendError::Other(anyhow::anyhow!("Serialization error: {}", e)))?;
        if data.len() > crate::connection::MAX_FRAME_LENGTH {
            return Err(SendError::Other(anyhow::anyhow!(
                "Serialized message is {} bytes, exceeding the {} byte frame limit",
                data.len(),
                crate::connection::MAX_FRAME_LENGTH
            )));
        }

        let bytes =
            u32::try_from(data.len()).map_err(|_| SendError::Other(anyhow::anyhow!("Serialized message length does not fit u32")))?;
        let permit = self.control.byte_budget.clone().try_acquire_many_owned(bytes).map_err(|_| {
            warn!("Outgoing byte budget exhausted for peer {}, closing connection", self.recipient_node_id);
            self.control.stop_connection();
            SendError::Timeout
        })?;

        self.tx.try_send(OutgoingFrame { data, _permit: permit }).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                warn!("Outgoing queue full for peer {}, closing connection", self.recipient_node_id);
                self.control.stop_connection();
                SendError::Timeout
            }
            mpsc::error::TrySendError::Closed(_) => {
                warn!("Failed to send message to peer {} - channel closed", self.recipient_node_id);
                SendError::ConnectionClosed
            }
        })
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }

    fn close(&self) { self.control.close_from_core() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> proto::NodeMessage {
        proto::NodeMessage::UnsubscribeQuery { from: proto::NodeId::from_bytes([7; 32]), query_id: proto::QueryId::new() }
    }

    #[test]
    fn outgoing_queue_is_bounded_and_stops_the_connection_on_overflow() {
        use ed25519_dalek::Signer;
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let node_id = proto::NodeId::from(key.verifying_key());
        let session = proto::HandshakeChallenge::new(proto::NodeId::from_bytes([8; 32]), [9; 32]);
        let signed = || {
            let message = message();
            let signature = key.sign(&proto::SignedPeerMessage::signable_bytes(session, 0, &message)).into();
            proto::SignedPeerMessage { session, sequence: 0, message, signature }
        };
        let (sender, _rx, control) = IrohPeerSender::new(node_id);

        for _ in 0..OUTGOING_QUEUE_CAPACITY {
            sender.send_message(signed()).expect("queue slot should be available");
        }

        assert!(matches!(sender.send_message(signed()), Err(SendError::Timeout)));
        assert!(control.stop.is_cancelled());
    }

    #[test]
    fn core_close_is_terminal_for_a_dialer() {
        let shutdown = CancellationToken::new();
        let (sender, _rx, control) = IrohPeerSender::new_with_terminal_shutdown(proto::NodeId::from_bytes([7; 32]), Some(shutdown.clone()));

        sender.close();

        assert!(control.is_stopped());
        assert!(shutdown.is_cancelled());
    }
}
