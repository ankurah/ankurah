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
/// Serializes and queues outbound `NodeMessage`s on a bounded channel. The
/// connection pump drains the channel and writes each message as a
/// length-delimited frame on the connection's bidirectional QUIC stream.
#[derive(Clone)]
pub struct IrohPeerSender {
    tx: mpsc::Sender<OutgoingFrame>,
    recipient_node_id: proto::EntityId,
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
    byte_budget: Arc<Semaphore>,
}

impl ConnectionControl {
    fn new() -> Arc<Self> {
        Arc::new(Self { stop: CancellationToken::new(), byte_budget: Arc::new(Semaphore::new(OUTGOING_BYTE_CAPACITY)) })
    }

    /// Ask the connection pump to stop. Its cleanup guard performs any current
    /// Node deregistration through the connector's registration registry.
    pub(crate) fn stop_connection(&self) { self.stop.cancel() }

    pub(crate) async fn stopped(&self) { self.stop.cancelled().await }

    #[cfg(test)]
    pub(crate) fn is_stopped(&self) -> bool { self.stop.is_cancelled() }
}

impl IrohPeerSender {
    pub(crate) fn new(recipient_node_id: proto::EntityId) -> (Self, mpsc::Receiver<OutgoingFrame>, Arc<ConnectionControl>) {
        let (tx, rx) = mpsc::channel(OUTGOING_QUEUE_CAPACITY);
        let control = ConnectionControl::new();
        (Self { tx, recipient_node_id, control: control.clone() }, rx, control)
    }
}

#[async_trait]
impl PeerSender for IrohPeerSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        debug!("Queuing message for peer {}", self.recipient_node_id);

        let data = bincode::serialize(&proto::Message::PeerMessage(message))
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

    fn recipient_node_id(&self) -> proto::EntityId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> proto::NodeMessage {
        proto::NodeMessage::UnsubscribeQuery { from: proto::EntityId::new(), query_id: proto::QueryId::new() }
    }

    #[test]
    fn outgoing_queue_is_bounded_and_stops_the_connection_on_overflow() {
        let (sender, _rx, control) = IrohPeerSender::new(proto::EntityId::new());

        for _ in 0..OUTGOING_QUEUE_CAPACITY {
            sender.send_message(message()).expect("queue slot should be available");
        }

        assert!(matches!(sender.send_message(message()), Err(SendError::Timeout)));
        assert!(control.stop.is_cancelled());
    }
}
