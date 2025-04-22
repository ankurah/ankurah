use std::sync::Arc;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, error};

// PeerSender for sending messages to a websocket client
#[derive(Clone)]
pub struct WebSocketClientSender {
    tx: mpsc::Sender<axum::extract::ws::Message>,
    inner: Arc<Inner>,
}
struct Inner {
    pub(crate) recipient_node_id: proto::EntityId,
    handle: Arc<tokio::task::JoinHandle<()>>,
}

impl WebSocketClientSender {
    pub fn new(
        node_id: proto::EntityId,
        mut sender: futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        use futures_util::SinkExt;
        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if sender.send(msg).await.is_err() {
                    error!("Failed to send message through websocket, breaking send loop");
                    break;
                }
            }
            debug!("WebSocket sender task completed");
        });
        Self { tx, inner: Arc::new(Inner { recipient_node_id: node_id, handle: Arc::new(handle) }) }
    }

    #[tracing::instrument(skip(self, message), fields(recipient = %self.inner.recipient_node_id, msg = %message))]
    pub fn send_message(&self, message: proto::Message) -> Result<(), SendError> {
        debug!("Serializing message");
        let data = bincode::serialize(&message).map_err(|e| SendError::Other(anyhow::anyhow!("Serialization error: {}", e)))?;

        debug!(bytes = data.len(), "Sending message through channel");
        self.tx.try_send(axum::extract::ws::Message::Binary(data.into())).map_err(|_| SendError::Unknown)?;
        debug!("Message sent successfully");

        Ok(())
    }
}

#[async_trait]
impl PeerSender for WebSocketClientSender {
    #[tracing::instrument(skip(self, message), fields(recipient = %self.inner.recipient_node_id))]
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let server_message = proto::Message::PeerMessage(message);
        self.send_message(server_message)
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.inner.recipient_node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

// LEFT OFF HERE:
// Heavily audit the Websocket client and peer code
// Update Node to consolidate pending requests into the peer map so unresolved requests get cleaned up when the node drops
// Implement timeouts for pending requests

impl Drop for Inner {
    fn drop(&mut self) {
        debug!("Dropping WebSocketPeerSender Inner for {}", self.recipient_node_id);
        self.handle.abort();
    }
}
