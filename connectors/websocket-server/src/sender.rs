use std::sync::Arc;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use axum::extract::ws::WebSocket;
use futures_util::stream::SplitSink;
use tokio::sync::mpsc;
use tracing::info;

// PeerSender for sending messages to a websocket client
#[derive(Clone)]
pub struct WebSocketClientSender {
    tx: mpsc::Sender<axum::extract::ws::Message>,
    pub(crate) inner: Arc<Inner>,
}
struct Inner {
    pub(crate) recipient_node_id: proto::NodeId,
    handle: Arc<tokio::task::JoinHandle<()>>,
}

impl WebSocketClientSender {
    pub fn new(
        node_id: proto::NodeId,
        mut sender: futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        use futures_util::SinkExt;
        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if sender.send(msg).await.is_err() {
                    // TODO: Handle this error - does this mean we're disconnected?
                    break;
                }
            }
        });
        Self { tx, inner: Arc::new(Inner { recipient_node_id: node_id, handle: Arc::new(handle) }) }
    }
    pub async fn send_message(&self, message: proto::Message) -> Result<(), SendError> {
        let data = bincode::serialize(&message).map_err(|e| SendError::Other(anyhow::anyhow!("Serialization error: {}", e)))?;

        self.tx.send(axum::extract::ws::Message::Binary(data.into())).await.map_err(|_| SendError::Unknown)?;

        Ok(())
    }
}

#[async_trait]
impl PeerSender for WebSocketClientSender {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let server_message = proto::Message::PeerMessage(message);
        self.send_message(server_message).await
    }
    fn recipient_node_id(&self) -> proto::NodeId { self.inner.recipient_node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

// LEFT OFF HERE:
// Heavily audit the Websocket client and peer code
// Update Node to consolidate pending requests into the peer map so unresolved requests get cleaned up when the node drops
// Implement timeouts for pending requests

impl Drop for Inner {
    fn drop(&mut self) {
        info!("Dropping WebSocketPeerSender Inner for {}", self.recipient_node_id);
        self.handle.abort();
    }
}
