use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error};

// PeerSender for sending messages to a websocket client
#[derive(Clone)]
pub struct WebSocketClientSender {
    tx: mpsc::Sender<axum::extract::ws::Message>,
    inner: Arc<Inner>,
}
struct Inner {
    pub(crate) recipient_node_id: proto::NodeId,
    handle: Arc<tokio::task::JoinHandle<()>>,
    close_tx: watch::Sender<bool>,
    closed: AtomicBool,
}

impl WebSocketClientSender {
    pub fn new(
        node_id: proto::NodeId,
        mut sender: futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>,
    ) -> (Self, watch::Receiver<bool>) {
        let (tx, mut rx) = mpsc::channel(32);
        let (close_tx, mut writer_close_rx) = watch::channel(false);
        let connection_close_rx = close_tx.subscribe();
        use futures_util::SinkExt;
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    changed = writer_close_rx.changed() => {
                        if changed.is_err() || *writer_close_rx.borrow_and_update() {
                            let _ = tokio::time::timeout(
                                Duration::from_millis(100),
                                sender.send(axum::extract::ws::Message::Close(None)),
                            ).await;
                            break;
                        }
                    }
                    msg = rx.recv() => {
                        let Some(msg) = msg else { break; };
                        if sender.send(msg).await.is_err() {
                            error!("Failed to send message through websocket, breaking send loop");
                            break;
                        }
                    }
                }
            }
            debug!("WebSocket sender task completed");
        });
        (
            Self {
                tx,
                inner: Arc::new(Inner { recipient_node_id: node_id, handle: Arc::new(handle), close_tx, closed: AtomicBool::new(false) }),
            },
            connection_close_rx,
        )
    }

    #[tracing::instrument(skip(self, message), fields(recipient = %self.inner.recipient_node_id, msg = %message))]
    pub fn send_message(&self, message: proto::Message) -> Result<(), SendError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::ConnectionClosed);
        }
        debug!("Serializing message");
        let data = proto::encode_message(&message).map_err(|e| SendError::Other(anyhow::anyhow!("Serialization error: {}", e)))?;

        debug!(bytes = data.len(), "Sending message through channel");
        self.tx.try_send(axum::extract::ws::Message::Binary(data.into())).map_err(|_| SendError::Unknown)?;
        debug!("Message sent successfully");

        Ok(())
    }
}

#[async_trait]
impl PeerSender for WebSocketClientSender {
    #[tracing::instrument(skip(self, message), fields(recipient = %self.inner.recipient_node_id))]
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), SendError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(SendError::ConnectionClosed);
        }
        let server_message = proto::Message::PeerMessage(message);
        self.send_message(server_message)
    }
    fn recipient_node_id(&self) -> proto::NodeId { self.inner.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }

    fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            self.inner.close_tx.send_replace(true);
        }
    }
}

// Heavily audit the Websocket client and peer code
// Update Node to consolidate pending requests into the peer map so unresolved requests get cleaned up when the node drops
// Implement timeouts for pending requests

impl Drop for Inner {
    fn drop(&mut self) {
        debug!("Dropping WebSocketPeerSender Inner for {}", self.recipient_node_id);
        self.handle.abort();
    }
}
