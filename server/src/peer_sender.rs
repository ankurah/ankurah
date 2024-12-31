use ankurah_core::connector::{PeerSender, SendError};
use ankurah_proto as proto;
use async_trait::async_trait;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use futures_util::stream::SplitSink;
use tokio::sync::mpsc;
pub enum SenderKind {
    Initial(Option<SplitSink<WebSocket, Message>>),
    Peer(WebSocketPeerSender),
}

// Add this new struct to handle peer sending
#[derive(Clone)]
pub struct WebSocketPeerSender {
    node_id: proto::NodeId,
    tx: mpsc::Sender<axum::extract::ws::Message>,
}

impl WebSocketPeerSender {
    pub fn new(
        node_id: proto::NodeId,
        mut sender: futures_util::stream::SplitSink<
            axum::extract::ws::WebSocket,
            axum::extract::ws::Message,
        >,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(32);
        use futures_util::SinkExt;
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if sender.send(msg).await.is_err() {
                    // TODO: Handle this error - does this mean we're disconnected?
                    break;
                }
            }
        });

        Self { node_id, tx }
    }
    pub async fn send_message(&self, message: proto::ServerMessage) -> Result<(), SendError> {
        let data = bincode::serialize(&message)
            .map_err(|e| SendError::Other(anyhow::anyhow!("Serialization error: {}", e)))?;

        self.tx
            .send(axum::extract::ws::Message::Binary(data))
            .await
            .map_err(|_| SendError::Unknown)?;

        Ok(())
    }
}

#[async_trait]
impl PeerSender for WebSocketPeerSender {
    fn node_id(&self) -> proto::NodeId {
        self.node_id.clone()
    }

    async fn send_message(&self, message: proto::PeerMessage) -> Result<(), SendError> {
        let server_message = proto::ServerMessage::PeerMessage(message);
        self.send_message(server_message).await
    }

    fn cloned(&self) -> Box<dyn PeerSender> {
        Box::new(self.clone())
    }
}
