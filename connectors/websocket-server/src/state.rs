use ankurah_core::connector::{IssuedChallenge, SendError};
use ankurah_proto as proto;
use axum::extract::ws::WebSocket;
use futures_util::stream::SplitSink;

pub enum Connection {
    /// Handshaking. `answered` records that this node answered the peer's one challenge; the
    /// sender and `challenge` are taken when the peer's presence, which must answer it, arrives.
    Initial {
        sender: Option<SplitSink<WebSocket, axum::extract::ws::Message>>,
        challenge: Option<IssuedChallenge>,
        answered: bool,
    },
    Established(super::sender::WebSocketClientSender),
}

impl Connection {
    pub async fn send(&mut self, message: proto::Message) -> Result<(), SendError> {
        match self {
            Connection::Initial { sender, .. } => {
                if let Ok(data) = bincode::serialize(&message) {
                    use futures_util::SinkExt;
                    if let Some(sender) = sender.as_mut() {
                        sender.send(axum::extract::ws::Message::Binary(data.into())).await.map_err(|_| SendError::Unknown)?;
                        Ok(())
                    } else {
                        Err(SendError::Unknown)
                    }
                } else {
                    Err(SendError::from(anyhow::anyhow!("Serialization error")))
                }
            }
            Connection::Established(peer_sender) => peer_sender.send_message(message),
        }
    }
}
