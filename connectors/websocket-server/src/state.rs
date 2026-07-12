use ankurah_core::connector::SendError;
use ankurah_proto as proto;
use axum::extract::ws::WebSocket;
use futures_util::stream::SplitSink;

pub enum Connection {
    Initial {
        sender: Option<SplitSink<WebSocket, axum::extract::ws::Message>>,
        handshake: Option<ankurah_core::connector::PeerHandshake>,
        outgoing_session: Option<proto::HandshakeChallenge>,
    },
    Established {
        peer_sender: super::sender::WebSocketClientSender,
        close_rx: tokio::sync::watch::Receiver<bool>,
        /// Challenge this server issued for the registration owned by this
        /// transport. Cleanup must present the same session capability.
        incoming_session: proto::HandshakeChallenge,
    },
}

impl Connection {
    pub async fn close_requested(&mut self) {
        let Connection::Established { close_rx, .. } = self else {
            std::future::pending::<()>().await;
            return;
        };
        loop {
            if *close_rx.borrow_and_update() {
                return;
            }
            if close_rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub async fn send(&mut self, message: proto::Message) -> Result<(), SendError> {
        match self {
            Connection::Initial { sender, .. } => {
                if let Ok(data) = proto::encode_message(&message) {
                    use futures_util::SinkExt;
                    if let Some(sender) = sender.as_mut() {
                        sender.send(axum::extract::ws::Message::Binary(data.into())).await.map_err(|_| SendError::Unknown)?;
                        Ok(())
                    } else {
                        Err(SendError::Unknown)
                    }
                } else {
                    Err(SendError::Other(anyhow::anyhow!("Serialization error")))
                }
            }
            Connection::Established { peer_sender, .. } => peer_sender.send_message(message),
        }
    }
}
