use ankurah_core::connector::{Handshake, SendError};
use ankurah_core::node::ConnectionRegistration;
use ankurah_proto as proto;
use axum::extract::ws::WebSocket;
use futures_util::stream::SplitSink;

pub enum Connection {
    /// Handshaking. The sender and this node's side of the handshake are taken when the peer's
    /// presence, which must complete the handshake, arrives.
    Initial { sender: Option<SplitSink<WebSocket, axum::extract::ws::Message>>, handshake: Option<Handshake> },
    /// Registered: frames go out through `sender`, and `registration` is given back to
    /// deregister the peer when the connection ends.
    Established { sender: super::sender::WebSocketClientSender, registration: ConnectionRegistration },
}

impl Connection {
    pub async fn send(&mut self, message: proto::Message) -> Result<(), SendError> {
        match self {
            Connection::Initial { sender, .. } => {
                use futures_util::SinkExt;
                if let Some(sender) = sender.as_mut() {
                    sender.send(axum::extract::ws::Message::Binary(message.encode().into())).await.map_err(|_| SendError::Unknown)?;
                    Ok(())
                } else {
                    Err(SendError::Unknown)
                }
            }
            Connection::Established { sender, .. } => sender.send_message(message),
        }
    }
}
