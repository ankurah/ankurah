use ankurah_core::connector::{Handshake, RegistrationGuard, SendError};
use ankurah_proto as proto;

use super::sender::Socket;

pub enum Connection {
    /// Handshaking: frames go straight to the socket. The socket and this node's side of the
    /// handshake are taken when the dialer's presence arrives, to register the dialer and answer it.
    Initial { socket: Option<Socket>, handshake: Option<Handshake> },
    /// Registered: frames go out through `sender`, and `registration` deregisters the peer when
    /// the connection ends, however it ends.
    Established { sender: super::sender::WebSocketClientSender, registration: RegistrationGuard },
}

impl Connection {
    pub async fn send(&mut self, message: proto::Message) -> Result<(), SendError> {
        match self {
            Connection::Initial { socket, .. } => {
                use futures_util::SinkExt;
                if let Some(socket) = socket.as_mut() {
                    socket.send(axum::extract::ws::Message::Binary(message.encode().into())).await.map_err(|_| SendError::Unknown)?;
                    Ok(())
                } else {
                    Err(SendError::Unknown)
                }
            }
            Connection::Established { sender, .. } => sender.send_message(message),
        }
    }
}
