use crate::connection_state::ConnectionState;
use ankurah_core::{connector::PeerSender, Node};
use ankurah_proto::{self as proto, Presence};
use async_trait::async_trait;
use js_sys::Uint8Array;
use reactive_graph::prelude::*;
use send_wrapper::SendWrapper;
use std::sync::Arc;
use tracing::{info, warn};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, Event, MessageEvent, WebSocket};

struct Inner {
    ws: Arc<WebSocket>,
    url: String,
    state: reactive_graph::signal::RwSignal<ConnectionState>,
    node: Arc<Node>,
}

impl Inner {
    fn new(node: Arc<Node>, url: String) -> Result<Self, JsValue> {
        let url = if url.starts_with("ws://") || url.starts_with("wss://") {
            format!("{}/ws", url)
        } else {
            format!("wss://{}/ws", url)
        };

        let ws = WebSocket::new(&url)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
        let state =
            reactive_graph::signal::RwSignal::new(ConnectionState::Connecting { url: url.clone() });

        Ok(Self {
            ws: Arc::new(ws),
            url,
            state,
            node,
        })
    }
    fn receive_message(&self, e: MessageEvent) {
        let array_buffer = if let Ok(array_buffer) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            array_buffer
        } else if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
            info!("Text message received (unexpected): {}", text);
            return;
        } else {
            return;
        };

        let array = Uint8Array::new(&array_buffer);
        let data = array.to_vec();

        if let Ok(message) = bincode::deserialize::<proto::ServerMessage>(&data) {
            match message {
                proto::ServerMessage::Presence(Presence { node_id }) => {
                    info!("Received server presence");
                    self.state.set(
                        ConnectionState::Connected {
                            url: self.url.clone(),
                            node_id,
                        },
                    );
                }
                proto::ServerMessage::PeerMessage(msg) => {
                    let node = self.node.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        if let Err(e) = node.handle_message(msg).await {
                            info!("Error handling message: {:?}", e);
                        }
                    });
                }
            }
        } else {
            warn!("Failed to deserialize message from server");
        }
    }
    fn send_message(&self, message: proto::ClientMessage) -> Result<(), JsValue> {
        let data = bincode::serialize(&message).map_err(|e| {
            info!("Failed to serialize client message: {:?}", e);
            JsValue::from_str("Serialization error")
        })?;

        let array = Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(&data);
        self.ws.send_with_array_buffer(&array.buffer())?;
        Ok(())
    }

    fn handle_open(&self) {
        info!("Connection opened (event)");

        // Send our presence message
        let presence = proto::Presence {
            node_id: self.node.id.clone(),
        };

        if let Err(e) = self.send_message(proto::ClientMessage::Presence(presence)) {
            info!("Failed to send presence message: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct Connection {
    inner: Arc<SendWrapper<Inner>>,
    _callbacks: Arc<SendWrapper<Vec<Box<dyn std::any::Any>>>>,
}
impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[async_trait]
impl PeerSender for Connection {
    fn node_id(&self) -> proto::NodeId {
        self.inner.node.id.clone()
    }
    async fn send_message(
        &self,
        message: proto::PeerMessage,
    ) -> Result<(), ankurah_core::connector::SendError> {
        self.send_message(message)
            .map_err(|_| ankurah_core::connector::SendError::ConnectionClosed)
    }

    fn cloned(&self) -> Box<dyn PeerSender> {
        Box::new(self.clone())
    }
}

impl Connection {
    pub fn new(server_url: String, node: Arc<Node>) -> Result<Connection, JsValue> {
        let inner = Arc::new(SendWrapper::new(Inner::new(node, server_url)?));

        let on_message = {
            let inner = inner.clone();
            Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |e: MessageEvent| {
                inner.receive_message(e);
            }))
        };

        let on_error = {
            let inner = inner.clone();
            Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_| {
                info!("Connection Error");
                inner.state.set(
                    ConnectionState::Error {
                        message: "".to_string(),
                    },
                );
            }))
        };

        let on_close = {
            let inner = inner.clone();
            Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |e: CloseEvent| {
                info!("Connection closed: {}", e.code());
                inner.state.set(ConnectionState::Closed);
            }))
        };

        let on_open = {
            let inner = inner.clone();
            Closure::<dyn FnMut()>::wrap(Box::new(move || {
                inner.handle_open();
            }))
        };

        // Set up WebSocket event handlers
        inner
            .ws
            .set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        inner
            .ws
            .set_onerror(Some(on_error.as_ref().unchecked_ref()));
        inner
            .ws
            .set_onclose(Some(on_close.as_ref().unchecked_ref()));
        inner.ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        on_close.forget();
        on_open.forget();
        on_error.forget();
        on_message.forget();

        Ok(Connection {
            inner,
            _callbacks: Arc::new(SendWrapper::new(vec![
                // Box::new(on_message),
                // Box::new(on_error),
                // Box::new(on_close),
                // Box::new(on_open),
            ])),
        })
    }

    pub fn state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> {
        self.inner.state.read_only()
    }

    pub fn send_message(&self, message: proto::PeerMessage) -> Result<(), JsValue> {
        let message = proto::ClientMessage::PeerMessage(message);
        self.inner.send_message(message)
    }
}

// impl Drop for Inner {
//     fn drop(&mut self) {
//         // Close the WebSocket connection with a normal closure (code 1000)
//         if let Err(e) = self.ws.close() {
//             warn!("Failed to close WebSocket connection: {:?}", e);
//         }
//     }
// }
