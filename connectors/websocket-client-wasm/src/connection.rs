use crate::client::ClientInner;
use crate::connection_state::ConnectionState;
use ankurah_core::{connector::PeerSender, Node};
use ankurah_proto::{self as proto};
use anyhow::anyhow;
use async_trait::async_trait;
use js_sys::Uint8Array;
use send_wrapper::SendWrapper;
use std::sync::RwLock;
use std::sync::{Arc, Weak};
use tracing::{info, warn};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, Event, MessageEvent, WebSocket};

struct Inner {
    ws: Arc<WebSocket>,
    url: String,
    state: RwLock<ConnectionState>,
    node: Arc<Node>,
    client: Weak<ClientInner>,
}

impl Inner {
    fn new(node: Arc<Node>, url: String, client: Weak<ClientInner>) -> Result<Self, JsValue> {
        let url = if url.starts_with("ws://") || url.starts_with("wss://") { format!("{}/ws", url) } else { format!("wss://{}/ws", url) };

        let ws = WebSocket::new(&url)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
        let state = RwLock::new(ConnectionState::Connecting { url: url.clone() });

        Ok(Self { ws: Arc::new(ws), url, state, node, client })
    }

    fn set_state(&self, new_state: ConnectionState) {
        if let Ok(mut state) = self.state.write() {
            *state = new_state.clone();
        }
        if let Some(client) = self.client.upgrade() {
            client.handle_state_change(new_state);
        }
    }

    fn get_state(&self) -> ConnectionState { self.state.read().map(|state| state.clone()).unwrap_or(ConnectionState::None) }

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

        if let Ok(message) = bincode::deserialize::<proto::Message>(&data) {
            match message {
                proto::Message::Presence(presence) => {
                    info!("Received server presence");
                    self.set_state(ConnectionState::Connected { url: self.url.clone(), presence });
                }
                proto::Message::PeerMessage(msg) => {
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

    fn send_message(&self, message: proto::Message) -> Result<(), JsValue> {
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
        let presence = proto::Presence { node_id: self.node.id.clone(), durable: self.node.durable };

        if let Err(e) = self.send_message(proto::Message::Presence(presence)) {
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
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.inner, &other.inner) }
}

#[derive(Clone)]
pub struct WSClientPeerSender {
    inner: Arc<SendWrapper<WebSocketInner>>,
}

struct WebSocketInner {
    ws: Arc<WebSocket>,
    node_id: proto::NodeId,
}

#[async_trait]
impl PeerSender for WSClientPeerSender {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), ankurah_core::connector::SendError> {
        let message = proto::Message::PeerMessage(message);
        let data = bincode::serialize(&message).map_err(|e| {
            info!("Failed to serialize client message: {:?}", e);
            ankurah_core::connector::SendError::Other(anyhow!("Serialization error"))
        })?;

        let array = Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(&data);
        self.inner.ws.send_with_array_buffer(&array.buffer()).map_err(|_| ankurah_core::connector::SendError::ConnectionClosed)?;
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.inner.node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

impl Connection {
    pub(crate) fn new(server_url: String, my_node: Arc<Node>, client: Weak<ClientInner>) -> Result<Connection, JsValue> {
        let inner = Arc::new(SendWrapper::new(Inner::new(my_node, server_url, client)?));

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
                inner.set_state(ConnectionState::Error { message: "".to_string() });
            }))
        };

        let on_close = {
            let inner = inner.clone();
            Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |e: CloseEvent| {
                info!("Connection closed: {}", e.code());
                inner.set_state(ConnectionState::Closed);
            }))
        };

        let on_open = {
            let inner = inner.clone();
            Closure::<dyn FnMut()>::wrap(Box::new(move || {
                inner.handle_open();
            }))
        };

        // Set up WebSocket event handlers
        inner.ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        inner.ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        inner.ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        inner.ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        Ok(Connection {
            inner,
            _callbacks: Arc::new(SendWrapper::new(vec![Box::new(on_message), Box::new(on_error), Box::new(on_close), Box::new(on_open)])),
        })
    }

    pub fn state(&self) -> ConnectionState { self.inner.get_state() }

    pub fn create_peer_sender(&self, node_id: proto::NodeId) -> WSClientPeerSender {
        WSClientPeerSender { inner: Arc::new(SendWrapper::new(WebSocketInner { ws: self.inner.ws.clone(), node_id })) }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Clean up WebSocket event handlers
        self.inner.ws.set_onmessage(None);
        self.inner.ws.set_onerror(None);
        self.inner.ws.set_onclose(None);
        self.inner.ws.set_onopen(None);

        // Close the WebSocket connection with a normal closure (code 1000)
        let _ = self.inner.ws.close();
    }
}
