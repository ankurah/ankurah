use crate::client::ClientInner;
use crate::connection_state::ConnectionState;
use ankurah_core::connector::NodeComms;
use ankurah_core::connector::PeerSender;
use ankurah_proto::{self as proto};
use anyhow::anyhow;
use async_trait::async_trait;
use js_sys::Uint8Array;
use send_wrapper::SendWrapper;
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock};
use tracing::{info, warn};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, ErrorEvent, Event, MessageEvent, WebSocket};

#[derive(Clone)]
pub struct Connection(Arc<SendWrapper<ConnectionInner>>);

pub struct ConnectionInner {
    ws: Arc<WebSocket>,
    url: String,
    state: RwLock<ConnectionState>,
    node: Box<dyn NodeComms>,
    client: Weak<ClientInner>,
    _callbacks: Mutex<Option<Vec<Box<dyn std::any::Any>>>>,
}
impl std::ops::Deref for Connection {
    type Target = ConnectionInner;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Connection {
    pub fn new(node: Box<dyn NodeComms>, url: String, client: Weak<ClientInner>) -> Result<Self, JsValue> {
        let url = if url.starts_with("ws://") || url.starts_with("wss://") { format!("{}/ws", url) } else { format!("wss://{}/ws", url) };

        let ws = WebSocket::new(&url)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let state = RwLock::new(ConnectionState::Connecting { url: url.clone() });

        let me = Connection(Arc::new(SendWrapper::new(ConnectionInner {
            ws: Arc::new(ws),
            url,
            state,
            node,
            client,
            _callbacks: Mutex::new(None),
        })));

        let on_message = {
            let me = me.clone();
            Closure::<dyn Fn(MessageEvent)>::wrap(Box::new(move |e| me.receive_message(e)))
        };

        let on_error = {
            let me = me.clone();
            Closure::<dyn FnMut(ErrorEvent)>::wrap(Box::new(move |e| me.handle_error(e)))
        };

        let on_close = {
            let me = me.clone();
            Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |e: CloseEvent| me.handle_close(e)))
        };

        let on_open = {
            let me = me.clone();
            Closure::<dyn FnMut(Event)>::wrap(Box::new(move |e| me.handle_open(e)))
        };

        // Set up WebSocket event handlers
        me.ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        me.ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        me.ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        me.ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        *me._callbacks.lock().unwrap() = Some(vec![Box::new(on_message), Box::new(on_error), Box::new(on_close), Box::new(on_open)]);

        Ok(me)
    }

    fn set_state(&self, new_state: ConnectionState) -> bool {
        if let Ok(mut state) = self.state.write() {
            *state = new_state.clone();
        }
        if let Some(client) = self.client.upgrade() {
            client.handle_state_change(&self, new_state)
        } else {
            false
        }
    }

    // fn get_state(&self) -> ConnectionState { self.state.read().map(|state| state.clone()).unwrap_or(ConnectionState::None) }

    fn handle_open(&self, e: Event) {
        info!("Connection opened (event): {:?}", e);
    }
    fn handle_close(&self, e: CloseEvent) {
        info!("Connection closed: {}", e.code());
        self.set_state(ConnectionState::Closed);
    }

    fn handle_error(&self, _e: ErrorEvent) {
        info!("Connection error");
        // TODO - figure out how to get the error message. e.message() crashes because it's expected to be a string, but it's null
        self.set_state(ConnectionState::Error { message: "Connection error".to_string() });
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

        if let Ok(message) = bincode::deserialize::<proto::Message>(&data) {
            match message {
                proto::Message::Presence(server_presence) => {
                    let state = { self.state.read().unwrap().clone() };
                    match state {
                        ConnectionState::Connected { .. } => warn!("Received duplicate server presence, ignoring"),
                        ConnectionState::Connecting { .. } => {
                            info!("Received server presence, {server_presence:?}");
                            if self
                                .set_state(ConnectionState::Connected { url: self.url.clone(), server_presence: server_presence.clone() })
                            {
                                self.node.register_peer(
                                    server_presence.clone(),
                                    Box::new(WebSocketPeerSender {
                                        recipient_node_id: server_presence.node_id,
                                        ws: SendWrapper::new(self.ws.clone()),
                                    }),
                                );
                                let presence = proto::Presence { node_id: self.node.id(), durable: self.node.durable() };
                                // Send our presence message
                                if let Err(e) = self.send_message(proto::Message::Presence(presence)) {
                                    info!("Failed to send presence message: {:?}", e);
                                }
                            }
                        }
                        _ => {
                            warn!("Sanity error: received server presence while not in connecting state");
                            self.disconnect();
                            self.set_state(ConnectionState::Error { message: "Received server presence, but not connected".to_string() });
                        }
                    }
                }
                proto::Message::PeerMessage(msg) => {
                    let node = self.node.cloned();
                    // TODO: determine the performance implications of spawning a new task for each message
                    // versus using a channel to send messages to the node.
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
}

impl ConnectionInner {
    fn disconnect(&self) {
        info!("Websocket disconnected from node {} to {}", self.node.id(), self.url);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        self.ws.set_onopen(None);
        // Close the WebSocket connection with a normal closure (code 1000)
        let _ = self.ws.close();
        self._callbacks.lock().unwrap().take();
        if let ConnectionState::Connected { server_presence, .. } = &*self.state.read().unwrap() {
            self.node.deregister_peer(server_presence.node_id.clone());
        }
    }
}
impl Drop for ConnectionInner {
    fn drop(&mut self) {
        // Clean up WebSocket event handlers
        self.disconnect();
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

#[derive(Clone)]
struct WebSocketPeerSender {
    recipient_node_id: proto::NodeId,
    ws: SendWrapper<Arc<WebSocket>>,
}

#[async_trait]
impl PeerSender for WebSocketPeerSender {
    async fn send_message(&self, message: proto::NodeMessage) -> Result<(), ankurah_core::connector::SendError> {
        let message = proto::Message::PeerMessage(message);
        let data = bincode::serialize(&message).map_err(|e| {
            info!("Failed to serialize client message: {:?}", e);
            ankurah_core::connector::SendError::Other(anyhow!("Serialization error"))
        })?;

        let array = Uint8Array::new_with_length(data.len() as u32);
        array.copy_from(&data);
        match self.ws.send_with_array_buffer(&array.buffer()) {
            Ok(_) => Ok(()),
            Err(e) => {
                info!("Connection failed to send message: {:?}", e);
                Err(ankurah_core::connector::SendError::ConnectionClosed)
            }
        }
    }

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient_node_id.clone() }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
