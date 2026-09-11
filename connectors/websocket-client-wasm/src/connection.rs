use crate::client::ClientInner;
use crate::connection_state::ConnectionState;
use ankurah::signals::Wait;
use ankurah_core::action_info;
use ankurah_core::connector::NodeComms;
use ankurah_core::connector::PeerConnectionError;
use ankurah_core::connector::PeerSender;
use ankurah_proto::{self as proto};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::{channel::mpsc, future::RemoteHandle, Future, FutureExt, StreamExt};
use js_sys::Uint8Array;
use send_wrapper::SendWrapper;
use std::cell::Cell;
use std::fmt;
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock};
use tracing::error;
use tracing::{info, warn};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

const MAX_QUEUED_MESSAGES: usize = 64;

#[derive(Clone)]
pub struct Connection(Arc<SendWrapper<ConnectionInner>>);

pub struct ConnectionInner {
    ws: Arc<WebSocket>,
    url: String,
    state: RwLock<ConnectionState>,
    node: Box<dyn NodeComms>,
    client: Weak<ClientInner>,
    registered_peer: Cell<Option<proto::EntityId>>,
    receive_task: Cell<Option<RemoteHandle<()>>>,
    _callbacks: Mutex<Option<Vec<Box<dyn std::any::Any>>>>,
}
impl std::ops::Deref for Connection {
    type Target = ConnectionInner;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "Connection") }
}

impl Connection {
    pub fn new(node: Box<dyn NodeComms>, url: String, client: Weak<ClientInner>, run: impl Wait<bool> + 'static) -> Result<Self, JsValue> {
        let url = if url.starts_with("ws://") || url.starts_with("wss://") { format!("{}/ws", url) } else { format!("wss://{}/ws", url) };

        let ws = WebSocket::new(&url)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let state = RwLock::new(ConnectionState::Connecting { url: url.clone() });
        let (mut incoming, mut messages) = mpsc::channel(MAX_QUEUED_MESSAGES);

        let me = Connection(Arc::new(SendWrapper::new(ConnectionInner {
            ws: Arc::new(ws),
            url,
            state,
            node,
            client,
            registered_peer: Cell::new(None),
            receive_task: Cell::new(None),
            _callbacks: Mutex::new(None),
        })));

        let on_message = {
            let weak = Arc::downgrade(&me.0);
            Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
                if incoming.try_send(event).is_err() {
                    if let Some(connection) = weak.upgrade().map(Connection) {
                        connection.disconnect();
                        connection.set_state(ConnectionState::Error { message: "Incoming message queue full".into(), cause: None });
                    }
                }
            }))
        };

        let on_error = {
            let weak = Arc::downgrade(&me.0);
            Closure::<dyn FnMut(ErrorEvent)>::wrap(Box::new(move |e| {
                if let Some(connection) = weak.upgrade().map(Connection) {
                    connection.handle_error(e);
                }
            }))
        };

        let on_close = {
            let weak = Arc::downgrade(&me.0);
            Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |e: CloseEvent| {
                if let Some(connection) = weak.upgrade().map(Connection) {
                    connection.handle_close(e);
                }
            }))
        };

        // Set up WebSocket event handlers
        me.ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        me.ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        me.ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        *me._callbacks.lock().unwrap() = Some(vec![Box::new(on_message), Box::new(on_error), Box::new(on_close)]);

        let weak = Arc::downgrade(&me.0);
        let client = me.client.clone();
        let (task, handle) = async move {
            // Peer registration may await local initialization; following frames stay queued until it finishes.
            let receive = async {
                while let Some(event) = messages.next().await {
                    let handshake = {
                        let Some(connection) = weak.upgrade().map(Connection) else { break };
                        if !connection.is_current() {
                            break;
                        }
                        connection.receive_message(event)
                    };
                    if let Some(handshake) = handshake {
                        handshake.await;
                    }
                }
            };
            futures::select_biased! {
                _ = run.wait_value(false).fuse() => {
                    if let Some(client) = client.upgrade() {
                        client.stop_if_node_halted();
                    }
                },
                _ = receive.fuse() => {},
            }
        }
        .remote_handle();
        me.receive_task.set(Some(handle));
        wasm_bindgen_futures::spawn_local(task);

        Ok(me)
    }

    fn set_state(&self, new_state: ConnectionState) -> bool {
        if let Ok(mut state) = self.state.write() {
            *state = new_state.clone();
        }
        if let Some(client) = self.client.upgrade() {
            client.handle_state_change(self, new_state)
        } else {
            false
        }
    }

    fn is_current(&self) -> bool {
        self.ws.ready_state() == WebSocket::OPEN && self.client.upgrade().is_some_and(|client| client.owns(self))
    }

    fn handle_close(&self, e: CloseEvent) {
        action_info!(self, "connection closed", "{}", &e.code());
        self.disconnect();
        self.set_state(ConnectionState::Closed);
    }

    fn handle_error(&self, _e: ErrorEvent) {
        info!("Connection error");
        // TODO - figure out how to get the error message. e.message() crashes because it's expected to be a string, but it's null
        self.disconnect();
        self.set_state(ConnectionState::Error { message: "Connection error".to_string(), cause: None });
    }

    /// Dispatch a browser message, returning any handshake work for the receive loop to await.
    fn receive_message(&self, e: MessageEvent) -> Option<impl Future<Output = ()> + 'static> {
        let array_buffer = if let Ok(array_buffer) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            array_buffer
        } else if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
            info!("Text message received (unexpected): {}", text);
            return None;
        } else {
            return None;
        };

        let array = Uint8Array::new(&array_buffer);
        let data = array.to_vec();

        if let Ok(message) = bincode::deserialize::<proto::Message>(&data) {
            match message {
                proto::Message::Presence(server_presence) => {
                    // Pre-check the version so the server learns why we are
                    // leaving; register_peer re-enforces this for every transport.
                    if !proto::protocol_compatible(server_presence.protocol_version) {
                        let rejection =
                            proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: server_presence.protocol_version };
                        error!("Refusing server {}: {}", self.url, rejection);
                        let _ = self.send_message(proto::Message::PresenceRejected(rejection.clone()));
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message: rejection.to_string(), cause: Some(rejection.into()) });
                        return None;
                    }
                    let state = { self.state.read().unwrap().clone() };
                    match state {
                        ConnectionState::Connected { .. } => warn!("Received duplicate server presence, ignoring"),
                        ConnectionState::Connecting { .. } => {
                            // Register BEFORE publishing Connected: observers of the
                            // state must never see a connection whose peer is not
                            // registered (or that registration is about to refuse).
                            let weak = Arc::downgrade(&self.0);
                            let node = self.node.cloned();
                            let sender = Box::new(WebSocketPeerSender {
                                recipient_node_id: server_presence.node_id,
                                ws: SendWrapper::new(self.ws.clone()),
                            });
                            return Some(async move {
                                let presence = match node.register_peer(server_presence.clone(), sender).await {
                                    Ok(()) => {
                                        if let Some(connection) = weak.upgrade().map(Connection) {
                                            connection.registered_peer.set(Some(server_presence.node_id));
                                        } else {
                                            node.deregister_peer(server_presence.node_id);
                                            return;
                                        }
                                        node.presence().await.map_err(PeerConnectionError::from)
                                    }
                                    Err(error) => Err(error),
                                };
                                let Some(connection) = weak.upgrade().map(Connection) else { return };
                                let presence = match presence {
                                    Ok(presence) => presence,
                                    Err(rejection) => {
                                        error!("Refusing server {}: {}", connection.url, rejection);
                                        connection.disconnect();
                                        connection
                                            .set_state(ConnectionState::Error { message: rejection.to_string(), cause: Some(rejection) });
                                        return;
                                    }
                                };
                                if !connection.is_current() {
                                    connection.disconnect();
                                    return;
                                }
                                // Advertise our root only after accepting the server's system.
                                if let Err(error) = connection.send_message(proto::Message::Presence(presence)) {
                                    connection.disconnect();
                                    connection.set_state(ConnectionState::Error {
                                        message: format!("Failed to send presence: {error:?}"),
                                        cause: None,
                                    });
                                    return;
                                }
                                if !connection.set_state(ConnectionState::Connected { url: connection.url.clone(), server_presence }) {
                                    connection.disconnect();
                                }
                            });
                        }
                        _ => {
                            warn!("Sanity error: received server presence while not in connecting state");
                            self.disconnect();
                            self.set_state(ConnectionState::Error {
                                message: "Received server presence, but not connected".to_string(),
                                cause: None,
                            });
                        }
                    }
                }
                proto::Message::PresenceRejected(rejection) => {
                    error!("Server {} refused connection: {}", self.url, rejection);
                    self.disconnect();
                    self.set_state(ConnectionState::Error { message: rejection.to_string(), cause: Some(rejection.into()) });
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
            let connecting = matches!(&*self.state.read().unwrap(), ConnectionState::Connecting { .. });
            if connecting {
                // A handshake we cannot read will never establish; close instead
                // of idling on a dead connection.
                let message = if proto::is_version0_presence(&data) {
                    format!("Server {} speaks a pre-versioning (0.9.x or older) protocol; refusing", self.url)
                } else {
                    format!("Failed to deserialize handshake message from {}; closing", self.url)
                };
                error!("{}", message);
                self.disconnect();
                self.set_state(ConnectionState::Error { message, cause: None });
            } else {
                warn!("Failed to deserialize message from server");
            }
        }
        None
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
    pub(crate) fn disconnect(&self) {
        self.receive_task.take();
        info!("Websocket disconnected from node {} to {}", self.node.id(), self.url);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        // Close the WebSocket connection with a normal closure (code 1000)
        let _ = self.ws.close();
        self._callbacks.lock().unwrap().take();
        if let Some(peer) = self.registered_peer.take() {
            self.node.deregister_peer(peer);
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
    recipient_node_id: proto::EntityId,
    ws: SendWrapper<Arc<WebSocket>>,
}

#[async_trait]
impl PeerSender for WebSocketPeerSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), ankurah_core::connector::SendError> {
        let message = proto::Message::PeerMessage(message);
        let data = bincode::serialize(&message).map_err(|e| {
            info!("Failed to serialize client message: {:?}", e);
            ankurah_core::connector::SendError::from(anyhow!("Serialization error"))
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

    fn recipient_node_id(&self) -> proto::EntityId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
