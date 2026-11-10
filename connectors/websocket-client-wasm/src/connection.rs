use crate::client::ClientInner;
use crate::connection_state::ConnectionState;
use ankurah::signals::Wait;
use ankurah_core::action_info;
use ankurah_core::connector::Handshake;
use ankurah_core::connector::NodeComms;
use ankurah_core::connector::PeerConnectionError;
use ankurah_core::connector::PeerProvenance;
use ankurah_core::connector::PeerSender;
use ankurah_core::node::ConnectionRegistration;
use ankurah_proto::{self as proto};

use async_trait::async_trait;
use futures::{channel::mpsc, future::RemoteHandle, Future, FutureExt, StreamExt};
use js_sys::Uint8Array;
use send_wrapper::SendWrapper;
use std::cell::{Cell, RefCell};
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
    /// This node's side of the handshake; taken when the server's presence arrives.
    handshake: RefCell<Option<Handshake>>,
    /// This node's registration of the server, given back to deregister it on disconnect.
    registration: Cell<Option<ConnectionRegistration>>,
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

        let handshake = RefCell::new(Some(node.begin_handshake()));
        let me = Connection(Arc::new(SendWrapper::new(ConnectionInner {
            ws: Arc::new(ws),
            url,
            state,
            node,
            client,
            handshake,
            registration: Cell::new(None),
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

        // The server's first frame is its opening.
        if self.handshake.borrow().as_ref().is_some_and(|handshake| !handshake.opened()) {
            self.answer_opening(&data);
            return None;
        }

        if let Ok(message) = proto::Message::decode(&data) {
            match message {
                proto::Message::Open(_) => warn!("Ignoring repeated opening from {}", self.url),
                proto::Message::NoCommonVersion(theirs) => {
                    let refusal = PeerConnectionError::NoCommonVersion { ours: proto::ProtocolVersions::SPOKEN, theirs };
                    error!("Server {} refused connection: {}", self.url, refusal);
                    self.disconnect();
                    self.set_state(ConnectionState::Error { message: refusal.to_string(), cause: Some(refusal) });
                }
                proto::Message::Presence(server_presence) => {
                    let state = { self.state.read().unwrap().clone() };
                    match state {
                        ConnectionState::Connected { .. } => warn!("Received duplicate server presence, ignoring"),
                        ConnectionState::Connecting { .. } => {
                            let Some(handshake) = self.handshake.borrow_mut().take() else {
                                warn!("Received duplicate server presence, ignoring");
                                return None;
                            };
                            // Register BEFORE publishing Connected: observers of the
                            // state must never see a connection whose peer is not
                            // registered (or that registration is about to refuse).
                            let weak = Arc::downgrade(&self.0);
                            let node = self.node.cloned();
                            let server = server_presence.claims.node_id;
                            let sender = Box::new(WebSocketPeerSender { recipient_node_id: server, ws: SendWrapper::new(self.ws.clone()) });
                            return Some(async move {
                                // Signed before registration spends the handshake.
                                let registered = match node.presence(&handshake).await {
                                    Ok(presence) => node
                                        .register_peer(server_presence.clone(), handshake, PeerProvenance::Configured, sender)
                                        .await
                                        .map(|registration| (registration, presence)),
                                    Err(error) => Err(error),
                                };
                                let Some(connection) = weak.upgrade().map(Connection) else {
                                    if let Ok((registration, _)) = registered {
                                        node.deregister_peer(registration);
                                    }
                                    return;
                                };
                                let presence = match registered {
                                    Ok((registration, presence)) => {
                                        connection.registration.set(Some(registration));
                                        presence
                                    }
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
                                // Send our presence only once the server is registered, so its frames never arrive first.
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
                proto::Message::PeerMessage(msg) => {
                    let Some(registration) = self.registration.get() else {
                        // As the native client: nothing an unregistered server says may reach the node.
                        let message = format!("Server {} sent application traffic before its presence registered; closing", self.url);
                        error!("{}", message);
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message, cause: None });
                        return None;
                    };
                    let node = self.node.cloned();
                    // TODO: determine the performance implications of spawning a new task for each message
                    // versus using a channel to send messages to the node.
                    wasm_bindgen_futures::spawn_local(async move {
                        if let Err(e) = node.handle_message(registration, msg).await {
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
                let message = format!("Failed to deserialize handshake message from {}; closing", self.url);
                error!("{}", message);
                self.disconnect();
                self.set_state(ConnectionState::Error { message, cause: None });
            } else {
                warn!("Failed to deserialize message from server");
            }
        }
        None
    }

    /// Answer the server's first frame, its opening, which arrives once the socket is open: send
    /// this node's opening, then read the server's, and close with the versions this node speaks
    /// when the two share none.
    fn answer_opening(&self, frame: &[u8]) {
        let Some((opening, read)) =
            self.handshake.borrow_mut().as_mut().map(|handshake| (handshake.opening(), handshake.read_opening(frame)))
        else {
            return;
        };
        let refusal = match self.send_message(proto::Message::Open(opening)) {
            Ok(()) => match read {
                Ok(()) => return,
                Err(refusal) => refusal,
            },
            Err(error) => {
                self.disconnect();
                self.set_state(ConnectionState::Error { message: format!("Failed to send opening: {error:?}"), cause: None });
                return;
            }
        };
        error!("Refusing server {}: {}", self.url, refusal);
        if let PeerConnectionError::NoCommonVersion { ours, .. } = refusal {
            // Best effort: the connection closes either way.
            let _ = self.send_message(proto::Message::NoCommonVersion(ours));
        }
        self.disconnect();
        self.set_state(ConnectionState::Error { message: refusal.to_string(), cause: Some(refusal) });
    }

    fn send_message(&self, message: proto::Message) -> Result<(), JsValue> {
        let data = message.encode();
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
        if let Some(registration) = self.registration.take() {
            self.node.deregister_peer(registration);
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
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), ankurah_core::connector::SendError> {
        let data = proto::Message::PeerMessage(message).encode();
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

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
