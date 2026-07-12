use crate::client::ClientInner;
use crate::connection_state::ConnectionState;
use ankurah_core::action_info;
use ankurah_core::connector::NodeComms;
use ankurah_core::connector::PeerHandshake;
use ankurah_core::connector::PeerSender;
use ankurah_proto::{self as proto};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable};
use js_sys::Uint8Array;
use send_wrapper::SendWrapper;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock};
use tracing::error;
use tracing::{info, warn};
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, ErrorEvent, Event, MessageEvent, WebSocket};

const DISPATCH_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct Connection(Arc<SendWrapper<ConnectionInner>>);

pub struct ConnectionInner {
    ws: Arc<WebSocket>,
    url: String,
    state: RwLock<ConnectionState>,
    node: Box<dyn NodeComms>,
    /// Challenge this client issued for the registration owned by this
    /// transport. A stale close may remove only this exact session.
    incoming_session: proto::HandshakeChallenge,
    handshake: Mutex<Option<PeerHandshake>>,
    outgoing_session: Mutex<Option<proto::HandshakeChallenge>>,
    /// Verification happens synchronously in websocket delivery order; this
    /// bounded queue preserves that order for async dispatch without allowing
    /// an authenticated peer to grow browser memory without limit.
    dispatch_tx: Mutex<futures::channel::mpsc::Sender<ankurah_core::connector::VerifiedPeerMessage>>,
    dispatch_abort: Arc<Mutex<Option<AbortHandle>>>,
    transport_closed: Arc<AtomicBool>,
    core_close_requested: Arc<AtomicBool>,
    client: Weak<ClientInner>,
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
    pub fn new(node: Box<dyn NodeComms>, url: String, client: Weak<ClientInner>) -> Result<Self, JsValue> {
        let url = if url.starts_with("ws://") || url.starts_with("wss://") { format!("{}/ws", url) } else { format!("wss://{}/ws", url) };

        let ws = WebSocket::new(&url)?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let state = RwLock::new(ConnectionState::Connecting { url: url.clone() });

        let handshake = node.begin_peer_handshake();
        let incoming_session = handshake.challenge();
        let (dispatch_tx, mut dispatch_rx) = futures::channel::mpsc::channel(DISPATCH_QUEUE_CAPACITY);
        let (dispatch_abort_handle, dispatch_registration) = AbortHandle::new_pair();
        let dispatch_abort = Arc::new(Mutex::new(Some(dispatch_abort_handle)));
        let transport_closed = Arc::new(AtomicBool::new(false));
        let core_close_requested = Arc::new(AtomicBool::new(false));
        let dispatch_node = node.cloned();
        wasm_bindgen_futures::spawn_local(async move {
            use futures::StreamExt;
            let dispatch = async move {
                while let Some(message) = dispatch_rx.next().await {
                    if let Err(error) = dispatch_node.handle_verified_peer_message(message).await {
                        info!("Error handling peer message: {:?}", error);
                    }
                }
            };
            let _ = Abortable::new(dispatch, dispatch_registration).await;
        });
        let me = Connection(Arc::new(SendWrapper::new(ConnectionInner {
            ws: Arc::new(ws),
            url,
            state,
            node,
            incoming_session,
            handshake: Mutex::new(Some(handshake)),
            outgoing_session: Mutex::new(None),
            dispatch_tx: Mutex::new(dispatch_tx),
            dispatch_abort,
            transport_closed,
            core_close_requested,
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
            client.handle_state_change(self, new_state)
        } else {
            false
        }
    }

    fn is_current(&self) -> bool { self.client.upgrade().is_some_and(|client| client.is_current_connection(self)) }

    // fn get_state(&self) -> ConnectionState { self.state.read().map(|state| state.clone()).unwrap_or(ConnectionState::None) }

    fn handle_open(&self, e: Event) {
        // `WebSocket::close` is asynchronous. An `open` callback that was
        // already queued before core tore down this peer must not restart the
        // handshake or turn the terminal close into an Error/reconnect.
        if self.core_close_requested.load(Ordering::Acquire) {
            self.disconnect();
            self.set_state(ConnectionState::None);
            return;
        }
        action_info!(self, "connection open", "{}", &e.type_());
        let challenge = self.handshake.lock().unwrap().as_ref().expect("fresh connection has handshake").challenge();
        if let Err(error) = self.send_message(proto::Message::HandshakeChallenge(challenge)) {
            error!("Failed to send handshake challenge: {:?}", error);
            self.disconnect();
            self.set_state(ConnectionState::Error { message: "Failed to send handshake challenge".to_string() });
        }
    }
    fn handle_close(&self, e: CloseEvent) {
        action_info!(self, "connection closed", "{}", &e.code());
        let core_close_requested = self.core_close_requested.load(Ordering::Acquire);
        self.disconnect();
        self.set_state(if core_close_requested { ConnectionState::None } else { ConnectionState::Closed });
    }

    fn handle_error(&self, _e: ErrorEvent) {
        info!("Connection error");
        // TODO - figure out how to get the error message. e.message() crashes because it's expected to be a string, but it's null
        let core_close_requested = self.core_close_requested.load(Ordering::Acquire);
        self.disconnect();
        if core_close_requested {
            self.set_state(ConnectionState::None);
        } else {
            self.set_state(ConnectionState::Error { message: "Connection error".to_string() });
        }
    }

    fn receive_message(&self, e: MessageEvent) {
        // Browser events already queued by the JavaScript event loop can run
        // after `PeerSender::close` has marked this transport terminal. Fail
        // silently into `None`: decoding or verifying such a frame would see
        // the core registration already removed, publish `Error`, and make
        // ClientInner reconnect to the old founder after hard reset.
        if self.core_close_requested.load(Ordering::Acquire) {
            self.disconnect();
            self.set_state(ConnectionState::None);
            return;
        }
        if self.transport_closed.load(Ordering::Acquire) {
            return;
        }

        let array_buffer = if let Ok(array_buffer) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            array_buffer
        } else if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
            let message =
                format!("Unsupported application text frame ({} UTF-16 code units) received from {}; closing", text.length(), self.url);
            error!("{}", message);
            self.disconnect();
            self.set_state(ConnectionState::Error { message });
            return;
        } else {
            let message = format!("Unsupported websocket application data received from {}; closing", self.url);
            error!("{}", message);
            self.disconnect();
            self.set_state(ConnectionState::Error { message });
            return;
        };

        let array = Uint8Array::new(&array_buffer);
        let data = array.to_vec();

        if let Ok(message) = proto::decode_message(&data) {
            match message {
                proto::Message::HandshakeChallenge(challenge) => {
                    let mut outgoing = self.outgoing_session.lock().unwrap();
                    if outgoing.is_some() {
                        drop(outgoing);
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message: "Duplicate handshake challenge".to_string() });
                        return;
                    }
                    *outgoing = Some(challenge);
                    drop(outgoing);
                    if let Err(e) = self.send_message(proto::Message::Presence(self.node.presence(challenge))) {
                        error!("Failed to send presence message: {:?}", e);
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message: "Failed to send Presence".to_string() });
                    }
                }
                proto::Message::Presence(server_presence) => {
                    // Pre-check the version so the server learns why we are
                    // leaving; register_peer re-enforces this for every transport.
                    if !proto::protocol_compatible(server_presence.protocol_version) {
                        let rejection =
                            proto::PresenceRejection { expected: proto::PROTOCOL_VERSION, received: server_presence.protocol_version };
                        error!("Refusing server {}: {}", self.url, rejection);
                        let _ = self.send_message(proto::Message::PresenceRejected(rejection.clone()));
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message: rejection.to_string() });
                        return;
                    }
                    let state = { self.state.read().unwrap().clone() };
                    match state {
                        ConnectionState::Connected { .. } => {
                            warn!("Received duplicate server presence; closing");
                            self.disconnect();
                            self.set_state(ConnectionState::Error { message: "Duplicate server Presence".to_string() });
                        }
                        ConnectionState::Connecting { .. } => {
                            if !self.is_current() {
                                return;
                            }
                            let Some(outgoing_session) = *self.outgoing_session.lock().unwrap() else {
                                self.disconnect();
                                self.set_state(ConnectionState::Error { message: "Presence arrived before server challenge".to_string() });
                                return;
                            };
                            let Some(handshake) = self.handshake.lock().unwrap().take() else {
                                self.disconnect();
                                self.set_state(ConnectionState::Error { message: "Duplicate server Presence".to_string() });
                                return;
                            };
                            if let Err(refusal) = self.node.register_peer(
                                server_presence.clone(),
                                handshake,
                                outgoing_session,
                                Box::new(WebSocketPeerSender {
                                    recipient_node_id: server_presence.node_id,
                                    ws: SendWrapper::new(self.ws.clone()),
                                    dispatch_abort: self.dispatch_abort.clone(),
                                    transport_closed: self.transport_closed.clone(),
                                    core_close_requested: self.core_close_requested.clone(),
                                }),
                            ) {
                                match &refusal {
                                    proto::PresenceRefusal::IncompatibleVersion(rejection) => {
                                        let _ = self.send_message(proto::Message::PresenceRejected(rejection.clone()));
                                    }
                                    proto::PresenceRefusal::InvalidSignature(_)
                                    | proto::PresenceRefusal::UnexpectedChallenge(_)
                                    | proto::PresenceRefusal::SelfConnection(_)
                                    | proto::PresenceRefusal::InvalidSystemRoot(_) => {}
                                }
                                error!("Refusing server {}: {}", self.url, refusal);
                                self.disconnect();
                                self.set_state(ConnectionState::Error { message: refusal.to_string() });
                                return;
                            }
                            if !self
                                .set_state(ConnectionState::Connected { url: self.url.clone(), server_presence: server_presence.clone() })
                            {
                                self.disconnect();
                            }
                        }
                        _ => {
                            warn!("Sanity error: received server presence while not in connecting state");
                            self.disconnect();
                            self.set_state(ConnectionState::Error { message: "Received server presence, but not connected".to_string() });
                        }
                    }
                }
                proto::Message::PresenceRejected(rejection) => {
                    error!("Server {} refused connection: {}", self.url, rejection);
                    self.disconnect();
                    self.set_state(ConnectionState::Error { message: rejection.to_string() });
                }
                proto::Message::PeerMessage(frame) => {
                    let connection_state = self.state.read().unwrap().clone();
                    let authenticated_peer = match connection_state {
                        ConnectionState::Connected { server_presence, .. } => server_presence.node_id,
                        _ => {
                            warn!("Received peer message from {} before its Presence was authenticated", self.url);
                            self.disconnect();
                            self.set_state(ConnectionState::Error {
                                message: "Received peer message before server Presence was authenticated".to_string(),
                            });
                            return;
                        }
                    };
                    let message = match self.node.verify_peer_message(authenticated_peer, frame) {
                        Ok(message) => message,
                        Err(error) => {
                            self.disconnect();
                            self.set_state(ConnectionState::Error { message: error.to_string() });
                            return;
                        }
                    };
                    if self.dispatch_tx.lock().unwrap().try_send(message).is_err() {
                        self.disconnect();
                        self.set_state(ConnectionState::Error { message: "Peer dispatch queue full or closed".to_string() });
                    }
                }
            }
        } else {
            let connecting = matches!(&*self.state.read().unwrap(), ConnectionState::Connecting { .. });
            // Exact decoding is a framing invariant after authentication too.
            // Any malformed or trailing bytes fail the connection closed.
            let message = if connecting && proto::is_version0_presence(&data) {
                format!("Server {} speaks a pre-versioning (0.9.x or older) protocol; refusing", self.url)
            } else if connecting {
                format!("Failed to deserialize handshake message from {}; closing", self.url)
            } else {
                format!("Failed to exactly decode established message from {}; closing", self.url)
            };
            error!("{}", message);
            self.disconnect();
            self.set_state(ConnectionState::Error { message });
        }
    }

    fn send_message(&self, message: proto::Message) -> Result<(), JsValue> {
        let data = proto::encode_message(&message).map_err(|e| {
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
        info!("Websocket disconnected from node {} to {}", self.node.id(), self.url);
        self.dispatch_tx.lock().unwrap().close_channel();
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        self.ws.set_onopen(None);
        close_wasm_transport(&self.ws, &self.dispatch_abort, &self.transport_closed);
        self._callbacks.lock().unwrap().take();
        if let ConnectionState::Connected { server_presence, .. } = &*self.state.read().unwrap() {
            self.node.deregister_peer(server_presence.node_id, self.incoming_session);
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
    dispatch_abort: Arc<Mutex<Option<AbortHandle>>>,
    transport_closed: Arc<AtomicBool>,
    core_close_requested: Arc<AtomicBool>,
}

#[async_trait]
impl PeerSender for WebSocketPeerSender {
    fn send_message(&self, message: proto::SignedPeerMessage) -> Result<(), ankurah_core::connector::SendError> {
        if self.transport_closed.load(Ordering::Acquire) {
            return Err(ankurah_core::connector::SendError::ConnectionClosed);
        }
        let message = proto::Message::PeerMessage(message);
        let data = proto::encode_message(&message).map_err(|e| {
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

    fn recipient_node_id(&self) -> proto::NodeId { self.recipient_node_id }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }

    fn close(&self) {
        self.core_close_requested.store(true, Ordering::Release);
        close_wasm_transport(&self.ws, &self.dispatch_abort, &self.transport_closed);
    }
}

fn close_wasm_transport(ws: &WebSocket, dispatch_abort: &Mutex<Option<AbortHandle>>, transport_closed: &AtomicBool) {
    if !transport_closed.swap(true, Ordering::AcqRel) {
        if let Some(dispatch_abort) = dispatch_abort.lock().unwrap().take() {
            dispatch_abort.abort();
        }
        // Browser close is nonblocking; onclose performs session cleanup.
        let _ = ws.close();
    }
}
