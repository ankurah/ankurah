use ankurah_core::Node;

use crate::connection_state::*;
use gloo_timers::future::sleep;
use reactive_graph::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use wasm_bindgen::prelude::*;

use crate::connection::Connection;
use wasm_bindgen_futures::spawn_local;

const MAX_RECONNECT_DELAY: u64 = 10000;

#[derive(Clone)]
#[wasm_bindgen]
pub struct WebsocketClient {
    inner: Arc<ClientInner>,
}

pub(crate) struct ClientInner {
    server_url: String,
    connection: RefCell<Option<Connection>>,
    state: reactive_graph::signal::RwSignal<ConnectionState>,
    node: Arc<Node>,
    current_delay: RefCell<u64>,
}

impl ClientInner {
    pub(crate) fn handle_state_change(self: &Arc<Self>, new_state: ConnectionState) {
        self.state.set(new_state.clone());

        match new_state {
            ConnectionState::Connected { .. } => {
                *self.current_delay.borrow_mut() = 0;
            }
            ConnectionState::Connecting { .. } => (),
            ConnectionState::None => {
                self.connect().expect("Failed to connect");
            }
            ConnectionState::Closed | ConnectionState::Error { .. } => {
                let next_delay = (*self.current_delay.borrow() + 500).min(MAX_RECONNECT_DELAY);
                *self.current_delay.borrow_mut() = next_delay;
                self.reconnect(next_delay);
            }
        }
    }

    pub fn connect(self: &Arc<Self>) -> Result<(), JsValue> {
        let connection = Connection::new(self.server_url.clone(), self.node.clone(), Arc::downgrade(self))?;

        info!("Connecting to {}", self.server_url);
        *self.connection.borrow_mut() = Some(connection);
        self.state.set(ConnectionState::Connecting { url: self.server_url.clone() });

        info!("Connecting to websocket");
        Ok(())
    }

    pub fn reconnect(self: &Arc<Self>, delay: u64) {
        info!("reconnect: removing old connection with delay {}ms", delay);
        // Deregister the old peer sender if we had one
        if let Some(connection) = self.connection.borrow_mut().take() {
            if let ConnectionState::Connected { presence, .. } = connection.state() {
                let node = self.node.clone();
                spawn_local(async move {
                    node.deregister_peer(presence.node_id).await;
                });
            }
        }

        let self2 = self.clone();
        spawn_local(async move {
            info!("reconnect: sleeping for {}ms", delay);
            sleep(Duration::from_millis(delay)).await;
            info!("reconnect: reconnecting");
            self2.connect().expect("Failed to reconnect");
        });
    }
}

/// Client provides a primary handle to speak to the server
impl WebsocketClient {
    pub fn new(node: Arc<Node>, server_url: &str) -> Result<WebsocketClient, JsValue> {
        info!("Creating new websocket client");
        let inner = Arc::new(ClientInner {
            server_url: server_url.to_string(),
            node,
            connection: RefCell::new(None),
            state: reactive_graph::signal::RwSignal::new(ConnectionState::None),
            current_delay: RefCell::new(0),
        });

        inner.connect()?;

        Ok(WebsocketClient { inner })
    }

    pub fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> { self.inner.state.read_only() }
    pub fn node(&self) -> Arc<Node> { self.inner.node.clone() }
}

#[wasm_bindgen]
impl WebsocketClient {
    // resolves when we have a connected state
    pub async fn ready(&self) {
        // If we're already connected, return immediately
        if let ConnectionState::Connected { .. } = self.inner.state.get() {
            return;
        }

        // Otherwise poll until connected
        loop {
            if let ConnectionState::Connected { .. } = self.inner.state.get() {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateEnumSignal {
        let state = self.inner.state.read_only();
        reactive_graph::computed::Memo::new(move |_| state.get().into()).into()
    }
}
