use ankurah::policy::PolicyAgent;
use ankurah::storage::StorageEngine;
use ankurah_core::connector::{NodeComms, PeerConnectionError};
use ankurah_core::{action_info, notice_info, Node, NodeState};

use crate::connection_state::*;
use ankurah::signals::{Calculated, Get, Mut, Read, Wait};
use futures::FutureExt;
use gloo_timers::future::sleep;
use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use wasm_bindgen::prelude::*;

use crate::connection::Connection;
use wasm_bindgen_futures::spawn_local;

const MAX_RECONNECT_DELAY: u64 = 10000;

#[cfg(all(test, target_arch = "wasm32"))]
mod tests;

#[derive(Clone)]
#[wasm_bindgen]
pub struct WebsocketClient {
    inner: Arc<ClientInner>,
}

pub(crate) struct ClientInner {
    server_url: String,
    connection: RefCell<Option<Connection>>,
    state: Mut<ConnectionState>,
    node: Box<dyn NodeComms>,
    run: Calculated<bool>,
    reconnect_delay: RefCell<u64>,
}

/// Client provides a primary handle to speak to the server
impl WebsocketClient {
    pub fn new<SE, PA>(node: Node<SE, PA>, server_url: &str) -> anyhow::Result<WebsocketClient>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        notice_info!("Created new websocket client");
        let state = node.state();
        let inner = Arc::new(ClientInner {
            server_url: server_url.to_string(),
            node: Box::new(node),
            run: Calculated::new(move || !matches!(state.get(), NodeState::Halted(_))),
            connection: RefCell::new(None),
            state: Mut::new(ConnectionState::None),
            reconnect_delay: RefCell::new(0),
        });

        inner.connect()?;

        Ok(WebsocketClient { inner })
    }

    pub fn connection_state(&self) -> Read<ConnectionState> { self.inner.state.read() }
}

#[wasm_bindgen]
impl WebsocketClient {
    /// Wait through transport retries; return on successful connection, explicit refusal, or node halt.
    pub async fn ready(&self) -> Result<(), String> {
        self.inner
            .state
            .read()
            .wait_for(|state| match state {
                ConnectionState::Connected { .. } => Some(Ok(())),
                ConnectionState::Error { message, cause: Some(_) } => Some(Err(message.clone())),
                _ => None,
            })
            .await
    }

    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateEnumSignal {
        let sig = Box::new(self.inner.state.read().map(|state| state.into()));

        ConnectionStateEnumSignal { sig, handle: Box::new(()) }
    }
}

impl fmt::Display for ClientInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "Client") }
}

impl ClientInner {
    pub(crate) fn owns(&self, connection: &Connection) -> bool {
        self.connection.borrow().as_ref().is_some_and(|current| current == connection)
    }

    pub(crate) fn handle_state_change(self: &Arc<Self>, connection: &Connection, new_state: ConnectionState) -> bool {
        if !self.owns(connection) || self.stop_if_node_halted() {
            return false;
        }

        self.state.set(new_state.clone());

        action_info!(self, "state changed", "{}", &new_state);
        match new_state {
            ConnectionState::Connected { .. } => {
                *self.reconnect_delay.borrow_mut() = 0;
            }
            ConnectionState::Connecting { .. } => (),
            ConnectionState::None => (),
            ConnectionState::Closed | ConnectionState::Error { .. } => {
                // Clear the existing connection before attempting to reconnect
                {
                    *self.connection.borrow_mut() = None;
                }

                let next_delay = (*self.reconnect_delay.borrow() + 500).min(MAX_RECONNECT_DELAY);
                *self.reconnect_delay.borrow_mut() = next_delay;
                self.reconnect(next_delay);
            }
        }
        true
    }

    pub fn connect(self: &Arc<Self>) -> anyhow::Result<()> {
        if self.stop_if_node_halted() {
            return Ok(());
        }
        let connection = Connection::new(self.node.cloned(), self.server_url.clone(), Arc::downgrade(self), self.run.clone())
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        action_info!(self, "connecting to", "{}", &self.server_url);
        *self.connection.borrow_mut() = Some(connection);
        // Keep the last refusal visible during retries; successful admission clears it.
        if !matches!(self.state.value(), ConnectionState::Error { .. }) {
            self.state.set(ConnectionState::Connecting { url: self.server_url.clone() });
        }

        Ok(())
    }

    pub fn reconnect(self: &Arc<Self>, delay: u64) {
        info!("reconnect: removing old connection with delay {}ms", delay);

        let weak = Arc::downgrade(self);
        let run = self.run.clone();
        spawn_local(async move {
            info!("reconnect: sleeping for {}ms", delay);
            futures::select_biased! {
                _ = run.wait_value(false).fuse() => {},
                _ = sleep(Duration::from_millis(delay)).fuse() => {},
            }
            info!("reconnect: reconnecting");
            let Some(client) = weak.upgrade() else { return };
            if let Err(error) = client.connect() {
                client.state.set(ConnectionState::Error { message: error.to_string(), cause: None });
                client.reconnect(MAX_RECONNECT_DELAY);
            }
        });
    }

    pub(crate) fn stop_if_node_halted(&self) -> bool {
        let NodeState::Halted(halt_reason) = self.node.state().value() else { return false };
        let connection = self.connection.borrow_mut().take();
        if let Some(connection) = connection {
            connection.disconnect();
        }
        self.state
            .set(ConnectionState::Error { message: halt_reason.to_string(), cause: Some(PeerConnectionError::NodeHalted(halt_reason)) });
        true
    }
}

impl std::ops::Drop for ClientInner {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.get_mut().take() {
            connection.disconnect();
        }
        info!("Websocket client inner dropped for node {}", self.node.id());
    }
}
