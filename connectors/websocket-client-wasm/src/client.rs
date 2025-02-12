use ankurah::policy::PolicyAgent;
use ankurah_core::connector::NodeComms;
use ankurah_core::Node;

use crate::connection_state::*;
use gloo_timers::future::sleep;
use reactive_graph::prelude::*;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
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
    node: Box<dyn NodeComms>,
    reconnect_delay: RefCell<u64>,
    pending_ready_wakers: RefCell<Vec<Waker>>,
}

/// Client provides a primary handle to speak to the server
impl WebsocketClient {
    pub fn new<PA: PolicyAgent + Send + Sync + 'static>(node: Node<PA>, server_url: &str) -> Result<WebsocketClient, JsValue> {
        info!("Created new websocket client for node {}", node.id);
        let inner = Arc::new(ClientInner {
            server_url: server_url.to_string(),
            node: Box::new(node),
            connection: RefCell::new(None),
            state: reactive_graph::signal::RwSignal::new(ConnectionState::None),
            reconnect_delay: RefCell::new(0),
            pending_ready_wakers: RefCell::new(Vec::new()),
        });

        inner.connect()?;

        Ok(WebsocketClient { inner })
    }

    pub fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> { self.inner.state.read_only() }
}

#[wasm_bindgen]
impl WebsocketClient {
    // resolves when we have a connected state
    pub async fn ready(&self) -> Result<(), String> {
        // If we're already connected, the future will resolve immediately
        ReadyFuture { client: self.inner.clone() }.await.map_err(|_| "unreachable".to_string())
    }

    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateEnumSignal {
        let state = self.inner.state.read_only();
        reactive_graph::computed::Memo::new(move |_| state.get().into()).into()
    }
}

impl ClientInner {
    pub(crate) fn handle_state_change(self: &Arc<Self>, connection: &Connection, new_state: ConnectionState) -> bool {
        // we are only interested in state changes for the current connection
        {
            let c = self.connection.borrow();
            let Some(existing_connection) = c.as_ref() else {
                return false;
            };
            if connection != existing_connection {
                // This is a stale state change, ignore it.
                // Not sure if this is possible, but lets not find out.
                return false;
            }
        }

        self.state.set(new_state.clone());

        info!("State changed: {:?}", new_state);
        match new_state {
            ConnectionState::Connected { .. } => {
                *self.reconnect_delay.borrow_mut() = 0;
                // Wake all pending futures
                let wakers = std::mem::take(&mut *self.pending_ready_wakers.borrow_mut());
                for waker in wakers {
                    waker.wake();
                }
            }
            ConnectionState::Connecting { .. } => (),
            ConnectionState::None => {
                // self.connect().expect("Failed to connect");
            }
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

    pub fn connect(self: &Arc<Self>) -> Result<(), JsValue> {
        let connection = Connection::new(self.node.cloned(), self.server_url.clone(), Arc::downgrade(self))?;

        info!("Connecting to {}", self.server_url);
        *self.connection.borrow_mut() = Some(connection);
        self.state.set(ConnectionState::Connecting { url: self.server_url.clone() });

        info!("Connecting to websocket");
        Ok(())
    }

    pub fn reconnect(self: &Arc<Self>, delay: u64) {
        info!("reconnect: removing old connection with delay {}ms", delay);

        let self2 = self.clone();
        spawn_local(async move {
            info!("reconnect: sleeping for {}ms", delay);
            sleep(Duration::from_millis(delay)).await;
            info!("reconnect: reconnecting");
            self2.connect().expect("Failed to reconnect");
        });
    }
}

impl std::ops::Drop for ClientInner {
    fn drop(&mut self) {
        info!("Websocket client inner dropped for node {}", self.node.id());
    }
}

pub struct ReadyFuture {
    pub(crate) client: Arc<ClientInner>,
}

impl Future for ReadyFuture {
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let ConnectionState::Connected { .. } = self.client.state.get() {
            Poll::Ready(Ok(()))
        } else {
            self.client.pending_ready_wakers.borrow_mut().push(cx.waker().clone());
            Poll::Pending
        }
    }
}
