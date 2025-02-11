use ankurah_core::{
    node::Node,
    traits::{Context as AnkurahContext, PolicyAgent},
};

use crate::connection_state::*;
use gloo_timers::future::sleep;
use reactive_graph::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use tracing::info;
use wasm_bindgen::prelude::*;

use crate::connection::Connection;
use wasm_bindgen_futures::spawn_local;

const MAX_RECONNECT_DELAY: u64 = 10000;

// Type-erased wrapper for wasm_bindgen
#[derive(Clone)]
#[wasm_bindgen]
pub struct WebsocketClient {
    inner: Arc<dyn WebsocketClientInner>,
}

pub(crate) trait WebsocketClientInner: Send + Sync {
    fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState>;
    fn ready(&self) -> Pin<Box<dyn Future<Output = Result<(), String>>>>;
    fn js_connection_state(&self) -> ConnectionStateEnumSignal;
}

pub(crate) struct WebsocketClientImpl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> {
    server_url: String,
    connection: RwLock<Option<Connection<Ctx, PA>>>,
    state: reactive_graph::signal::RwSignal<ConnectionState>,
    node: Arc<Node<Ctx, PA>>,
    reconnect_delay: RwLock<u64>,
    pending_ready_wakers: RwLock<Vec<Waker>>,
}

impl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> Clone for WebsocketClientImpl<Ctx, PA> {
    fn clone(&self) -> Self {
        Self {
            server_url: self.server_url.clone(),
            connection: RwLock::new(None),
            state: self.state.clone(),
            node: self.node.clone(),
            reconnect_delay: RwLock::new(*self.reconnect_delay.read().unwrap()),
            pending_ready_wakers: RwLock::new(Vec::new()),
        }
    }
}

impl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> WebsocketClientInner
    for WebsocketClientImpl<Ctx, PA>
{
    fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> { self.state.read_only() }

    fn ready(&self) -> Pin<Box<dyn Future<Output = Result<(), String>>>> { Box::pin(ReadyFuture { client: Arc::new(self.clone()) }) }

    fn js_connection_state(&self) -> ConnectionStateEnumSignal {
        let state = self.state.read_only();
        reactive_graph::computed::Memo::new(move |_| state.get().into()).into()
    }
}

impl WebsocketClient {
    pub fn new<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static>(
        node: Arc<Node<Ctx, PA>>,
        server_url: &str,
    ) -> Result<WebsocketClient, JsValue> {
        info!("Created new websocket client for node {}", node.id);
        let inner = Arc::new(WebsocketClientImpl {
            server_url: server_url.to_string(),
            node,
            connection: RwLock::new(None),
            state: reactive_graph::signal::RwSignal::new(ConnectionState::None),
            reconnect_delay: RwLock::new(0),
            pending_ready_wakers: RwLock::new(Vec::new()),
        });

        inner.connect()?;

        Ok(WebsocketClient { inner })
    }

    pub fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> { self.inner.connection_state() }
}

#[wasm_bindgen]
impl WebsocketClient {
    pub async fn ready(&self) -> Result<(), String> { self.inner.ready().await }

    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateEnumSignal { self.inner.js_connection_state() }
}

impl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> WebsocketClientImpl<Ctx, PA> {
    pub(crate) fn handle_state_change(self: &Arc<Self>, connection: &Connection<Ctx, PA>, new_state: ConnectionState) -> bool {
        // we are only interested in state changes for the current connection
        {
            let c = self.connection.read().unwrap();
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
                *self.reconnect_delay.write().unwrap() = 0;
                // Wake all pending futures
                let wakers = std::mem::take(&mut *self.pending_ready_wakers.write().unwrap());
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
                    *self.connection.write().unwrap() = None;
                }

                let next_delay = (*self.reconnect_delay.read().unwrap() + 500).min(MAX_RECONNECT_DELAY);
                *self.reconnect_delay.write().unwrap() = next_delay;
                self.reconnect(next_delay);
            }
        }
        true
    }

    pub fn connect(self: &Arc<Self>) -> Result<(), JsValue> {
        let connection = Connection::new(self.node.clone(), self.server_url.clone(), Arc::downgrade(self))?;

        info!("Connecting to {}", self.server_url);
        *self.connection.write().unwrap() = Some(connection);
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

impl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> std::ops::Drop
    for WebsocketClientImpl<Ctx, PA>
{
    fn drop(&mut self) {
        info!("Websocket client inner dropped for node {}", self.node.id);
    }
}

pub struct ReadyFuture<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> {
    pub(crate) client: Arc<WebsocketClientImpl<Ctx, PA>>,
}

impl<Ctx: AnkurahContext + 'static, PA: PolicyAgent<Context = Ctx> + Send + Sync + 'static> Future for ReadyFuture<Ctx, PA> {
    type Output = Result<(), String>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        if let ConnectionState::Connected { .. } = self.client.state.get() {
            Poll::Ready(Ok(()))
        } else {
            self.client.pending_ready_wakers.write().unwrap().push(cx.waker().clone());
            Poll::Pending
        }
    }
}
