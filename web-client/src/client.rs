use ankurah_core::Node;

use crate::connection_state::*;
use gloo_timers::future::sleep;
use reactive_graph::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use wasm_bindgen::prelude::*;

use crate::connection::Connection;
use reactive_graph::effect::Effect;
use wasm_bindgen_futures::spawn_local;

const MAX_RECONNECT_DELAY: u64 = 10000;

struct Inner {
    server_url: String,
    connection: RefCell<Option<(Connection, reactive_graph::owner::Owner)>>,
    state: reactive_graph::signal::RwSignal<ConnectionState>,
    node: Arc<Node>,
}

#[derive(Clone)]
#[wasm_bindgen]
pub struct WebsocketClient {
    inner: Rc<Inner>,
    owner: reactive_graph::owner::Owner,
}

/// Client provides a primary handle to speak to the server
impl WebsocketClient {
    pub fn new(node: Arc<Node>, server_url: &str) -> Result<WebsocketClient, JsValue> {
        info!("Creating new websocket client");
        let inner = Rc::new(Inner {
            server_url: server_url.to_string(),
            node,
            connection: RefCell::new(None),
            state: reactive_graph::signal::RwSignal::new(ConnectionState::None),
        });

        let current_delay = Rc::new(RefCell::new(0u64));
        let inner_clone = inner.clone();

        let owner = reactive_graph::owner::Owner::new();
        owner.set();

        inner.connect()?;
        Effect::new(move |_| {
            info!("MARK 1");
            //     let connection_state = inner_clone.state.get();

            //     match connection_state {
            //         ConnectionState::Connected { .. } => {
            //             *current_delay.borrow_mut() = 0;
            //         }
            //         ConnectionState::Connecting { .. } => (),
            //         ConnectionState::None => {
            //             inner_clone.connect().expect("Failed to connect");
            //         }
            //         ConnectionState::Closed | ConnectionState::Error { .. } => {
            //             let next_delay = (*current_delay.borrow() + 500).min(MAX_RECONNECT_DELAY);
            //             *current_delay.borrow_mut() = next_delay;
            //             inner_clone.reconnect(next_delay);
            //         }
            //     }
        });

        Ok(WebsocketClient { inner, owner })
    }

    pub fn connection_state(&self) -> reactive_graph::signal::ReadSignal<ConnectionState> {
        self.inner.state.read_only()
    }
    pub fn node(&self) -> Arc<Node> {
        self.inner.node.clone()
    }
}

#[wasm_bindgen]
impl WebsocketClient {
    // resolves when we have a connected state
    pub async fn ready(&self) {
        // If we're already connected, the effect might not fire, so check current state
        if let ConnectionState::Connected { .. } = self.inner.state.get() {
            return;
        }
        let (tx, rx) = futures::channel::oneshot::channel();

        let mut tx = Some(tx);
        // yeah no - this needs to use a different signals library
        Effect::new({
            let state = self.inner.state;
            move |_| {
                if let ConnectionState::Connected { .. } = state.get() {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(());
                    }
                }
            }
        });

        rx.await.expect("Failed to await ready state");
    }

    //     #[cfg(feature = "react")]
    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateEnumSignal {
        let state = self.inner.state.read_only();
        reactive_graph::computed::Memo::new(move |_| state.get().into()).into()
    }
}

impl Inner {
    pub fn connect(self: &Rc<Self>) -> Result<(), JsValue> {
        let connection = Connection::new(self.server_url.clone(), self.node.clone())?;
        let state = connection.state();

        let self_clone = self.clone();

        // Yeah this is nasty. I think we need to use a different signals library in the rust code.
        //We can still potentially use reactive_graph for leptos/react interface, but it seems ill suited for this use case.
        let owner = reactive_graph::owner::Owner::new();

        info!("Connecting to {}", self.server_url);
        owner.set();
        {
            let connection = connection.clone();
            Effect::new(move |_| {
                info!("Effect 1");
                // only act this connection = the current one
                if let Some((c, _)) = self_clone.connection.borrow().as_ref() {
                    if c != &connection {
                        return;
                    }
                }

                info!("Effect 2");
                let connection_state: ConnectionState = state.get();
                self_clone.state.set(connection_state.clone());

                info!("Effect 2.1");
                if let ConnectionState::Connected { .. } = connection_state {
                    info!("Effect 2.2");
                    let self_clone = self_clone.clone();
                    let connection = connection.clone();
                    spawn_local(async move {
                        info!("Effect 2.3");
                        self_clone
                            .node
                            .register_peer_sender(Box::new(connection))
                            .await;
                    });
                }
            });
        }

        *self.connection.borrow_mut() = Some((connection, owner));

        self.state.set(
            ConnectionState::Connecting {
                url: self.server_url.clone(),
            },
        );

        info!("Connecting to websocket");
        Ok(())
    }

    pub fn reconnect(self: &Rc<Self>, delay: u64) {
        info!("reconnect: removing old connection with delay {}ms", delay);
        self.connection.borrow_mut().take();

        let self2 = self.clone();
        spawn_local(async move {
            info!("reconnect: sleeping for {}ms", delay);
            sleep(Duration::from_millis(delay)).await;
            info!("reconnect: reconnecting");
            self2.connect().expect("Failed to reconnect");
        });
    }
}
