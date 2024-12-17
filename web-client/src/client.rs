// use ankurah_wasm_signal::WasmSignal;

use gloo_timers::future::sleep;
use log::info;
use reactive_graph::prelude::*;
use std::cell::RefCell;
use std::fmt::Display;
use std::rc::Rc;
use std::time::Duration;
use wasm_bindgen::prelude::*;

use wasm_bindgen_futures::spawn_local;

use crate::connection::Connection;
use reactive_graph::effect::Effect;

const MAX_RECONNECT_DELAY: u64 = 10000;

#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Debug /* , WasmSignal*/)]
pub enum ConnectionState {
    None,
    Connecting,
    Open,
    Closed,
    Error,
}

impl Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConnectionState::None => write!(f, "None"),
            ConnectionState::Connecting => write!(f, "Connecting"),
            ConnectionState::Open => write!(f, "Open"),
            ConnectionState::Closed => write!(f, "Closed"),
            ConnectionState::Error => write!(f, "Error"),
        }
    }
}

impl ConnectionState {
    pub fn str(&self) -> &'static str {
        match self {
            ConnectionState::None => "None",
            ConnectionState::Connecting => "Connecting",
            ConnectionState::Open => "Open",
            ConnectionState::Closed => "Closed",
            ConnectionState::Error => "Error",
        }
    }
}

// #[wasm_bindgen]
// impl ConnectionState {
//     pub fn display(&self) -> String {
//         format!("{:?}", self)
//     }
// }

#[wasm_bindgen]
pub struct ConnectionStateSignal(reactive_graph::signal::ReadSignal<&'static str>);

// impl ConnectionStateSignal {
//     pub fn new(signal: Box<dyn ::futures_signals::signal::Signal<Item = ConnectionState>>) -> Self {
//         Self(signal)
//     }
// }

#[wasm_bindgen]
impl ConnectionStateSignal {
    #[wasm_bindgen(js_name = "subscribe")]
    pub fn js_subscribe(&self, callback: js_sys::Function) -> ankurah_wasm_signal::Subscription {
        let signal = self.0;
        let effect = Effect::new(move |_| {
            let value = signal.get();
            let js_value = wasm_bindgen::JsValue::from_str(value);
            callback
                .call1(&wasm_bindgen::JsValue::NULL, &js_value)
                .unwrap();
        });

        ankurah_wasm_signal::Subscription::new(effect)
    }

    #[wasm_bindgen(getter)]
    pub fn value(&self) -> String {
        self.0.get().to_string()
    }
}

struct ClientInner {
    server_url: String,
    connection: RefCell<Option<Connection>>,
    state: reactive_graph::signal::RwSignal<&'static str>,
}

#[wasm_bindgen]
pub struct Client {
    inner: Rc<ClientInner>,
}

/// Client provides a primary handle to speak to the server
#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(server_url: &str) -> Result<Client, JsValue> {
        let _ = any_spawner::Executor::init_wasm_bindgen();
        let inner = Rc::new(ClientInner {
            server_url: server_url.to_string(),
            connection: RefCell::new(None),
            state: reactive_graph::signal::RwSignal::new(ConnectionState::None.str()),
        });

        inner.connect(0)?;

        Ok(Client { inner })
    }

    pub async fn ready(&self) {
        // self.inner
        //     .state
        //     .signal()
        //     .wait_for(ConnectionState::Open)
        //     .await;
    }

    pub fn send_message(&self, message: &str) {
        info!("send_message: Sending message: {}", message);

        if let Some(connection) = self.inner.connection.borrow_mut().as_ref() {
            // TODO: queue these messages?
            connection.send_message(message);
        }
    }

    #[wasm_bindgen(getter, js_name = "connection_state")]
    pub fn js_connection_state(&self) -> ConnectionStateSignal {
        ConnectionStateSignal(self.inner.state.read_only())
    }
}

impl Client {
    pub fn connection_state(&self) -> reactive_graph::signal::ReadSignal<&'static str> {
        self.inner.state.read_only()
    }
}

impl ClientInner {
    pub fn connect(self: &Rc<Self>, mut delay: u64) -> Result<(), JsValue> {
        let connection = Connection::new(&self.server_url)?;
        let state = connection.state.clone();
        *self.connection.borrow_mut() = Some(connection);

        self.state.set(ConnectionState::Connecting.str());
        let client_inner = Rc::clone(self);
        let self2 = self.clone();

        info!("Connecting to websocket");

        Effect::new(move |_| {
            let connection_state = state.get();
            info!("connect: state changed to {:?}", connection_state);
            let state_ref: &'static str = connection_state.str();
            client_inner.state.set(state_ref);

            match connection_state {
                ConnectionState::Open => {
                    delay = 0;
                }
                ConnectionState::Connecting => (),
                _ => self2.reconnect(delay + 500),
            }
        });

        Ok(())
    }

    pub fn reconnect(self: &Rc<Self>, mut delay: u64) {
        delay = delay.min(MAX_RECONNECT_DELAY);
        info!("reconnect: removing old connection");
        self.connection.borrow_mut().take();

        let self2 = self.clone();
        spawn_local(async move {
            info!("reconnect: sleeping for {}ms", delay);
            sleep(Duration::from_millis(delay)).await;
            info!("reconnect: reconnecting");
            self2.connect(delay).expect("Failed to reconnect");
        });
    }
}
