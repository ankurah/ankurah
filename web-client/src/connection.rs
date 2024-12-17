use crate::client::ConnectionState;
use log::info;
use reactive_graph::prelude::*;
use wasm_bindgen::prelude::*;
use web_sys::{CloseEvent, Event, MessageEvent, WebSocket};

pub struct Connection {
    ws: WebSocket,
    on_message: Closure<dyn FnMut(MessageEvent)>,
    on_error: Closure<dyn FnMut(Event)>,
    on_close: Closure<dyn FnMut(CloseEvent)>,
    on_open: Closure<dyn FnMut()>,
    pub state: reactive_graph::signal::ReadSignal<ConnectionState>,
}

impl Connection {
    pub fn new(server_url: &str) -> Result<Connection, JsValue> {
        let ws_url = format!("wss://{}/ws", server_url);
        let ws = WebSocket::new(&ws_url)?;

        let (rstate, wstate) =
            reactive_graph::signal::RwSignal::new(ConnectionState::Connecting).split();

        let on_message =
            Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |e: MessageEvent| {
                if let Ok(text) = e.data().dyn_into::<js_sys::JsString>() {
                    info!("Message received: {}", text);
                }
            }));

        let on_error = {
            let wstate = wstate.clone();
            Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_| {
                info!("Connection Error");
                wstate.set(ConnectionState::Error);
            }))
        };

        let on_close = {
            let wstate = wstate.clone();
            Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |e: CloseEvent| {
                info!("Connection closed: {}", e.code());
                wstate.set(ConnectionState::Closed);
            }))
        };

        // convert ready into a future
        let on_open = {
            Closure::<dyn FnMut()>::wrap(Box::new(move || {
                info!("Connection opened (event)");
                wstate.set(ConnectionState::Open);
            }))
        };

        // Set up WebSocket event handlers
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        Ok(Connection {
            ws,
            on_message,
            on_error,
            on_close,
            on_open,
            state: rstate,
        })
    }

    pub fn send_message(&self, message: &str) {
        self.ws.send_with_str(message).unwrap_or_else(|err| {
            info!("Failed to send message: {:?}", err);
        });
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        info!("Dropping connection");
        // unbind the listeners and close the connection
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        self.ws.close().unwrap();
    }
}
