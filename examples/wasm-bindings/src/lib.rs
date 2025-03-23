use std::{panic, sync::Arc};

use ankurah::core::context::Context;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
pub use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
pub use ankurah_websocket_client_wasm::WebsocketClient;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use send_wrapper::SendWrapper;
use tracing::{error, info};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

pub use example_model::*;

lazy_static! {
    static ref NODE: OnceCell<Node<IndexedDBStorageEngine, PermissiveAgent>> = OnceCell::new();
    static ref CLIENT: OnceCell<SendWrapper<WebsocketClient>> = OnceCell::new();
    static ref NOTIFY: tokio::sync::Notify = tokio::sync::Notify::new();
}

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    tracing_wasm::set_as_global_default();
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let _ = any_spawner::Executor::init_wasm_bindgen();

    let storage_engine = IndexedDBStorageEngine::open("ankurah_example_app").await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    let node = Node::new(Arc::new(storage_engine), PermissiveAgent::new());
    let connector = WebsocketClient::new(node.clone(), "ws://127.0.0.1:9797")?;
    if let Err(_) = NODE.set(node) {
        error!("Failed to set node");
    }
    if let Err(_) = CLIENT.set(SendWrapper::new(connector)) {
        error!("Failed to set connector");
    }
    NOTIFY.notify_waiters();

    Ok(())
}

pub fn get_node() -> Node<IndexedDBStorageEngine, PermissiveAgent> { NODE.get().expect("Node not initialized").clone() }

#[wasm_bindgen]
pub fn ctx() -> Context { get_node().context(c) }

#[wasm_bindgen]
pub fn ws_client() -> WebsocketClient { (**CLIENT.get().expect("Client not initialized")).clone() }

#[wasm_bindgen]
pub async fn ready() -> Result<(), JsValue> {
    match CLIENT.get() {
        Some(client) => client.ready().await,
        None => {
            NOTIFY.notified().await;
            CLIENT.get().expect("Client not initialized").ready().await
        }
    }
    .map_err(|_| JsValue::from_str("Failed to connect to server"))
}
