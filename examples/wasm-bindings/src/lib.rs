use std::{panic, sync::Arc};

use ankurah::core::context::Context;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
pub use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
pub use ankurah_websocket_client_wasm::WebsocketClient;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use tracing::{error, info};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

pub use example_model::*;

lazy_static! {
    static ref NODE: OnceCell<Node<IndexedDBStorageEngine, PermissiveAgent>> = OnceCell::new();
    static ref NOTIFY: tokio::sync::Notify = tokio::sync::Notify::new();
}

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    tracing_wasm::set_as_global_default();
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let _ = any_spawner::Executor::init_wasm_bindgen();

    let storage_engine = IndexedDBStorageEngine::open("ankurah_example_app").await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    let node = Node::new(Arc::new(storage_engine), PermissiveAgent::new());
    if let Err(_) = NODE.set(node) {
        error!("Failed to set node");
    }
    NOTIFY.notify_waiters();

    Ok(())
}
pub async fn get_node() -> Node<IndexedDBStorageEngine, PermissiveAgent> {
    if NODE.get().is_none() {
        NOTIFY.notified().await;
    }
    NODE.get().unwrap().clone()
}
#[wasm_bindgen]
pub async fn get_context() -> Context {
    let node = get_node().await;
    node.context(c)
}

#[wasm_bindgen]
pub async fn create_client() -> Result<WebsocketClient, JsValue> {
    let node = get_node().await;

    let connector = WebsocketClient::new(node.clone(), "ws://127.0.0.1:9797")?;

    info!("Waiting for client to connect");

    Ok(connector)
}
