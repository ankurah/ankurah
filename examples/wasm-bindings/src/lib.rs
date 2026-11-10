use std::{panic, sync::Arc};

use ankurah::core::context::Context;
use ankurah::error::{NodeHaltReason, NodeReadinessError};
use ankurah::signals::Wait;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, NodeState, PermissiveAgent};
pub use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use ankurah_system_reset::{OnReseed, SystemReset};
pub use ankurah_websocket_client_wasm::WebsocketClient;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use send_wrapper::SendWrapper;
use tracing::error;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

pub use example_model::*;

// Re-export the new useObserve hook from ankurah-signals
pub use ankurah_signals::react::*;

lazy_static! {
    static ref NODE: OnceCell<Node<IndexedDBStorageEngine, PermissiveAgent>> = OnceCell::new();
    static ref CLIENT: OnceCell<SendWrapper<WebsocketClient>> = OnceCell::new();
    static ref NOTIFY: tokio::sync::Notify = tokio::sync::Notify::new();
}

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    // Configure tracing_wasm to filter out DEBUG logs
    tracing_wasm::set_as_global_default_with_config(
        tracing_wasm::WASMLayerConfigBuilder::new()
            .set_max_level(tracing::Level::INFO) // Only show INFO, WARN, ERROR
            .build(),
    );
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let storage_engine =
        Arc::new(IndexedDBStorageEngine::open("ankurah_example_app").await.map_err(|e| JsValue::from_str(&e.to_string()))?);
    let node = Node::builder(storage_engine, PermissiveAgent::new())
        .plug(SystemReset::<System, _>::new(OnReseed::Reset, c))
        .build()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    reload_after_reset(node.clone());
    let connector = WebsocketClient::new(node.clone(), "ws://127.0.0.1:9797").map_err(|e| JsValue::from_str(&e.to_string()))?;
    match node.wait_ready().await {
        // The server was seeded again; the page reloads once the store is wiped.
        Err(NodeReadinessError::Halted(NodeHaltReason::Reset)) => return Ok(()),
        ready => ready.map_err(|e| JsValue::from_str(&e.to_string()))?,
    }
    if let Err(_) = NODE.set(node) {
        error!("Failed to set node");
    }
    if let Err(_) = CLIENT.set(SendWrapper::new(connector)) {
        error!("Failed to set connector");
    }
    NOTIFY.notify_waiters();

    Ok(())
}

/// Reload the page once a reset of `node`, the plugin's when the server was seeded again, has
/// wiped its store, so the app starts over on the new seeding, at start-up or at any time after.
fn reload_after_reset(node: Node<IndexedDBStorageEngine, PermissiveAgent>) {
    wasm_bindgen_futures::spawn_local(async move {
        node.state().wait_for(|state| matches!(state, NodeState::Halted(NodeHaltReason::Reset)).then_some(())).await;
        if let Err(e) = node.reset().await {
            error!("The reset did not wipe the store, so the page stays as it is: {e}");
            return;
        }
        if let Some(window) = web_sys::window() {
            if let Err(e) = window.location().reload() {
                error!("Failed to reload the page after a reset: {e:?}");
            }
        }
    });
}

pub fn get_node() -> Node<IndexedDBStorageEngine, PermissiveAgent> { NODE.get().expect("Node not initialized").clone() }

#[wasm_bindgen]
pub fn ctx() -> Result<Context, JsValue> { get_node().context(c).map_err(|e| JsValue::from_str(&e.to_string())) }

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
