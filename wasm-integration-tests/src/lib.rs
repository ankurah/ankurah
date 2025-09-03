use std::{panic, sync::Arc};

use ankurah::core::context::Context;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
pub use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;

use serde::{Deserialize, Serialize};
use tracing::error;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

// Note: React features not included in this test build

// Test models - equivalent to those in tests/tests/common.rs
use ankurah::Model;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

#[derive(Debug, Clone, Model, Serialize, Deserialize)]
pub struct Pet {
    pub name: String,
    pub age: String,
}

lazy_static! {
    static ref NODE: OnceCell<Node<IndexedDBStorageEngine, PermissiveAgent>> = OnceCell::new();
    static ref NOTIFY: tokio::sync::Notify = tokio::sync::Notify::new();
}

#[wasm_bindgen(start)]
pub async fn start() -> Result<(), JsValue> {
    // Configure tracing_wasm to filter out DEBUG logs (ignore if already set)
    // Use a static flag to prevent multiple initialization attempts
    use std::sync::atomic::{AtomicBool, Ordering};
    static TRACING_INITIALIZED: AtomicBool = AtomicBool::new(false);

    if !TRACING_INITIALIZED.swap(true, Ordering::SeqCst) {
        tracing_wasm::set_as_global_default_with_config(
            tracing_wasm::WASMLayerConfigBuilder::new()
                .set_max_level(tracing::Level::INFO) // Only show INFO, WARN, ERROR
                .build(),
        );
    }
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let storage_engine = IndexedDBStorageEngine::open("ankurah_integration_tests").await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    // Initialize the node's system catalog
    node.system.create().await.map_err(|e| JsValue::from_str(&e.to_string()))?;

    if let Err(_) = NODE.set(node) {
        error!("Failed to set node");
    }
    NOTIFY.notify_waiters();

    Ok(())
}

pub fn get_node() -> Node<IndexedDBStorageEngine, PermissiveAgent> { NODE.get().expect("Node not initialized").clone() }

#[wasm_bindgen]
pub fn ctx() -> Result<Context, JsValue> { get_node().context(c).map_err(|e| JsValue::from_str(&e.to_string())) }

#[wasm_bindgen]
pub async fn ready() -> Result<(), JsValue> {
    match NODE.get() {
        Some(_) => Ok(()),
        None => {
            NOTIFY.notified().await;
            Ok(())
        }
    }
}

// Test utility functions for TypeScript tests
#[wasm_bindgen]
pub async fn clear_database() -> Result<(), JsValue> {
    // For now, we'll recreate the node with a fresh database
    // TODO: Implement proper clear method if needed
    Ok(())
}
