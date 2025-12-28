//! WASM bindings for Vitest browser tests.
//!
//! Provides test models and setup functions for testing Ref<T> preprocessing
//! at the JS→Rust boundary.

use std::sync::Arc;

use ankurah::{policy::DEFAULT_CONTEXT, property::Json, Context, Model, Node, PermissiveAgent, Ref};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Initialize console_error_panic_hook for better error messages
#[wasm_bindgen(start)]
pub fn init() { console_error_panic_hook::set_once(); }

// ============================================================================
// Test Models
// ============================================================================

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TestUser {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TestRoom {
    pub name: String,
}

/// Message with Ref<T> fields - the main test subject
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TestMessage {
    #[active_type(LWW)]
    pub user: Ref<TestUser>,
    #[active_type(LWW)]
    pub room: Ref<TestRoom>,
    pub text: String,
}

/// Entity with Json field - tests Json serialization to JS
#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TestConfig {
    pub name: String,
    pub settings: Json,
}

// ============================================================================
// Test Context Setup
// ============================================================================

/// Create a test context with ephemeral IndexedDB storage.
/// Returns Context for use in tests.
#[wasm_bindgen]
pub async fn create_test_context(db_name: &str) -> Result<Context, JsValue> {
    let storage = IndexedDBStorageEngine::open(db_name).await.map_err(|e| JsValue::from_str(&e.to_string()))?;

    let node = Node::new_durable(Arc::new(storage), PermissiveAgent::new());

    node.system.create().await.map_err(|e| JsValue::from_str(&e.to_string()))?;

    node.context(DEFAULT_CONTEXT).map_err(|e| JsValue::from_str(&e.to_string()))
}

/// Clean up a test database
#[wasm_bindgen]
pub async fn cleanup_test_db(db_name: &str) -> Result<(), JsValue> {
    IndexedDBStorageEngine::cleanup(db_name).await.map_err(|e| JsValue::from_str(&e.to_string()))
}
