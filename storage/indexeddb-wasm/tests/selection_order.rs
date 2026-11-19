use std::sync::Arc;

use ankurah_core::test_helpers::selection_conformance;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn numeric_order_matches_digest_trees() -> anyhow::Result<()> {
    let name = format!("numeric_selection_order_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    let result = selection_conformance::numeric_ordering(engine.clone()).await;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    result
}

#[wasm_bindgen_test]
async fn json_order_matches_digest_trees() -> anyhow::Result<()> {
    let name = format!("json_selection_order_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    let result = selection_conformance::json_ordering(engine.clone()).await;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    result
}

#[wasm_bindgen_test]
async fn json_path_order_matches_digest_trees() -> anyhow::Result<()> {
    let name = format!("json_path_selection_order_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    let result = selection_conformance::json_path_ordering(engine.clone()).await;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    result
}

#[wasm_bindgen_test]
async fn mixed_order_matches_digest_trees() -> anyhow::Result<()> {
    let name = format!("mixed_selection_order_{}", ulid::Ulid::new());
    let engine = Arc::new(IndexedDBStorageEngine::open(&name).await?);
    let result = selection_conformance::mixed_ordering(engine.clone()).await;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    result
}
