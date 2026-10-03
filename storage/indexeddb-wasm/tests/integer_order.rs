#[path = "../../common/tests/support/integer_order.rs"]
mod cases;

use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn integer_comparisons_follow_numeric_order() -> anyhow::Result<()> {
    let name = format!("integer_comparisons_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check_comparisons(&engine).await?;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn integer_ordering_follows_numeric_order() -> anyhow::Result<()> {
    let name = format!("integer_ordering_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check_ordering(&engine).await?;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
