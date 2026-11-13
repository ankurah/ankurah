#[path = "../../common/tests/support/id_order.rs"]
mod cases;

use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn id_comparisons_and_ordering_follow_byte_order() -> anyhow::Result<()> {
    let name = format!("id_order_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check(&engine).await?;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
