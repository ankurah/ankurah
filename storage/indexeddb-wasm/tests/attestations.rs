#[path = "../../common/tests/support/attestations.rs"]
mod cases;

use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn every_attestation_reads_back_byte_for_byte() -> anyhow::Result<()> {
    let name = format!("attestations_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    cases::check(&engine).await?;
    engine.db.close().await;
    IndexedDBStorageEngine::cleanup(&name).await?;
    Ok(())
}
