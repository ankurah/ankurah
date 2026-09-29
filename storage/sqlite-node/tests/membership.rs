//! The shared membership cases, run against the node engine.

#[path = "../../common/tests/support/membership.rs"]
mod cases;

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
async fn joined_memberships() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sqlite_node::SqliteNodeStorageEngine::open_in_memory().await?).await
}
