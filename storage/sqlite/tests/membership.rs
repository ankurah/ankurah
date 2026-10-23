#[path = "../../common/tests/support/membership.rs"]
mod cases;
// The same tests run natively under tokio, and on wasm32 under wasm-bindgen-test in Node.
#[cfg(not(target_arch = "wasm32"))]
use tokio::test as engine_test;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as engine_test;

#[engine_test]
async fn joined_memberships() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
