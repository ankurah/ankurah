#[path = "../../common/tests/support/integer_order.rs"]
mod cases;

#[cfg(not(target_arch = "wasm32"))]
use tokio::test as engine_test;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test as engine_test;

#[engine_test]
async fn integer_comparisons_follow_numeric_order() -> anyhow::Result<()> {
    cases::check_comparisons(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}

#[engine_test]
async fn integer_ordering_follows_numeric_order() -> anyhow::Result<()> {
    cases::check_ordering(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
