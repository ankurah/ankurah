#[path = "../../common/tests/support/integer_order.rs"]
mod cases;

#[tokio::test]
async fn integer_comparisons_follow_numeric_order() -> anyhow::Result<()> {
    cases::check_comparisons(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}

#[tokio::test]
async fn integer_ordering_follows_numeric_order() -> anyhow::Result<()> {
    cases::check_ordering(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
