#[path = "../../common/tests/support/membership.rs"]
mod cases;

#[tokio::test]
async fn joined_memberships() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
