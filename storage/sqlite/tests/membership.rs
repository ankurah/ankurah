#[path = "../../common/tests/support/membership.rs"]
mod cases;

#[tokio::test]
async fn joined_memberships() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}

#[path = "../../common/tests/support/membership_removal.rs"]
mod removal;

#[tokio::test]
async fn removed_memberships() -> anyhow::Result<()> {
    cases::check_removal(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}

#[tokio::test]
async fn a_removed_membership_leaves_live_queries() -> anyhow::Result<()> {
    removal::check_live_removal(ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
