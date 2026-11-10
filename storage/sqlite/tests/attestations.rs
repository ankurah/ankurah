#[path = "../../common/tests/support/attestations.rs"]
mod cases;

#[tokio::test]
async fn every_attestation_reads_back_byte_for_byte() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sqlite::SqliteStorageEngine::open_in_memory().await?).await
}
