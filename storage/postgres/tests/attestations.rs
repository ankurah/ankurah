#[path = "../../common/tests/support/attestations.rs"]
mod cases;
mod common;

#[tokio::test]
async fn every_attestation_reads_back_byte_for_byte() -> anyhow::Result<()> {
    let (_container, engine) = common::create_postgres_container().await?;
    cases::check(&engine).await
}
