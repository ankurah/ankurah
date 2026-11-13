#[path = "../../common/tests/support/id_order.rs"]
mod cases;

#[tokio::test]
async fn id_comparisons_and_ordering_follow_byte_order() -> anyhow::Result<()> {
    cases::check(&ankurah_storage_sled::SledStorageEngine::new_test()?).await
}
