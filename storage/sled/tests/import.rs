#[path = "../../common/tests/support/import.rs"]
mod cases;

#[tokio::test]
async fn imported_definitions_and_minted_entities_are_the_same_entities_under_two_roots() -> anyhow::Result<()> {
    use ankurah_storage_sled::SledStorageEngine;
    cases::check(SledStorageEngine::new_test()?, SledStorageEngine::new_test()?).await
}
