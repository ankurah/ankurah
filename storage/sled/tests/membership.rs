#[path = "../../common/tests/support/membership.rs"]
mod cases;

#[tokio::test]
async fn indexed_entity_id_order() -> anyhow::Result<()> {
    cases::check_id_order(&ankurah_storage_sled::SledStorageEngine::new_test()?).await
}

#[tokio::test]
async fn joined_memberships() -> anyhow::Result<()> { cases::check(&ankurah_storage_sled::SledStorageEngine::new_test()?).await }

#[tokio::test]
async fn indexed_memberships() -> anyhow::Result<()> {
    let engine = ankurah_storage_sled::SledStorageEngine::new_test()?;
    cases::check_indexed(&engine).await?;
    let database = engine.database.lock().unwrap();
    let indexes = database.index_manager.indexes.read().unwrap();
    assert_eq!(indexes.len(), 1, "both memberships must use the same property index, not scan or create model-owned indexes");
    let index = indexes.values().next().unwrap();
    assert_eq!(index.spec().keyparts[0].key, "__materialization");
    assert_eq!(index.tree().len(), 7, "one entry per membership, including the membership added after backfill");
    Ok(())
}

#[path = "../../common/tests/support/membership_removal.rs"]
mod removal;

#[tokio::test]
async fn removed_memberships() -> anyhow::Result<()> {
    let engine = ankurah_storage_sled::SledStorageEngine::new_test()?;
    cases::check_removal(&engine).await?;
    let database = engine.database.lock().unwrap();
    let indexes = database.index_manager.indexes.read().unwrap();
    let entries: usize = indexes.values().map(|index| index.tree().len()).sum();
    assert_eq!(entries, 1, "only the membership the store still holds keeps a property index entry");
    Ok(())
}

#[tokio::test]
async fn a_removed_membership_leaves_live_queries() -> anyhow::Result<()> {
    removal::check_live_removal(ankurah_storage_sled::SledStorageEngine::new_test()?).await
}
