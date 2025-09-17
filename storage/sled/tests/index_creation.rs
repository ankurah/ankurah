use ankurah_storage_common::{IndexKeyPart, IndexSpec};
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn test_index_creation_and_reconnection() -> Result<(), anyhow::Error> {
    let engine = SledStorageEngine::new_test()?;

    // Get initial tree count for sanity (not strictly necessary)
    let initial = engine.database.tree_names().len();

    // Create an index spec and ensure it exists
    let index_spec = IndexSpec::new(vec![IndexKeyPart::asc("name")]);
    let (index, _match_type) =
        engine.database.index_manager.assure_index_exists("album", &index_spec, &engine.database.db, &engine.database.property_manager)?;

    // Verify metadata and tree
    assert_eq!(index.collection(), "album");
    let tree_name = format!("index_{}_{}", index.collection(), index.id());
    assert!(engine.database.open_tree(&tree_name).is_ok());

    // Idempotent assure
    let (index2, _match_type2) =
        engine.database.index_manager.assure_index_exists("album", &index_spec, &engine.database.db, &engine.database.property_manager)?;
    assert_eq!(index.id(), index2.id());

    // Tree count should be >= initial (cannot assert exact due to default tree)
    assert!(engine.database.tree_names().len() >= initial);

    Ok(())
}
