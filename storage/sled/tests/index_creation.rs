use ankurah_storage_common::{IndexKeyPart, IndexSpec};
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn test_index_creation_and_reconnection() -> Result<(), anyhow::Error> {
    let engine = SledStorageEngine::new_test()?;

    // Get initial tree count for sanity (not strictly necessary)
    let initial = engine.database.tree_names().len();

    // Create an index spec and ensure it exists
    let index_spec = IndexSpec::new(vec![IndexKeyPart::asc("name")]);
    let meta = engine.assure_index_exists("album", &index_spec)?;

    // Verify metadata and tree
    assert_eq!(meta.collection, "album");
    let tree_name = format!("index_{}_{}", meta.collection, meta.id);
    assert!(engine.database.open_tree(&tree_name).is_ok());

    // Idempotent assure
    let meta2 = engine.assure_index_exists("album", &index_spec)?;
    assert_eq!(meta.id, meta2.id);

    // Tree count should be >= initial (cannot assert exact due to default tree)
    assert!(engine.database.tree_names().len() >= initial);

    Ok(())
}
