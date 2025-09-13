mod common;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;

use wasm_bindgen_test::*;

// Note: Database connection and index creation tests moved to separate files
// Range constraint tests have been replaced by planner integration tests

#[wasm_bindgen_test]
pub async fn test_indexeddb_basic_workflow() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create a simple test - just verify IndexedDB storage works
    create_albums(&ctx, vec![("Walking on a Dream", "2008")]).await?;

    // Verify we can query the album
    assert_eq!(names(&ctx.fetch("name = 'Walking on a Dream'").await?), vec!["Walking on a Dream"]);
    assert_eq!(years(&ctx.fetch("name = 'Walking on a Dream'").await?), vec!["2008"]);

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
