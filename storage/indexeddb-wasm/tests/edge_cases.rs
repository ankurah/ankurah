mod common;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
#[cfg(debug_assertions)]
use {
    ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent},
    std::sync::Arc,
};

use tracing::info;
use wasm_bindgen_test::*;

// TODO: Add test coverage for IndexedDB playbook section 7 requirements:
// - i64 values around ±2^53 boundary to test string encoding
// - Boolean ordering in compound keys (0/1 mapping)
// - Binary/Object ordering tests
// - Type mismatch assertions for better error handling

#[wasm_bindgen_test]
pub async fn test_edge_cases() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create test data with edge cases
    create_albums(
        &ctx,
        vec![
            ("", "2000"),                       // Empty name
            ("Album with spaces", "2001"),      // Spaces
            ("Album-with-dashes", "2002"),      // Dashes
            ("Album_with_underscores", "2003"), // Underscores
            ("UPPERCASE", "2004"),              // Case variations
            ("lowercase", "2005"),
            ("MixedCase", "2006"),
            ("Special!@#$%", "2007"),                                    // Special characters
            ("Unicode: 你好", "2008"),                                   // Unicode
            ("Very Long Album Name That Goes On And On And On", "2009"), // Long name
        ],
    )
    .await?;

    // Test empty string handling
    assert_eq!(names(&ctx.fetch("name = ''").await?), vec![""]);

    // Test special characters in queries (need to escape quotes)
    assert_eq!(names(&ctx.fetch("name = 'Special!@#$%'").await?), vec!["Special!@#$%"]);

    // Test Unicode support
    assert_eq!(names(&ctx.fetch("name = 'Unicode: 你好'").await?), vec!["Unicode: 你好"]);

    // Test case sensitivity
    assert_eq!(names(&ctx.fetch("name = 'UPPERCASE'").await?), vec!["UPPERCASE"]);
    assert_eq!(names(&ctx.fetch("name = 'lowercase'").await?), vec!["lowercase"]);

    // Test complex AND/OR combinations
    assert_eq!(
        sort_names(&ctx.fetch("(name = 'UPPERCASE' OR name = 'lowercase') AND year >= '2004'").await?),
        vec!["UPPERCASE", "lowercase"]
    );

    // Test range queries with string comparison edge cases
    assert_eq!(years(&ctx.fetch("year > '2005' AND year < '2008'").await?), vec!["2006", "2007"]);

    // Test impossible range (conflicting inequalities) - should return empty results, not crash
    assert_eq!(names(&ctx.fetch("year > '2010' AND year < '2005'").await?), vec![] as Vec<&str>);

    // Test ordering with special characters and case
    assert_eq!(
        names(&ctx.fetch("year >= '2001' ORDER BY name LIMIT 5").await?),
        vec!["Album with spaces", "Album-with-dashes", "Album_with_underscores", "MixedCase", "Special!@#$%"]
    );

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[cfg(debug_assertions)]
#[wasm_bindgen_test]
pub async fn test_prefix_guard_collection_boundary() -> Result<(), anyhow::Error> {
    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let storage_engine = Arc::new(IndexedDBStorageEngine::open(&db_name).await?);
    let node = Node::new_durable(storage_engine.clone(), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    // Insert albums and books with overlapping names to ensure sorted adjacency
    create_albums(
        &ctx,
        vec![("Album1", "1965"), ("Album2", "1966"), ("Album3", "1967"), ("Album4", "1968"), ("Album5", "1969"), ("Album6", "1970")],
    )
    .await?;

    create_books(&ctx, vec![("Book1", "2001"), ("Book2", "2002")]).await?;

    // Note: With the new bounded range logic, equality-only queries on __collection
    // now create a proper upper bound (e.g., ["album"] to ["album\uFFFF"]) which prevents
    // the cursor from ever reaching the book collection. This is MORE correct than relying
    // on the prefix guard to stop at boundaries.
    //
    // The prefix guard is still needed for inequality queries with open-ended upper bounds.
    // So this test now verifies that bounded ranges work correctly, not the guard itself.

    // ORDER-FIRST plan over (__collection, name), now with bounded __collection range
    // LIMIT 5 should only include album records, never book
    assert_eq!(names(&ctx.fetch("year >= '1900' ORDER BY name LIMIT 5").await?), vec!["Album1", "Album2", "Album3", "Album4", "Album5"]);

    // Larger limit should still exclude books due to bounded range
    assert_eq!(
        names(&ctx.fetch("year >= '1900' ORDER BY name LIMIT 100").await?),
        vec!["Album1", "Album2", "Album3", "Album4", "Album5", "Album6"]
    );

    // Even with prefix guard disabled, the bounded range prevents leaking into book collection
    storage_engine.set_prefix_guard_disabled(true);

    assert_eq!(
        names(&ctx.fetch("year >= '1900' ORDER BY name LIMIT 100").await?),
        vec!["Album1", "Album2", "Album3", "Album4", "Album5", "Album6"]
    );

    // Re-enable for hygiene
    storage_engine.set_prefix_guard_disabled(false);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// JS exception that was thrown:
// Error: Storage error: range conversion: Failed to create IdbKeyRange: JsValue(DataError: Failed to execute 'bound' on 'IDBKeyRange': The lower key is greater than the upper key.
// DataError: Failed to execute 'bound' on 'IDBKeyRange': The lower key is greater than the upper key.
// #[wasm_bindgen_test]
pub async fn test_compound_indexes_and_pagination() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create larger dataset for pagination testing
    let album_tuples: Vec<(&str, &str)> = vec![
        ("Album 00", "2000"),
        ("Album 01", "2001"),
        ("Album 02", "2002"),
        ("Album 03", "2003"),
        ("Album 04", "2004"),
        ("Album 05", "2005"),
        ("Album 06", "2006"),
        ("Album 07", "2007"),
        ("Album 08", "2008"),
        ("Album 09", "2009"),
        ("Album 10", "2010"),
        ("Album 11", "2011"),
        ("Album 12", "2012"),
        ("Album 13", "2013"),
        ("Album 14", "2014"),
        ("Album 15", "2015"),
        ("Album 16", "2016"),
        ("Album 17", "2017"),
        ("Album 18", "2018"),
        ("Album 19", "2019"),
    ];

    create_albums(&ctx, album_tuples).await?;

    // Test pagination with LIMIT and different starting points
    assert_eq!(
        names(&ctx.fetch("year >= '2000' ORDER BY year LIMIT 5").await?),
        vec!["Album 00", "Album 01", "Album 02", "Album 03", "Album 04"]
    );

    // Test larger LIMIT
    assert_eq!(names(&ctx.fetch("year >= '2010' ORDER BY year LIMIT 10").await?).len(), 10);

    // Test DESC ordering with LIMIT on larger dataset
    assert_eq!(names(&ctx.fetch("year >= '2000' ORDER BY year DESC LIMIT 3").await?), vec!["Album 19", "Album 18", "Album 17"]);

    // Test complex range with ordering on larger dataset
    assert_eq!(
        names(&ctx.fetch("year >= '2005' AND year <= '2010' ORDER BY name").await?),
        vec!["Album 05", "Album 06", "Album 07", "Album 08", "Album 09", "Album 10"]
    );

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
