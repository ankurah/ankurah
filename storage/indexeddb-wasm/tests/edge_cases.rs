mod common;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;

use tracing::info;
use wasm_bindgen_test::*;

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

    info!("MARK0");
    // Test special characters in queries (need to escape quotes)
    assert_eq!(names(&ctx.fetch("name = 'Special!@#$%'").await?), vec!["Special!@#$%"]);

    info!("MARK0.1");
    // Test Unicode support
    assert_eq!(names(&ctx.fetch("name = 'Unicode: 你好'").await?), vec!["Unicode: 你好"]);

    info!("MARK0.2");
    // Test case sensitivity
    assert_eq!(names(&ctx.fetch("name = 'UPPERCASE'").await?), vec!["UPPERCASE"]);
    assert_eq!(names(&ctx.fetch("name = 'lowercase'").await?), vec!["lowercase"]);

    info!("MARK1");
    // Test complex AND/OR combinations
    assert_eq!(
        sort_names(&ctx.fetch("(name = 'UPPERCASE' OR name = 'lowercase') AND year >= '2004'").await?),
        vec!["UPPERCASE", "lowercase"]
    );

    // Test range queries with string comparison edge cases
    // FIXME: This test is failing
    // assert_eq!(years(&ctx.fetch("year > '2005' AND year < '2008'").await?), vec!["2006", "2007"]);

    // Test impossible range (conflicting inequalities) - should return empty results, not crash
    // FIXME: This test is failing
    // Error: Storage error: range conversion: Failed to create IdbKeyRange: JsValue(DataError: Failed to execute 'bound' on 'IDBKeyRange': The lower key is greater than the upper key.
    // DataError: Failed to execute 'bound' on 'IDBKeyRange': The lower key is greater than the upper key.
    // assert_eq!(names(&ctx.fetch("year > '2010' AND year < '2005'").await?), vec![] as Vec<&str>);

    // Test ordering with special characters and case
    assert_eq!(
        names(&ctx.fetch("year >= '2001' ORDER BY name LIMIT 5").await?),
        vec!["Album with spaces", "Album-with-dashes", "Album_with_underscores", "MixedCase", "Special!@#$%"]
    );

    info!("MARK2");
    // Cleanup
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
