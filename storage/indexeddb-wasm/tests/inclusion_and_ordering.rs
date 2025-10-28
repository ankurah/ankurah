mod common;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;

use wasm_bindgen_test::*;

#[wasm_bindgen_test]
pub async fn test_comprehensive_set_inclusion_and_ordering() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create test data with duplicates and edge cases
    create_albums(
        &ctx,
        vec![
            ("Abbey Road", "1969"),
            ("Revolver", "1966"),
            ("Sgt. Pepper", "1967"),
            ("The White Album", "1968"),
            ("Let It Be", "1970"),
            ("Abbey Road Deluxe", "1969"), // Duplicate year
            ("Help!", "1965"),
        ],
    )
    .await?;

    // DESC ordering (reverse scan direction)
    assert_eq!(years(&ctx.fetch("year >= '1965' ORDER BY year DESC").await?), vec!["1970", "1969", "1969", "1968", "1967", "1966", "1965"]);

    // single operators
    assert_eq!(sort_names(&ctx.fetch("year = '1969'").await?), vec!["Abbey Road", "Abbey Road Deluxe"]);
    assert_eq!(years(&ctx.fetch("year < '1968'").await?), vec!["1965", "1966", "1967"]);
    assert_eq!(years(&ctx.fetch("year <= '1968'").await?), vec!["1965", "1966", "1967", "1968"]);
    assert_eq!(names(&ctx.fetch("year = '1999'").await?), Vec::<String>::new());
    assert_eq!(names(&ctx.fetch("name = 'Help!'").await?), vec!["Help!"]);

    // Complex range with ordering
    assert_eq!(
        names(&ctx.fetch("year >= '1967' AND year <= '1969' ORDER BY name").await?),
        vec!["Abbey Road", "Abbey Road Deluxe", "Sgt. Pepper", "The White Album"]
    );

    // DESC ordering by name
    assert_eq!(
        names(&ctx.fetch("year >= '1965' ORDER BY name DESC").await?),
        vec!["The White Album", "Sgt. Pepper", "Revolver", "Let It Be", "Help!", "Abbey Road Deluxe", "Abbey Road"]
    );

    // LIMIT with DESC ordering
    assert_eq!(years(&ctx.fetch("year >= '1965' ORDER BY year DESC LIMIT 3").await?), vec!["1970", "1969", "1969"]);

    // Set exclusion validation. Should NOT contain Help! (1965), Revolver (1966), Sgt. Pepper (1967)
    assert_eq!(sort_names(&ctx.fetch("year >= '1968'").await?), vec!["Abbey Road", "Abbey Road Deluxe", "Let It Be", "The White Album"]);

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_i64_timestamp_range_queries() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create test data with large i64-like timestamps (milliseconds since epoch)
    // Using Album model with year field to test i64 range queries
    // Base: 1761455309171 (~2025-10-26), increments of 90000ms (90 seconds)
    create_albums(
        &ctx,
        vec![
            ("Event0", "1761455309171"), // base
            ("Event1", "1761455399171"), // base + 90k
            ("Event2", "1761455489171"), // base + 180k
            ("Event3", "1761455579171"), // base + 270k
            ("Event4", "1761455669171"), // base + 360k
            ("Event5", "1761455759171"), // base + 450k
            ("Event6", "1761455849171"), // base + 540k
            ("Event7", "1761455939171"), // base + 630k
            ("Event8", "1761456029171"), // base + 720k
            ("Event9", "1761456119171"), // base + 810k
        ],
    )
    .await?;

    // Test 1: Exact equality on i64-like value (single value)
    let results = ctx.fetch::<AlbumView>("year = '1761455309171'").await?;
    assert_eq!(results.len(), 1, "Should find exactly one album with exact year");

    // Test 2: Range query with exclusive bounds (> and <)
    // Tests: year > 1761455399171 AND year < 1761455759171
    let results = ctx.fetch::<AlbumView>("year > '1761455399171' AND year < '1761455759171'").await?;
    assert_eq!(results.len(), 3, "Should find 3 albums in exclusive range (years 2-4)");

    // Test 3: Range query with inclusive bounds (>= and <=)
    let results = ctx.fetch::<AlbumView>("year >= '1761455399171' AND year <= '1761455759171'").await?;
    assert_eq!(results.len(), 5, "Should find 5 albums in inclusive range (years 1-5)");

    // Test 4: ORDER BY year DESC with LIMIT
    let results = ctx.fetch::<AlbumView>("year >= '0' ORDER BY year DESC LIMIT 3").await?;
    assert_eq!(results.len(), 3, "Should return 3 most recent albums");
    // Verify descending order (string comparison, but should still work)
    let year0 = results[0].year().unwrap().parse::<i64>().unwrap();
    let year1 = results[1].year().unwrap().parse::<i64>().unwrap();
    let year2 = results[2].year().unwrap().parse::<i64>().unwrap();
    assert!(year0 > year1);
    assert!(year1 > year2);

    // Test 5: Verify values are stored and retrieved correctly
    let all_results = ctx.fetch::<AlbumView>("year >= '0'").await?;
    assert_eq!(all_results.len(), 10, "Should retrieve all 10 albums");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
