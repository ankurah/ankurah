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
pub async fn test_i64_bool_indexing() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create events with actual i64 timestamps and bool flags
    // This test exposes asymmetry in current code:
    // - extract_all_fields uses JsValue::from() → bool becomes true/false
    // - idb_key_tuple converts bool to 0/1
    create_events(
        &ctx,
        vec![
            ("Event0", 100, true),
            ("Event1", 200, false),
            ("Event2", 300, true),
            ("Event3", 400, false),
            ("Event4", 500, true),
            ("Event5", 600, false),
        ],
    )
    .await?;

    // Test 1: Range query on i64 timestamp
    let results = ctx.fetch::<EventView>("timestamp > 350").await?;
    assert_eq!(results.len(), 3, "Should find 3 events with timestamp > 350");

    // Test 2: Query by bool field (equality)
    let active_results = ctx.fetch::<EventView>("active = true").await?;
    assert_eq!(active_results.len(), 3, "Should find 3 active events");

    let inactive_results = ctx.fetch::<EventView>("active = false").await?;
    assert_eq!(inactive_results.len(), 3, "Should find 3 inactive events");

    // Test 3: Compound query (bool AND i64 range)
    let results = ctx.fetch::<EventView>("active = true AND timestamp >= 200").await?;
    assert_eq!(results.len(), 2, "Should find 2 active events with timestamp >= 200");

    // Test 4: ORDER BY timestamp DESC
    let results = ctx.fetch::<EventView>("timestamp > 0 ORDER BY timestamp DESC LIMIT 3").await?;
    assert_eq!(results.len(), 3, "Should return 3 most recent events");
    assert!(results[0].timestamp()? > results[1].timestamp()?, "Timestamps should be in descending order");
    assert!(results[1].timestamp()? > results[2].timestamp()?, "Timestamps should be in descending order");

    // Test 5: Disjunction with boolean forces residual predicate evaluation
    // Query: timestamp > 200 AND (active = true OR name = 'Event0')
    // - timestamp > 200 uses index (returns Event2, Event3, Event4, Event5)
    // - (active = true OR name = 'Event0') is residual predicate that filters to Event2, Event4
    let results = ctx.fetch::<EventView>("timestamp > 200 AND (active = true OR name = 'Event0')").await?;
    assert_eq!(results.len(), 2, "Should find 2 active events with timestamp > 200: Event2, Event4");

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

#[wasm_bindgen_test]
pub async fn test_large_i64_timestamp() -> Result<(), anyhow::Error> {
    // Test that very large i64 values (beyond safe integer range) work correctly
    let (ctx, db_name) = setup_context().await?;

    // Use timestamps beyond MAX_SAFE_INTEGER (9,007,199,254,740,991)
    // These will be stored as zero-padded strings but maintain correct ordering
    create_events(
        &ctx,
        vec![
            ("Event1", 9_007_199_254_740_990, true),  // Just below threshold (number)
            ("Event2", 9_007_199_254_740_991, false), // At threshold (number)
            ("Event3", 9_007_199_254_740_992, true),  // Beyond threshold (string)
            ("Event4", 9_007_199_254_741_000, false), // Beyond threshold (string)
        ],
    )
    .await?;

    // Range query spanning the number/string threshold
    let results = ctx.fetch::<EventView>("timestamp > 9007199254740990").await?;
    assert_eq!(results.len(), 3, "Should find 3 events after threshold");

    // Verify ordering is maintained across threshold
    let all_results = ctx.fetch::<EventView>("timestamp > 0 ORDER BY timestamp ASC").await?;
    assert_eq!(all_results.len(), 4);
    for i in 0..3 {
        assert!(all_results[i].timestamp()? < all_results[i + 1].timestamp()?, "Timestamps should be in ascending order");
    }

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
