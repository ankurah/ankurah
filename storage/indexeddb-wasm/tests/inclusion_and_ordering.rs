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
pub async fn test_room_filter_desc_limit() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    let base_ts: i64 = 1_762_643_440_000;
    let mut events = Vec::new();

    // Primary room events (36 items)
    for i in 0..36 {
        events.push(("chat-main", base_ts + i, true));
    }

    // Some events that should be filtered out
    for i in 0..5 {
        events.push(("chat-main", base_ts + 1_000 + i as i64, false));
    }

    // Other room noise
    for i in 0..10 {
        events.push(("chat-other", base_ts + i, true));
    }

    create_events(&ctx, events).await?;

    // Test ASC ordering first
    let results_asc = ctx.fetch::<EventView>("name = 'chat-main' AND active = true ORDER BY timestamp ASC LIMIT 20").await?;
    assert_eq!(results_asc.len(), 20, "Expected 20 events for chat-main (ASC)");
    let ts_asc = event_timestamps(&results_asc);
    assert!(ts_asc.windows(2).all(|pair| pair[0] < pair[1]), "Timestamps should be strictly increasing: {:?}", ts_asc);

    // Test DESC ordering
    let results_desc = ctx.fetch::<EventView>("name = 'chat-main' AND active = true ORDER BY timestamp DESC LIMIT 20").await?;
    assert_eq!(results_desc.len(), 20, "Expected 20 events for chat-main (DESC)");
    let ts_desc = event_timestamps(&results_desc);
    assert!(ts_desc.windows(2).all(|pair| pair[0] > pair[1]), "Timestamps should be strictly decreasing: {:?}", ts_desc);

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

#[wasm_bindgen_test]
pub async fn test_equality_prefix_edge_cases() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Test 1: Single equality column (the old eq_prefix_len == 1 case)
    // This should create an open-ended range, not a closed one
    create_events(&ctx, vec![("alpha", 100, true), ("alpha", 200, true), ("alpha", 300, true), ("bravo", 100, true)]).await?;

    let results = ctx.fetch::<EventView>("name = 'alpha' ORDER BY timestamp DESC LIMIT 2").await?;
    assert_eq!(results.len(), 2, "Single equality prefix should work with DESC");
    assert_eq!(event_timestamps(&results), vec![300, 200]);

    // Test 2: Bool boundary - ensure Bool(true) → I32(2) transition works
    // Create events where active=true is followed by other data
    create_events(&ctx, vec![("charlie", 1000, false), ("charlie", 2000, true), ("charlie", 3000, true)]).await?;

    let true_results = ctx.fetch::<EventView>("name = 'charlie' AND active = true ORDER BY timestamp DESC").await?;
    assert_eq!(true_results.len(), 2, "Should find exactly 2 true records");
    assert_eq!(event_timestamps(&true_results), vec![3000, 2000]);

    let false_results = ctx.fetch::<EventView>("name = 'charlie' AND active = false ORDER BY timestamp DESC").await?;
    assert_eq!(false_results.len(), 1, "Should find exactly 1 false record");
    assert_eq!(event_timestamps(&false_results), vec![1000]);

    // Test 3: Negative timestamps
    create_events(
        &ctx,
        vec![("delta", -300, true), ("delta", -200, true), ("delta", -100, true), ("delta", 0, true), ("delta", 100, true)],
    )
    .await?;

    let neg_results = ctx.fetch::<EventView>("name = 'delta' AND active = true ORDER BY timestamp DESC LIMIT 3").await?;
    assert_eq!(neg_results.len(), 3, "Should handle negative timestamps");
    assert_eq!(event_timestamps(&neg_results), vec![100, 0, -100]);

    let neg_asc = ctx.fetch::<EventView>("name = 'delta' AND active = true ORDER BY timestamp ASC LIMIT 3").await?;
    assert_eq!(event_timestamps(&neg_asc), vec![-300, -200, -100]);

    // Test 4: Zero values
    create_events(&ctx, vec![("echo", 0, false), ("echo", 0, true), ("echo", 1, false), ("echo", 1, true)]).await?;

    let zero_false = ctx.fetch::<EventView>("name = 'echo' AND timestamp = 0 AND active = false").await?;
    assert_eq!(zero_false.len(), 1, "Should find zero timestamp with false");

    let zero_true = ctx.fetch::<EventView>("name = 'echo' AND timestamp = 0 AND active = true").await?;
    assert_eq!(zero_true.len(), 1, "Should find zero timestamp with true");

    // Test 5: Empty result set with bounded range
    let empty = ctx.fetch::<EventView>("name = 'foxtrot' AND active = true ORDER BY timestamp DESC LIMIT 10").await?;
    assert_eq!(empty.len(), 0, "Empty result set should work with bounded range");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
