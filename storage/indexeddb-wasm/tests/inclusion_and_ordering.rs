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
    assert_eq!(sort_names(&ctx.fetch("year >= '1968'").await?), vec!["Abbey Road", "Abbey Road Deluxe", "The White Album", "Let It Be"]);

    // Cleanup
    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
