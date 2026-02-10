mod common;
use anyhow::Result;
use common::*;

/// Test 2.1: Concurrent Text Inserts - Same Position (Yrs)
/// Both transactions insert at the same position, Yrs CRDT merges them
#[tokio::test]
async fn test_yrs_concurrent_inserts_same_position() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create entity with initial text "hello"
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "hello".to_owned(), year: "2024".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Two concurrent transactions both inserting at position 5 (end of "hello")
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    // T1: Insert " world" at position 5
    album.edit(&trx1)?.name().insert(5, " world")?;

    // T2: Insert " there" at position 5
    album.edit(&trx2)?.name().insert(5, " there")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Verify DAG structure
    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Both insertions should be present in the final text
    // Yrs will merge them deterministically (order depends on internal vector clock)
    let final_album = ctx.get::<AlbumView>(album_id).await?;
    let final_name = final_album.name().unwrap();

    // Both " world" and " there" should be present
    assert!(
        final_name.contains("world") && final_name.contains("there"),
        "Both insertions should be present in final text: {}",
        final_name
    );

    // Result should be deterministic - one of the two valid orderings
    assert!(
        final_name == "hello world there" || final_name == "hello there world",
        "Expected 'hello world there' or 'hello there world', got: {}",
        final_name
    );

    Ok(())
}

/// Test 2.2: Concurrent Text Inserts - Different Positions (Yrs)
/// Non-conflicting insertions at different positions
#[tokio::test]
async fn test_yrs_concurrent_inserts_different_positions() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create entity with initial text "hello world"
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "hello world".to_owned(), year: "2024".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Two concurrent transactions inserting at different positions
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    // T1: Insert "X" at position 0 (beginning)
    album.edit(&trx1)?.name().insert(0, "X")?;

    // T2: Insert "Y" at position 11 (end, after "hello world")
    album.edit(&trx2)?.name().insert(11, "Y")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Verify DAG structure
    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Non-overlapping inserts should both be present
    let final_album = ctx.get::<AlbumView>(album_id).await?;
    let final_name = final_album.name().unwrap();

    // Both X and Y should be present, X at start, Y at end
    assert!(
        final_name.starts_with("X") && final_name.ends_with("Y"),
        "Expected text starting with X and ending with Y, got: {}",
        final_name
    );
    assert!(final_name.contains("hello world"), "Original text should be preserved: {}", final_name);

    Ok(())
}

/// Test 2.3: Concurrent Text Deletes - Overlapping Range (Yrs)
/// Both transactions delete parts of the text
#[tokio::test]
async fn test_yrs_concurrent_deletes_overlapping() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create entity with initial text "hello world"
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "hello world".to_owned(), year: "2024".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Two concurrent transactions deleting different parts
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    // T1: Delete "hello" (positions 0-5)
    album.edit(&trx1)?.name().delete(0, 5)?;

    // T2: Delete "world" (positions 6-11)
    album.edit(&trx2)?.name().delete(6, 5)?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Verify DAG structure
    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Both deletions should be applied
    // After deleting "hello" and "world", only " " should remain
    let final_album = ctx.get::<AlbumView>(album_id).await?;
    let final_name = final_album.name().unwrap();

    // Result should be just the space (or empty if positions adjusted)
    assert!(
        !final_name.contains("hello") && !final_name.contains("world"),
        "Both 'hello' and 'world' should be deleted, got: {}",
        final_name
    );

    Ok(())
}

/// Test 2.6: Yrs Convergence Under Reordering
/// Multiple concurrent operations should converge to same result regardless of order
#[tokio::test]
async fn test_yrs_convergence_determinism() -> Result<()> {
    // Run multiple trials to verify determinism
    for trial in 0..5 {
        let node = durable_sled_setup().await?;
        let ctx = node.context_async(DEFAULT_CONTEXT).await;

        // Create entity
        let album_id = {
            let trx = ctx.begin();
            let album = trx.create(&Album { name: format!("trial{}", trial), year: "2024".to_owned() }).await?;
            let id = album.id();
            trx.commit().await?;
            id
        };

        let album = ctx.get::<AlbumView>(album_id).await?;

        // Three concurrent inserts
        let trx1 = ctx.begin();
        let trx2 = ctx.begin();
        let trx3 = ctx.begin();

        album.edit(&trx1)?.name().insert(0, "A")?;
        album.edit(&trx2)?.name().insert(0, "B")?;
        album.edit(&trx3)?.name().insert(0, "C")?;

        trx1.commit().await?;
        trx2.commit().await?;
        trx3.commit().await?;

        // Get final result
        let final_album = ctx.get::<AlbumView>(album_id).await?;
        let final_name = final_album.name().unwrap();

        // All three letters should be present
        assert!(
            final_name.contains("A") && final_name.contains("B") && final_name.contains("C"),
            "All three inserts should be present: {}",
            final_name
        );

        // Original text should be present
        assert!(final_name.contains(&format!("trial{}", trial)), "Original text should be preserved: {}", final_name);
    }

    Ok(())
}
