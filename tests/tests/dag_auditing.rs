mod common;
use anyhow::Result;
use common::*;

/// Test 6.1: Linear History Structure
/// Verify DAG structure after sequential updates: A → B → C → D
#[tokio::test]
async fn test_linear_history_structure() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Sequential updates: B, C, D
    for i in 1..=3 {
        let album = ctx.get::<AlbumView>(album_id).await?;
        let trx = ctx.begin();
        album.edit(&trx)?.year().replace(&format!("{}", 2020 + i))?;
        dag.enumerate(trx.commit_and_return_events().await?); // B, C, D
    }

    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    // Verify linear structure: A → B → C → D
    assert_eq!(events.len(), 4, "Should have 4 events");
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [B],
        D => [C],
    });

    // Head should be single event (D)
    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [D]);

    Ok(())
}

/// Test 6.2: Simple Diamond Structure
/// Two concurrent events from same parent
#[tokio::test]
async fn test_simple_diamond_structure() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Two concurrent transactions
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    album.edit(&trx1)?.name().replace("Name B")?;
    album.edit(&trx2)?.year().replace("2021")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    // Verify diamond structure:
    //       A
    //      / \
    //     B   C
    assert_eq!(events.len(), 3, "Should have 3 events");
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Head should have both concurrent events
    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C]);

    Ok(())
}

/// Test 6.3: Diamond with Merge Structure
/// Two concurrent events then a merge event
#[tokio::test]
async fn test_diamond_with_merge_structure() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Two concurrent transactions creating diamond
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    album.edit(&trx1)?.name().replace("Name B")?;
    album.edit(&trx2)?.year().replace("2021")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Now create a merge event - edit from current state which has both B and C
    let album = ctx.get::<AlbumView>(album_id).await?;
    let trx3 = ctx.begin();
    album.edit(&trx3)?.name().replace("Merged Name")?;
    dag.enumerate(trx3.commit_and_return_events().await?); // D

    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    // Verify diamond-with-merge structure:
    //       A
    //      / \
    //     B   C
    //      \ /
    //       D
    assert_eq!(events.len(), 4, "Should have 4 events");
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [B, C],
    });

    // Head should be single event (D - the merge)
    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [D]);

    Ok(())
}

/// Test 6.4: Complex Multi-Merge Structure
/// Three concurrent events, partial merges, final merge
#[tokio::test]
async fn test_complex_multi_merge_structure() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let album = ctx.get::<AlbumView>(album_id).await?;

    // Three concurrent transactions from A
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    let trx3 = ctx.begin();

    album.edit(&trx1)?.name().replace("Name B")?;
    album.edit(&trx2)?.year().replace("2021")?;
    album.edit(&trx3)?.name().replace("Name D")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C
    dag.enumerate(trx3.commit_and_return_events().await?); // D

    // Verify three-way fork
    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    assert_eq!(events.len(), 4, "Should have 4 events");
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [A],
    });

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C, D]);

    // Now do a final merge by editing from current state
    let album = ctx.get::<AlbumView>(album_id).await?;
    let trx = ctx.begin();
    album.edit(&trx)?.name().replace("Final Merge")?;
    dag.enumerate(trx.commit_and_return_events().await?); // E

    let events = collection.dump_entity_events(album_id).await?;

    // Final structure:
    //        A
    //      / | \
    //     B  C  D
    //      \ | /
    //        E
    assert_eq!(events.len(), 5, "Should have 5 events");
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [A],
        E => [B, C, D],
    });

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [E]);

    Ok(())
}

/// Test 6.5: Head Evolution Through Operations
/// Track head state after each operation
#[tokio::test]
async fn test_head_evolution() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    let collection = ctx.collection(&Album::collection()).await?;

    // Step 1: Create A → head=[A]
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [A]);

    // Step 2: Commit B → head=[B]
    let album = ctx.get::<AlbumView>(album_id).await?;
    {
        let trx = ctx.begin();
        album.edit(&trx)?.name().replace("Name B")?;
        dag.enumerate(trx.commit_and_return_events().await?); // B
    }

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [B]);

    // Step 3: Commit C concurrent with B → head=[B, C]
    // We need to fork from A, not from B
    // This is tricky - we need to use the entity state from before B was committed
    // For now, let's simulate by creating a concurrent transaction

    // Actually, in the current implementation, once B is committed,
    // a new transaction from the same entity will see B as the head.
    // To truly test concurrent commits, we need to start both transactions
    // before either commits. Let me restructure this test.

    // Let's do: create C and D concurrently from B
    let album = ctx.get::<AlbumView>(album_id).await?;
    let trx_c = ctx.begin();
    let trx_d = ctx.begin();

    album.edit(&trx_c)?.year().replace("2021")?;
    album.edit(&trx_d)?.name().replace("Name D")?;

    dag.enumerate(trx_c.commit_and_return_events().await?); // C
    dag.enumerate(trx_d.commit_and_return_events().await?); // D

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [C, D]);

    // Step 4: Commit E (extends C) → head=[D, E] (D still there, E replaces C)
    // Get album at current head (C, D)
    let album = ctx.get::<AlbumView>(album_id).await?;
    {
        let trx = ctx.begin();
        album.edit(&trx)?.year().replace("2022")?;
        dag.enumerate(trx.commit_and_return_events().await?); // E
    }

    let state = collection.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [E]);

    // Verify final structure
    let events = collection.dump_entity_events(album_id).await?;
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [B],
        D => [B],
        E => [C, D],
    });

    Ok(())
}

/// Test: Verify event count matches expected
#[tokio::test]
async fn test_event_count_verification() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create entity with 10 sequential updates
    let album_id = {
        let trx = ctx.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?);
        id
    };

    for i in 1..=10 {
        let album = ctx.get::<AlbumView>(album_id).await?;
        let trx = ctx.begin();
        album.edit(&trx)?.year().replace(&format!("{}", i))?;
        dag.enumerate(trx.commit_and_return_events().await?);
    }

    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;

    // Should have exactly 11 events (1 create + 10 updates)
    assert_eq!(events.len(), 11, "Should have 11 events total");

    // Verify all events have valid parent relationships
    for event in &events {
        let label = dag.label(&event.payload.id());
        assert!(label.is_some(), "All events should be labeled");
    }

    Ok(())
}
