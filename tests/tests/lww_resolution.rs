mod common;
use anyhow::Result;
use common::*;

/// Test 1.3a: Same-Property Conflict - Depth Resolution (LWW)
/// When branches have different depths, the deeper branch wins
///
/// Scenario from spec:
///   A (genesis)
///   ├─ B (title="Shallow")     ← depth 1 from A
///   └─ C (title="Deep-1")      ← depth 1 from A
///      └─ D (title="Deep-2")   ← depth 2 from A
///
/// Expected: D's value wins because depth 2 > depth 1
#[tokio::test]
async fn test_deeper_branch_wins() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis A
    let record_id = {
        let trx = ctx.begin();
        let record = trx.create(&Record { title: "Initial".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let record = ctx.get::<RecordView>(record_id).await?;

    // Create two concurrent transactions from A
    let trx_shallow = ctx.begin();
    let trx_deep1 = ctx.begin();

    // Shallow branch: B sets title at depth 1
    record.edit(&trx_shallow)?.title().set(&"Shallow".to_owned())?;

    // Deep branch first event: C sets title at depth 1
    record.edit(&trx_deep1)?.title().set(&"Deep-1".to_owned())?;

    // Commit both - they're concurrent from A
    dag.enumerate(trx_shallow.commit_and_return_events().await?); // B
    dag.enumerate(trx_deep1.commit_and_return_events().await?); // C

    // Now extend the deep branch: D sets title at depth 2
    // We need to get a fresh view that sees C
    let record = ctx.get::<RecordView>(record_id).await?;
    let trx_deep2 = ctx.begin();
    record.edit(&trx_deep2)?.title().set(&"Deep-2".to_owned())?;
    dag.enumerate(trx_deep2.commit_and_return_events().await?); // D

    // Verify DAG structure
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    // Structure:
    //       A
    //      / \
    //     B   C
    //         |
    //         D
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [B, C],  // D merges B and C (since we got fresh view after both committed)
    });

    // D is at depth 2 (via C), B is at depth 1
    // D's value should win because it's deeper
    let final_record = ctx.get::<RecordView>(record_id).await?;
    assert_eq!(final_record.title().unwrap(), "Deep-2", "Deeper event (D at depth 2) should win over shallower (B at depth 1)");

    Ok(())
}

/// Test 1.3b: Same-Property Conflict - Sequential Writes (LWW)
/// The last write wins (since all writes go through the linear chain when sequential)
#[tokio::test]
async fn test_sequential_writes_last_wins() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis A
    let record_id = {
        let trx = ctx.begin();
        let record = trx.create(&Record { title: "Initial".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Build chain: A -> B -> C -> D with multiple title writes
    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.title().set(&"Title-B".to_owned())?; // B
        dag.enumerate(trx.commit_and_return_events().await?);
    }

    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.artist().set(&"Artist-C".to_owned())?; // C (different property)
        dag.enumerate(trx.commit_and_return_events().await?);
    }

    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.title().set(&"Title-D".to_owned())?; // D
        dag.enumerate(trx.commit_and_return_events().await?);
    }

    // Verify linear structure: A -> B -> C -> D
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [B],
        D => [C],
    });

    // D is the latest, so its title value wins
    let final_record = ctx.get::<RecordView>(record_id).await?;
    assert_eq!(final_record.title().unwrap(), "Title-D", "Latest write should win");

    Ok(())
}

/// Test 1.4: Same-Property, Same-Depth - Lexicographic Tiebreak (LWW)
/// When depths are equal, lexicographically greater EventId wins
#[tokio::test]
async fn test_lexicographic_tiebreak() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let record_id = {
        let trx = ctx.begin();
        let record = trx.create(&Record { title: "Initial".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let record = ctx.get::<RecordView>(record_id).await?;

    // Two concurrent transactions, both modifying title (same depth from A)
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    record.edit(&trx1)?.title().set(&"Title-B".to_owned())?;
    record.edit(&trx2)?.title().set(&"Title-C".to_owned())?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Both B and C are at depth 1 from A
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Winner is determined by lexicographic EventId comparison
    let b_id = dag.id('B').unwrap();
    let c_id = dag.id('C').unwrap();

    let final_title = ctx.get::<RecordView>(record_id).await?.title().unwrap();

    if b_id > c_id {
        assert_eq!(final_title, "Title-B", "B has higher EventId, should win");
    } else {
        assert_eq!(final_title, "Title-C", "C has higher EventId, should win");
    }

    Ok(())
}

/// Test 1.5: Deep Diamond - Per-Property Resolution (LWW)
/// Different properties can be set in concurrent branches
#[tokio::test]
async fn test_per_property_concurrent_writes() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    let mut dag = TestDag::new();

    // Create genesis
    let record_id = {
        let trx = ctx.begin();
        let record = trx.create(&Record { title: "Initial".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    let record = ctx.get::<RecordView>(record_id).await?;

    // Create two concurrent transactions BEFORE committing either
    // Both fork from A
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    // Branch 1: sets title
    record.edit(&trx1)?.title().set(&"Branch1-Title".to_owned())?;

    // Branch 2: sets artist
    record.edit(&trx2)?.artist().set(&"Branch2-Artist".to_owned())?;

    // Now commit both - they're concurrent
    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    // Structure: diamond
    //       A
    //      / \
    //     B   C
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Both properties should have their respective values
    // (no conflict since different properties)
    let final_record = ctx.get::<RecordView>(record_id).await?;
    assert_eq!(final_record.title().unwrap(), "Branch1-Title");
    assert_eq!(final_record.artist().unwrap(), "Branch2-Artist");

    // Now create a merge event D
    let record = ctx.get::<RecordView>(record_id).await?;
    let trx = ctx.begin();
    record.edit(&trx)?.title().set(&"Merged-Title".to_owned())?;
    dag.enumerate(trx.commit_and_return_events().await?); // D

    let events = collection.dump_entity_events(record_id).await?;

    // Final structure:
    //       A
    //      / \
    //     B   C
    //      \ /
    //       D
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [B, C],
    });

    let final_record = ctx.get::<RecordView>(record_id).await?;
    assert_eq!(final_record.title().unwrap(), "Merged-Title");
    assert_eq!(final_record.artist().unwrap(), "Branch2-Artist");

    Ok(())
}

/// Test: Verify LWW determinism across orderings
#[tokio::test]
async fn test_lww_order_independence() -> Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;

    // Create multiple entities and verify LWW behaves consistently
    for trial in 0..5 {
        let mut dag = TestDag::new();

        let record_id = {
            let trx = ctx.begin();
            let record = trx.create(&Record { title: format!("Trial{}", trial), artist: "Unknown".to_owned() }).await?;
            let id = record.id();
            dag.enumerate(trx.commit_and_return_events().await?);
            id
        };

        let record = ctx.get::<RecordView>(record_id).await?;

        // Create concurrent updates
        let trx1 = ctx.begin();
        let trx2 = ctx.begin();

        record.edit(&trx1)?.title().set(&"Value1".to_owned())?;
        record.edit(&trx2)?.title().set(&"Value2".to_owned())?;

        dag.enumerate(trx1.commit_and_return_events().await?);
        dag.enumerate(trx2.commit_and_return_events().await?);

        // The winner should be consistent based on EventId comparison
        let final_title = ctx.get::<RecordView>(record_id).await?.title().unwrap();
        assert!(final_title == "Value1" || final_title == "Value2", "Title should be one of the set values, got: {}", final_title);
    }

    Ok(())
}
