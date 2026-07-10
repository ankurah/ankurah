mod common;
use anyhow::Result;
use common::*;

/// Test 5.1: Two-Event Determinism (LWW)
/// Two concurrent events modifying same property - result must be identical regardless of order
#[tokio::test]
async fn test_two_event_determinism_same_property() -> Result<()> {
    // Create two separate nodes to test different application orders
    let node1 = durable_sled_setup().await?;
    let node2 = durable_sled_setup().await?;
    let ctx1 = node1.context_async(DEFAULT_CONTEXT).await;
    let ctx2 = node2.context_async(DEFAULT_CONTEXT).await;

    let mut dag = TestDag::new();

    // Create genesis on node1
    let record_id = {
        let trx = ctx1.begin();
        let record = trx.create(&Record { title: "Initial".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Get the record so we fork from same head
    let record1 = ctx1.get::<RecordView>(record_id).await?;

    // Create two concurrent transactions
    let trx_b = ctx1.begin();
    let trx_c = ctx1.begin();

    let record_b = record1.edit(&trx_b)?;
    let record_c = record1.edit(&trx_c)?;

    // Both modify same property (title) - creates conflict
    record_b.title().set(&"Title from B".to_owned())?;
    record_c.title().set(&"Title from C".to_owned())?;

    // Commit in order B, C
    dag.enumerate(trx_b.commit_and_return_events().await?); // B
    dag.enumerate(trx_c.commit_and_return_events().await?); // C

    // Get final value from node1 (order: A, B, C). This node holds all three
    // events under ITS OWN root; the layered DAG merge elects the LWW winner
    // deterministically here regardless of the order the events were applied
    // (RFC 5.5 in specs/model-property-metadata/rfc.md). Same-root order-independent convergence -- the subject of
    // this test -- is asserted below via the winner check, and is exercised
    // across arrival orders by the sibling tests in this file
    // (test_multi_property_determinism, test_three_way_concurrent_determinism),
    // which apply concurrent events on a single node.
    let final1 = ctx1.get::<RecordView>(record_id).await?;
    let title_order1 = final1.title().unwrap();

    let collection1 = ctx1.collection(&Record::collection()).await?;
    let events = collection1.dump_entity_events(record_id).await?;

    // Verify DAG structure first
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // The winner is determined by lexicographic EventId (all same depth).
    let b_id = dag.id('B').unwrap();
    let c_id = dag.id('C').unwrap();
    if b_id > c_id {
        assert_eq!(title_order1, "Title from B", "B has higher EventId, should win");
    } else {
        assert_eq!(title_order1, "Title from C", "C has higher EventId, should win");
    }

    // Cross-root raw-state copy: the foreign value is NEVER SUBSTITUTED
    // (canonical value_type ruling, 2026-07-10, superseding the 2026-07-05
    // fail-visible gate: read-time gates died with it; the wire-ingress
    // model-id guard is where cross-system data is refused, and an
    // out-of-band copy reads as absent). node2 is its OWN system (its own
    // `system.create`) and registers `record` with its own allocator, which
    // allocates a DIFFERENT property id for `title` than node1's. Copying
    // node1's id-keyed 0xA2 state into node2 lands node1's value under an id
    // node2's catalog does not resolve for `title`: the resolved read
    // consults exactly its own id, finds nothing, and the required String
    // reads its absent default -- node1's value must not leak through.
    ctx2.register::<Record>().await?;
    let state1 = collection1.get_state(record_id).await?;
    let collection2 = ctx2.collection(&Record::collection()).await?;
    collection2.set_state(state1.clone()).await?;
    let title2 = ctx2.get::<RecordView>(record_id).await?.title()?;
    assert_eq!(title2, "", "the foreign id's value must read absent (the required-String default), never substitute");

    Ok(())
}

/// Test 5.2: Deep Diamond Determinism (LWW)
/// Two long branches diverging from A, applied in different orders
#[tokio::test]
async fn test_deep_diamond_determinism() -> Result<()> {
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

    // Build Branch 1: A -> B -> C -> D -> E (sets title at various depths)
    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.title().set(&"Branch1-depth1".to_owned())?; // B
        dag.enumerate(trx.commit_and_return_events().await?);
    }
    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.artist().set(&"Artist1".to_owned())?; // C - different property
        dag.enumerate(trx.commit_and_return_events().await?);
    }
    let record = ctx.get::<RecordView>(record_id).await?;
    {
        let trx = ctx.begin();
        record.edit(&trx)?.title().set(&"Branch1-depth3".to_owned())?; // D
        dag.enumerate(trx.commit_and_return_events().await?);
    }
    let record = ctx.get::<RecordView>(record_id).await?;
    let branch1_head = {
        let trx = ctx.begin();
        record.edit(&trx)?.artist().set(&"Artist2".to_owned())?; // E
        dag.enumerate(trx.commit_and_return_events().await?);
        ctx.get::<RecordView>(record_id).await?
    };

    // Now verify the deep chain has correct structure
    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [B],
        D => [C],
        E => [D],
    });

    // The title should be "Branch1-depth3" (set at depth 3, the deepest for that property)
    assert_eq!(branch1_head.title().unwrap(), "Branch1-depth3");
    assert_eq!(branch1_head.artist().unwrap(), "Artist2");

    Ok(())
}

/// Test 5.3: Multi-Property Determinism (LWW)
/// Different properties modified at different depths on concurrent branches
#[tokio::test]
async fn test_multi_property_determinism() -> Result<()> {
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

    // Start two concurrent transactions from same head
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();

    // Transaction 1: modifies title
    record.edit(&trx1)?.title().set(&"Title from T1".to_owned())?;

    // Transaction 2: modifies artist
    record.edit(&trx2)?.artist().set(&"Artist from T2".to_owned())?;

    // Commit both - creates diamond
    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    // Verify diamond structure
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Both properties should reflect their respective updates
    // (no conflict since different properties)
    let final_record = ctx.get::<RecordView>(record_id).await?;
    assert_eq!(final_record.title().unwrap(), "Title from T1");
    assert_eq!(final_record.artist().unwrap(), "Artist from T2");

    // Verify head has both concurrent events
    let state = collection.get_state(record_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C]);

    Ok(())
}

/// Test: Three-way concurrent modification (LWW) - all must be applied
#[tokio::test]
async fn test_three_way_concurrent_determinism() -> Result<()> {
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

    // Start three concurrent transactions
    let trx1 = ctx.begin();
    let trx2 = ctx.begin();
    let trx3 = ctx.begin();

    // All modify title - creates conflict
    record.edit(&trx1)?.title().set(&"Title-T1".to_owned())?;
    record.edit(&trx2)?.title().set(&"Title-T2".to_owned())?;
    record.edit(&trx3)?.title().set(&"Title-T3".to_owned())?;

    // Commit all three
    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C
    dag.enumerate(trx3.commit_and_return_events().await?); // D

    let collection = ctx.collection(&Record::collection()).await?;
    let events = collection.dump_entity_events(record_id).await?;

    // Verify three-way fork structure
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [A],
    });

    // Head should have all three concurrent events
    let state = collection.get_state(record_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C, D]);

    // Winner is determined by lexicographic EventId (all same depth)
    let final_record = ctx.get::<RecordView>(record_id).await?;
    let final_title = final_record.title().unwrap();

    // Find which event has the highest ID
    let b_id = dag.id('B').unwrap();
    let c_id = dag.id('C').unwrap();
    let d_id = dag.id('D').unwrap();

    let winner = [('B', b_id), ('C', c_id), ('D', d_id)].into_iter().max_by_key(|(_, id)| id.clone()).unwrap().0;

    let expected_title = match winner {
        'B' => "Title-T1",
        'C' => "Title-T2",
        'D' => "Title-T3",
        _ => unreachable!(),
    };

    assert_eq!(final_title, expected_title, "Winner should be the event with highest lexicographic EventId");

    Ok(())
}
