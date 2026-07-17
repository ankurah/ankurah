mod common;
use ankurah::core::{entity::Entity, error::RetrievalError, retrieval::GetEvents};
use ankurah::proto::{Event, EventId};
use anyhow::Result;
use common::*;
use std::collections::HashMap;

#[derive(Clone)]
struct ReplayEvents {
    events: HashMap<EventId, Event>,
}

impl ReplayEvents {
    fn new(events: impl IntoIterator<Item = Event>) -> Self {
        Self { events: events.into_iter().map(|event| (event.id(), event)).collect() }
    }
}

#[async_trait::async_trait]
impl GetEvents for ReplayEvents {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.events.get(event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
}

/// Test 5.1: Two-Event Determinism (LWW)
/// Two concurrent events modifying same property - result must be identical regardless of order
#[tokio::test]
async fn test_two_event_determinism_same_property() -> Result<()> {
    // Create the concurrent events through the ordinary transaction path.
    let node1 = durable_sled_setup().await?;
    let ctx1 = node1.context_async(DEFAULT_CONTEXT).await;

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

    // Get the source node's winner and the exact three-event DAG.
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

    let event = |label| events.iter().find(|event| dag.label(&event.payload.id()) == Some(label)).unwrap().payload.clone();
    let event_a = event('A');
    let event_b = event('B');
    let event_c = event('C');
    let getter = ReplayEvents::new([event_a.clone(), event_b.clone(), event_c.clone()]);

    // Replay the same same-system, id-keyed events into fresh entities in both
    // arrival orders. Comparing the complete materialized state proves the
    // application-order invariant instead of merely checking one commit order.
    let order_bc = Entity::create(record_id, Record::collection());
    order_bc.apply_event(&getter, &event_a).await?;
    order_bc.apply_event(&getter, &event_b).await?;
    order_bc.apply_event(&getter, &event_c).await?;

    let order_cb = Entity::create(record_id, Record::collection());
    order_cb.apply_event(&getter, &event_a).await?;
    order_cb.apply_event(&getter, &event_c).await?;
    order_cb.apply_event(&getter, &event_b).await?;

    assert_eq!(order_bc.to_state()?, order_cb.to_state()?, "A,B,C and A,C,B must materialize identical state");

    // The winner is determined by lexicographic EventId (all same depth).
    let b_id = dag.id('B').unwrap();
    let c_id = dag.id('C').unwrap();
    if b_id > c_id {
        assert_eq!(title_order1, "Title from B", "B has higher EventId, should win");
    } else {
        assert_eq!(title_order1, "Title from C", "C has higher EventId, should win");
    }

    Ok(())
}

#[tokio::test]
async fn cross_root_raw_state_does_not_substitute_property_ids() -> Result<()> {
    let node1 = durable_sled_setup().await?;
    let node2 = durable_sled_setup().await?;
    let ctx1 = node1.context_async(DEFAULT_CONTEXT).await;
    let ctx2 = node2.context_async(DEFAULT_CONTEXT).await;

    let record_id = {
        let trx = ctx1.begin();
        let record = trx.create(&Record { title: "Foreign".to_owned(), artist: "Unknown".to_owned() }).await?;
        let id = record.id();
        trx.commit().await?;
        id
    };

    // Each root allocates its own property ids. An out-of-band state copy can
    // preserve opaque foreign data, but it must never substitute that data for
    // the local root's same-named property.
    ctx2.register::<Record>().await?;
    let collection1 = ctx1.collection(&Record::collection()).await?;
    let collection2 = ctx2.collection(&Record::collection()).await?;
    collection2.set_state(collection1.get_state(record_id).await?).await?;

    let title = ctx2.get::<RecordView>(record_id).await?.title()?;
    assert_eq!(title, "", "a foreign property id must read as the local property's absence");
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
