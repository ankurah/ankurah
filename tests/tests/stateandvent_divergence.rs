mod common;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent, core::node::nocache};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test: StateAndEvent with divergence via subscription
///
/// This tests the bug in node_applier.rs where StateAndEvent updates
/// don't apply events when there's divergence between local and incoming state.
///
/// Bug location: node_applier.rs:96-125
/// When apply_state returns Ok(false) for DivergedSince, events are saved
/// to storage but NOT applied to the entity in memory.
///
/// This test uses LiveQuery subscription to trigger the StateAndEvent path:
/// 1. Durable D creates entity A
/// 2. Ephemeral E subscribes via LiveQuery (sets up subscription)
/// 3. E and D make concurrent commits
/// 4. D's commit goes to E via StateAndEvent subscription update
/// 5. If E committed before receiving D's update, DivergedSince occurs
#[tokio::test]
async fn test_stateandvent_divergence_subscription() -> Result<()> {
    // Setup: Durable D and Ephemeral E connected
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // 1. Durable creates entity with genesis A
    let record_id = {
        let trx = ctx_d.begin();
        let record = trx.create(&Record { title: "Genesis".to_owned(), artist: "Original".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // 2. Ephemeral sets up a subscription via LiveQuery
    // This is key - query_wait establishes a predicate subscription with the server
    // Future changes will come via StateAndEvent updates
    let _livequery = ctx_e.query_wait::<RecordView>(nocache(format!("id = '{}'", record_id).as_str())?).await?;

    // Verify ephemeral has the entity from subscription initialization
    let record_e = ctx_e.get::<RecordView>(record_id).await?;
    assert_eq!(record_e.title().unwrap(), "Genesis");
    assert_eq!(record_e.artist().unwrap(), "Original");

    // 3. Both begin transactions from the same point (A)
    let record_d = ctx_d.get::<RecordView>(record_id).await?;
    let record_e = ctx_e.get::<RecordView>(record_id).await?;

    let trx_b = ctx_e.begin(); // E's transaction (will be B)
    let trx_c = ctx_d.begin(); // D's transaction (will be C)

    // Set different properties to avoid CRDT merging issues
    record_e.edit(&trx_b)?.title().set(&"Title-from-E".to_owned())?;
    record_d.edit(&trx_c)?.artist().set(&"Artist-from-D".to_owned())?;

    // 4. D commits C first
    // This triggers D to send StateAndEvent(C) to E via subscription
    dag.enumerate(trx_c.commit_and_return_events().await?); // C

    // Small delay to let StateAndEvent be sent (but not necessarily processed by E yet)
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // 5. E commits B
    // At this moment, E's local head may be [A] (if StateAndEvent(C) hasn't been processed)
    // or [C] (if StateAndEvent(C) was already processed)
    // Either way, the concurrent commit should work
    dag.enumerate(trx_b.commit_and_return_events().await?); // B

    // Wait for all propagation to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Verify final state on durable - should have both changes
    let final_d = ctx_d.get::<RecordView>(record_id).await?;
    assert_eq!(final_d.title().unwrap(), "Title-from-E", "Durable should have E's title change");
    assert_eq!(final_d.artist().unwrap(), "Artist-from-D", "Durable should have D's artist change");

    // THE BUG TEST: Verify ephemeral has BOTH changes
    // If the bug is present, ephemeral will only have its own change (B) and not D's change (C)
    let final_e = ctx_e.get::<RecordView>(record_id).await?;

    assert_eq!(
        final_e.title().unwrap(), "Title-from-E",
        "Ephemeral should have its own title change"
    );
    assert_eq!(
        final_e.artist().unwrap(), "Artist-from-D",
        "BUG CHECK: Ephemeral should have D's artist change via StateAndEvent divergence handling"
    );

    // Verify DAG structure on durable
    let collection_d = ctx_d.collection(&Record::collection()).await?;
    let events = collection_d.dump_entity_events(record_id).await?;

    // B and C should both have parent A (true concurrency)
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Head should have both concurrent events
    let state = collection_d.get_state(record_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C]);

    Ok(())
}

/// Test: Two ephemeral nodes with divergence via subscriptions
///
/// This test uses two ephemeral nodes with LiveQuery subscriptions.
/// When one ephemeral commits, it goes to durable via CommitTransaction,
/// then durable sends StateAndEvent to the OTHER ephemeral.
#[tokio::test]
async fn test_two_ephemeral_divergence_subscription() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;

    let ephemeral1 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral2 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn1 = LocalProcessConnection::new(&ephemeral1, &durable).await?;
    let _conn2 = LocalProcessConnection::new(&ephemeral2, &durable).await?;

    ephemeral1.system.wait_system_ready().await;
    ephemeral2.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e1 = ephemeral1.context(DEFAULT_CONTEXT)?;
    let ctx_e2 = ephemeral2.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // Durable creates entity
    let record_id = {
        let trx = ctx_d.begin();
        let record = trx.create(&Record { title: "Genesis".to_owned(), artist: "Original".to_owned() }).await?;
        let id = record.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Both ephemeral nodes subscribe via LiveQuery
    let query_str = format!("id = '{}'", record_id);
    let _lq1 = ctx_e1.query_wait::<RecordView>(nocache(query_str.as_str())?).await?;
    let _lq2 = ctx_e2.query_wait::<RecordView>(nocache(query_str.as_str())?).await?;

    // Verify both have the entity
    let record_e1 = ctx_e1.get::<RecordView>(record_id).await?;
    let record_e2 = ctx_e2.get::<RecordView>(record_id).await?;
    assert_eq!(record_e1.title().unwrap(), "Genesis");
    assert_eq!(record_e2.title().unwrap(), "Genesis");

    // Both begin transactions from A
    let trx_b = ctx_e1.begin();
    let trx_c = ctx_e2.begin();

    record_e1.edit(&trx_b)?.title().set(&"Title-from-E1".to_owned())?;
    record_e2.edit(&trx_c)?.artist().set(&"Artist-from-E2".to_owned())?;

    // E1 commits B - goes to D via CommitTransaction, D sends StateAndEvent to E2
    dag.enumerate(trx_b.commit_and_return_events().await?); // B

    // Small delay then E2 commits C
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    dag.enumerate(trx_c.commit_and_return_events().await?); // C

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // All nodes should have converged to the same state
    let final_d = ctx_d.get::<RecordView>(record_id).await?;
    let final_e1 = ctx_e1.get::<RecordView>(record_id).await?;
    let final_e2 = ctx_e2.get::<RecordView>(record_id).await?;

    // Durable should have both changes
    assert_eq!(final_d.title().unwrap(), "Title-from-E1", "Durable should have E1's title");
    assert_eq!(final_d.artist().unwrap(), "Artist-from-E2", "Durable should have E2's artist");

    // Ephemeral1 should have both changes
    assert_eq!(final_e1.title().unwrap(), "Title-from-E1", "E1 should have its own title");
    assert_eq!(
        final_e1.artist().unwrap(), "Artist-from-E2",
        "BUG CHECK: E1 should have E2's artist via StateAndEvent"
    );

    // Ephemeral2 should have both changes
    assert_eq!(
        final_e2.title().unwrap(), "Title-from-E1",
        "BUG CHECK: E2 should have E1's title via StateAndEvent"
    );
    assert_eq!(final_e2.artist().unwrap(), "Artist-from-E2", "E2 should have its own artist");

    // Verify DAG structure
    let collection_d = ctx_d.collection(&Record::collection()).await?;
    let events = collection_d.dump_entity_events(record_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    Ok(())
}
