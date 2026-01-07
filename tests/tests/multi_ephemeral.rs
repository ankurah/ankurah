mod common;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test 4.1: Two Ephemeral - Independent Writes
/// Two ephemeral nodes write different properties, all should converge
#[tokio::test]
async fn test_two_ephemeral_independent_writes() -> Result<()> {
    // Setup: Durable D, Ephemeral E1, E2 all connected
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;

    let ephemeral1 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral2 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect both ephemeral nodes to durable
    let _conn1 = LocalProcessConnection::new(&ephemeral1, &durable).await?;
    let _conn2 = LocalProcessConnection::new(&ephemeral2, &durable).await?;

    ephemeral1.system.wait_system_ready().await;
    ephemeral2.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e1 = ephemeral1.context(DEFAULT_CONTEXT)?;
    let ctx_e2 = ephemeral2.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // 1. Create entity on D
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // 2. E1 and E2 both fetch the entity
    let query = format!("id = '{}'", album_id);
    let _fetch1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    let _fetch2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;

    // Get entity handles for editing
    let album_e1 = ctx_e1.get::<AlbumView>(album_id).await?;
    let album_e2 = ctx_e2.get::<AlbumView>(album_id).await?;

    // 3. E1: name="From-E1", E2: year="2025" (concurrent, different properties)
    let trx_e1 = ctx_e1.begin();
    let trx_e2 = ctx_e2.begin();

    album_e1.edit(&trx_e1)?.name().replace("From-E1")?;
    album_e2.edit(&trx_e2)?.year().replace("2025")?;

    dag.enumerate(trx_e1.commit_and_return_events().await?); // B
    dag.enumerate(trx_e2.commit_and_return_events().await?); // C

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify: D has both events and correct state
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;
    assert!(final_d.name().unwrap().contains("From-E1"), "D should have E1's name");
    assert_eq!(final_d.year().unwrap(), "2025", "D should have E2's year");

    // Verify: E1 converges (via fetch to get fresh state)
    let results_e1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    assert!(results_e1[0].name().unwrap().contains("From-E1"));
    assert_eq!(results_e1[0].year().unwrap(), "2025");

    // Verify: E2 converges
    let results_e2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;
    assert!(results_e2[0].name().unwrap().contains("From-E1"));
    assert_eq!(results_e2[0].year().unwrap(), "2025");

    // Verify DAG structure on D
    let collection_d = ctx_d.collection(&Album::collection()).await?;
    let events = collection_d.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    Ok(())
}

/// Test 4.2: Two Ephemeral - Same Property Conflict
/// Two ephemeral nodes write same property, winner determined by LWW rules
#[tokio::test]
async fn test_two_ephemeral_same_property_conflict() -> Result<()> {
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

    // 1. Entity with name="Initial" on D
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Both ephemeral nodes fetch the entity
    let query = format!("id = '{}'", album_id);
    let _fetch1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    let _fetch2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;

    let album_e1 = ctx_e1.get::<AlbumView>(album_id).await?;
    let album_e2 = ctx_e2.get::<AlbumView>(album_id).await?;

    // 2. E1: name="From-E1", E2: name="From-E2" (racing on same property)
    let trx_e1 = ctx_e1.begin();
    let trx_e2 = ctx_e2.begin();

    album_e1.edit(&trx_e1)?.name().replace("From-E1")?;
    album_e2.edit(&trx_e2)?.name().replace("From-E2")?;

    dag.enumerate(trx_e1.commit_and_return_events().await?); // B
    dag.enumerate(trx_e2.commit_and_return_events().await?); // C

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // With Yrs CRDT strings, concurrent replacements will merge
    // Both "From-E1" and "From-E2" content will be present in some form
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;
    let final_name = final_d.name().unwrap();

    // Verify: D, E1, E2 all converge to same value
    let results_e1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    let results_e2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;

    // All nodes should have the same final value (CRDT convergence)
    assert_eq!(results_e1[0].name().unwrap(), final_name);
    assert_eq!(results_e2[0].name().unwrap(), final_name);

    Ok(())
}

/// Test 4.3: Three Ephemeral - True Three-Way Race
/// Three ephemeral nodes write different properties concurrently
#[tokio::test]
async fn test_three_ephemeral_three_way_race() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;

    let ephemeral1 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral2 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral3 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn1 = LocalProcessConnection::new(&ephemeral1, &durable).await?;
    let _conn2 = LocalProcessConnection::new(&ephemeral2, &durable).await?;
    let _conn3 = LocalProcessConnection::new(&ephemeral3, &durable).await?;

    ephemeral1.system.wait_system_ready().await;
    ephemeral2.system.wait_system_ready().await;
    ephemeral3.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e1 = ephemeral1.context(DEFAULT_CONTEXT)?;
    let ctx_e2 = ephemeral2.context(DEFAULT_CONTEXT)?;
    let ctx_e3 = ephemeral3.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // 1. Create entity on D
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // All ephemeral nodes fetch the entity
    let query = format!("id = '{}'", album_id);
    let _fetch1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    let _fetch2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;
    let _fetch3 = ctx_e3.fetch::<AlbumView>(query.as_str()).await?;

    let album_e1 = ctx_e1.get::<AlbumView>(album_id).await?;
    let album_e2 = ctx_e2.get::<AlbumView>(album_id).await?;
    let album_e3 = ctx_e3.get::<AlbumView>(album_id).await?;

    // 2. E1: name, E2: year, E3: name (E1 and E3 conflict on name, E2 independent)
    // For a true three-way: E1 sets name, E2 sets year, E3 also sets name
    let trx_e1 = ctx_e1.begin();
    let trx_e2 = ctx_e2.begin();
    let trx_e3 = ctx_e3.begin();

    album_e1.edit(&trx_e1)?.name().replace("From-E1")?;
    album_e2.edit(&trx_e2)?.year().replace("2025")?;
    album_e3.edit(&trx_e3)?.name().replace("From-E3")?;

    dag.enumerate(trx_e1.commit_and_return_events().await?); // B
    dag.enumerate(trx_e2.commit_and_return_events().await?); // C
    dag.enumerate(trx_e3.commit_and_return_events().await?); // D

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Verify DAG structure on D - all three concurrent from A
    let collection_d = ctx_d.collection(&Album::collection()).await?;
    let events = collection_d.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
        D => [A],
    });

    // Verify year is set (no conflict on year)
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;
    assert_eq!(final_d.year().unwrap(), "2025");

    // With Yrs CRDT, concurrent name replacements will merge
    let final_name = final_d.name().unwrap();

    // Verify all nodes converge to same state (CRDT convergence)
    let results_e1 = ctx_e1.fetch::<AlbumView>(query.as_str()).await?;
    let results_e2 = ctx_e2.fetch::<AlbumView>(query.as_str()).await?;
    let results_e3 = ctx_e3.fetch::<AlbumView>(query.as_str()).await?;

    assert_eq!(results_e1[0].name().unwrap(), final_name);
    assert_eq!(results_e1[0].year().unwrap(), "2025");

    assert_eq!(results_e2[0].name().unwrap(), final_name);
    assert_eq!(results_e2[0].year().unwrap(), "2025");

    assert_eq!(results_e3[0].name().unwrap(), final_name);
    assert_eq!(results_e3[0].year().unwrap(), "2025");

    Ok(())
}
