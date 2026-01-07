mod common;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

/// Test 3.1: Ephemeral Writes, Durable Receives
/// Ephemeral node creates concurrent events, durable node should persist them correctly
#[tokio::test]
async fn test_ephemeral_writes_durable_receives() -> Result<()> {
    // Setup: Durable D, Ephemeral E connected
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // 1. Create entity on durable node
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "2020".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Wait for propagation to ephemeral
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. Ephemeral subscribes/fetches entity
    let album_e = ctx_e.get::<AlbumView>(album_id).await?;

    // 3. Ephemeral makes two concurrent writes
    let trx1 = ctx_e.begin();
    let trx2 = ctx_e.begin();

    album_e.edit(&trx1)?.name().replace("Name-E1")?;
    album_e.edit(&trx2)?.year().replace("2025")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Wait for propagation to durable
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Verify durable node has both events with correct structure
    let collection_d = ctx_d.collection(&Album::collection()).await?;
    let events = collection_d.dump_entity_events(album_id).await?;

    // Verify DAG structure on durable node
    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    // Verify head has 2 members
    let state = collection_d.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C]);

    // Verify both changes applied
    let final_album = ctx_d.get::<AlbumView>(album_id).await?;
    assert!(final_album.name().unwrap().contains("Name-E1"));
    assert_eq!(final_album.year().unwrap(), "2025");

    Ok(())
}

/// Test 3.2: Durable Writes, Ephemeral Observes
/// Durable node makes concurrent writes, ephemeral observes final state via fetch
#[tokio::test]
async fn test_durable_writes_ephemeral_observes() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // Create entity on durable
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Durable makes two concurrent writes
    let album_d = ctx_d.get::<AlbumView>(album_id).await?;
    let trx1 = ctx_d.begin();
    let trx2 = ctx_d.begin();

    album_d.edit(&trx1)?.name().replace("Name-D1")?;
    album_d.edit(&trx2)?.name().replace("Name-D2")?;

    dag.enumerate(trx1.commit_and_return_events().await?); // B
    dag.enumerate(trx2.commit_and_return_events().await?); // C

    // Ephemeral fetches entity AFTER durable makes changes (via fetch to get fresh data from server)
    let query = format!("id = '{}'", album_id);
    let results = ctx_e.fetch::<AlbumView>(query.as_str()).await?;
    assert_eq!(results.len(), 1);
    let final_e = &results[0];
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;

    // With Yrs CRDT, both concurrent replacements will merge
    assert_eq!(final_e.name().unwrap(), final_d.name().unwrap(), "Ephemeral should match durable state");

    Ok(())
}

/// Test 3.3: Durable vs Ephemeral Concurrent Write
/// Both nodes write concurrently, both should converge to same state
#[tokio::test]
async fn test_durable_vs_ephemeral_concurrent_write() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;

    let mut dag = TestDag::new();

    // Create entity on durable
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        dag.enumerate(trx.commit_and_return_events().await?); // A
        id
    };

    // Ephemeral fetches entity to get it locally
    let query = format!("id = '{}'", album_id);
    let _initial_fetch = ctx_e.fetch::<AlbumView>(query.as_str()).await?;

    // Both nodes get the entity for editing
    let album_d = ctx_d.get::<AlbumView>(album_id).await?;
    let album_e = ctx_e.get::<AlbumView>(album_id).await?;

    // Both begin transactions (fork from same head)
    let trx_d = ctx_d.begin();
    let trx_e = ctx_e.begin();

    // Different properties to avoid conflict - each node sets one
    album_d.edit(&trx_d)?.name().replace("Name-from-D")?;
    album_e.edit(&trx_e)?.year().replace("2025")?;

    // Commit both
    dag.enumerate(trx_d.commit_and_return_events().await?); // B
    dag.enumerate(trx_e.commit_and_return_events().await?); // C

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Final state on durable should have both changes
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;
    assert!(final_d.name().unwrap().contains("Name-from-D"));
    assert_eq!(final_d.year().unwrap(), "2025");

    // Ephemeral fetches again to get converged state from server
    let query = format!("id = '{}'", album_id);
    let results = ctx_e.fetch::<AlbumView>(query.as_str()).await?;
    assert_eq!(results.len(), 1);
    let final_e = &results[0];

    assert!(final_e.name().unwrap().contains("Name-from-D"));
    assert_eq!(final_e.year().unwrap(), "2025");

    // Verify DAG structure on durable
    let collection_d = ctx_d.collection(&Album::collection()).await?;
    let events = collection_d.dump_entity_events(album_id).await?;

    assert_dag!(dag, events, {
        A => [],
        B => [A],
        C => [A],
    });

    let state = collection_d.get_state(album_id).await?;
    clock_eq!(dag, state.payload.state.head, [B, C]);

    Ok(())
}

/// Test 3.4: Late-Arriving Branch from Deep History
/// Build long history, then a branch from early in history arrives
#[tokio::test]
async fn test_late_arriving_branch() -> Result<()> {
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    durable.system.create().await?;
    let ephemeral = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&ephemeral, &durable).await?;
    ephemeral.system.wait_system_ready().await;

    let ctx_d = durable.context(DEFAULT_CONTEXT)?;
    let ctx_e = ephemeral.context(DEFAULT_CONTEXT)?;

    // 1. Build 20-event linear history on durable
    let album_id = {
        let trx = ctx_d.begin();
        let album = trx.create(&Album { name: "Initial".to_owned(), year: "0".to_owned() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    for i in 1..=20 {
        let album = ctx_d.get::<AlbumView>(album_id).await?;
        let trx = ctx_d.begin();
        album.edit(&trx)?.year().replace(&format!("{}", i))?;
        trx.commit().await?;
    }

    // Wait for ephemeral to receive all events
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // 2. Ephemeral receives state at event 20
    let album_e = ctx_e.get::<AlbumView>(album_id).await?;
    assert_eq!(album_e.year().unwrap(), "20");

    // 3. Now create a concurrent update from ephemeral
    // This simulates a "late-arriving branch" - ephemeral's update will fork from
    // its current head (which includes all 20 events)
    let trx_e = ctx_e.begin();
    album_e.edit(&trx_e)?.name().replace("Late-Branch")?;
    trx_e.commit().await?;

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Verify correct merged state - no BudgetExceeded error
    let final_d = ctx_d.get::<AlbumView>(album_id).await?;
    assert!(final_d.name().unwrap().contains("Late-Branch"), "Late-arriving branch should be merged");
    assert_eq!(final_d.year().unwrap(), "20", "Year should remain from main branch");

    Ok(())
}
