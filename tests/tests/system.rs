mod common;
use ankurah::{policy::DEFAULT_CONTEXT, proto::CollectionId, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::{Album, Pet, PetView};
use std::sync::Arc;

fn sorted_collections(mut collections: Vec<CollectionId>) -> Vec<CollectionId> {
    collections.sort();
    collections
}

#[tokio::test]
async fn test_system() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test().unwrap());
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());

        node.system.create().await?;

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);

        let items = node.system.items();
        assert_eq!(items.len(), 1);
    }

    {
        let node = Node::new_durable(engine, PermissiveAgent::new());

        // assert that this fails because the system already exists
        assert!(node.system.create().await.is_err());

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);

        let items = node.system.items();
        assert_eq!(items.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn test_system_ready_behavior() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let empty_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    // Fresh ephemeral node with no cached root should remain unready after loading
    {
        let node = Node::new(empty_engine, PermissiveAgent::new());
        assert!(!node.system.is_system_ready()); // Not ready immediately

        node.system.wait_loaded().await;
        assert!(!node.system.is_system_ready()); // Still not ready without a cached root
        assert_eq!(node.system.root(), None);
    }

    // First create and initialize with a durable node
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        assert!(!node.system.is_system_ready()); // Not ready before initialize

        node.system.create().await?;
        assert!(node.system.is_system_ready()); // Ready after initialize

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);
    }

    // Create another durable node - should be ready after loading since system exists
    {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        assert!(!node.system.is_system_ready()); // Not ready immediately

        // Wait for load
        node.system.wait_loaded().await;
        assert!(node.system.is_system_ready()); // Ready after load since we're durable

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);
    }

    // Create an ephemeral node with cached root - should be ready after loading (offline-first)
    {
        let node = Node::new(engine.clone(), PermissiveAgent::new());
        assert!(!node.system.is_system_ready()); // Not ready immediately

        // Wait for load
        node.system.wait_loaded().await;
        assert!(node.system.is_system_ready()); // Ready after load with cached root (offline-first)

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_system_persistence_across_reconstruction() -> Result<()> {
    // Create separate storage engines for durable and ephemeral nodes
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    // First setup: Create both durable and ephemeral nodes
    let root_state = {
        // Create and initialize durable node
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.create().await?;
        assert!(durable_node.system.is_system_ready());

        // Get root state for later comparison
        let root_state = durable_node.system.root().expect("Should have root state");
        assert_eq!(root_state.payload.state.head.len(), 1);

        // Create ephemeral node
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(!ephemeral_node.system.is_system_ready());

        // Connect nodes using LocalProcessConnection
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // Wait for ephemeral node to be ready
        ephemeral_node.system.wait_system_ready().await;
        assert!(ephemeral_node.system.is_system_ready());

        // Verify both nodes match the root state
        assert_eq!(durable_node.system.root(), Some(root_state.clone()), "durable root should match");
        assert_eq!(ephemeral_node.system.root(), Some(root_state.clone()), "ephemeral root should match");

        // Return root state for later comparison
        root_state
    }; // Both nodes and connection are dropped here

    // Second setup: Reconstruct both nodes with their respective storage engines
    {
        // Create new durable node - should automatically load existing system
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;
        assert!(durable_node.system.is_system_ready(), "Durable node should be ready after loading existing system");

        // Verify root state persisted in durable storage
        assert_eq!(
            durable_node.system.root().expect("Should have root").payload.state.head,
            root_state.payload.state.head,
            "Durable node should have same root state after reconstruction"
        );

        // Create new ephemeral node - ready immediately with cached root (offline-first)
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(ephemeral_node.system.is_system_ready(), "Ephemeral node should be ready with cached root (offline-first)");

        // Connect nodes using LocalProcessConnection
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // Wait for ephemeral node to be ready
        ephemeral_node.system.wait_system_ready().await;
        assert!(ephemeral_node.system.is_system_ready(), "Ephemeral node should be ready after connection");

        // Verify all roots match
        assert_eq!(
            durable_node.system.root().expect("Should have root"),
            ephemeral_node.system.root().expect("Should have root"),
            "Both nodes should have same root after reconstruction"
        );
        assert_eq!(
            ephemeral_node.system.root().expect("Should have root"),
            root_state,
            "Reconstructed nodes should have same root as original"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_system_root_change_behavior() -> Result<()> {
    // Create separate storage engines for durable and ephemeral nodes
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    // Get initial root state
    let initial_root = {
        // Create and initialize durable node
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.create().await?;
        assert!(durable_node.system.is_system_ready());

        // Create ephemeral node
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;

        // Not ready because we haven't joined the system
        assert!(!ephemeral_node.system.is_system_ready());

        // Connect nodes
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // Wait for ephemeral node to be ready
        ephemeral_node.system.wait_system_ready().await;

        // now we should be ready because we joined the system
        assert!(ephemeral_node.system.is_system_ready());

        // Store initial root state for comparison
        let initial_root = durable_node.system.root().expect("Should have root state");

        // Verify both nodes have same root
        assert_eq!(
            durable_node.system.root().expect("Should have root").payload.state.head,
            ephemeral_node.system.root().expect("Should have root").payload.state.head,
            "Both nodes should have same root state after initial setup"
        );

        let trx = ephemeral_node.context(DEFAULT_CONTEXT)?.begin();
        trx.create(&Pet { name: "Fido".into(), age: "3".to_string() }).await?;
        trx.commit().await?;

        assert_eq!(
            ephemeral_engine.list_collections()?,
            vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")]
        );

        assert_eq!(durable_engine.list_collections()?, vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")]);

        initial_root
    }; // Both nodes and connection are dropped here

    // Restart ephemeral node offline and verify it can still use the cached root/state
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(ephemeral_node.system.is_system_ready(), "Ephemeral node should be ready offline with cached root");
        assert_eq!(ephemeral_node.system.root(), Some(initial_root.clone()), "Ephemeral node should preserve cached root offline");
        assert_eq!(
            sorted_collections(ephemeral_engine.list_collections()?),
            vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")]
        );

        let offline = ephemeral_node.context(DEFAULT_CONTEXT)?;
        let pets = offline.query_wait::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.ids().len(), 1, "Ephemeral node should serve cached data while offline");
    }

    // Reset durable node's system (creating new root) but NOT ephemeral node
    let second_root = {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;

        // should be ready because we previously initialized a system
        assert!(durable_node.system.is_system_ready());

        assert_eq!(durable_engine.list_collections()?, vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")]);

        // Reset storage and reinitialize
        durable_node.system.hard_reset().await?;

        assert_eq!(durable_engine.list_collections()?, Vec::<CollectionId>::new());

        assert!(!durable_node.system.is_system_ready());

        durable_node.system.create().await?;

        assert_eq!(durable_engine.list_collections()?, vec![CollectionId::fixed_name("_ankurah_system")]);

        // Verify root has changed
        let second_root = durable_node.system.root().expect("Should have new root state");
        assert_ne!(second_root.payload.state.head, initial_root.payload.state.head, "Root state should be different after reset");

        assert_eq!(second_root.payload.state.head.len(), 1);

        let trx = durable_node.context(DEFAULT_CONTEXT)?.begin();
        trx.create(&Album { name: "Leonard Skynyrd".into(), year: "1973".to_string() }).await?;
        trx.commit().await?;

        assert_eq!(
            durable_engine.list_collections()?,
            vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("album")]
        );

        second_root
    }; // Drop durable node

    // Ephemeral node later reconnects, detects the root mismatch, and resets everything
    {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;
        assert!(durable_node.system.is_system_ready()); // should be ready when loaded
        assert_eq!(durable_node.system.root(), Some(second_root.clone()));
        assert_eq!(
            durable_engine.list_collections()?,
            vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("album")]
        );

        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        // Ephemeral node is ready with cached (stale) root — offline-first
        // join_system will detect the mismatch and hard_reset when it connects
        assert!(ephemeral_node.system.is_system_ready());
        assert_eq!(ephemeral_node.system.root(), Some(initial_root), "Ephemeral node should have old root prior to joining");
        assert_eq!(
            sorted_collections(ephemeral_engine.list_collections()?),
            vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")]
        );

        // Connect nodes — join_system runs async, detects root mismatch, hard_resets, and re-joins
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // The ephemeral node is already "ready" with stale root (offline-first),
        // so wait_system_ready returns immediately. We need to wait for join_system
        // to detect the mismatch, hard_reset, and re-join with the new root.
        for _ in 0..100 {
            if ephemeral_node.system.root() == Some(second_root.clone()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(ephemeral_node.system.root(), Some(second_root.clone()), "Ephemeral node should have new root after joining");

        assert_eq!(ephemeral_engine.list_collections()?, vec![CollectionId::fixed_name("_ankurah_system")]);

        let rejoined = ephemeral_node.context(DEFAULT_CONTEXT)?;
        let pets = rejoined.query_wait::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.ids().len(), 0, "Ephemeral node should drop stale cached data after adopting a new root");
        assert_eq!(ephemeral_node.system.root(), Some(second_root), "Ephemeral node should remain on the new root after reset");
    }

    Ok(())
}

#[tokio::test]
async fn test_ephemeral_cached_root_supports_offline_queries_after_restart() -> Result<()> {
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.create().await?;

        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;
        ephemeral_node.system.wait_system_ready().await;

        let ephemeral = ephemeral_node.context_async(DEFAULT_CONTEXT).await;
        let trx = ephemeral.begin();
        trx.create(&Pet { name: "Fido".into(), age: "3".to_string() }).await?;
        trx.commit().await?;
        let pets = ephemeral.query_wait::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.ids().len(), 1, "Ephemeral node should serve locally persisted data before restart");
    }

    {
        let ephemeral_node = Node::new(ephemeral_engine, PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(ephemeral_node.system.is_system_ready(), "Cached root should make the system usable offline after restart");

        let offline = ephemeral_node.context(DEFAULT_CONTEXT)?;
        let pets = offline.query_wait::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.ids().len(), 1, "Offline ephemeral node should serve cached query results without reconnecting");
    }

    Ok(())
}

#[tokio::test]
async fn test_ephemeral_cached_fetch_supports_offline_after_restart() -> Result<()> {
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    {
        let durable_node = Node::new_durable(durable_engine, PermissiveAgent::new());
        durable_node.system.create().await?;

        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;
        ephemeral_node.system.wait_system_ready().await;

        let ephemeral = ephemeral_node.context_async(DEFAULT_CONTEXT).await;
        let trx = ephemeral.begin();
        trx.create(&Pet { name: "Fido".into(), age: "3".to_string() }).await?;
        trx.commit().await?;

        let pets = ephemeral.fetch::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.len(), 1, "Connected ephemeral node should serve cached fetch results");
    }

    {
        let ephemeral_node = Node::new(ephemeral_engine, PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(ephemeral_node.system.is_system_ready(), "Cached root should make the system usable offline after restart");

        let offline = ephemeral_node.context(DEFAULT_CONTEXT)?;
        let pets = offline.fetch::<PetView>("name = 'Fido'").await?;
        assert_eq!(pets.len(), 1, "Offline fetch should fall back to local cached results");

        let err = offline
            .fetch::<PetView>(ankurah::core::node::nocache("name = 'Fido'")?)
            .await
            .expect_err("nocache fetch should still require a durable peer");
        assert!(matches!(err, ankurah::core::error::RetrievalError::NoDurablePeers));
    }

    Ok(())
}
