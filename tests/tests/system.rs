mod common;
use ankurah::{policy::DEFAULT_CONTEXT, proto::CollectionId, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::{Album, Pet};
use std::sync::Arc;

/// The engine's DATA collections (system + user models), sorted, with the
/// three metadata catalog collections filtered out.
///
/// With A11b a create auto-registers its model, so the catalog collections
/// (`_ankurah_model`/`_ankurah_property`/`_ankurah_model_property`) come and
/// go alongside the data collections. Their exact presence is orthogonal to
/// what this test verifies (system root changes and storage reset) and is
/// timing-sensitive besides -- a later durable node instance sharing the
/// engine warms its catalog asynchronously, which can re-materialize catalog
/// trees just after a hard_reset. Filtering them out keeps these assertions
/// about the data/system collections, which is the test's actual subject,
/// and stable regardless of catalog-warm timing.
fn data_collections(engine: &Arc<SledStorageEngine>) -> Result<Vec<CollectionId>> {
    let mut v: Vec<CollectionId> =
        engine.list_collections()?.into_iter().filter(|c| !ankurah::core::schema::is_catalog_collection(c)).collect();
    v.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    Ok(v)
}

/// The given names as a sorted `Vec<CollectionId>`, matching `data_collections`.
fn sorted_names(names: &[&str]) -> Vec<CollectionId> {
    let mut v: Vec<CollectionId> = names.iter().map(|n| CollectionId::fixed_name(n)).collect();
    v.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    v
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

    // Create an ephemeral node - should NOT be ready even after loading
    {
        let node = Node::new(engine.clone(), PermissiveAgent::new());
        assert!(!node.system.is_system_ready()); // Not ready immediately

        // Wait for load
        node.system.wait_loaded().await;
        assert!(!node.system.is_system_ready()); // Still not ready after load

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

        // Create new ephemeral node
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(!ephemeral_node.system.is_system_ready(), "Ephemeral node should not be ready before connection");

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

        // Data collections (ignoring the metadata catalog): the `pet` entity
        // lands on both nodes; the durable also EXECUTES the RFC 5.2 (specs/model-property-metadata/rfc.md)
        // registration the ephemeral forwarded (A11b auto-assert), which
        // additionally creates the catalog collections filtered out here.
        assert_eq!(data_collections(&ephemeral_engine)?, sorted_names(&["_ankurah_system", "pet"]));
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system", "pet"]));

        initial_root
    }; // Both nodes and connection are dropped here

    // Reset durable node's system (creating new root) but NOT ephemeral node
    let second_root = {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;

        // should be ready because we previously initialized a system
        assert!(durable_node.system.is_system_ready());

        // Reopening the durable engine still shows the persisted `pet` data
        // collection (catalog collections filtered out).
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system", "pet"]));

        // Reset storage and reinitialize
        durable_node.system.hard_reset().await?;

        assert_eq!(durable_engine.list_collections()?, Vec::<CollectionId>::new());

        assert!(!durable_node.system.is_system_ready());

        durable_node.system.create().await?;
        tokio::time::timeout(std::time::Duration::from_secs(2), durable_node.catalog.wait_catalog_ready())
            .await
            .expect("durable catalog should rewarm promptly after recreating the system root");
        assert!(durable_node.catalog.is_catalog_ready(), "durable catalog should rewarm after recreating the system root");

        // Only the system data collection (catalog collections filtered out:
        // a previously-constructed durable node sharing this engine may warm
        // its catalog asynchronously and re-touch catalog trees here).
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system"]));

        // Verify root has changed
        let second_root = durable_node.system.root().expect("Should have new root state");
        assert_ne!(second_root.payload.state.head, initial_root.payload.state.head, "Root state should be different after reset");

        assert_eq!(second_root.payload.state.head.len(), 1);

        let trx = durable_node.context(DEFAULT_CONTEXT)?.begin();
        trx.create(&Album { name: "Leonard Skynyrd".into(), year: "1973".to_string() }).await?;
        trx.commit().await?;

        // A durable node executes its own registration locally (A11b); here
        // we assert the `album` data collection (catalog collections filtered).
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system", "album"]));

        second_root
    }; // Drop durable node

    // Ephemeral node joins the new system and resets everything
    {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;
        assert!(durable_node.system.is_system_ready()); // should be ready when loaded
        assert_eq!(durable_node.system.root(), Some(second_root.clone()));
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system", "album"]));

        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(!ephemeral_node.system.is_system_ready()); // should not be ready before joining
        assert_eq!(ephemeral_node.system.root(), Some(initial_root), "Ephemeral node should have old root prior to joining");
        // The ephemeral engine never executed a registration (it forwarded
        // to the durable), so only `_ankurah_system` and `pet` persisted.
        assert_eq!(data_collections(&ephemeral_engine)?, sorted_names(&["_ankurah_system", "pet"]));

        // Connect nodes
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // Wait for ephemeral node to be ready
        ephemeral_node.system.wait_system_ready().await;

        assert_eq!(ephemeral_node.system.root(), Some(second_root), "Ephemeral node should have new root after joining");

        // Joining the new system hard-resets the ephemeral (root mismatch),
        // leaving only the system data collection (catalog filtered out).
        assert_eq!(data_collections(&ephemeral_engine)?, sorted_names(&["_ankurah_system"]));
    }

    Ok(())
}
