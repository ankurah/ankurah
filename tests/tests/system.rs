mod common;
use ankurah::core::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT, proto::CollectionId, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::{Album, Pet};
use std::sync::Arc;

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

/// A persisted ephemeral root does not confer readiness after restart.
#[tokio::test]
async fn a_persisted_join_reloads_without_conferring_readiness() -> Result<()> {
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
    durable_node.system.create().await?;
    let root = durable_node.system.root().expect("the durable node has a root to join");

    // Join with nobody connected: ready in memory, and the root is written
    // durably as part of the join itself.
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        ephemeral_node.system.join_system(root.clone()).await?;
        assert!(ephemeral_node.system.is_system_ready(), "the join completes in memory");
        ephemeral_node.catalog.wait_ready().await?;
        assert_eq!(ephemeral_node.system.root(), Some(root.clone()), "the node holds the root it joined");
        assert!(root_persisted(&ephemeral_engine).await?);
    } // kill

    // Reopen: the root loads ("joined once"), but readiness is not loaded
    // with it -- an ephemeral node must join again to be part of a system.
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert_eq!(
            ephemeral_node.system.root().map(|loaded| loaded.payload.state.head),
            Some(root.payload.state.head.clone()),
            "the persisted join survives a restart"
        );
        assert!(!ephemeral_node.system.is_system_ready(), "a loaded root never confers readiness on an ephemeral node");

        // The re-join is the same join: the matching root marks ready
        // without resetting anything.
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;
        ephemeral_node.system.wait_system_ready().await;
        assert_eq!(ephemeral_node.system.root(), Some(root.clone()), "the replayed join reaches the same system");
        assert_eq!(ephemeral_node.system.items().len(), 1, "exactly one root was ever written");
    }

    Ok(())
}

/// Joining a different system resets the old root and storage before writing
/// the replacement. The whole transition is serialized with other root work.
#[tokio::test]
async fn joining_a_different_system_replaces_a_persisted_root() -> Result<()> {
    let abandoned_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let joined_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    let abandoned = Node::new_durable(abandoned_engine, PermissiveAgent::new());
    abandoned.system.create().await?;
    let abandoned_root = abandoned.system.root().expect("the abandoned system has a root");

    let joined = Node::new_durable(joined_engine, PermissiveAgent::new());
    joined.system.create().await?;
    let joined_root = joined.system.root().expect("the joined system has a root");

    let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
    ephemeral_node.system.wait_loaded().await;

    // Join a system nobody is connected to: the join writes its root
    // durably right away.
    ephemeral_node.system.join_system(abandoned_root.clone()).await?;
    assert!(root_persisted(&ephemeral_engine).await?);

    // Connect to a DIFFERENT system. The mismatched root resets this node
    // (wiping the abandoned root with the rest of storage) and joins that
    // one instead.
    let _conn = LocalProcessConnection::new(&joined, &ephemeral_node).await?;
    ephemeral_node.system.wait_system_ready().await;
    assert!(root_persisted(&ephemeral_engine).await?);
    assert_eq!(ephemeral_node.system.root(), Some(joined_root.clone()), "the node holds the system it actually joined");

    // Reopen: one root on disk, and it is the system this node joined.
    let reopened = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
    reopened.system.wait_loaded().await;
    assert_eq!(
        reopened.system.root().map(|root| root.payload.state.head),
        Some(joined_root.payload.state.head),
        "the abandoned join must not resurrect itself over the joined one"
    );
    assert_ne!(reopened.system.root().map(|root| root.payload.entity_id), Some(abandoned_root.payload.entity_id));
    assert_eq!(reopened.system.items().len(), 1, "exactly one root was ever written");

    Ok(())
}

/// Whether this engine holds a system root: exactly what a node reopened
/// over it would load. Reads what is there without opening the collection,
/// so asking the question cannot itself materialize it.
async fn root_persisted(engine: &Arc<SledStorageEngine>) -> Result<bool> {
    let collection_id = CollectionId::fixed_name("_ankurah_system");
    if !engine.list_collections()?.contains(&collection_id) {
        return Ok(false);
    }
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    Ok(!engine.collection(&collection_id).await?.fetch_states(&selection).await?.is_empty())
}

/// The three catalog collections the durable warm opens at startup, in
/// sorted order for census comparisons.
fn with_catalog(mut others: Vec<CollectionId>) -> Vec<CollectionId> {
    others.extend(["_ankurah_model", "_ankurah_model_property", "_ankurah_property"].map(CollectionId::fixed_name));
    others.sort();
    others
}

fn sorted(mut v: Vec<CollectionId>) -> Vec<CollectionId> {
    v.sort();
    v
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

        // A later block reopens this engine and expects the joined root.
        assert!(root_persisted(&ephemeral_engine).await?);

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

        // Every node materializes the catalog collections now: readiness means
        // the catalog projection has run, and its queries open them.
        assert_eq!(
            sorted(ephemeral_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")])
        );

        durable_node.catalog.wait_ready().await?;
        assert_eq!(
            sorted(durable_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")])
        );

        initial_root
    }; // Both nodes and connection are dropped here

    // Reset durable node's system (creating new root) but NOT ephemeral node
    let second_root = {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;

        // should be ready because we previously initialized a system
        assert!(durable_node.system.is_system_ready());

        durable_node.catalog.wait_ready().await?;
        assert_eq!(
            sorted(durable_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")])
        );

        // Reset storage and reinitialize
        durable_node.system.hard_reset().await?;

        assert_eq!(durable_engine.list_collections()?, Vec::<CollectionId>::new());

        assert!(!durable_node.system.is_system_ready());

        durable_node.system.create().await?;

        durable_node.catalog.wait_ready().await?;
        assert_eq!(sorted(durable_engine.list_collections()?), with_catalog(vec![CollectionId::fixed_name("_ankurah_system")]));

        // Verify root has changed
        let second_root = durable_node.system.root().expect("Should have new root state");
        assert_ne!(second_root.payload.state.head, initial_root.payload.state.head, "Root state should be different after reset");

        assert_eq!(second_root.payload.state.head.len(), 1);

        let trx = durable_node.context(DEFAULT_CONTEXT)?.begin();
        trx.create(&Album { name: "Leonard Skynyrd".into(), year: "1973".to_string() }).await?;
        trx.commit().await?;

        assert_eq!(
            sorted(durable_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("album")])
        );

        second_root
    }; // Drop durable node

    // Ephemeral node joins the new system and resets everything
    {
        let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
        durable_node.system.wait_loaded().await;
        assert!(durable_node.system.is_system_ready()); // should be ready when loaded
        assert_eq!(durable_node.system.root(), Some(second_root.clone()));
        durable_node.catalog.wait_ready().await?;
        assert_eq!(
            sorted(durable_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("album")])
        );

        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert!(!ephemeral_node.system.is_system_ready()); // should not be ready before joining
        assert_eq!(ephemeral_node.system.root(), Some(initial_root), "Ephemeral node should have old root prior to joining");
        assert_eq!(
            sorted(ephemeral_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")])
        );

        // Connect nodes
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

        // Wait for ephemeral node to be ready
        ephemeral_node.system.wait_system_ready().await;

        assert_eq!(ephemeral_node.system.root(), Some(second_root), "Ephemeral node should have new root after joining");

        // The re-join wiped storage and wrote the replacement root.
        assert!(root_persisted(&ephemeral_engine).await?);
        ephemeral_node.catalog.wait_ready().await?;
        assert_eq!(sorted(ephemeral_engine.list_collections()?), with_catalog(vec![CollectionId::fixed_name("_ankurah_system")]));
    }

    Ok(())
}
