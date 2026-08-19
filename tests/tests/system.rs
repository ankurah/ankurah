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

/// A node that joins a system it never hears back from has joined in memory
/// only: the root is written after the first catalog sync, so a crash before
/// that leaves nothing behind and the node joins again from scratch.
///
/// The alternative -- writing the root at join -- is what makes that crash
/// silent: the reopened node loads a root, stops joining because it has one,
/// and serves an empty catalog as if it were the system's.
#[tokio::test]
async fn a_join_that_never_synced_leaves_no_root_behind() -> Result<()> {
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    let durable_node = Node::new_durable(durable_engine.clone(), PermissiveAgent::new());
    durable_node.system.create().await?;
    let root = durable_node.system.root().expect("the durable node has a root to join");

    // Join with nobody connected: the catalog projection runs over local
    // storage, so the node is READY, and no peer has answered a word of it.
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        ephemeral_node.system.join_system(root.clone()).await?;
        assert!(ephemeral_node.system.is_system_ready(), "the join completes in memory");
        ephemeral_node.catalog.wait_catalog_ready().await;
        assert_eq!(ephemeral_node.system.root(), Some(root.clone()), "the node holds the root it joined");
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(!root_persisted(&ephemeral_engine).await?, "an unanswered catalog must leave the join in memory");
    } // kill

    // Reopen: nothing was joined, so this node joins rather than pretending
    // it already had.
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert_eq!(ephemeral_node.system.root(), None, "the unsynced join left nothing to load");
        assert!(!ephemeral_node.system.is_system_ready());

        // The re-join is the same join, and this time a peer answers it.
        let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;
        ephemeral_node.system.wait_system_ready().await;
        assert_eq!(ephemeral_node.system.root(), Some(root.clone()), "the replayed join reaches the same system");
        wait_root_persisted(&ephemeral_engine).await?;
    }

    // Reopen once more: a synced join IS durable.
    {
        let ephemeral_node = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        ephemeral_node.system.wait_loaded().await;
        assert_eq!(
            ephemeral_node.system.root().map(|loaded| loaded.payload.state.head),
            Some(root.payload.state.head),
            "the synced join survives a restart"
        );
    }

    Ok(())
}

/// A join whose catalog is never answered stays pending for as long as the
/// node lives, so joining a DIFFERENT system in the meantime has to win: the
/// pending write is conditional on the node still holding the root it was
/// asked to write, and by then it holds another one.
#[tokio::test]
async fn a_pending_join_never_overwrites_the_system_actually_joined() -> Result<()> {
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

    // Join a system nobody is connected to: in memory, and its write is
    // still waiting for a catalog answer that has not come.
    ephemeral_node.system.join_system(abandoned_root.clone()).await?;
    assert!(!root_persisted(&ephemeral_engine).await?);

    // Connect to a DIFFERENT system. The mismatched root resets this node and
    // joins that one instead, and its catalog is answered -- which is also
    // when the abandoned join's pending write gets its turn.
    let _conn = LocalProcessConnection::new(&joined, &ephemeral_node).await?;
    ephemeral_node.system.wait_system_ready().await;
    wait_root_persisted(&ephemeral_engine).await?;
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
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

/// Wait for a join to become durable. An ephemeral node's join is held in
/// memory until its catalog has been answered, and the write that follows
/// that answer is a task hop behind it, so a test that reopens the engine
/// waits for the write rather than for readiness.
async fn wait_root_persisted(engine: &Arc<SledStorageEngine>) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root_persisted(engine).await? {
            tokio::task::yield_now().await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("the joined root was never written to storage"))?
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

        // A later block reopens this engine and expects the old root there,
        // so wait for the join to become durable, which is the first catalog
        // sync plus the write behind it.
        wait_root_persisted(&ephemeral_engine).await?;

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

        durable_node.catalog.wait_catalog_ready().await;
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

        durable_node.catalog.wait_catalog_ready().await;
        assert_eq!(
            sorted(durable_engine.list_collections()?),
            with_catalog(vec![CollectionId::fixed_name("_ankurah_system"), CollectionId::fixed_name("pet")])
        );

        // Reset storage and reinitialize
        durable_node.system.hard_reset().await?;

        assert_eq!(durable_engine.list_collections()?, Vec::<CollectionId>::new());

        assert!(!durable_node.system.is_system_ready());

        durable_node.system.create().await?;

        durable_node.catalog.wait_catalog_ready().await;
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
        durable_node.catalog.wait_catalog_ready().await;
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

        // The re-join wiped storage and starts over: the system collection
        // comes back with the root, once the replacement catalog answers.
        wait_root_persisted(&ephemeral_engine).await?;
        assert_eq!(sorted(ephemeral_engine.list_collections()?), with_catalog(vec![CollectionId::fixed_name("_ankurah_system")]));
    }

    Ok(())
}
