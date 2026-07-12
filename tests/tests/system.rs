mod common;
use ankurah::{
    error::{MutationError, RetrievalError, StateError},
    model::View,
    policy::DEFAULT_CONTEXT,
    property::PropertyError,
    proto::{self, Attested, CollectionId},
    storage::{StorageCollection, StorageEngine, SystemRootClaim},
    Node, PermissiveAgent,
};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use common::{Album, AlbumView, Pet};
use ed25519_dalek::SigningKey;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;

/// One-shot barriers around the storage-wide root metadata calls. They make
/// cross-Node reset races deterministic while delegating every durable byte
/// to the real sled engine.
struct PausingStorageEngine {
    inner: Arc<SledStorageEngine>,
    pause_next_claim_read: AtomicBool,
    claim_read_seen: Notify,
    release_claim_read: Notify,
    pause_next_claim_attempt: AtomicBool,
    fail_paused_claim: AtomicBool,
    claim_attempt_seen: Notify,
    release_claim_attempt: Notify,
}

impl PausingStorageEngine {
    fn new(inner: Arc<SledStorageEngine>) -> Self {
        Self {
            inner,
            pause_next_claim_read: AtomicBool::new(false),
            claim_read_seen: Notify::new(),
            release_claim_read: Notify::new(),
            pause_next_claim_attempt: AtomicBool::new(false),
            fail_paused_claim: AtomicBool::new(false),
            claim_attempt_seen: Notify::new(),
            release_claim_attempt: Notify::new(),
        }
    }

    fn pause_next_claim_read(&self) { self.pause_next_claim_read.store(true, Ordering::Release); }
    async fn wait_for_claim_read(&self) { self.claim_read_seen.notified().await; }
    fn release_claim_read(&self) { self.release_claim_read.notify_one(); }

    fn pause_next_claim_attempt(&self) { self.pause_next_claim_attempt.store(true, Ordering::Release); }
    fn fail_next_claim_after_pause(&self) {
        self.fail_paused_claim.store(true, Ordering::Release);
        self.pause_next_claim_attempt();
    }
    async fn wait_for_claim_attempt(&self) { self.claim_attempt_seen.notified().await; }
    fn release_failed_claim(&self) { self.release_claim_attempt.notify_one(); }
}

#[async_trait]
impl StorageEngine for PausingStorageEngine {
    type Value = <SledStorageEngine as StorageEngine>::Value;

    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        <SledStorageEngine as StorageEngine>::collection(self.inner.as_ref(), id).await
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        <SledStorageEngine as StorageEngine>::delete_all_collections(self.inner.as_ref()).await
    }

    async fn claim_system_root(&self, candidate: &proto::SystemRootProof) -> Result<SystemRootClaim, MutationError> {
        if self.pause_next_claim_attempt.swap(false, Ordering::AcqRel) {
            self.claim_attempt_seen.notify_one();
            self.release_claim_attempt.notified().await;
            if self.fail_paused_claim.swap(false, Ordering::AcqRel) {
                return Err(MutationError::General(Box::new(std::io::Error::other("injected root-claim failure"))));
            }
        }
        <SledStorageEngine as StorageEngine>::claim_system_root(self.inner.as_ref(), candidate).await
    }

    async fn system_root_claim(&self) -> Result<Option<proto::SystemRootProof>, RetrievalError> {
        let claim = <SledStorageEngine as StorageEngine>::system_root_claim(self.inner.as_ref()).await?;
        if self.pause_next_claim_read.swap(false, Ordering::AcqRel) {
            self.claim_read_seen.notify_one();
            self.release_claim_read.notified().await;
        }
        Ok(claim)
    }

    async fn release_system_root_claim(&self, expected: &proto::SystemRootProof) -> Result<bool, MutationError> {
        <SledStorageEngine as StorageEngine>::release_system_root_claim(self.inner.as_ref(), expected).await
    }

    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        <SledStorageEngine as StorageEngine>::list_collections(self.inner.as_ref()).await
    }

    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::property::PropertyResolver>) {
        <SledStorageEngine as StorageEngine>::set_property_resolver(self.inner.as_ref(), resolver);
    }
}

/// Tests that reopen durable storage must also reopen the durable identity.
/// Production embedders persist this seed with `node_key`; a fixed key keeps
/// these reconstruction scenarios honest without involving the filesystem.
fn persisted_durable_key() -> SigningKey { SigningKey::from_bytes(&[0x5A; 32]) }

fn competing_durable_key() -> SigningKey { SigningKey::from_bytes(&[0x6B; 32]) }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_system_create_persists_exactly_one_root() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    node.system.wait_loaded().await;

    let (first, second) = tokio::join!(node.system.create(), node.system.create());
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert_eq!(node.system.items().len(), 1);

    let root_id = node.system.root_id().expect("one create pins the root");
    let collection = engine.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    assert_eq!(collection.dump_entity_events(root_id).await?.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_nodes_sharing_storage_converge_on_one_root() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let first = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    let second = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), competing_durable_key());
    tokio::join!(first.system.wait_loaded(), second.system.wait_loaded());

    let (first_result, second_result) = tokio::join!(first.system.create(), second.system.create());
    assert_eq!(usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()), 1);

    let claim = engine.system_root_claim().await?.expect("one complete root proof is claimed");
    let collection = engine.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    let states =
        collection.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?;
    assert_eq!(states.len(), 1, "the losing node must not write a second root state");
    assert_eq!(states[0].payload.entity_id, claim.entity_id());
    assert_eq!(collection.dump_entity_events(claim.entity_id()).await?.len(), 1);

    let winner_root = first.system.root_id().or_else(|| second.system.root_id()).expect("the winning manager is ready");
    assert_eq!(winner_root, claim.entity_id());
    Ok(())
}

#[tokio::test]
async fn loading_multiple_valid_system_roots_fails_closed() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let founder = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    founder.system.create().await?;

    let foreign_engine = Arc::new(SledStorageEngine::new_test()?);
    let foreign = Node::new_durable_with_signing_key(foreign_engine, PermissiveAgent::new(), competing_durable_key());
    foreign.system.create().await?;
    let foreign_proof = foreign
        .presence(proto::HandshakeChallenge::new(foreign.id, [0x91; 32]))
        .system_root
        .expect("durable presence carries its root proof");

    let collection = engine.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    collection.add_event(&Attested::opt(foreign_proof.genesis.clone(), None)).await?;
    collection.set_state(foreign_proof.state).await?;

    let reopened = Node::new_durable_with_signing_key(engine, PermissiveAgent::new(), persisted_durable_key());
    reopened.system.wait_loaded().await;
    assert!(!reopened.system.is_system_ready());
    assert!(reopened.system.root().is_none(), "load must not pick one valid root by scan order");
    assert!(reopened.system.items().is_empty(), "failed load must not partially publish system items");
    assert!(reopened.system.create().await.is_err(), "load failure must remain fail-closed");
    Ok(())
}

#[tokio::test]
async fn claimed_root_proof_with_state_attestations_is_rejected() -> Result<()> {
    let source_engine = Arc::new(SledStorageEngine::new_test()?);
    let source = Node::new_durable_with_signing_key(source_engine, PermissiveAgent::new(), persisted_durable_key());
    source.system.create().await?;
    let mut proof = source
        .presence(proto::HandshakeChallenge::new(source.id, [0x92; 32]))
        .system_root
        .expect("durable presence carries its root proof");
    let body = proto::AttestationBody::EventAdmitted {
        system_root: proof.entity_id(),
        event: proof.genesis.id(),
        model: proof.genesis.model,
        claims: vec![],
    };
    proof.state.attestations.push(proto::Attestation {
        attester: proto::NodeId::from_bytes([0x77; 32]),
        body,
        signature: proto::Signature::from_bytes([0x88; 64]),
    });

    let engine = Arc::new(SledStorageEngine::new_test()?);
    assert_eq!(engine.claim_system_root(&proof).await?, SystemRootClaim::Claimed);
    let reopened = Node::new_durable_with_signing_key(engine, PermissiveAgent::new(), persisted_durable_key());
    reopened.system.wait_loaded().await;
    assert!(!reopened.system.is_system_ready());
    assert!(reopened.system.root().is_none());
    assert!(reopened.system.create().await.is_err());
    Ok(())
}

#[tokio::test]
async fn complete_claim_recovers_crash_before_root_writes() -> Result<()> {
    let source_engine = Arc::new(SledStorageEngine::new_test()?);
    let source = Node::new_durable_with_signing_key(source_engine, PermissiveAgent::new(), persisted_durable_key());
    source.system.create().await?;
    let proof = source
        .presence(proto::HandshakeChallenge::new(source.id, [0x93; 32]))
        .system_root
        .expect("durable presence carries its root proof");

    // Model a process dying immediately after the metadata CAS: the target
    // engine has the complete proposal but no root event or state yet.
    let engine = Arc::new(SledStorageEngine::new_test()?);
    assert_eq!(engine.claim_system_root(&proof).await?, SystemRootClaim::Claimed);
    let reopened = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    reopened.system.wait_loaded().await;

    assert!(reopened.system.is_system_ready());
    assert_eq!(reopened.system.root_id(), Some(proof.entity_id()));
    let collection = engine.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    assert_eq!(collection.get_state(proof.entity_id()).await?.payload, proof.state.payload);
    assert_eq!(collection.dump_entity_events(proof.entity_id()).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_system() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test().unwrap());
    {
        let node = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());

        node.system.create().await?;

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);

        let items = node.system.items();
        assert_eq!(items.len(), 1);
    }

    {
        let node = Node::new_durable_with_signing_key(engine, PermissiveAgent::new(), persisted_durable_key());

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
        let node = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
        assert!(!node.system.is_system_ready()); // Not ready before initialize

        node.system.create().await?;
        assert!(node.system.is_system_ready()); // Ready after initialize

        let root = node.system.root();
        assert_eq!(root.expect("Should have root").payload.state.head.len(), 1);
    }

    // Create another durable node - should be ready after loading since system exists
    {
        let node = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
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
        let durable_node = Node::new_durable_with_signing_key(durable_engine.clone(), PermissiveAgent::new(), persisted_durable_key());
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
        let durable_node = Node::new_durable_with_signing_key(durable_engine.clone(), PermissiveAgent::new(), persisted_durable_key());
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
        let durable_node = Node::new_durable_with_signing_key(durable_engine.clone(), PermissiveAgent::new(), persisted_durable_key());
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
        let durable_node = Node::new_durable_with_signing_key(durable_engine.clone(), PermissiveAgent::new(), persisted_durable_key());
        durable_node.system.wait_loaded().await;

        // should be ready because we previously initialized a system
        assert!(durable_node.system.is_system_ready());

        // Reopening the durable engine still shows the persisted `pet` data
        // collection (catalog collections filtered out).
        assert_eq!(data_collections(&durable_engine)?, sorted_names(&["_ankurah_system", "pet"]));

        // Reset storage and reinitialize
        durable_node.system.hard_reset().await?;

        assert_eq!(data_collections(&durable_engine)?, Vec::<CollectionId>::new());

        assert!(!durable_node.system.is_system_ready());

        durable_node.system.create().await?;

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
        let durable_node = Node::new_durable_with_signing_key(durable_engine.clone(), PermissiveAgent::new(), persisted_durable_key());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_reset_invalidates_old_handles_transactions_and_peer_routes() -> Result<()> {
    let founder_a_engine = Arc::new(SledStorageEngine::new_test()?);
    let founder_b_engine = Arc::new(SledStorageEngine::new_test()?);
    let client_engine = Arc::new(SledStorageEngine::new_test()?);

    let founder_a = Node::new_durable_with_signing_key(founder_a_engine, PermissiveAgent::new(), persisted_durable_key());
    let founder_b = Node::new_durable_with_signing_key(founder_b_engine, PermissiveAgent::new(), competing_durable_key());
    founder_a.system.create().await?;
    founder_b.system.create().await?;

    let client = Node::new(client_engine, PermissiveAgent::new());
    client.system.wait_loaded().await;
    let _old_connection = LocalProcessConnection::new(&founder_a, &client).await?;
    client.system.wait_system_ready().await;
    for _ in 0..100 {
        if client.get_durable_peers() == vec![founder_a.id] {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(client.get_durable_peers(), vec![founder_a.id]);

    // Warm the client catalog before creating the application entity so the
    // retained View uses the ordinary id-keyed projection path.
    let client_context = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;
    let founder_context = founder_a.context(DEFAULT_CONTEXT)?;
    let create = founder_context.begin();
    let album_id = create.create(&Album { name: "old-system album".into(), year: "1973".into() }).await?.id();
    create.commit().await?;

    let old_view: AlbumView = client_context.get(album_id).await?;
    assert_eq!(old_view.name()?, "old-system album");
    let old_entity = old_view.entity().clone();
    assert!(client.get_resident_entity(album_id).is_some());

    // Keep both a staged transaction and an otherwise-unused transaction
    // alive across reset. The latter exercises synchronous edit and async
    // create after its generation has been invalidated.
    let staged_transaction = client_context.begin();
    let staged_mutable = old_view.edit(&staged_transaction)?;
    staged_mutable.name().replace("must never persist")?;
    let unused_transaction = client_context.begin();

    client.system.hard_reset().await?;

    assert!(client.get_resident_entity(album_id).is_none(), "reset must evict every resident lookup entry");
    assert!(client.get_durable_peers().is_empty(), "old founder must leave durable routing immediately");
    assert!(matches!(old_view.name(), Err(PropertyError::SystemReset)));
    assert!(matches!(old_entity.to_state(), Err(StateError::SystemReset)));
    assert!(matches!(
        staged_mutable.name().replace("write through stale mutable"),
        Err(MutationError::PropertyError(PropertyError::SystemReset))
    ));
    assert!(matches!(old_view.edit(&unused_transaction), Err(ankurah::policy::AccessDenied::PropertyError(_))));
    assert!(matches!(
        unused_transaction.create(&Album { name: "stale create".into(), year: "2000".into() }).await,
        Err(MutationError::SystemReset)
    ));
    drop(staged_mutable);
    assert!(matches!(staged_transaction.commit().await, Err(MutationError::SystemReset)));
    assert!(client_context.get::<AlbumView>(album_id).await.is_err(), "the deleted old id must not resolve without a current peer");

    // Join a different system while the old transport object is deliberately
    // still alive. Only the new founder may be promoted into routing, and the
    // old system's self-certifying entity id remains absent.
    let _new_connection = LocalProcessConnection::new(&founder_b, &client).await?;
    client.system.wait_system_ready().await;
    for _ in 0..100 {
        if client.get_durable_peers() == vec![founder_b.id] {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(client.get_durable_peers(), vec![founder_b.id]);
    assert!(!client.get_durable_peers().contains(&founder_a.id));
    let new_context = client.context_async(DEFAULT_CONTEXT).await;
    assert!(new_context.get::<AlbumView>(album_id).await.is_err(), "old-system id must not resolve after joining a new root");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_engine_reset_waits_for_old_load_and_stales_the_other_manager() -> Result<()> {
    let source_engine = Arc::new(SledStorageEngine::new_test()?);
    let source = Node::new_durable_with_signing_key(source_engine, PermissiveAgent::new(), persisted_durable_key());
    source.system.create().await?;
    let old_proof =
        source.presence(proto::HandshakeChallenge::new(source.id, [0xA1; 32])).system_root.expect("source advertises its root proof");

    // A complete claim with no collection rows makes the first manager's
    // startup recovery perform durable writes after the paused claim read.
    let sled = Arc::new(SledStorageEngine::new_test()?);
    assert_eq!(sled.claim_system_root(&old_proof).await?, SystemRootClaim::Claimed);
    let engine = Arc::new(PausingStorageEngine::new(sled.clone()));
    engine.pause_next_claim_read();
    let stale = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    tokio::time::timeout(std::time::Duration::from_secs(5), engine.wait_for_claim_read()).await?;

    // A separately-built Node over the exact same Arc<StorageEngine> must
    // share the load/reset lease. Its reset cannot pass the paused old load.
    let successor = Node::new_durable_with_signing_key(engine.clone(), PermissiveAgent::new(), persisted_durable_key());
    let reset_node = successor.clone();
    let mut reset = tokio::spawn(async move { reset_node.system.hard_reset().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut reset).await.is_err(),
        "shared-engine reset must wait for the old manager's load lease"
    );

    engine.release_claim_read();
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut reset).await???;
    tokio::time::timeout(std::time::Duration::from_secs(5), successor.system.wait_loaded()).await?;
    successor.system.create().await?;
    let new_root = successor.system.root_id().expect("successor creates a fresh root");
    assert_ne!(new_root, old_proof.entity_id());

    tokio::time::timeout(std::time::Duration::from_secs(5), stale.system.wait_loaded()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), stale.system.wait_system_ready()).await?;
    assert!(!stale.system.is_system_ready(), "the old manager is permanently stale after the shared epoch advances");
    assert!(stale.system.root_id().is_none());
    assert!(stale.presence(proto::HandshakeChallenge::new(stale.id, [0xA2; 32])).system_root.is_none());
    assert!(stale.system.create().await.is_err(), "a stale manager cannot resume writes into the successor system");

    let claim = sled.system_root_claim().await?.expect("successor root claim remains durable");
    assert_eq!(claim.entity_id(), new_root);
    let collection = sled.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    let states =
        collection.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?;
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].payload.entity_id, new_root);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_join_abort_cannot_delete_successor_join_on_shared_engine() -> Result<()> {
    let founder_a_engine = Arc::new(SledStorageEngine::new_test()?);
    let founder_b_engine = Arc::new(SledStorageEngine::new_test()?);
    let founder_a = Node::new_durable_with_signing_key(founder_a_engine, PermissiveAgent::new(), persisted_durable_key());
    let founder_b = Node::new_durable_with_signing_key(founder_b_engine, PermissiveAgent::new(), competing_durable_key());
    founder_a.system.create().await?;
    founder_b.system.create().await?;
    let proof_a = founder_a.presence(proto::HandshakeChallenge::new(founder_a.id, [0xB1; 32])).system_root.expect("founder A proof");
    let root_b = founder_b.system.root_id().expect("founder B root");

    // Seed the shared ephemeral cache with A, so A's rejoin is a no-reset
    // path while B's different-root join is one atomic reset+persist writer.
    let sled = Arc::new(SledStorageEngine::new_test()?);
    assert_eq!(sled.claim_system_root(&proof_a).await?, SystemRootClaim::Claimed);
    let system_collection = sled.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    system_collection.add_event(&Attested::opt(proof_a.genesis.clone(), None)).await?;
    system_collection.set_state(proof_a.state.clone()).await?;

    let engine = Arc::new(PausingStorageEngine::new(sled.clone()));
    let client_a = Node::new(engine.clone(), PermissiveAgent::new());
    let client_b = Node::new(engine.clone(), PermissiveAgent::new());
    tokio::join!(client_a.system.wait_loaded(), client_b.system.wait_loaded());
    assert_eq!(client_a.system.root_id(), Some(proof_a.entity_id()));
    assert_eq!(client_b.system.root_id(), Some(proof_a.entity_id()));

    // A owns the shared writer and pauses at an injected claim failure. B's
    // different-root join queues behind it first; after A releases, B resets
    // and publishes its root before A's abort cleanup can acquire the writer.
    engine.fail_next_claim_after_pause();
    let _connection_a = LocalProcessConnection::new(&founder_a, &client_a).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), engine.wait_for_claim_attempt()).await?;
    let _connection_b = LocalProcessConnection::new(&founder_b, &client_b).await?;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    engine.release_failed_claim();

    tokio::time::timeout(std::time::Duration::from_secs(5), client_b.system.wait_system_ready()).await?;
    assert!(client_b.system.is_system_ready());
    assert_eq!(client_b.system.root_id(), Some(root_b));

    // Let A's queued abort acquire the writer. Its bound epoch is stale, so
    // only A-local caches may be invalidated; B's new claim/rows must survive.
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(!client_a.system.is_system_ready());
    assert!(client_b.system.is_system_ready(), "stale abort must not advance the shared epoch again");
    assert_eq!(sled.system_root_claim().await?.map(|proof| proof.entity_id()), Some(root_b));
    let system_collection = sled.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    let states = system_collection
        .fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None })
        .await?;
    assert_eq!(states.len(), 1, "stale abort must not erase the successor root state");
    assert_eq!(states[0].payload.entity_id, root_b);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_first_joins_on_empty_shared_engine_leave_one_root() -> Result<()> {
    let founder_a =
        Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), persisted_durable_key());
    let founder_b =
        Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), competing_durable_key());
    founder_a.system.create().await?;
    founder_b.system.create().await?;
    let root_a = founder_a.system.root_id().unwrap();

    let sled = Arc::new(SledStorageEngine::new_test()?);
    let engine = Arc::new(PausingStorageEngine::new(sled.clone()));
    let client_a = Node::new(engine.clone(), PermissiveAgent::new());
    let client_b = Node::new(engine.clone(), PermissiveAgent::new());
    tokio::join!(client_a.system.wait_loaded(), client_b.system.wait_loaded());

    engine.pause_next_claim_attempt();
    let _connection_a = LocalProcessConnection::new(&founder_a, &client_a).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), engine.wait_for_claim_attempt()).await?;
    let _connection_b = LocalProcessConnection::new(&founder_b, &client_b).await?;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    engine.release_failed_claim();

    tokio::time::timeout(std::time::Duration::from_secs(5), client_a.system.wait_system_ready()).await?;
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
    assert!(client_a.system.is_system_ready());
    assert!(!client_b.system.is_system_ready());
    assert_eq!(sled.system_root_claim().await?.map(|proof| proof.entity_id()), Some(root_a));
    let collection = sled.collection(&CollectionId::fixed_name(ankurah::core::system::SYSTEM_COLLECTION_ID)).await?;
    let states =
        collection.fetch_states(&ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }).await?;
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].payload.entity_id, root_a);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "setup currently hits the pre-existing album descriptor race before the reset assertion; see the Claude handoff"]
async fn sibling_reset_immediately_invalidates_retained_handles_and_routes() -> Result<()> {
    let founder =
        Node::new_durable_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), persisted_durable_key());
    founder.system.create().await?;

    let shared = Arc::new(SledStorageEngine::new_test()?);
    let sibling = Node::new(shared.clone(), PermissiveAgent::new());
    let resetter = Node::new(shared, PermissiveAgent::new());
    tokio::join!(sibling.system.wait_loaded(), resetter.system.wait_loaded());
    let _connection = LocalProcessConnection::new(&founder, &sibling).await?;
    sibling.system.wait_system_ready().await;

    let sibling_context = sibling.context_async(DEFAULT_CONTEXT).await;
    sibling.catalog.wait_catalog_ready().await;
    let create = founder.context(DEFAULT_CONTEXT)?.begin();
    let id = create.create(&Album { name: "shared old".into(), year: "1973".into() }).await?.id();
    create.commit().await?;
    // An id-addressed get does not itself perform first-use registration.
    // Register explicitly so this test reaches retained-handle invalidation
    // instead of depending on an unrelated descriptor-shipping race.
    sibling_context.register::<Album>().await?;
    let retained: AlbumView = sibling_context.get(id).await?;
    let transaction = sibling_context.begin();
    let mutable = retained.edit(&transaction)?;
    assert_eq!(sibling.get_durable_peers(), vec![founder.id]);

    resetter.system.hard_reset().await?;
    assert!(matches!(retained.name(), Err(PropertyError::SystemReset)));
    assert!(matches!(mutable.name().replace("stale"), Err(MutationError::PropertyError(PropertyError::SystemReset))));
    assert!(sibling.get_durable_peers().is_empty());
    let stale_get = tokio::time::timeout(std::time::Duration::from_secs(2), sibling_context.get::<AlbumView>(id)).await?;
    assert!(stale_get.is_err());
    Ok(())
}
