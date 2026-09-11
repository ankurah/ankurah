mod common;

use ankurah::core::{connector::PeerConnectionError, error::NodeHaltReason, storage::StorageEngine};
use common::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn test_system() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let (root, epoch) = {
            let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
            node.system.create().await?;
            let root = node.system.root().expect("created root");
            assert_eq!(root.payload.state.head.len(), 1);
            assert_eq!(node.system.items().len(), 1);
            (root, node.system.system_epoch())
        };

        let node = Node::new_durable(engine, PermissiveAgent::new());
        node.system.wait_loaded().await?;
        assert!(node.system.create().await.is_err(), "a persisted system cannot be created twice");
        assert_eq!(node.system.root(), Some(root));
        assert_eq!(node.system.items().len(), 1);
        assert_ne!(node.system.system_epoch(), epoch, "each Node owns its own epoch");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn persisted_root_restores_the_system_before_ephemeral_catalog_readiness() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let root = {
            let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
            node.system.create().await?;
            node.system.root().expect("persisted root")
        };

        for durable in [true, false] {
            let node = if durable {
                Node::new_durable(engine.clone(), PermissiveAgent::new())
            } else {
                Node::new(engine.clone(), PermissiveAgent::new())
            };
            node.system.wait_loaded().await?;
            node.system.wait_system_ready().await?;
            if durable {
                node.wait_ready().await?;
            } else {
                let mut ready = Box::pin(node.wait_ready());
                assert!(futures_util::poll!(&mut ready).is_pending());
                assert_eq!(node.state().peek(), NodeState::Startup);
            }
            assert!(node.system.is_system_ready());
            assert_eq!(node.system.root(), Some(root.clone()));
            assert!(node.system.system_epoch().is_some());
            assert!(node.get_durable_peers().is_empty());
            assert!(node.state().peek().halt_reason().is_none());
            node.context_async(DEFAULT_CONTEXT).await?;
        }
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn adopted_root_persists_and_reconnecting_keeps_the_epoch() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let root = server.system.root().expect("durable root");
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let epoch = {
            let node = Node::new(engine.clone(), PermissiveAgent::new());
            node.system.wait_loaded().await?;
            assert!(!node.system.is_system_ready());
            assert!(node.system.system_epoch().is_none());
            assert_eq!(node.state().peek(), NodeState::Uninitialized);
            node.system.adopt_system(root.clone()).await?;
            node.system.wait_system_ready().await?;
            let _connection = LocalProcessConnection::new(&server, &node).await?;
            node.wait_ready().await?;
            let epoch = node.system.system_epoch();
            assert!(epoch.is_some());
            node.system.adopt_system(root.clone()).await?;
            assert_eq!(node.state().peek(), NodeState::Running);
            assert_eq!(node.system.system_epoch(), epoch);
            assert_eq!(node.system.root(), Some(root.clone()));
            epoch
        };

        let node = Node::new(engine.clone(), PermissiveAgent::new());
        node.system.wait_loaded().await?;
        node.system.wait_system_ready().await?;
        assert_eq!(node.system.root(), Some(root.clone()));
        assert_ne!(node.system.system_epoch(), epoch);
        assert!(node.get_durable_peers().is_empty());
        let roots = engine.collection(&root.payload.collection).await?;
        let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert_eq!(roots.fetch_states(&selection).await?, vec![root]);
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn different_system_halts_by_default_without_wiping_storage() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let server_context = server.context(DEFAULT_CONTEXT)?;
        let transaction = server_context.begin();
        let id = transaction.create(&Pet { name: "Fido".into(), age: "3".into() }).await?.id();
        transaction.commit().await?;

        let engine = Arc::new(SledStorageEngine::new_test()?);
        let client = Node::new(engine.clone(), PermissiveAgent::new());
        let _connection = LocalProcessConnection::new(&server, &client).await?;
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        let query = context.query_wait::<PetView>("true").await?;
        query.wait_durable_answered().await?;
        let retained = context.get::<PetView>(id).await?;
        let root = client.system.root().expect("adopted root");
        let epoch = client.system.system_epoch();
        let selection = query.selection().peek();
        let collection = engine.collection(&Pet::collection()).await?;
        let state = collection.get_state(id).await?;
        let collections = engine.list_collections()?;

        let other = durable_sled_setup().await?;
        let proposed = other.system.root().expect("different root");
        assert_ne!(root.payload.entity_id, proposed.payload.entity_id);
        let reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: proposed.payload.entity_id };
        assert_eq!(client.system.adopt_system(proposed.clone()).await, Err(PeerConnectionError::NodeHalted(reason.clone())));

        assert_eq!(client.state().peek(), NodeState::Halted(reason));
        assert!(!client.system.is_system_ready());
        assert_eq!(client.system.root(), Some(root.clone()));
        assert_eq!(client.system.system_epoch(), epoch);
        assert_eq!(engine.list_collections()?, collections);
        assert_eq!(collection.get_state(id).await?, state);
        assert_eq!(query.selection().peek(), selection);
        assert_eq!(retained.name()?, "Fido");

        let transaction = context.begin();
        retained.edit(&transaction)?.age()?.replace("4")?;
        assert!(transaction.commit().await.is_err());

        let reopened = Node::new(engine, PermissiveAgent::new());
        reopened.system.wait_system_ready().await?;
        let _reconnection = LocalProcessConnection::new(&server, &reopened).await?;
        reopened.wait_ready().await?;
        assert_eq!(reopened.system.root(), Some(root));
        assert_eq!(reopened.context(DEFAULT_CONTEXT)?.get_cached::<PetView>(id).await?.age()?, "3");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn enabled_replacement_wipes_storage_for_the_next_node() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let transaction = server.context(DEFAULT_CONTEXT)?.begin();
        let id = transaction.create(&Pet { name: "preserved".into(), age: "3".into() }).await?.id();
        transaction.commit().await?;

        let engine = Arc::new(SledStorageEngine::new_test()?);
        let client = Node::new(engine.clone(), PermissiveAgent::new());
        let _connection = LocalProcessConnection::new(&server, &client).await?;
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        context.get::<PetView>(id).await?;
        client.wait_ready().await?;
        let root = client.system.root().expect("original root");
        let collection = engine.collection(&Pet::collection()).await?;
        assert!(collection.get_state(id).await.is_ok());
        let roots = engine.collection(&root.payload.collection).await?;
        assert!(roots.get_state(root.payload.entity_id).await.is_ok());
        let node_state = client.state();
        assert!(node_state.peek().halt_reason().is_none());

        let other = durable_sled_setup().await?;
        let proposed = other.system.root().expect("replacement root");
        let halt_reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: proposed.payload.entity_id };
        client.set_allow_system_replacement(true);
        assert_eq!(client.system.adopt_system(proposed.clone()).await, Err(PeerConnectionError::NodeHalted(halt_reason.clone())));
        assert_eq!(node_state.peek(), NodeState::Halted(halt_reason.clone()));
        assert!(!client.system.is_system_ready());
        assert!(client.system.root().is_none());
        assert!(client.system.system_epoch().is_none());
        assert!(client.system.items().is_empty());
        assert!(engine.list_collections()?.is_empty());

        client.set_allow_system_replacement(false);
        assert_eq!(client.system.adopt_system(root.clone()).await, Err(PeerConnectionError::NodeHalted(halt_reason.clone())));
        assert_eq!(client.system.wait_loaded().await, Err(halt_reason.clone()));
        assert_eq!(client.system.wait_system_ready().await, Err(halt_reason));

        let reopened = Node::new(engine.clone(), PermissiveAgent::new());
        reopened.system.wait_loaded().await?;
        assert!(reopened.state().peek().halt_reason().is_none(), "the halt reason belongs to the old Node, not its store");
        assert!(reopened.system.root().is_none());
        let collection = engine.collection(&Pet::collection()).await?;
        assert!(collection.get_state(id).await.is_err());
        assert!(collection.dump_entity_events(id).await?.is_empty());
        reopened.system.adopt_system(proposed.clone()).await?;
        assert_eq!(reopened.system.root(), Some(proposed));
        Ok(())
    })
    .await?
}
