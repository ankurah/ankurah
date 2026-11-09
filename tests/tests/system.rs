//! A node's local system: readiness from its own storage, a fresh epoch per
//! Node, and the reset primitive that wipes a store for a new node.

mod common;

use ankurah::core::{error::NodeHaltReason, storage::StorageEngine};
use common::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn a_restarted_durable_node_keeps_its_entities_under_a_new_epoch() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let (id, epoch) = {
            let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
            let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
            let id = transaction.create(&Pet { name: "Fido".into(), age: "3".into() }).await?.id();
            transaction.commit().await?;
            (id, node.system.system_epoch())
        };

        let node = Node::new_durable(engine, PermissiveAgent::new());
        let context = node.context_async(DEFAULT_CONTEXT).await?;
        assert_eq!(context.get::<PetView>(id).await?.name()?, "Fido");
        assert!(node.system.system_epoch().is_some());
        assert_ne!(node.system.system_epoch(), epoch, "each Node owns its own epoch");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn reset_local_storage_halts_and_wipes_and_a_fresh_node_starts_clean() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = durable_sled_setup().await?;
        let transaction = server.context(DEFAULT_CONTEXT)?.begin();
        let id = transaction.create(&Pet { name: "preserved".into(), age: "3".into() }).await?.id();
        transaction.commit().await?;

        let engine = Arc::new(SledStorageEngine::new_test()?);
        let client = Node::new(engine.clone(), PermissiveAgent::new());
        let _connection = LocalProcessConnection::new(&server, &client).await?;
        let context = client.context_async(DEFAULT_CONTEXT).await?;
        let retained = context.get::<PetView>(id).await?;
        assert!(engine.get_state(id).await.is_ok());

        client.reset_local_storage().await?;
        let halt_reason = NodeHaltReason::LocalReset;
        assert_eq!(client.state().peek(), NodeState::Halted(halt_reason.clone()));
        assert!(!client.system.is_system_ready());
        assert!(client.system.system_epoch().is_none());
        assert!(engine.get_state(id).await.is_err(), "the reset wiped the entity's state");
        assert!(engine.dump_entity_events(id).await?.is_empty(), "and its events");
        assert!(engine.list_materializations().await?.is_empty());
        assert_eq!(retained.name()?, "preserved", "a retained view keeps what it read");
        assert_eq!(client.system.wait_system_ready().await, Err(halt_reason.clone()));
        assert_eq!(client.context_async(DEFAULT_CONTEXT).await.err(), Some(halt_reason.into()));

        let fresh = Node::new(engine.clone(), PermissiveAgent::new());
        fresh.system.wait_system_ready().await?;
        assert!(fresh.state().peek().halt_reason().is_none(), "the halt reason belongs to the old Node, not its store");
        assert!(fresh.system.items().is_empty());
        assert!(engine.get_state(id).await.is_err(), "the fresh node starts from the wiped store");
        let _reconnection = LocalProcessConnection::new(&server, &fresh).await?;
        let context = fresh.context_async(DEFAULT_CONTEXT).await?;
        assert_eq!(context.get::<PetView>(id).await?.name()?, "preserved", "and reads again from its durable peer");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn reset_local_storage_finishes_its_wipe_when_the_caller_stops_waiting() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
        let id = transaction.create(&Pet { name: "wiped".into(), age: "2".into() }).await?.id();
        transaction.commit().await?;

        let mut reset = Box::pin(node.reset_local_storage());
        let _ = futures_util::poll!(&mut reset);
        assert_eq!(node.state().peek(), NodeState::Halted(NodeHaltReason::LocalReset), "the node halts before the wipe");
        drop(reset);
        tokio::time::timeout(Duration::from_secs(2), async {
            while engine.get_state(id).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    })
    .await?
}
