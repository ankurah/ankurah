//! Startup and explicit storage reset, independent of peer ancestry.
mod common;

use ankurah::core::{
    error::{NodeHaltReason, NodeReadinessError},
    storage::StorageEngine,
};
use common::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn durable_startup_restores_keys_and_uses_a_fresh_epoch_without_creating_a_root() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let storage = Arc::new(SledStorageEngine::new_test()?);
        let (id, epoch) = {
            let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
            node.wait_ready().await?;
            (node.id(), node.epoch())
        };
        let node = Node::new_durable(storage, PermissiveAgent::new());
        node.wait_ready().await?;
        assert_eq!(node.id(), id);
        assert_ne!(node.epoch(), epoch);
        let legacy = ankql::ast::Predicate::MemberOf(proto::ModelId::System(proto::SystemModel::System)).into();
        assert!(node.storage.fetch_states(&legacy).await?.is_empty());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn independent_durable_nodes_connect_without_replacing_each_others_data() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let first = durable_sled_setup().await?;
        let second = durable_sled_setup().await?;
        let context = first.context(DEFAULT_CONTEXT)?;
        let trx = context.begin();
        let id = trx.create(&Pet { name: "retained".into(), age: "3".into() }).await?.id();
        trx.commit().await?;
        let epoch = first.epoch();
        let _connection = LocalProcessConnection::new(&first, &second).await?;
        assert!(first.check_ready().is_ok());
        assert!(second.check_ready().is_ok());
        assert_eq!(first.epoch(), epoch);
        assert_eq!(context.get::<PetView>(id).await?.name()?, "retained");
        assert!(second.storage.get_state(id).await.is_err(), "a connection alone does not request replication");
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn reset_wipes_storage_and_requires_a_new_node() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let storage = Arc::new(SledStorageEngine::new_test()?);
        let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
        let context = node.context_async(DEFAULT_CONTEXT).await?;
        let trx = context.begin();
        let id = trx.create(&Pet { name: "removed".into(), age: "3".into() }).await?.id();
        trx.commit().await?;
        let old_id = node.id();
        let epoch = node.epoch();
        node.reset().await?;
        assert_eq!(node.check_ready(), Err(NodeReadinessError::Halted(NodeHaltReason::Reset)));
        assert!(storage.get_state(id).await.is_err());
        assert!(storage.dump_entity_events(id).await?.is_empty());
        assert!(context.fetch::<PetView>("true").await.is_err());
        let fresh = Node::new_durable(storage, PermissiveAgent::new());
        fresh.wait_ready().await?;
        assert_ne!(fresh.id(), old_id);
        assert_ne!(fresh.epoch(), epoch);
        // An already successful reset is idempotent, including after its successor starts.
        node.reset().await?;
        assert_eq!(fresh.wait_id().await?, fresh.id().unwrap());
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn legacy_genesis_provenance_remains_valid_without_a_local_root() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let legacy = proto::Event::genesis(
        Some(EntityId::random()),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(EntityId::random())))]),
    );
    let bytes = bincode::serialize(&legacy)?;
    let restored: proto::Event = bincode::deserialize(&bytes)?;
    restored.validate_structure()?;
    assert_eq!(restored.id(), legacy.id());
    node.import_events(&[restored.into()]).await?;
    assert_eq!(node.storage.dump_entity_events(legacy.entity_id).await?[0].payload, legacy);
    assert_eq!(node.storage.get_state(legacy.entity_id).await?.payload.state.head, proto::Clock::genesis(legacy.id()));
    Ok(())
}
