//! Nodes that share one keypair register under one node id. A node they connect to keeps their connections apart.

mod common;
use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT, EntityId, Node, NodeKey, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

async fn create_album(node: &TestNode, name: &str) -> Result<EntityId> {
    let trx = node.context(DEFAULT_CONTEXT)?.begin();
    let id = trx.create(&Album { name: name.into(), year: "2024".into() }).await?.id();
    trx.commit().await?;
    Ok(id)
}

/// Nodes that share a keypair need separate subscriptions.
#[tokio::test]
async fn two_nodes_with_one_keypair_each_keep_their_own_subscription_to_a_durable_node() -> Result<()> {
    let durable = durable_sled_setup().await?;
    let key = NodeKey::generate();
    let [first, second] =
        [(); 2].map(|()| Node::new_with_key(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new(), key.clone()));
    assert_eq!(first.id().unwrap(), second.id().unwrap());

    let first_connection = LocalProcessConnection::new(&first, &durable).await?;
    let _second_connection = LocalProcessConnection::new(&second, &durable).await?;
    first.wait_ready().await?;
    second.wait_ready().await?;

    let (first_watcher, second_watcher) = (TestWatcher::changeset(), TestWatcher::changeset());
    let first_query = first.context(DEFAULT_CONTEXT)?.query_wait::<AlbumView>("year = '2024'").await?;
    let second_query = second.context(DEFAULT_CONTEXT)?.query_wait::<AlbumView>("year = '2024'").await?;
    let _first_handle = first_query.subscribe(&first_watcher);
    let _second_handle = second_query.subscribe(&second_watcher);

    let both = create_album(&durable, "Both").await?;
    assert_eq!(first_watcher.take_one().await, vec![(both, ChangeKind::Add)]);
    assert_eq!(second_watcher.take_one().await, vec![(both, ChangeKind::Add)]);

    // Closing one of the two connections leaves the other node subscribed.
    drop(first_connection);
    let second_only = create_album(&durable, "Second only").await?;
    assert_eq!(second_watcher.take_one().await, vec![(second_only, ChangeKind::Add)]);
    Ok(())
}
