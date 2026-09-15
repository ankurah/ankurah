mod common;
use ankurah::Context;
use ankurah::core::node::MatchArgs;
use ankurah::signals::Peek;
use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

#[derive(ankurah::Model, serde::Serialize, serde::Deserialize)]
pub struct Pet {
    pub name: String,
}

#[tokio::test]
async fn pending_resolution_releases_node_and_storage() -> Result<()> {
    for weak in [true, false] {
        let engine = Arc::new(SledStorageEngine::new_test()?);
        let node = Node::new(engine.clone(), PermissiveAgent::new());
        let weak_node = node.weak();
        let args: MatchArgs<ankql::ast::Parsed> = "true".try_into()?;
        let query = if weak {
            Context::new_weak(&node, DEFAULT_CONTEXT).query::<PetView>(args)?
        } else {
            Context::new(node.clone(), DEFAULT_CONTEXT).query::<PetView>(args)?
        };
        tokio::task::yield_now().await;
        assert!(query.selection().peek().is_none(), "resolution waits for the node to adopt a system");

        drop(node);
        if weak {
            tokio::time::timeout(Duration::from_secs(1), async {
                while weak_node.upgrade().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("pending resolution must not keep a weak query's node alive");
        } else {
            assert!(weak_node.upgrade().is_some(), "a strong query still owns its node");
        }
        drop(query);
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_node.upgrade().is_some() || Arc::strong_count(&engine) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping a pending query must release its wait and storage");
    }
    Ok(())
}

#[tokio::test]
async fn test_weak_node_livequery_does_not_keep_node_alive() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(engine, PermissiveAgent::new());
    node.system.create().await?;
    node.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Pet>().await?;

    // Create a LiveQuery with a weak node binding
    let args: MatchArgs<ankql::ast::Parsed> = "true".try_into()?;
    let weak_lq = Context::new_weak(&node, DEFAULT_CONTEXT).query::<PetView>(args)?;

    // Get the query_id before dropping the node
    let query_id = weak_lq.query_id();

    // Drop the node — the weak-node LiveQuery should NOT keep it alive
    drop(node);

    // The LiveQuery itself should still be valid (Arc<Inner> is alive),
    // but its internal node ref should be dead.
    let _ = query_id;
    let _ = weak_lq;

    // Also verify explicitly with a weak node ref
    let engine2 = Arc::new(SledStorageEngine::new_test()?);
    let node2 = Node::new_durable(engine2, PermissiveAgent::new());
    node2.system.create().await?;
    node2.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Pet>().await?;
    let weak_node = node2.weak();

    let args2: MatchArgs<ankql::ast::Parsed> = "true".try_into()?;
    let weak_lq2 = Context::new_weak(&node2, DEFAULT_CONTEXT).query::<PetView>(args2)?;

    // Node should be alive while we hold it
    assert!(weak_node.upgrade().is_some(), "Node should be alive");

    // Drop the node — the weak-node LiveQuery should NOT keep it alive
    drop(node2);
    assert!(weak_node.upgrade().is_none(), "weak-node LiveQuery should NOT keep node alive");

    drop(weak_lq2);

    Ok(())
}

#[tokio::test]
async fn test_entity_livequery_keeps_node_alive() -> Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(engine, PermissiveAgent::new());
    node.system.create().await?;
    node.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Pet>().await?;

    // Create a regular EntityLiveQuery
    let args: MatchArgs<ankql::ast::Parsed> = "true".try_into()?;
    let lq = Context::new(node.clone(), DEFAULT_CONTEXT).query::<PetView>(args)?;

    // Get weak ref to test node liveness
    let weak_node = node.weak();

    // Drop the original node handle
    drop(node);

    // The EntityLiveQuery should keep the node alive via its strong node ref
    assert!(weak_node.upgrade().is_some(), "EntityLiveQuery should keep node alive");

    // Drop the LiveQuery
    drop(lq);

    // Allow the spawned initialization task to complete and release its Arc<Inner>
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Now the node should be dead
    assert!(weak_node.upgrade().is_none(), "Node should be dropped after LiveQuery is dropped");

    Ok(())
}
