use super::*;
use crate::{storage::StorageEngine, sync::WriteBehind};
use ankql::ast::Predicate;
use std::time::Duration;

#[tokio::test]
async fn cancelled_commits_leave_views_unavailable_until_storage_is_reconciled() -> anyhow::Result<()> {
    for committed in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
        node.system.create().await?;
        node.wait_ready().await?;
        node.set_tree_write_behind(WriteBehind { max_changes: usize::MAX, max_delay: Duration::from_secs(3600) });
        let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([91; 32]));
        let view = node.trees.view(Predicate::MemberOf(model).into()).await?;
        assert_eq!(view.count(..).await?, 0);

        let event = proto::Event::genesis(
            node.system.root_id(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(model))]),
        );
        let id = event.entity_id;
        let (entered, entering) = oneshot::channel();
        let (_release, released) = oneshot::channel();
        if committed {
            *storage.hold_after_commit.lock().unwrap() = Some((entered, released));
        } else {
            *storage.hold_commit.lock().unwrap() = Some((entered, released));
        }
        let writer = node.clone();
        let write =
            tokio::spawn(
                async move { commit_transaction(&writer, &DEFAULT_CONTEXT, proto::TransactionId::new(), vec![event.into()]).await },
            );
        entering.await?;
        assert!(view.count(..).await.is_err(), "an in-flight commit cannot leave an apparently current old view");
        write.abort();
        assert!(write.await.unwrap_err().is_cancelled());

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(view.count(..).await, Ok(count) if count == u64::from(committed)) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(storage.get_state(id).await.is_ok(), committed);
    }
    Ok(())
}

#[tokio::test]
async fn maintenance_wakes_do_not_keep_the_node_alive() -> anyhow::Result<()> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let publication = node.commit_publication_lock.clone().lock_owned().await;
    node.invalidate_trees();
    let weak = node.weak();
    drop(node);
    drop(publication);
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn a_live_query_retains_its_digest_tree_after_idle_eviction() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let context = node.context(DEFAULT_CONTEXT)?;
    let selection: ankql::ast::Selection<_> = Predicate::MemberOf(proto::ModelId::EntityId(proto::EntityId::from_bytes([92; 32]))).into();
    let query = crate::livequery::EntityLiveQuery::new(
        context.0,
        crate::node::CachePolicy::Local,
        crate::livequery::QueryResolution::Resolved(selection.clone()),
    )?;
    query.wait_initialized().await?;
    let tree = node.trees.open(selection.clone()).await?;
    let weak = tree.downgrade();
    drop(tree);

    // Node retains 64 idle trees. Distinct limits use distinct Selection keys.
    for limit in 0..65 {
        node.trees.open(ankql::ast::Selection { limit: Some(limit), ..selection.clone() }).await?;
    }
    // Ensure no maintenance pass temporarily keeps the evicted tree alive.
    let _publication = node.commit_publication_lock.lock().await;
    assert!(weak.upgrade().is_some(), "the live query retains the digest crate's view");
    drop(query);
    assert!(weak.upgrade().is_none(), "dropping the last query releases an evicted tree");
    Ok(())
}
