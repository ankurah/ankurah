//! Commit outcomes: which commits publish to maintenance, and which fail the
//! readers that exist when the commit's outcome is in doubt.

use super::*;

/// A caller dropped while its applier write stages leaves the write running in
/// its own task, here paused in storage: it has published nothing and holds no
/// commit gate yet, so a fence passes with no batch and a new subscription
/// opens.
#[tokio::test]
async fn dropping_a_caller_while_its_write_stages_leaves_the_source_available() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_set_state.lock().unwrap() = Some((entered, resume));
    {
        let event = genesis(3);
        let applying = apply(&node, &event);
        tokio::pin!(applying);
        assert!(futures::poll!(&mut applying).is_pending());
        wait.await?;
    }
    assert!(cursor.fence().await?.batch.is_none());
    subscribe(&node).await?;
    Ok(())
}

#[tokio::test]
async fn a_commit_stopped_after_storage_committed_fails_existing_readers_and_admits_new_ones() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    apply(&node, &genesis(13)).await?;
    // A commit dropped after storage committed it and before its publication must leave no
    // successful read barrier for a reader that missed it.
    let mut commit = node.reactor.begin_commit().await;
    commit.record_storage_outcome(Ok(crate::storage::StorageCommitOutcome::Committed(Default::default())))?;
    drop(commit);
    assert!(matches!(cursor.next().await, Err(SourceError::CommitInDoubt)));
    assert!(matches!(cursor.fence().await, Err(SourceError::CommitInDoubt)));
    // A reader admitted after the gate is released reads the outcome from storage.
    let (_, mut admitted) = subscribe(&node).await?;
    let later = genesis(14);
    apply(&node, &later).await?;
    assert_eq!(admitted.next().await?.changes.iter().map(|change| change.id).collect::<Vec<_>>(), [later.entity_id]);
    Ok(())
}

#[tokio::test]
async fn definite_storage_rejection_does_not_poison_source() -> anyhow::Result<()> {
    let source: crate::reactor::Reactor = crate::reactor::Reactor::new(ankurah_signals::Mut::new(true));
    let (start, mut cursor) = source.subscribe_maintenance(project).await?;
    let mut commit = source.begin_commit().await;
    assert!(matches!(
        commit.record_storage_outcome(Ok(crate::storage::StorageCommitOutcome::Conflict { observed: Default::default() }))?,
        crate::storage::StorageCommitOutcome::Conflict { .. }
    ));
    drop(commit);
    assert_eq!(cursor.fence().await?.revision, start);
    source.subscribe_maintenance(project).await?;
    Ok(())
}

#[tokio::test]
async fn a_commit_batch_and_its_fence_cannot_be_half_published() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    let events: Vec<_> = [11, 12]
        .into_iter()
        .map(|seed| {
            Event::genesis(
                node.system.root_id(),
                AuthorId::Unknown,
                OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([seed; 32]))))]),
            )
        })
        .collect();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_after_commit.lock().unwrap() = Some((entered, resume));
    let transaction = crate::node::handler::commit_transaction(
        &node,
        &crate::policy::DEFAULT_CONTEXT,
        proto::TransactionId::new(),
        events.iter().cloned().map(Attested::from).collect(),
    );
    tokio::pin!(transaction);
    // Its gated commit runs in a task of its own, which the transaction releases once staged.
    tokio::select! {
        _ = &mut transaction => anyhow::bail!("the transaction finished while storage held its commit"),
        entered = wait => entered?,
    }
    for event in &events {
        assert_eq!(node.storage.get_state(event.entity_id).await?.payload.state.head, proto::Clock::singleton(event));
    }
    let fence = cursor.fence();
    tokio::pin!(fence);
    assert!(futures::poll!(&mut fence).is_pending(), "a source fence must wait for the resident entities' advance and index projection");
    release.send(()).unwrap();
    let (transaction, window) = tokio::join!(transaction, fence);
    transaction?;
    let window = window?;
    assert!(window.batch.is_some());
    assert_eq!(window.batch.as_ref().unwrap().changes.len(), 2);
    for (change, event) in window.batch.as_ref().unwrap().changes.iter().zip(&events) {
        assert_eq!(change.head, proto::Clock::singleton(event));
    }
    Ok(())
}

#[tokio::test]
async fn an_unclassified_storage_error_fails_existing_readers_and_admits_new_ones() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    node.storage.fail_next_commit();
    assert!(apply(&node, &genesis(6)).await.is_err());
    // This fixture rejected before commit, but the generic storage error does
    // not prove that outcome to its caller. It must not authorize a clean fence.
    assert!(matches!(cursor.fence().await, Err(SourceError::CommitInDoubt)));
    let (_, mut admitted) = subscribe(&node).await?;
    let later = genesis(7);
    apply(&node, &later).await?;
    assert_eq!(admitted.next().await?.changes.iter().map(|change| change.id).collect::<Vec<_>>(), [later.entity_id]);
    Ok(())
}

#[tokio::test]
async fn a_local_transaction_publishes_its_entities_to_maintenance() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    let transaction = node.privileged_context().begin();
    let row = crate::schema::catalog::SysModelRow { label: "fixture".into(), name: "Fixture".into() };
    let id = transaction.create(&row).await?.id();
    transaction.commit().await?;
    let head = node.storage.get_state(id).await?.payload.state.head;
    let batch = cursor.next().await?;
    assert!(batch.catalog_changed);
    assert_eq!(batch.changes.iter().find(|change| change.id == id).map(|change| change.head.clone()), Some(head));
    Ok(())
}

#[tokio::test]
async fn closing_the_reactor_fails_every_maintenance_reader() -> anyhow::Result<()> {
    let source: crate::reactor::Reactor = crate::reactor::Reactor::new(ankurah_signals::Mut::new(true));
    let (_, mut cursor) = source.subscribe_maintenance(project).await?;
    // The node stopped: a reader that kept its window open would wait for
    // commits that never come.
    source.close().await;
    assert!(matches!(cursor.next().await, Err(SourceError::Closed)));
    assert!(matches!(source.subscribe_maintenance(project).await, Err(SourceError::Closed)));
    Ok(())
}
