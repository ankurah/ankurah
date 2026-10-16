use super::*;
use crate::{
    node::{
        applier::{Delivery, NodeApplier},
        Node,
    },
    policy::PermissiveAgent,
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    storage::StorageEngine,
    test_utils::TestStorage,
    value::Value,
};
use ankurah_proto::{self as proto, AuthorId, EntityId, Event, Membership, ModelId, Operation, OperationSet};

fn genesis(seed: u8) -> Event {
    Event::genesis(
        Some(EntityId::from_bytes([seed; 32])),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([7; 32]))))]),
    )
}

async fn node() -> anyhow::Result<Node<TestStorage, PermissiveAgent>> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    Ok(node)
}

async fn apply(node: &Node<TestStorage, PermissiveAgent>, event: &Event, delivery: Delivery) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), false);
    events.stage_event(event.clone());
    NodeApplier::apply_delta(
        node,
        &EntityId::from_bytes([8; 32]),
        proto::EntityDelta {
            entity_id: event.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
        },
        delivery,
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn captured_window_precedes_reactor_notification_and_keeps_old_rows() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut first) = node.source_publications.subscribe().await?;
    let (_, mut second) = node.source_publications.subscribe().await?;
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([31; 32]));
    let set = |value: &str| -> anyhow::Result<Operation> {
        let backend = LWWBackend::new();
        backend.set(property, Some(Value::String(value.into())));
        Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
    };
    let a = Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([7; 32])))), set("before")?]),
    );
    // apply_delta returns the reactor notification to its caller. No reactor
    // dispatch has run, but the committed source publication must already exist.
    apply(&node, &a, Delivery::Live).await?;
    let captured = first.fence().await?;
    assert!(captured.revision > start);
    assert_eq!(captured.batches.len(), 1);
    let row = captured.batches[0].changes[0].state.clone();
    assert_eq!(row.state.head, proto::Clock::singleton(&a));
    assert_eq!(captured.batches[0].changes[0].events[0].payload, a);

    let b = Event::update(a.entity_id, proto::Clock::singleton(&a), AuthorId::Unknown, OperationSet(vec![set("after")?]));
    apply(&node, &b, Delivery::Initial).await?;
    assert_eq!(row.state.head, proto::Clock::singleton(&a), "a delayed snapshot must not read the live resident");
    assert_eq!(crate::entity::TemporaryEntity::new(row.entity_id, &row.state)?.value(&property), Some(Value::String("before".into())));
    let subsequent = first.next().await?;
    let current = &subsequent.changes[0].state;
    assert_eq!(
        crate::entity::TemporaryEntity::new(current.entity_id, &current.state)?.value(&property),
        Some(Value::String("after".into()))
    );
    assert!(subsequent.revision > captured.revision);
    assert_eq!(subsequent.changes[0].state.state.head, proto::Clock::singleton(&b));
    assert!(subsequent.changes[0].events.is_empty(), "initial application must not become a live event update");
    let second_window = second.fence().await?;
    assert_eq!(second_window.batches.len(), 2, "one reader cannot consume another reader's bootstrap window");
    Ok(())
}

#[tokio::test]
async fn changed_refill_cannot_be_admitted_and_its_window_is_not_consumed() -> anyhow::Result<()> {
    let node = node().await?;
    let (read_start, mut cursor) = node.source_publications.subscribe().await?;
    // A LIMIT refill started before this commit may have missed an unchanged
    // replacement. Its complete query must be read again, rather than accepting
    // a stale continuation just because the returned entity still matches.
    apply(&node, &genesis(2), Delivery::Live).await?;
    assert!(matches!(cursor.validate(read_start).await, Err(SourceError::Changed)));
    let window = cursor.fence().await?;
    assert_eq!(window.batches.len(), 1);
    let validated = cursor.validate(window.revision).await?;
    assert_eq!(validated.revision, window.revision);
    assert!(validated.batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn cancellation_before_commit_does_not_invalidate_source() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = node.source_publications.subscribe().await?;
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_set_state.lock().unwrap() = Some((entered, resume));
    {
        let event = genesis(3);
        let applying = apply(&node, &event, Delivery::Live);
        tokio::pin!(applying);
        assert!(futures::poll!(&mut applying).is_pending());
        wait.await?;
    }
    assert!(cursor.fence().await?.batches.is_empty());
    node.source_publications.subscribe().await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_after_storage_commit_invalidates_existing_and_new_captures() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = node.source_publications.subscribe().await?;
    let event = genesis(4);
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (_release, resume) = tokio::sync::oneshot::channel();
    *node.storage.hold_after_commit.lock().unwrap() = Some((entered, resume));
    {
        let applying = apply(&node, &event, Delivery::Live);
        tokio::pin!(applying);
        assert!(futures::poll!(&mut applying).is_pending());
        wait.await?;
        assert_eq!(node.storage.get_state(event.entity_id).await?.payload.state.head, proto::Clock::singleton(&event));
    }
    assert!(matches!(cursor.next().await, Err(SourceError::Unavailable)));
    assert!(matches!(cursor.fence().await, Err(SourceError::Unavailable)));
    assert!(matches!(node.source_publications.subscribe().await, Err(SourceError::Unavailable)));
    Ok(())
}

#[tokio::test]
async fn definite_storage_rejection_does_not_poison_source() -> anyhow::Result<()> {
    let source = SourcePublications::default();
    let (start, mut cursor) = source.subscribe().await?;
    let mut commit = source.begin_commit().await;
    assert!(matches!(
        commit.storage_result(Ok(crate::storage::StorageCommitOutcome::Conflict { observed: Default::default() }))?,
        crate::storage::StorageCommitOutcome::Conflict { .. }
    ));
    drop(commit);
    assert_eq!(cursor.validate(start).await?.revision, start);
    source.subscribe().await?;
    Ok(())
}

#[tokio::test]
async fn lagged_reader_fails_without_invalidating_an_independent_reader() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut slow) = node.source_publications.subscribe().await?;
    let (_, mut current) = node.source_publications.subscribe().await?;
    let mut event = genesis(5);
    for _ in 0..=MAX_PENDING_BATCHES {
        apply(&node, &event, Delivery::Initial).await?;
        let batch = current.next().await?;
        assert_eq!(batch.changes[0].state.state.head, proto::Clock::singleton(&event));
        event = Event::update(event.entity_id, proto::Clock::singleton(&event), AuthorId::Unknown, OperationSet(vec![]));
    }
    assert!(matches!(slow.fence().await, Err(SourceError::Lagged)));
    assert!(current.fence().await?.batches.is_empty());
    node.source_publications.subscribe().await?;
    Ok(())
}

#[tokio::test]
async fn a_commit_batch_and_its_fence_cannot_be_half_published() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let (_, mut cursor) = node.source_publications.subscribe().await?;
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
    assert!(futures::poll!(&mut transaction).is_pending());
    wait.await?;
    for event in &events {
        assert_eq!(node.storage.get_state(event.entity_id).await?.payload.state.head, proto::Clock::singleton(event));
    }
    let fence = cursor.fence();
    tokio::pin!(fence);
    assert!(futures::poll!(&mut fence).is_pending(), "a source fence must wait for resident publication and immutable admission");
    release.send(()).unwrap();
    let (transaction, window) = tokio::join!(transaction, fence);
    transaction?;
    let window = window?;
    assert_eq!(window.batches.len(), 1);
    assert_eq!(window.batches[0].changes.len(), 2);
    for (change, event) in window.batches[0].changes.iter().zip(&events) {
        assert_eq!(change.state.state.head, proto::Clock::singleton(event));
        assert_eq!(change.events[0].payload, *event);
    }
    Ok(())
}

#[tokio::test]
async fn an_unclassified_storage_error_requires_source_reconstruction() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = node.source_publications.subscribe().await?;
    node.storage.fail_next_commit();
    assert!(apply(&node, &genesis(6), Delivery::Live).await.is_err());
    // This fixture rejected before commit, but the generic storage error does
    // not prove that outcome to its caller. It must not authorize a clean fence.
    assert!(matches!(cursor.fence().await, Err(SourceError::Unavailable)));
    assert!(matches!(node.source_publications.subscribe().await, Err(SourceError::Unavailable)));
    Ok(())
}
