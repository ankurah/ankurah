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
use ankurah_proto::{self as proto, Attested, AuthorId, EntityId, Event, Membership, ModelId, Operation, OperationSet};

#[derive(Clone, Debug)]
struct Head {
    id: EntityId,
    head: proto::Clock,
    key: Option<String>,
    events: Vec<proto::EventId>,
}

fn project(change: &EntityChange, live_events: bool) -> anyhow::Result<ProjectedChange<Head>> {
    change.entity().with_read(|read| {
        let key = match read.value(&proto::PropertyId::EntityId(EntityId::from_bytes([31; 32]))) {
            Some(Value::String(value)) => Some(value),
            _ => None,
        };
        let head = read.head().clone();
        let events = if live_events { change.events().iter().map(|event| event.payload.id()).collect() } else { Vec::new() };
        let bytes = bincode::serialized_size(&(&head, &key, &events))? as usize;
        Ok(ProjectedChange { value: Head { id: change.entity().id(), head, key, events }, member: true, bytes })
    })?
}

async fn subscribe(node: &Node<TestStorage, PermissiveAgent>) -> Result<(SourceRevision, MaintenanceSubscription<Head>), SourceError> {
    node.reactor.subscribe_maintenance(project).await
}

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
async fn maintenance_projects_committed_metadata_before_public_notifications_without_retaining_entities() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut first) = subscribe(&node).await?;
    let (_, mut second) = subscribe(&node).await?;
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
    // dispatch has run, but maintenance metadata must already be queued.
    apply(&node, &a, Delivery::Live).await?;
    let captured = first.fence().await?;
    assert!(captured.revision > start);
    assert_eq!(captured.batches.len(), 1);
    let row = captured.batches[0].changes[0].clone();
    assert!(node.entities.get(&a.entity_id).is_none(), "queued metadata must not pin the resident entity");
    assert_eq!(row.head, proto::Clock::singleton(&a));
    assert_eq!(captured.batches[0].changes[0].events[0], a.id());

    let b = Event::update(a.entity_id, proto::Clock::singleton(&a), AuthorId::Unknown, OperationSet(vec![set("after")?]));
    apply(&node, &b, Delivery::Initial).await?;
    assert_eq!(row.head, proto::Clock::singleton(&a), "an earlier update describes the earlier committed head");
    assert_eq!(row.key.clone().map(Value::String), Some(Value::String("before".into())));
    let subsequent = first.next().await?;
    let current = &subsequent.changes[0];
    assert_eq!(current.key.clone().map(Value::String), Some(Value::String("after".into())));
    assert!(subsequent.revision > captured.revision);
    assert_eq!(subsequent.changes[0].head, proto::Clock::singleton(&b));
    assert!(subsequent.changes[0].events.is_empty(), "initial application must not become a live event update");
    let second_window = second.fence().await?;
    assert_eq!(second_window.batches.len(), 2, "one reader cannot consume another reader's bootstrap window");
    Ok(())
}

#[tokio::test]
async fn a_fence_drains_its_finite_window_and_leaves_later_commits_for_the_next_window() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    apply(&node, &genesis(2), Delivery::Live).await?;
    let first = cursor.fence().await?;
    assert!(first.revision > start);
    assert_eq!(first.batches.len(), 1);
    let empty = cursor.fence().await?;
    assert_eq!(empty.revision, first.revision);
    assert!(empty.batches.is_empty());
    let later = genesis(3);
    apply(&node, &later, Delivery::Live).await?;
    let next = cursor.fence().await?;
    assert!(next.revision > first.revision);
    assert_eq!(next.batches.len(), 1);
    assert_eq!(next.batches[0].changes[0].id, later.entity_id);
    Ok(())
}

#[tokio::test]
async fn cancellation_before_commit_does_not_invalidate_source() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
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
    subscribe(&node).await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_after_storage_commit_invalidates_existing_and_new_subscriptions() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
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
    assert!(matches!(subscribe(&node).await, Err(SourceError::Unavailable)));
    Ok(())
}

#[tokio::test]
async fn definite_storage_rejection_does_not_poison_source() -> anyhow::Result<()> {
    let source: crate::reactor::Reactor = crate::reactor::Reactor::new(ankurah_signals::Mut::new(true));
    let (start, mut cursor) = source.subscribe_maintenance(project).await?;
    let mut commit = source.begin_commit().await;
    assert!(matches!(
        commit.storage_result(Ok(crate::storage::StorageCommitOutcome::Conflict { observed: Default::default() }))?,
        crate::storage::StorageCommitOutcome::Conflict { .. }
    ));
    drop(commit);
    assert_eq!(cursor.fence().await?.revision, start);
    source.subscribe_maintenance(project).await?;
    Ok(())
}

#[tokio::test]
async fn lagged_reader_fails_without_invalidating_an_independent_reader() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut slow) = subscribe(&node).await?;
    let (_, mut current) = subscribe(&node).await?;
    let mut event = genesis(5);
    for _ in 0..=MAX_PENDING_BATCHES {
        apply(&node, &event, Delivery::Initial).await?;
        let batch = current.next().await?;
        assert_eq!(batch.changes[0].head, proto::Clock::singleton(&event));
        event = Event::update(event.entity_id, proto::Clock::singleton(&event), AuthorId::Unknown, OperationSet(vec![]));
    }
    assert!(matches!(slow.fence().await, Err(SourceError::Lagged)));
    assert!(current.fence().await?.batches.is_empty());
    subscribe(&node).await?;
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
    assert!(futures::poll!(&mut transaction).is_pending());
    wait.await?;
    for event in &events {
        assert_eq!(node.storage.get_state(event.entity_id).await?.payload.state.head, proto::Clock::singleton(event));
    }
    let fence = cursor.fence();
    tokio::pin!(fence);
    assert!(futures::poll!(&mut fence).is_pending(), "a source fence must wait for resident publication and index projection");
    release.send(()).unwrap();
    let (transaction, window) = tokio::join!(transaction, fence);
    transaction?;
    let window = window?;
    assert_eq!(window.batches.len(), 1);
    assert_eq!(window.batches[0].changes.len(), 2);
    for (change, event) in window.batches[0].changes.iter().zip(&events) {
        assert_eq!(change.head, proto::Clock::singleton(event));
        assert_eq!(change.events[0], event.id());
    }
    Ok(())
}

#[tokio::test]
async fn an_unclassified_storage_error_requires_source_reconstruction() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    node.storage.fail_next_commit();
    assert!(apply(&node, &genesis(6), Delivery::Live).await.is_err());
    // This fixture rejected before commit, but the generic storage error does
    // not prove that outcome to its caller. It must not authorize a clean fence.
    assert!(matches!(cursor.fence().await, Err(SourceError::Unavailable)));
    assert!(matches!(subscribe(&node).await, Err(SourceError::Unavailable)));
    Ok(())
}

#[tokio::test]
async fn limited_read_validation_preserves_crossing_publications_for_retry() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    apply(&node, &genesis(17), Delivery::Live).await?;
    assert!(matches!(cursor.validate_limited_read(start).await, Err(SourceError::Changed)));
    let window = cursor.fence().await?;
    assert_eq!(window.batches.len(), 1);
    let unchanged = cursor.validate_limited_read(window.revision).await?;
    assert_eq!(unchanged.revision, window.revision);
    assert!(unchanged.batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn finishing_bootstrap_cannot_drop_a_commit_crossing_the_scan() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    let event = genesis(41);
    apply(&node, &event, Delivery::Initial).await?;
    assert!(matches!(cursor.finish_bootstrap(Predicate::False, [], start).await, Err(SourceError::Changed)));
    let window = cursor.fence().await?;
    assert_eq!(window.batches[0].changes[0].head, proto::Clock::singleton(&event));
    cursor.finish_bootstrap(Predicate::False, [event.entity_id], window.revision).await?;

    // Even a predicate with no entry candidates must retain the explicitly
    // installed membership watch until the projector observes a departure.
    let later = Event::update(event.entity_id, proto::Clock::singleton(&event), AuthorId::Unknown, OperationSet::default());
    apply(&node, &later, Delivery::Initial).await?;
    assert_eq!(cursor.next().await?.changes[0].head, proto::Clock::singleton(&later));
    Ok(())
}

#[tokio::test]
async fn maintenance_routes_model_candidates_and_removes_departed_entity_watches() -> anyhow::Result<()> {
    let node = node().await?;
    let model = ModelId::EntityId(EntityId::from_bytes([7; 32]));
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([31; 32]));
    let predicate = Predicate::And(
        Box::new(Predicate::MemberOf(model)),
        Box::new(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(ankql::ast::PropertyPath::from(property))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("inside".into()))),
        }),
    );
    let project_predicate = predicate.clone();
    let (revision, mut cursor) = node
        .reactor
        .subscribe_maintenance(move |change, live| {
            let mut projected = project(change, live)?;
            projected.member =
                change.entity().with_read(|read| crate::selection::filter::evaluate_predicate(read, &project_predicate))??;
            Ok(projected)
        })
        .await?;
    cursor.finish_bootstrap(predicate, [], revision).await?;

    let unrelated = Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([8; 32]))))]),
    );
    apply(&node, &unrelated, Delivery::Initial).await?;
    assert!(cursor.fence().await?.batches.is_empty(), "other models are not projected");

    let set = |value: &str| -> anyhow::Result<Operation> {
        let backend = LWWBackend::new();
        backend.set(property, Some(Value::String(value.into())));
        Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
    };
    let entered =
        Event::genesis(None, AuthorId::Unknown, OperationSet(vec![Operation::Membership(Membership::Add(model)), set("inside")?]));
    apply(&node, &entered, Delivery::Initial).await?;
    assert_eq!(cursor.next().await?.changes[0].key.as_deref(), Some("inside"));
    assert!(cursor.maintenance.0.watchers.lock().unwrap().debug_data().2.contains_key(&entered.entity_id));
    let departed =
        Event::update(entered.entity_id, proto::Clock::singleton(&entered), AuthorId::Unknown, OperationSet(vec![set("outside")?]));
    apply(&node, &departed, Delivery::Initial).await?;
    assert_eq!(cursor.next().await?.changes[0].key.as_deref(), Some("outside"));
    assert!(!cursor.maintenance.0.watchers.lock().unwrap().debug_data().2.contains_key(&entered.entity_id));
    Ok(())
}

#[tokio::test]
async fn dropping_maintenance_unregisters_its_watchers_and_projector() -> anyhow::Result<()> {
    let node = node().await?;
    let owned = Arc::new(());
    let weak = Arc::downgrade(&owned);
    let (revision, mut cursor) = node
        .reactor
        .subscribe_maintenance(move |change, live| {
            let _ = &owned;
            project(change, live)
        })
        .await?;
    let maintenance = cursor.maintenance.clone();
    let id = cursor.reader.id;
    cursor.finish_bootstrap(Predicate::True, [EntityId::from_bytes([32; 32])], revision).await?;
    drop(cursor);
    assert!(weak.upgrade().is_none(), "the subscription owns its projection closure");
    assert!(!maintenance.0.state.lock().unwrap().readers.contains_key(&id));
    let watchers = maintenance.0.watchers.lock().unwrap();
    let (_, wildcard, entities) = watchers.debug_data();
    assert!(!wildcard.iter().any(|(subscription, _)| *subscription == id));
    assert!(entities.is_empty());
    Ok(())
}

#[tokio::test]
async fn catalog_changes_reach_maintenance_without_any_predicate_candidate() -> anyhow::Result<()> {
    let node = node().await?;
    let (revision, mut cursor) = subscribe(&node).await?;
    cursor.finish_bootstrap(Predicate::False, [], revision).await?;
    let event = Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(crate::schema::CATALOG_MODELS[0]))]),
    );
    let state = proto::State {
        memberships: [crate::schema::CATALOG_MODELS[0]].into_iter().collect(),
        head: proto::Clock::singleton(&event),
        ..Default::default()
    };
    let (_, entity) = node
        .entities
        .with_state(
            &LocalStateGetter::new(node.storage.clone()),
            &LocalEventGetter::new(node.storage.clone(), false),
            event.entity_id,
            state,
        )
        .await?;
    node.reactor.begin_commit().await.publish(&[EntityChange::new(entity, Vec::new())], false);
    let window = cursor.fence().await?;
    assert_eq!(window.batches.len(), 1);
    assert!(window.batches[0].catalog_changed);
    assert!(window.batches[0].changes.is_empty());
    Ok(())
}

#[tokio::test]
async fn projection_failure_invalidates_only_the_affected_subscription() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut failed) =
        node.reactor.subscribe_maintenance(|_, _| Err::<ProjectedChange<Head>, _>(anyhow::anyhow!("fixture projection refused"))).await?;
    let (_, mut healthy) = subscribe(&node).await?;
    let event = genesis(51);
    apply(&node, &event, Delivery::Initial).await?;
    assert!(matches!(failed.next().await, Err(SourceError::Changed)));
    assert_eq!(healthy.next().await?.changes[0].head, proto::Clock::singleton(&event));
    assert!(subscribe(&node).await.is_ok());
    Ok(())
}
