//! Maintenance windows: what each reader's queue holds between fences, and
//! how it coalesces, lags and fails. Commit outcomes and watch routing sit in
//! the child modules.

use super::*;
use crate::{
    node::{applier::NodeApplier, Node},
    policy::PermissiveAgent,
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    selection::filter::Filterable,
    storage::StorageEngine,
    test_utils::TestStorage,
    value::Value,
};
use ankurah_proto::{self as proto, Attested, AuthorId, EntityId, Event, Membership, ModelId, Operation, OperationSet};

mod commits;
mod watches;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Head {
    id: EntityId,
    head: proto::Clock,
    key: Option<String>,
}

fn project(entities: &[&Entity]) -> anyhow::Result<Vec<ProjectedChange<Head>>> {
    entities.iter().map(|entity| project_entity(entity)).collect()
}

fn project_entity(entity: &Entity) -> anyhow::Result<ProjectedChange<Head>> {
    entity.with_read(|read| {
        let key = match read.value(&proto::PropertyId::EntityId(EntityId::from_bytes([31; 32]))) {
            Some(Value::String(value)) => Some(value),
            _ => None,
        };
        let head = read.head().clone();
        let bytes = bincode::serialized_size(&(&head, &key))? as usize;
        Ok(ProjectedChange { value: Head { id: entity.id(), head, key }, member: true, bytes })
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

/// Apply `event` as a peer's push, committing it through the applier.
async fn apply(node: &Node<TestStorage, PermissiveAgent>, event: &Event) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), false);
    events.stage_event(event.clone());
    NodeApplier::apply_update(
        node,
        &EntityId::from_bytes([8; 32]),
        proto::SubscriptionUpdateItem {
            entity_id: event.entity_id,
            content: proto::UpdateContent::EventOnly(vec![Attested::from(event.clone()).into()]),
            predicate_relevance: vec![],
        },
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}

/// A resident entity at `event`'s head, holding only `model`'s membership.
async fn entity_at(node: &Node<TestStorage, PermissiveAgent>, event: &Event, model: ModelId) -> anyhow::Result<Entity> {
    let (_, entity) = node
        .entities
        .with_state(
            &LocalStateGetter::new(node.storage.clone()),
            &LocalEventGetter::new(node.storage.clone(), false),
            event.entity_id,
            proto::State { memberships: [model].into_iter().collect(), head: proto::Clock::singleton(event), ..Default::default() },
        )
        .await?;
    Ok(entity)
}

#[tokio::test]
async fn maintenance_queues_committed_metadata_for_each_reader_without_retaining_entities() -> anyhow::Result<()> {
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
    apply(&node, &a).await?;
    let first_window = first.fence().await?;
    assert!(first_window.revision > start);
    assert!(first_window.batch.is_some());
    let row = first_window.batch.as_ref().unwrap().changes[0].clone();
    assert!(node.entities.get(&a.entity_id).is_none(), "queued metadata must not pin the resident entity");
    assert_eq!(row.head, proto::Clock::singleton(&a));

    let b = Event::update(a.entity_id, proto::Clock::singleton(&a), AuthorId::Unknown, OperationSet(vec![set("after")?]));
    apply(&node, &b).await?;
    assert_eq!(row.head, proto::Clock::singleton(&a), "an earlier update describes the earlier committed head");
    assert_eq!(row.key.clone().map(Value::String), Some(Value::String("before".into())));
    let subsequent = first.next().await?;
    let current = &subsequent.changes[0];
    assert_eq!(current.key.clone().map(Value::String), Some(Value::String("after".into())));
    assert!(subsequent.revision > first_window.revision);
    assert_eq!(subsequent.changes[0].head, proto::Clock::singleton(&b));
    let second_window = second.fence().await?;
    assert!(second_window.batch.is_some());
    assert_eq!(second_window.batch.as_ref().unwrap().changes.len(), 2, "one reader cannot consume another reader's bootstrap window");
    Ok(())
}

#[tokio::test]
async fn a_fence_drains_its_finite_window_and_leaves_later_commits_for_the_next_window() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    apply(&node, &genesis(2)).await?;
    let first = cursor.fence().await?;
    assert!(first.revision > start);
    assert!(first.batch.is_some());
    let empty = cursor.fence().await?;
    assert_eq!(empty.revision, first.revision);
    assert!(empty.batch.is_none());
    let later = genesis(3);
    apply(&node, &later).await?;
    let next = cursor.fence().await?;
    assert!(next.revision > first.revision);
    assert!(next.batch.is_some());
    assert_eq!(next.batch.as_ref().unwrap().changes[0].id, later.entity_id);
    Ok(())
}

#[tokio::test]
async fn lagged_reader_fails_without_invalidating_an_independent_reader() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut slow) = node
        .reactor
        .subscribe_maintenance(|entities| {
            let mut projected = project(entities)?;
            for projected in &mut projected {
                projected.bytes = MAX_PENDING_BYTES / 2;
            }
            Ok(projected)
        })
        .await?;
    let (_, mut current) = subscribe(&node).await?;
    let mut event = genesis(5);
    for count in [2, 3] {
        for _ in 0..count {
            apply(&node, &event).await?;
            let batch = current.next().await?;
            assert_eq!(batch.changes[0].head, proto::Clock::singleton(&event));
            event = Event::update(event.entity_id, proto::Clock::singleton(&event), AuthorId::Unknown, OperationSet(vec![]));
        }
        if count == 2 {
            let window = slow.fence().await?;
            assert_eq!(window.batch.iter().map(|batch| batch.bytes).sum::<usize>(), MAX_PENDING_BYTES);
            assert_eq!(window.batch.iter().map(|batch| batch.changes.len()).sum::<usize>(), 2);
        }
    }
    assert!(matches!(slow.fence().await, Err(SourceError::Lagged)));
    assert!(matches!(slow.next().await, Err(SourceError::Lagged)));
    assert!(current.fence().await?.batch.is_none());
    subscribe(&node).await?;
    Ok(())
}

#[tokio::test]
async fn one_commit_and_4096_separate_commits_preserve_the_same_bounded_metadata_window() -> anyhow::Result<()> {
    let node = node().await?;
    let one = crate::reactor::Reactor::new(ankurah_signals::Mut::new(true));
    let many = crate::reactor::Reactor::new(ankurah_signals::Mut::new(true));
    let (one_start, mut one_cursor) = one.subscribe_maintenance(project).await?;
    let (many_start, mut many_cursor) = many.subscribe_maintenance(project).await?;
    let model = ModelId::EntityId(EntityId::from_bytes([7; 32]));
    let mut entities = Vec::new();
    for _ in 0..4096 {
        let event = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![Operation::Membership(Membership::Add(model))]));
        entities.push(entity_at(&node, &event, model).await?);
    }
    one.begin_commit().await.publish(&entities);
    for entity in &entities {
        many.begin_commit().await.publish([entity]);
    }
    let one_window = one_cursor.fence().await?;
    let many_window = many_cursor.fence().await?;
    assert_eq!(one_window.revision.0, one_start.0 + 1);
    assert_eq!(many_window.revision.0, many_start.0 + 4096);
    assert!(one_window.batch.is_some());
    assert!(many_window.batch.is_some());
    let one_batch = one_window.batch.as_ref().unwrap();
    let many_batch = many_window.batch.as_ref().unwrap();
    assert_eq!(many_batch.revision, many_window.revision, "the coalesced window ends at its newest publication");
    assert_eq!(many_batch.changes.len(), 4096);
    assert_eq!(one_batch.changes, many_batch.changes, "entity heads retain publication order");
    assert_eq!(one_batch.bytes, many_batch.bytes);
    assert!(many_batch.bytes < MAX_PENDING_BYTES);
    assert_eq!(many_batch.changes.iter().map(|head| head.id).collect::<Vec<_>>(), entities.iter().map(Entity::id).collect::<Vec<_>>());
    drop(entities);
    assert!(many_batch.changes.iter().all(|head| node.entities.get(&head.id).is_none()), "the pending window must not pin entities");
    Ok(())
}

#[tokio::test]
async fn next_detaches_a_window_before_later_publications() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    let first = genesis(61);
    let second = Event::update(first.entity_id, proto::Clock::singleton(&first), AuthorId::Unknown, OperationSet::default());
    apply(&node, &first).await?;
    apply(&node, &second).await?;
    let detached = cursor.next().await?;
    let later = Event::update(first.entity_id, proto::Clock::singleton(&second), AuthorId::Unknown, OperationSet::default());
    apply(&node, &later).await?;
    let next = cursor.fence().await?;
    assert_eq!(
        detached.changes.iter().map(|head| head.head.clone()).collect::<Vec<_>>(),
        vec![proto::Clock::singleton(&first), proto::Clock::singleton(&second)]
    );
    assert!(next.batch.is_some());
    assert_eq!(next.batch.as_ref().unwrap().changes.len(), 1);
    assert_eq!(next.batch.as_ref().unwrap().changes[0].head, proto::Clock::singleton(&later));
    assert!(next.revision > detached.revision);
    Ok(())
}

#[tokio::test]
async fn coalescing_preserves_catalog_invalidation_before_later_ordinary_changes() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut cursor) = subscribe(&node).await?;
    let first = genesis(62);
    apply(&node, &first).await?;
    let model = crate::schema::CATALOG_MODELS[0];
    let catalog = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![Operation::Membership(Membership::Add(model))]));
    let entity = entity_at(&node, &catalog, model).await?;
    node.reactor.begin_commit().await.publish([&entity]);
    let later = genesis(63);
    apply(&node, &later).await?;
    let window = cursor.fence().await?;
    assert!(window.batch.is_some());
    let batch = window.batch.as_ref().unwrap();
    assert!(batch.catalog_changed);
    assert_eq!(batch.revision, window.revision);
    assert_eq!(batch.changes.iter().map(|head| head.id).collect::<Vec<_>>(), vec![first.entity_id, catalog.entity_id, later.entity_id]);
    Ok(())
}

#[tokio::test]
async fn limited_read_validation_preserves_crossing_publications_for_retry() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    apply(&node, &genesis(17)).await?;
    assert!(matches!(cursor.fence_unchanged_since(start).await, Err(SourceError::Changed)));
    let window = cursor.fence().await?;
    assert!(window.batch.is_some());
    let unchanged = cursor.fence_unchanged_since(window.revision).await?;
    assert_eq!(unchanged.revision, window.revision);
    assert!(unchanged.batch.is_none());
    Ok(())
}

#[tokio::test]
async fn projection_failure_invalidates_only_the_affected_subscription() -> anyhow::Result<()> {
    let node = node().await?;
    let (_, mut failed) =
        node.reactor.subscribe_maintenance(|_| Err::<Vec<ProjectedChange<Head>>, _>(anyhow::anyhow!("fixture projection refused"))).await?;
    let (_, mut healthy) = subscribe(&node).await?;
    let event = genesis(51);
    apply(&node, &event).await?;
    assert!(matches!(failed.next().await, Err(SourceError::Changed)));
    assert_eq!(healthy.next().await?.changes[0].head, proto::Clock::singleton(&event));
    assert!(subscribe(&node).await.is_ok());
    Ok(())
}
