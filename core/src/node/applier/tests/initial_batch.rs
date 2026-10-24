//! The initial-transfer batch: what it persists, what it leaves unloaded, and what it must not overwrite.
use super::*;

#[tokio::test]
async fn initial_batch_persists_without_retaining_entities() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    let older = source.entity.to_state()?;
    let next = Event::update(source.entity.id(), source.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("next")?]));
    source.commit_event(next).await?;
    let current = source.entity.to_state()?;

    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    NodeApplier::apply_deltas(
        &node,
        &peer(),
        vec![proto::EntityDelta {
            entity_id: source.entity.id(),
            content: proto::DeltaContent::StateSnapshot { state: fragment(current.clone()) },
        }],
        &LocalEventGetter::new(storage.clone(), false),
        &LocalStateGetter::new(storage.clone()),
    )
    .await?;
    assert!(node.entities.get(&source.entity.id()).is_none(), "the batch must not keep an entity resident");
    assert_eq!(storage.get_state(source.entity.id()).await?.payload.state, current);

    // An older offered snapshot must not overwrite the newer local state.
    NodeApplier::apply_deltas(
        &source.node,
        &peer(),
        vec![proto::EntityDelta {
            entity_id: source.entity.id(),
            content: proto::DeltaContent::StateSnapshot { state: fragment(older.clone()) },
        }],
        &source.events,
        &LocalStateGetter::new(source.storage.clone()),
    )
    .await?;
    assert_eq!(source.entity.to_state()?, current);
    Ok(())
}

#[tokio::test]
async fn initial_batch_empty_bridge_preserves_unloaded_state() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let id = fixture.entity.id();
    let expected = fixture.entity.to_state()?;
    let Fixture { node, storage, events, entity, .. } = fixture;
    drop(entity);
    assert!(node.entities.get(&id).is_none());
    NodeApplier::apply_deltas(
        &node,
        &peer(),
        vec![proto::EntityDelta { entity_id: id, content: proto::DeltaContent::EventBridge { events: Vec::new() } }],
        &events,
        &LocalStateGetter::new(storage.clone()),
    )
    .await?;
    assert!(node.entities.get(&id).is_none());
    assert_eq!(storage.get_state(id).await?.payload.state, expected);
    Ok(())
}

#[tokio::test]
async fn initial_batch_does_not_overwrite_a_concurrent_state() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let remote =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
    fixture.events.stage_event(remote.clone());
    let snapshot = fixture.remote_snapshot(&remote).await?;
    let local =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(other_property(), "local")?]));
    fixture.commit_event(local).await?;
    let before = fixture.entity.to_state()?;
    NodeApplier::apply_deltas(
        &fixture.node,
        &peer(),
        vec![proto::EntityDelta {
            entity_id: fixture.entity.id(),
            content: proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) },
        }],
        &fixture.events,
        &LocalStateGetter::new(fixture.storage.clone()),
    )
    .await?;
    assert_eq!(fixture.entity.to_state()?, before);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
    assert!(fixture.storage.get_events(vec![remote.id()]).await?.is_empty());
    Ok(())
}
