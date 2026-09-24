use super::*;
use crate::{
    entity::Entity,
    policy::PermissiveAgent,
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::LocalEventGetter,
    test_utils::TestStorage,
    value::Value,
};
use proto::{AuthorId, Clock, EntityId, Event, Membership, ModelId, Operation, OperationSet, PropertyId, State};
use std::sync::{mpsc, Arc};

fn model_a() -> ModelId { ModelId::EntityId(EntityId::from_bytes([1; 32])) }
fn model_b() -> ModelId { ModelId::EntityId(EntityId::from_bytes([2; 32])) }
fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([3; 32])) }

fn value_operation(value: &str) -> anyhow::Result<Operation> {
    let backend = LWWBackend::new();
    backend.set(property(), Some(Value::String(value.into())));
    Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
}

struct Fixture {
    node: Node<TestStorage, PermissiveAgent>,
    storage: Arc<TestStorage>,
    events: LocalEventGetter<TestStorage>,
    entity: Entity,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new(storage.clone(), PermissiveAgent::new());
        node.system.wait_loaded().await?;
        let events = LocalEventGetter::new(storage.clone(), false);
        let genesis = Event::genesis(
            Some(EntityId::from_bytes([4; 32])),
            AuthorId::Unknown,
            OperationSet(vec![Operation::Membership(Membership::Add(model_a())), value_operation("initial")?]),
        );
        events.stage_event(genesis.clone());
        let (entity, _) = NodeApplier::save_events(&node, genesis.entity_id, &[Attested::opt(genesis, None)], &events)
            .await?.unwrap().into_parts();
        Ok(Self { node, storage, events, entity })
    }

    async fn commit_event(&self, event: Event) -> anyhow::Result<()> {
        self.events.stage_event(event.clone());
        NodeApplier::save_events(&self.node, self.entity.id(), &[Attested::opt(event, None)], &self.events).await?;
        Ok(())
    }

    async fn apply_snapshot(&self, ingress: SnapshotIngress, state: State, event: Event) -> Result<(), MutationError> {
        let states = LocalStateGetter::new(self.storage.clone());
        let state = proto::StateFragment { state, attestations: Default::default() };
        let peer = EntityId::from_bytes([5; 32]);
        match ingress {
            SnapshotIngress::Snapshot => {
                let delta = proto::EntityDelta { entity_id: self.entity.id(), content: proto::DeltaContent::StateSnapshot { state } };
                NodeApplier::apply_delta_inner(&self.node, &peer, delta, &self.events, &states).await?;
            }
            SnapshotIngress::StateAndEvent | SnapshotIngress::DivergedStateAndEvent => {
                let update = proto::SubscriptionUpdateItem {
                    entity_id: self.entity.id(),
                    content: proto::UpdateContent::StateAndEvent(state, vec![Attested::opt(event, None).into()]),
                    predicate_relevance: vec![],
                };
                NodeApplier::apply_update(&self.node, &peer, update, &self.events, &states, &mut Vec::new(), &mut ()).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SnapshotIngress {
    Snapshot,
    StateAndEvent,
    DivergedStateAndEvent,
}

#[tokio::test]
async fn first_snapshot_initializes_an_entity_without_its_events() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let state = source.storage.get_state(source.entity.id()).await?.payload.state;
    assert!(!state.head.is_empty());
    let delta = proto::EntityDelta {
        entity_id: source.entity.id(),
        content: proto::DeltaContent::StateSnapshot {
            state: proto::StateFragment { state: state.clone(), attestations: Default::default() },
        },
    };
    storage.fail_next_commit();
    let failed = NodeApplier::apply_delta_inner(
        &node, &EntityId::from_bytes([5; 32]), delta.clone(),
        &LocalEventGetter::new(storage.clone(), false), &LocalStateGetter::new(storage.clone()),
    ).await;
    assert!(failed.is_err());
    assert!(node.entities.get(&source.entity.id()).is_none());
    assert!(matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))));

    let change = NodeApplier::apply_delta_inner(
        &node, &EntityId::from_bytes([5; 32]), delta,
        &LocalEventGetter::new(storage.clone(), false), &LocalStateGetter::new(storage.clone()),
    ).await?.expect("the first snapshot initializes the entity");
    let (entity, _) = change.into_parts();
    assert_eq!(entity.to_state()?, state);
    assert_eq!(storage.get_state(entity.id()).await?.payload.state, state);
    assert_eq!(node.entities.get(&entity.id()), Some(entity));
    Ok(())
}

async fn assert_failed_snapshot_keeps_resident(ingress: SnapshotIngress) -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming = Event::update(
        fixture.entity.id(),
        fixture.entity.head(),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("incoming")?, Operation::Membership(Membership::Add(model_b()))]),
    );
    fixture.events.stage_event(incoming.clone());
    let candidate = RemoteTrxEntity::edit(&fixture.entity, Arc::new(AtomicBool::new(true)))?;
    assert!(candidate.apply_event(&fixture.events, &mut Attested::from(incoming.clone()), |_| Ok(None)).await?);
    let snapshot = candidate.to_state()?;
    if matches!(ingress, SnapshotIngress::DivergedStateAndEvent) {
        let local = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
        fixture.commit_event(local).await?;
    }

    let retained = fixture.entity.clone();
    let before = retained.to_state()?;
    let before_values = retained.values()?;
    let (notifications, observed) = mpsc::channel();
    let _listener = retained.broadcast().reference().listen(notifications);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *fixture.storage.hold_commit.lock().unwrap() = Some((entered, release_rx));
    fixture.storage.fail_next_commit();

    let mut applying = Box::pin(fixture.apply_snapshot(ingress, snapshot.clone(), incoming.clone()));
    assert!(futures::poll!(&mut applying).is_pending());
    entered_rx.await?;
    assert_eq!(retained.to_state()?, before);
    assert_eq!(retained.values()?, before_values);
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(fixture.storage.get_state(retained.id()).await?.payload.state, before);

    release.send(()).unwrap();
    let error = applying.await.unwrap_err();
    assert_eq!(error.to_string(), "general error: test storage commit failed");
    assert_eq!(retained.to_state()?, before);
    assert_eq!(retained.values()?, before_values);
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(fixture.storage.get_state(retained.id()).await?.payload.state, before);
    assert!(fixture.storage.get_events(vec![incoming.id()], &ankql::ast::Predicate::True).await?.is_empty());
    assert!(!fixture.storage.list_materializations().await?.contains(&model_b()));

    // The same listener must see the successful retry, proving it stayed attached.
    fixture.apply_snapshot(ingress, snapshot, incoming).await?;
    assert_eq!(observed.try_recv()?, ());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert!(retained.has_membership(&model_b()));
    assert_eq!(retained.values()?, vec![(property(), Some(Value::String("incoming".into())))]);
    assert_eq!(fixture.storage.get_state(retained.id()).await?.payload.state, retained.to_state()?);
    Ok(())
}

#[tokio::test]
async fn snapshot_commit_failure_keeps_retained_entity_and_listener_unchanged() -> anyhow::Result<()> {
    assert_failed_snapshot_keeps_resident(SnapshotIngress::Snapshot).await
}

#[tokio::test]
async fn state_and_event_commit_failure_keeps_retained_entity_and_listener_unchanged() -> anyhow::Result<()> {
    assert_failed_snapshot_keeps_resident(SnapshotIngress::StateAndEvent).await
}

#[tokio::test]
async fn divergent_state_and_event_commit_failure_keeps_retained_entity_and_listener_unchanged() -> anyhow::Result<()> {
    assert_failed_snapshot_keeps_resident(SnapshotIngress::DivergedStateAndEvent).await
}

#[tokio::test]
async fn event_only_commits_accepted_prefix_without_partial_failed_operations() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let accepted = Event::update(
        fixture.entity.id(),
        fixture.entity.head(),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("accepted")?]),
    );
    let failed = Event::update(
        fixture.entity.id(),
        accepted.id().into(),
        AuthorId::Unknown,
        OperationSet(vec![
            value_operation("failed")?,
            Operation::Membership(Membership::Add(model_b())),
            Operation::Backend { backend: "unknown".into(), operations: vec![] },
        ]),
    );
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    let update = proto::SubscriptionUpdateItem {
        entity_id: fixture.entity.id(),
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(accepted.clone(), None).into(), Attested::opt(failed.clone(), None).into()]),
        predicate_relevance: vec![],
    };
    let mut changes = Vec::new();
    let error = NodeApplier::apply_update(
        &fixture.node,
        &EntityId::from_bytes([5; 32]),
        update,
        &fixture.events,
        &LocalStateGetter::new(fixture.storage.clone()),
        &mut changes,
        &mut (),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unknown"));
    assert_eq!(fixture.entity.head(), Clock::from(accepted.id()));
    assert_eq!(fixture.entity.values()?, vec![(property(), Some(Value::String("accepted".into())))]);
    assert_eq!(fixture.entity.memberships(), std::collections::BTreeSet::from([model_a()]));
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
    assert_eq!(fixture.storage.get_events(vec![accepted.id(), failed.id()], &ankql::ast::Predicate::True).await?, vec![Attested::opt(accepted.clone(), None)]);
    assert!(!fixture.storage.list_materializations().await?.contains(&model_b()));
    let selection = ankql::ast::Selection::<ankql::ast::Resolved> {
        predicate: ankql::ast::Predicate::MemberOf(model_b()),
        order_by: None,
        limit: None,
    };
    assert!(fixture.storage.fetch_states(&selection).await?.is_empty());
    assert_eq!(changes.len(), 1);
    assert_eq!(changes.pop().unwrap().into_parts().1, vec![Attested::opt(accepted, None)]);
    assert_eq!(observed.try_recv()?, ());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}
