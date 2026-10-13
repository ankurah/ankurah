use super::*;
use crate::{
    entity::Entity,
    error::{LineageError, ValidationError},
    policy::{AccessDenied, DefaultContext, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::LocalEventGetter,
    test_utils::TestStorage,
    util::Iterable,
    value::Value,
};
use ankql::ast::{Predicate, Resolved};
use proto::{
    AuthorId, Clock, EntityId, Event, EventId, EventStructureError, Membership, ModelId, Operation, OperationSet, PropertyId, State,
};
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};

fn model_a() -> ModelId { ModelId::EntityId(EntityId::from_bytes([1; 32])) }
fn model_b() -> ModelId { ModelId::EntityId(EntityId::from_bytes([2; 32])) }
fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([3; 32])) }
fn other_property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([6; 32])) }
fn peer() -> EntityId { EntityId::from_bytes([5; 32]) }

fn set(property: PropertyId, value: &str) -> anyhow::Result<Operation> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::String(value.into())));
    Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
}

fn value_operation(value: &str) -> anyhow::Result<Operation> { set(property(), value) }

fn fragment(state: State) -> proto::StateFragment { proto::StateFragment { state, attestations: Default::default() } }

fn fragments(events: impl IntoIterator<Item = Event>) -> Vec<proto::EventFragment> {
    events.into_iter().map(|event| Attested::opt(event, None).into()).collect()
}

fn values(entity: &Entity) -> anyhow::Result<BTreeMap<PropertyId, Option<Value>>> { Ok(entity.values()?.into_iter().collect()) }

struct Fixture {
    node: Node<TestStorage, PermissiveAgent>,
    storage: Arc<TestStorage>,
    events: LocalEventGetter<TestStorage>,
    genesis: Event,
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
        let (entity, _) =
            NodeApplier::apply_and_commit_events(&node, &[Attested::opt(genesis.clone(), None)], &events).await?.unwrap().into_parts();
        Ok(Self { node, storage, events, genesis, entity })
    }

    async fn commit_event(&self, event: Event) -> anyhow::Result<()> {
        self.events.stage_event(event.clone());
        NodeApplier::apply_and_commit_events(&self.node, &[Attested::opt(event, None)], &self.events).await?;
        Ok(())
    }

    /// Deliver one delta for the fixture's entity.
    async fn apply(&self, content: proto::DeltaContent, delivery: Delivery) -> Result<DeltaOutcome, MutationError> {
        let delta = proto::EntityDelta { entity_id: self.entity.id(), content };
        NodeApplier::apply_delta(&self.node, &peer(), delta, delivery, &self.events, &LocalStateGetter::new(self.storage.clone())).await
    }

    /// The sender's branch: `event` applied over the fixture's current state, as the sender would snapshot it.
    async fn remote_snapshot(&self, event: &Event) -> anyhow::Result<State> {
        let fork = RemoteTrxEntity::edit(&self.entity)?;
        fork.apply_events(&self.events, &mut [Attested::from(event.clone())], |_| Ok(None)).await?;
        Ok(fork.to_state()?)
    }

    async fn apply_snapshot(&self, ingress: SnapshotIngress, state: State, event: Event) -> Result<DeltaOutcome, MutationError> {
        let (content, delivery) = match ingress {
            SnapshotIngress::InitialSnapshot => (proto::DeltaContent::StateSnapshot { state: fragment(state) }, Delivery::Initial),
            SnapshotIngress::LiveStateAndEvents | SnapshotIngress::DivergedLiveStateAndEvents => {
                (proto::DeltaContent::StateAndEvents { state: fragment(state), events: fragments([event]) }, Delivery::Live)
            }
        };
        self.apply(content, delivery).await
    }

    /// Deliver `events` for the fixture's entity through `ingress`; a state shape carries the current state.
    async fn deliver(&self, ingress: EventIngress, events: Vec<Event>) -> Result<DeltaOutcome, MutationError> {
        let (content, delivery) = match ingress {
            EventIngress::LiveBridge => (proto::DeltaContent::EventBridge { events: fragments(events) }, Delivery::Live),
            EventIngress::LiveStateAndEvents => (
                proto::DeltaContent::StateAndEvents { state: fragment(self.entity.to_state()?), events: fragments(events) },
                Delivery::Live,
            ),
            EventIngress::InitialBridge => (proto::DeltaContent::EventBridge { events: fragments(events) }, Delivery::Initial),
        };
        self.apply(content, delivery).await
    }
}

#[derive(Clone, Copy)]
enum SnapshotIngress {
    InitialSnapshot,
    LiveStateAndEvents,
    DivergedLiveStateAndEvents,
}

/// The shapes that carry events from a peer, with the delivery each arrives by.
#[derive(Clone, Copy, Debug)]
enum EventIngress {
    LiveBridge,
    LiveStateAndEvents,
    InitialBridge,
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
    let failed = NodeApplier::apply_delta(
        &node,
        &EntityId::from_bytes([5; 32]),
        delta.clone(),
        Delivery::Initial,
        &LocalEventGetter::new(storage.clone(), false),
        &LocalStateGetter::new(storage.clone()),
    )
    .await;
    assert!(failed.is_err());
    assert!(node.entities.get(&source.entity.id()).is_none());
    assert!(matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))));

    let change = NodeApplier::apply_delta(
        &node,
        &EntityId::from_bytes([5; 32]),
        delta,
        Delivery::Initial,
        &LocalEventGetter::new(storage.clone(), false),
        &LocalStateGetter::new(storage.clone()),
    )
    .await?
    .change
    .expect("the first snapshot initializes the entity");
    let (entity, _) = change.into_parts();
    assert_eq!(entity.to_state()?, state);
    assert_eq!(storage.get_state(entity.id()).await?.payload.state, state);
    assert_eq!(node.entities.get(&entity.id()), Some(entity));
    Ok(())
}

#[tokio::test]
async fn adopted_snapshot_rejects_false_parent_generations_without_changing_the_resident() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let older = Event::update(fixture.entity.id(), Clock::singleton(&fixture.genesis), AuthorId::Unknown, OperationSet::default());
    let sibling = Event::update(fixture.entity.id(), Clock::singleton(&fixture.genesis), AuthorId::Unknown, OperationSet::default());
    let newer = Event::update(fixture.entity.id(), Clock::singleton(&sibling), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(older.clone()).await?;
    fixture.commit_event(sibling).await?;
    fixture.commit_event(newer.clone()).await?;
    let before = fixture.entity.to_state()?;
    assert_eq!(before.head, Clock::from_events([&older, &newer]));
    // Both false claims for the older parent leave the update's generation at 4.
    for claimed in [1, 3] {
        let forged = Event::update(
            fixture.entity.id(),
            Clock::new(vec![(claimed, older.id()), (newer.generation(), newer.id())])?,
            AuthorId::Unknown,
            OperationSet::default(),
        );
        assert_eq!(forged.generation(), 4);
        let mut snapshot = before.clone();
        snapshot.head = Clock::singleton(&forged);
        let error = fixture.apply_snapshot(SnapshotIngress::LiveStateAndEvents, snapshot, forged.clone()).await.unwrap_err();
        assert!(
            matches!(
                &error,
                MutationError::EventStructure(EventStructureError::GenerationMismatch { event: parent, claimed: c, known: 2 })
                    if parent == &older.id() && *c == claimed
            ),
            "{error}"
        );
        assert_eq!(fixture.entity.to_state()?, before);
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
        assert!(fixture.storage.get_events(vec![forged.id()]).await?.is_empty());
    }

    let honest = Event::update(fixture.entity.id(), before.head.clone(), AuthorId::Unknown, OperationSet::default());
    let mut snapshot = before;
    snapshot.head = Clock::singleton(&honest);
    fixture.apply_snapshot(SnapshotIngress::LiveStateAndEvents, snapshot.clone(), honest.clone()).await?;
    assert_eq!(fixture.entity.to_state()?, snapshot);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
    assert_eq!(fixture.storage.get_events(vec![honest.id()]).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn first_snapshot_checks_cargo_against_local_or_carried_parents_but_allows_missing_history() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    for parent_source in ["local", "carried", "missing"] {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new(storage.clone(), PermissiveAgent::new());
        node.system.wait_loaded().await?;
        if parent_source == "local" {
            cache(&storage, &[&source.genesis]).await?;
        }
        let forged =
            Event::update(source.entity.id(), Clock::new(vec![(2, source.genesis.id())])?, AuthorId::Unknown, OperationSet::default());
        let mut snapshot = source.entity.to_state()?;
        snapshot.head = Clock::singleton(&forged);
        let mut cargo = vec![Attested::from(forged.clone()).into()];
        if parent_source == "carried" {
            cargo.push(Attested::from(source.genesis.clone()).into());
        }
        let update = proto::EntityDelta {
            entity_id: source.entity.id(),
            content: proto::DeltaContent::StateAndEvents {
                state: proto::StateFragment { state: snapshot.clone(), attestations: Default::default() },
                events: cargo,
            },
        };
        // A first snapshot must not ask its event getter for missing history; the generation check is local-only.
        let result = NodeApplier::apply_delta(
            &node,
            &EntityId::from_bytes([5; 32]),
            update,
            Delivery::Live,
            &NoEventReads,
            &LocalStateGetter::new(storage.clone()),
        )
        .await;
        if parent_source == "missing" {
            result?;
            assert_eq!(storage.get_state(source.entity.id()).await?.payload.state, snapshot);
            assert_eq!(storage.get_events(vec![forged.id()]).await?.len(), 1);
        } else {
            let error = result.unwrap_err();
            assert!(
                matches!(error, MutationError::EventStructure(EventStructureError::GenerationMismatch { claimed: 2, known: 1, .. })),
                "{parent_source}: {error}"
            );
            assert!(node.entities.get(&source.entity.id()).is_none());
            assert!(matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))));
            assert!(storage.get_events(vec![forged.id()]).await?.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_event_consistency_is_checked_before_persistence_and_publication() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    let initial = source.entity.to_state()?;
    let first = Event::update(source.entity.id(), initial.head.clone(), AuthorId::Unknown, OperationSet::default());
    let tip = Event::update(source.entity.id(), Clock::singleton(&first), AuthorId::Unknown, OperationSet(vec![value_operation("tip")?]));
    let stray = Event::update(source.entity.id(), initial.head.clone(), AuthorId::Unknown, OperationSet(vec![value_operation("stray")?]));
    source.commit_event(first.clone()).await?;
    source.commit_event(tip.clone()).await?;
    let snapshot = source.entity.to_state()?;

    for existing in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new(storage.clone(), PermissiveAgent::new());
        node.system.wait_loaded().await?;
        let events = LocalEventGetter::new(storage.clone(), false);
        events.stage_event(source.genesis.clone());
        let states = LocalStateGetter::new(storage.clone());
        let resident = if existing {
            NodeApplier::save_new_entity(
                &node,
                &proto::EntityState { entity_id: source.entity.id(), state: initial.clone() },
                &[],
                &events,
                &states,
            )
            .await?
        } else {
            None
        };
        let (notifications, observed) = mpsc::channel();
        let _listener = resident.as_ref().map(|entity| entity.broadcast().reference().listen(notifications));
        let update = |carried: Vec<Attested<Event>>| proto::EntityDelta {
            entity_id: source.entity.id(),
            content: proto::DeltaContent::StateAndEvents {
                state: proto::StateFragment { state: snapshot.clone(), attestations: Default::default() },
                events: carried.into_iter().map(Into::into).collect(),
            },
        };
        // The stray event is a sibling of first, so the snapshot does not include it.
        let error = NodeApplier::apply_delta(
            &node,
            &EntityId::from_bytes([5; 32]),
            update(vec![tip.clone().into(), stray.clone().into(), first.clone().into()]),
            Delivery::Live,
            &events,
            &states,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, MutationError::InvalidEvent), "{error}");
        assert!(storage.get_events(vec![first.id(), tip.id(), stray.id()]).await?.is_empty());
        if let Some(resident) = &resident {
            assert_eq!(resident.to_state()?, initial);
            assert_eq!(storage.get_state(source.entity.id()).await?.payload.state, initial);
            assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
        } else {
            assert!(node.entities.get(&source.entity.id()).is_none());
            assert!(matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))));
        }

        // A carried ancestor is valid even though only its descendant remains in the head.
        let change = NodeApplier::apply_delta(
            &node,
            &EntityId::from_bytes([5; 32]),
            update(vec![tip.clone().into(), first.clone().into()]),
            Delivery::Live,
            &events,
            &states,
        )
        .await?
        .change
        .expect("the snapshot was adopted");
        assert_eq!(change.events(), &[Attested::from(first.clone()), Attested::from(tip.clone())]);
        assert_eq!(change.entity().to_state()?, snapshot);
        assert_eq!(storage.get_state(source.entity.id()).await?.payload.state, snapshot);
    }
    Ok(())
}

struct NoEventReads;

#[async_trait::async_trait]
impl crate::retrieval::GetEvents for NoEventReads {
    async fn get_event(&self, _: &EventId) -> Result<Event, RetrievalError> { panic!("this path must not fetch events") }
    async fn event_stored(&self, _: &EventId) -> Result<bool, RetrievalError> { panic!("this path must not fetch events") }
}

impl SuspenseEvents for NoEventReads {
    fn stage_event(&self, _: Event) {}
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
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, std::slice::from_mut(&mut Attested::from(incoming.clone())), |_| Ok(None)).await?;
    let snapshot = candidate.to_state()?;
    if matches!(ingress, SnapshotIngress::DivergedLiveStateAndEvents) {
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
    assert!(fixture.storage.get_events(vec![incoming.id()]).await?.is_empty());
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
    assert_failed_snapshot_keeps_resident(SnapshotIngress::InitialSnapshot).await
}

#[tokio::test]
async fn state_and_event_commit_failure_keeps_retained_entity_and_listener_unchanged() -> anyhow::Result<()> {
    assert_failed_snapshot_keeps_resident(SnapshotIngress::LiveStateAndEvents).await
}

#[tokio::test]
async fn divergent_state_and_event_commit_failure_keeps_retained_entity_and_listener_unchanged() -> anyhow::Result<()> {
    assert_failed_snapshot_keeps_resident(SnapshotIngress::DivergedLiveStateAndEvents).await
}

#[tokio::test]
async fn snapshot_retry_merges_the_carried_chain_after_a_concurrent_commit() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let first = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    let second = Event::update(fixture.entity.id(), Clock::singleton(&first), AuthorId::Unknown, OperationSet::default());
    let last =
        Event::update(fixture.entity.id(), Clock::singleton(&second), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let mut events = vec![Attested::from(first), Attested::from(second), Attested::from(last.clone())];
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, &mut events, |_| Ok(None)).await?;
    let update = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::StateAndEvents {
            state: proto::StateFragment { state: candidate.to_state()?, attestations: Default::default() },
            events: events.iter().cloned().map(Into::into).collect(),
        },
    };
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *fixture.storage.hold_set_state.lock().unwrap() = Some((entered, release_rx));
    let mut applying = Box::pin(async {
        NodeApplier::apply_delta(
            &fixture.node,
            &EntityId::from_bytes([5; 32]),
            update,
            Delivery::Live,
            &fixture.events,
            &LocalStateGetter::new(fixture.storage.clone()),
        )
        .await
    });
    assert!(futures::poll!(&mut applying).is_pending());
    entered_rx.await?;

    // Advance storage and the resident after adoption, but before its expected-head check at commit.
    let local = Event::update(
        fixture.entity.id(),
        fixture.entity.head(),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(model_b()))]),
    );
    fixture.commit_event(local.clone()).await?;
    assert!(fixture.storage.get_events(events.iter().map(|event| event.payload.id()).collect()).await?.is_empty());
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    release.send(()).unwrap();
    // Replaying only the snapshot tip would jump from generation 2 to 4 and fail with BeyondHead.
    let change = applying.await?.change.expect("the carried events merged");

    assert_eq!(fixture.entity.head(), Clock::from_events([&local, &last]));
    assert_eq!(fixture.entity.values()?, vec![(property(), Some(Value::String("incoming".into())))]);
    assert!(fixture.entity.has_membership(&model_b()));
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
    assert_eq!(fixture.storage.get_events(events.iter().map(|event| event.payload.id()).collect()).await?.len(), 3);
    assert_eq!(change.events(), events);
    assert_eq!(observed.try_recv()?, ());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

#[tokio::test]
async fn snapshot_retry_does_not_notify_for_events_a_concurrent_commit_already_applied() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, &mut [incoming.clone().into()], |_| Ok(None)).await?;
    let snapshot = candidate.to_state()?;
    let update = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::StateAndEvents {
            state: proto::StateFragment { state: snapshot.clone(), attestations: Default::default() },
            events: vec![Attested::from(incoming.clone()).into()],
        },
    };
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *fixture.storage.hold_set_state.lock().unwrap() = Some((entered, release_rx));
    let mut applying = Box::pin(async {
        NodeApplier::apply_delta(
            &fixture.node,
            &EntityId::from_bytes([5; 32]),
            update,
            Delivery::Live,
            &fixture.events,
            &LocalStateGetter::new(fixture.storage.clone()),
        )
        .await
    });
    assert!(futures::poll!(&mut applying).is_pending());
    entered_rx.await?;
    fixture.commit_event(incoming.clone()).await?;
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    release.send(()).unwrap();
    assert!(applying.await?.change.is_none());

    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(fixture.entity.to_state()?, snapshot);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
    assert_eq!(fixture.storage.get_events(vec![incoming.id()]).await?, vec![Attested::from(incoming)]);
    Ok(())
}

#[tokio::test]
async fn snapshot_retry_without_events_keeps_the_divergent_resident() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    fixture.events.stage_event(incoming.clone());
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, &mut [incoming.clone().into()], |_| Ok(None)).await?;
    let delta = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::StateSnapshot {
            state: proto::StateFragment { state: candidate.to_state()?, attestations: Default::default() },
        },
    };
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *fixture.storage.hold_set_state.lock().unwrap() = Some((entered, release_rx));
    let mut applying = Box::pin(async {
        NodeApplier::apply_delta(
            &fixture.node,
            &EntityId::from_bytes([5; 32]),
            delta,
            Delivery::Initial,
            &fixture.events,
            &LocalStateGetter::new(fixture.storage.clone()),
        )
        .await
    });
    assert!(futures::poll!(&mut applying).is_pending());
    entered_rx.await?;
    let local = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(local).await?;
    let before = fixture.entity.to_state()?;
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    release.send(()).unwrap();

    // An initially divergent bare snapshot is ignored too: a retry must make the same decision.
    let outcome = applying.await?;
    assert!(outcome.change.is_none());
    let divergence = outcome.divergence.expect("the concurrent commit leaves the snapshot uncovered");
    assert_eq!(divergence.target, Clock::singleton(&incoming));
    assert_eq!(divergence.actual, before.head);
    assert_eq!(fixture.entity.to_state()?, before);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
    assert!(fixture.storage.get_events(vec![incoming.id()]).await?.is_empty());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

#[tokio::test]
async fn unchanged_bare_snapshots_do_not_write_storage() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let older = fixture.entity.to_state()?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    fixture.events.stage_event(incoming.clone());
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, &mut [incoming.into()], |_| Ok(None)).await?;
    let concurrent = candidate.to_state()?;
    let local = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("local")?]));
    fixture.commit_event(local).await?;
    let current = fixture.entity.to_state()?;

    // Any attempted commit would fail, even if it only rewrote the same state.
    fixture.storage.fail_next_commit();
    for snapshot in [current.clone(), older, concurrent] {
        let delta = proto::EntityDelta {
            entity_id: fixture.entity.id(),
            content: proto::DeltaContent::StateSnapshot {
                state: proto::StateFragment { state: snapshot, attestations: Default::default() },
            },
        };
        assert!(NodeApplier::apply_delta(
            &fixture.node,
            &EntityId::from_bytes([5; 32]),
            delta,
            Delivery::Initial,
            &fixture.events,
            &LocalStateGetter::new(fixture.storage.clone()),
        )
        .await?
        .change
        .is_none());
        assert_eq!(fixture.entity.to_state()?, current);
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, current);
    }
    Ok(())
}

#[tokio::test]
async fn state_and_event_persists_payloads_for_an_already_adopted_snapshot() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    fixture.events.stage_event(incoming.clone());
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    candidate.apply_events(&fixture.events, &mut [incoming.clone().into()], |_| Ok(None)).await?;
    let snapshot = candidate.to_state()?;
    fixture.apply_snapshot(SnapshotIngress::InitialSnapshot, snapshot.clone(), incoming.clone()).await?;
    assert!(fixture.storage.get_events(vec![incoming.id()]).await?.is_empty());

    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    fixture.deliver(EventIngress::LiveStateAndEvents, vec![incoming.clone()]).await?;
    assert_eq!(fixture.storage.get_events(vec![incoming.id()]).await?, vec![Attested::from(incoming)]);
    assert_eq!(fixture.entity.to_state()?, snapshot);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

#[tokio::test]
async fn event_ingresses_reject_a_disjoint_input_in_a_connected_batch() -> anyhow::Result<()> {
    for ingress in [EventIngress::LiveBridge, EventIngress::LiveStateAndEvents, EventIngress::InitialBridge] {
        let fixture = Fixture::new().await?;
        let foreign = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![Operation::Membership(Membership::Add(model_b()))]));
        cache(&fixture.storage, &[&foreign]).await?;
        let valid =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("valid")?]));
        let disjoint = Event::update(fixture.entity.id(), Clock::singleton(&foreign), AuthorId::Unknown, OperationSet::default());
        for event in [&foreign, &valid, &disjoint] {
            event.validate_structure()?;
        }
        let before = fixture.entity.to_state()?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        // The valid sibling must not vouch for an input whose history belongs to another entity.
        let error = fixture.deliver(ingress, vec![valid.clone(), disjoint.clone()]).await.unwrap_err();
        assert!(matches!(error, MutationError::LineageError(LineageError::Disjoint)), "{ingress:?}: {error}");
        assert_eq!(fixture.entity.to_state()?, before, "{ingress:?}");
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before, "{ingress:?}");
        assert!(fixture.storage.get_events(vec![valid.id(), disjoint.id()]).await?.is_empty(), "{ingress:?}");
        assert!(!fixture.storage.list_materializations().await?.contains(&model_b()), "{ingress:?}");
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)), "{ingress:?}");

        for mut events in [vec![valid.clone().into(), disjoint.clone().into()], vec![disjoint.into(), valid.into()]] {
            let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
            let error = candidate
                .apply_events(&fixture.events, &mut events, |_| panic!("disjoint batch must be rejected before policy checks"))
                .await
                .unwrap_err();
            assert!(matches!(error, MutationError::LineageError(LineageError::Disjoint)));
            assert_eq!(candidate.to_state()?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn event_only_discards_the_batch_without_partial_failed_operations() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let accepted =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("accepted")?]));
    let failed = Event::update(
        fixture.entity.id(),
        Clock::singleton(&accepted),
        AuthorId::Unknown,
        OperationSet(vec![
            value_operation("failed")?,
            Operation::Membership(Membership::Add(model_b())),
            // LWW and membership changes occur before this Yrs update fails.
            Operation::Backend { backend: "yrs".into(), operations: vec![proto::BackendOperation { diff: vec![0xff] }] },
        ]),
    );
    let later =
        Event::update(fixture.entity.id(), Clock::singleton(&failed), AuthorId::Unknown, OperationSet(vec![value_operation("later")?]));
    let before = fixture.entity.to_state()?;
    let getter = CountedHistory { inner: &fixture.events, tip: fixture.genesis.id(), tip_reads: 0.into() };
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    let update = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::EventBridge {
            events: vec![
                Attested::opt(accepted.clone(), None).into(),
                Attested::opt(failed.clone(), None).into(),
                Attested::from(later.clone()).into(),
            ],
        },
    };
    let error = NodeApplier::apply_delta(
        &fixture.node,
        &EntityId::from_bytes([5; 32]),
        update,
        Delivery::Live,
        &getter,
        &LocalStateGetter::new(fixture.storage.clone()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, MutationError::StateError(_)), "{error}");
    assert_eq!(getter.tip_reads.load(std::sync::atomic::Ordering::Relaxed), 0, "the chain must not fetch the head");
    assert_eq!(fixture.entity.to_state()?, before);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
    assert!(fixture.storage.get_events(vec![accepted.id(), failed.id(), later.id()]).await?.is_empty());
    assert!(!fixture.storage.list_materializations().await?.contains(&model_b()));
    let selection = ankql::ast::Selection::<ankql::ast::Resolved> {
        predicate: ankql::ast::Predicate::MemberOf(model_b()),
        order_by: None,
        limit: None,
    };
    assert!(fixture.storage.fetch_states(&selection).await?.is_empty());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

#[tokio::test]
async fn state_and_event_bridges_discard_the_batch_on_operation_failure() -> anyhow::Result<()> {
    for ingress in [EventIngress::LiveStateAndEvents, EventIngress::InitialBridge] {
        let fixture = Fixture::new().await?;
        let before = fixture.entity.to_state()?;
        let first =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("first")?]));
        let failed = Event::update(
            fixture.entity.id(),
            Clock::singleton(&first),
            AuthorId::Unknown,
            OperationSet(vec![Operation::Backend {
                backend: "yrs".into(),
                operations: vec![proto::BackendOperation { diff: vec![0xff] }],
            }]),
        );
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);
        let error = fixture.deliver(ingress, vec![first.clone(), failed.clone()]).await.unwrap_err();
        assert!(matches!(error, MutationError::StateError(_)), "{ingress:?}: {error}");
        assert_eq!(fixture.entity.to_state()?, before, "{ingress:?}");
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before, "{ingress:?}");
        assert!(fixture.storage.get_events(vec![first.id(), failed.id()]).await?.is_empty(), "{ingress:?}");
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)), "{ingress:?}");
    }
    Ok(())
}

/// Each event ingress rejects a false parent annotation before storage: a claim above the head is refused by the
/// bound before any read. Preparation failure rejects the entire batch on every ingress.
#[tokio::test]
async fn event_ingress_rejects_false_annotations_for_known_parents() -> anyhow::Result<()> {
    for ingress in [EventIngress::LiveBridge, EventIngress::LiveStateAndEvents, EventIngress::InitialBridge] {
        for parent_in_batch in [false, true] {
            let fixture = Fixture::new().await?;
            let genesis = fixture.entity.head();
            let honest = Event::update(fixture.entity.id(), genesis.clone(), AuthorId::Unknown, OperationSet::default());
            let (parent, known) = if parent_in_batch { (&honest, 2) } else { (&fixture.genesis, 1) };
            let claimed = known + 1;
            let forged =
                Event::update(fixture.entity.id(), Clock::new(vec![(claimed, parent.id())])?, AuthorId::Unknown, OperationSet::default());
            let events = if parent_in_batch { vec![honest.clone(), forged] } else { vec![forged] };
            let error = fixture.deliver(ingress, events.clone()).await.unwrap_err();
            assert!(
                matches!(error, MutationError::LineageError(LineageError::BeyondHead { claimed: c, head: h, .. }) if (c, h) == (claimed + 1, known)),
                "{ingress:?}, claimed {claimed}, known {known}: {error}"
            );
            assert!(fixture.storage.get_events(events.iter().map(Event::id).collect()).await?.is_empty(), "{ingress:?}");
            assert_eq!(fixture.entity.head(), genesis, "{ingress:?}");

            fixture.deliver(ingress, vec![honest.clone()]).await?;
            assert_eq!(fixture.entity.head(), Clock::singleton(&honest), "{ingress:?}");
            assert_eq!(fixture.storage.get_events(vec![honest.id()]).await?.len(), 1, "{ingress:?}");
        }
    }
    Ok(())
}

/// Comparison's missing-parent error takes precedence over a known generation mismatch.
/// No event in the batch is applied when history preparation fails.
#[tokio::test]
async fn an_update_naming_an_unreadable_parent_is_stopped_before_its_generation_is_judged() -> anyhow::Result<()> {
    for ingress in [EventIngress::LiveBridge, EventIngress::LiveStateAndEvents, EventIngress::InitialBridge] {
        let fixture = Fixture::new().await?;
        let genesis = fixture.entity.head();
        let honest = Event::update(fixture.entity.id(), genesis.clone(), AuthorId::Unknown, OperationSet::default());
        let unreadable = EventId::from_bytes([9; 32]);
        // The known parent is generation 2, but the update claims 1: a false annotation within the bound.
        let over_both = Event::update(
            fixture.entity.id(),
            Clock::new(vec![(1, honest.id()), (1, unreadable.clone())])?,
            AuthorId::Unknown,
            OperationSet::default(),
        );

        let error = fixture.deliver(ingress, vec![honest.clone(), over_both.clone()]).await.unwrap_err();
        assert!(
            matches!(&error, MutationError::RetrievalError(RetrievalError::EventNotFound(id)) if *id == unreadable),
            "{ingress:?}: {error}"
        );
        assert!(fixture.storage.get_events(vec![honest.id(), over_both.id()]).await?.is_empty(), "{ingress:?}");
        assert_eq!(fixture.entity.head(), genesis, "{ingress:?}");
    }
    Ok(())
}

/// A snapshot's older tip retains its own generation, even without the payload or the newer tip as a parent.
#[tokio::test]
async fn an_update_over_one_tip_of_a_snapshot_head_is_checked_without_that_tips_payload() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let genesis = Event::genesis(
        Some(EntityId::from_bytes([4; 32])),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(model_a()))]),
    );
    let over = |parent: &Event| Event::update(genesis.entity_id, Clock::singleton(parent), AuthorId::Unknown, OperationSet::default());
    let mut newer = genesis.clone();
    let mut newer_history = Vec::new();
    for _ in 0..4 {
        newer = over(&newer);
        newer_history.push(newer.clone());
    }
    let older = over(&genesis);
    assert_eq!((newer.generation(), older.generation()), (5, 2));
    let head = Clock::from_events([&newer, &older]);

    let peer = EntityId::from_bytes([5; 32]);
    let events = LocalEventGetter::new(storage.clone(), false);
    let states = LocalStateGetter::new(storage.clone());
    let snapshot = State { memberships: [model_a()].into(), head: head.clone(), ..State::default() };
    let state = proto::StateFragment { state: snapshot, attestations: Default::default() };
    let delta = proto::EntityDelta { entity_id: genesis.entity_id, content: proto::DeltaContent::StateSnapshot { state } };
    let change = NodeApplier::apply_delta(&node, &peer, delta, Delivery::Initial, &events, &states)
        .await?
        .change
        .expect("the snapshot creates the entity");
    let (entity, _) = change.into_parts();
    assert!(storage.get_events(vec![newer.id(), older.id()]).await?.is_empty(), "a snapshot brings no events");

    // Only the newer branch is available. The comparison reaches the older tip from both sides
    // without its payload; admission must use that tip's annotation, not the head maximum.
    for event in newer_history.iter().chain([&genesis]) {
        events.stage_event(event.clone());
    }
    // Understate the older parent's known generation of 2.
    let under_claimed = Event::update(genesis.entity_id, Clock::new(vec![(1, older.id())])?, AuthorId::Unknown, OperationSet::default());
    let update = proto::EntityDelta {
        entity_id: genesis.entity_id,
        content: proto::DeltaContent::EventBridge { events: vec![Attested::opt(under_claimed.clone(), None).into()] },
    };
    let error = NodeApplier::apply_delta(&node, &peer, update, Delivery::Live, &events, &states).await.unwrap_err();
    assert!(
        matches!(error, MutationError::EventStructure(EventStructureError::GenerationMismatch { claimed: 1, known: 2, .. })),
        "{error}"
    );
    assert_eq!(entity.head(), head);
    assert!(storage.get_events(vec![under_claimed.id()]).await?.is_empty());
    Ok(())
}

/// Store `events` outside any entity's commit, as the subscription path's getter caches what it fetches.
async fn cache(storage: &TestStorage, events: &[&Event]) -> anyhow::Result<()> {
    let mut transaction = storage.transaction();
    transaction.add_events(&events.iter().map(|event| Attested::opt((*event).clone(), None)).collect::<Vec<_>>()).await?;
    transaction.commit().await?.committed()?;
    Ok(())
}

#[tokio::test]
async fn unchanged_resident_publishes_prepared_state_and_all_applied_events_without_reads() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let before = fixture.entity.to_state()?;
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let view = candidate.read();
    let first = Event::update(
        fixture.entity.id(),
        Clock::singleton(&fixture.genesis),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("first")?, Operation::Membership(Membership::Add(model_b()))]),
    );
    let intermediate = Event::update(fixture.entity.id(), Clock::singleton(&first), AuthorId::Unknown, OperationSet::default());
    let last = Event::update(
        fixture.entity.id(),
        Clock::singleton(&intermediate),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("last")?]),
    );
    let mut applied = vec![Attested::from(first), Attested::from(intermediate), Attested::from(last)];
    for event in &mut applied {
        candidate.apply_events(&fixture.events, std::slice::from_mut(event), |_| Ok(None)).await?;
    }
    let prepared = candidate.to_state()?;
    let mut transaction = fixture.storage.transaction();
    transaction
        .set_state(&before.head, &Attested::from(proto::EntityState { entity_id: fixture.entity.id(), state: prepared.clone() }))
        .await?;
    transaction.add_events(&applied).await?;
    transaction.commit().await?.committed()?;
    assert_eq!(fixture.entity.to_state()?, before);

    let (notifications, received) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    let change = candidate.commit(&fixture.node.entities, &NoEventReads).await?;
    // Every applied event is published; no notification may be lost.
    assert_eq!(change.events(), applied);
    assert_eq!(change.entity(), &fixture.entity);
    assert_eq!(fixture.entity.to_state()?, prepared);
    assert_eq!(fixture.entity.values()?, vec![(property(), Some(Value::String("last".into())))]);
    assert!(fixture.entity.has_membership(&model_b()));
    assert_eq!(view.to_state()?, prepared);
    assert_eq!(received.try_recv()?, ());
    assert!(matches!(received.try_recv(), Err(mpsc::TryRecvError::Empty)));

    // The retained view must follow the resident, not keep reading the unchanged transaction copy.
    let next = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("next")?]));
    fixture.commit_event(next).await?;
    assert_eq!(view.values()?, vec![(property(), Some(Value::String("next".into())))]);
    assert_eq!(view.head(), fixture.entity.head());
    Ok(())
}

#[tokio::test]
async fn publication_keeps_a_resident_advanced_by_a_reader() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let incoming = Event::update(
        fixture.entity.id(),
        Clock::singleton(&fixture.genesis),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("incoming")?]),
    );
    candidate.apply_events(&fixture.events, std::slice::from_mut(&mut Attested::from(incoming.clone())), |_| Ok(None)).await?;
    let prepared = candidate.to_state()?;
    let mut transaction = fixture.storage.transaction();
    transaction
        .set_state(&fixture.entity.head(), &Attested::from(proto::EntityState { entity_id: fixture.entity.id(), state: prepared.clone() }))
        .await?;
    transaction.add_events(&[incoming.clone().into()]).await?;
    transaction.commit().await?.committed()?;

    // A reader loads our committed snapshot before publication finishes, then another update advances it.
    let states = LocalStateGetter::new(fixture.storage.clone());
    fixture.node.entities.with_state(&states, &fixture.events, fixture.entity.id(), prepared).await?;
    let later =
        Event::update(fixture.entity.id(), Clock::singleton(&incoming), AuthorId::Unknown, OperationSet(vec![value_operation("later")?]));
    fixture.commit_event(later).await?;
    let latest = fixture.entity.to_state()?;
    let (notifications, received) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    let change = candidate.commit(&fixture.node.entities, &fixture.events).await?;
    assert!(change.events().is_empty(), "the reader's newer state already includes this event");
    assert_eq!(fixture.entity.to_state()?, latest);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, latest);
    assert!(matches!(received.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

/// A fresh application attempt checks newly available parent generations,
/// even if an earlier, unpublished fork accepted the update without that evidence.
#[tokio::test]
async fn a_parent_arriving_before_persistence_causes_generation_to_be_rechecked() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let genesis = Event::genesis(
        Some(EntityId::from_bytes([4; 32])),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(model_a()))]),
    );
    let over = |parent: &Event, parent_generation| {
        Event::update(
            genesis.entity_id,
            Clock::new(vec![(parent_generation, parent.id())]).unwrap(),
            AuthorId::Unknown,
            OperationSet::default(),
        )
    };
    let parent = over(&genesis, genesis.generation());
    let tip = over(&parent, parent.generation());
    // Falsify P's generation below its own, within the bound: its payload derives 2.
    let update = over(&parent, 1);

    let peer = EntityId::from_bytes([5; 32]);
    let events = LocalEventGetter::new(storage.clone(), false);
    let states = LocalStateGetter::new(storage.clone());
    let snapshot = State { memberships: [model_a()].into(), head: Clock::singleton(&tip), ..State::default() };
    let state = proto::StateFragment { state: snapshot, attestations: Default::default() };
    let delta = proto::EntityDelta { entity_id: genesis.entity_id, content: proto::DeltaContent::StateSnapshot { state } };
    let change = NodeApplier::apply_delta(&node, &peer, delta, Delivery::Initial, &events, &states)
        .await?
        .change
        .expect("the snapshot creates the entity");
    let (entity, _) = change.into_parts();
    cache(&storage, &[&genesis, &tip]).await?;
    let admitted_head = Clock::from_events([&tip, &update]);

    // An unpublished fork admits the update while its parent is unavailable.
    let candidate = RemoteTrxEntity::edit(&entity)?;
    let mut admitted = Attested::opt(update.clone(), None);
    candidate.apply_events(&events, std::slice::from_mut(&mut admitted), |_| Ok(None)).await?;
    assert_eq!(candidate.head(), admitted_head);

    // The parent arrives before a fresh application attempt.
    cache(&storage, &[&parent]).await?;
    let before = entity.to_state()?;
    let error = NodeApplier::apply_and_commit_events(&node, &[admitted], &events).await.unwrap_err();
    assert!(
        matches!(error, MutationError::EventStructure(EventStructureError::GenerationMismatch { claimed: 1, known: 2, .. })),
        "{error}"
    );
    assert_eq!(storage.get_state(genesis.entity_id).await?.payload.state, before);
    assert_eq!(entity.to_state()?, before);
    assert!(storage.get_events(vec![update.id()]).await?.is_empty());
    Ok(())
}

/// Once storage committed the prepared state, learning a parent's generation must not prevent publication.
#[tokio::test]
async fn a_parent_arriving_after_storage_commit_does_not_prevent_publication() -> anyhow::Result<()> {
    for reader_loads_snapshot in [false, true] {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new(storage.clone(), PermissiveAgent::new());
        node.system.wait_loaded().await?;
        let genesis = Event::genesis(
            Some(EntityId::from_bytes([4; 32])),
            AuthorId::Unknown,
            OperationSet(vec![Operation::Membership(Membership::Add(model_a()))]),
        );
        let over = |parent: &Event, parent_generation| {
            Event::update(
                genesis.entity_id,
                Clock::new(vec![(parent_generation, parent.id())]).unwrap(),
                AuthorId::Unknown,
                OperationSet::default(),
            )
        };
        let parent = over(&genesis, genesis.generation());
        let tip = over(&parent, parent.generation());
        // Falsify P's generation below its own, within the bound: its payload derives 2.
        let update = over(&parent, 1);

        let peer = EntityId::from_bytes([5; 32]);
        let events = LocalEventGetter::new(storage.clone(), false);
        let states = LocalStateGetter::new(storage.clone());
        let snapshot = State { memberships: [model_a()].into(), head: Clock::singleton(&tip), ..State::default() };
        let state = proto::StateFragment { state: snapshot, attestations: Default::default() };
        let delta = proto::EntityDelta { entity_id: genesis.entity_id, content: proto::DeltaContent::StateSnapshot { state } };
        let change = NodeApplier::apply_delta(&node, &peer, delta, Delivery::Initial, &events, &states)
            .await?
            .change
            .expect("the snapshot creates the entity");
        let (entity, _) = change.into_parts();
        cache(&storage, &[&genesis, &tip]).await?;
        let admitted_head = Clock::from_events([&tip, &update]);

        let candidate = RemoteTrxEntity::edit(&entity)?;
        let mut admitted = Attested::from(update.clone());
        candidate.apply_events(&events, std::slice::from_mut(&mut admitted), |_| Ok(None)).await?;
        let prepared = candidate.to_state()?;
        let mut transaction = storage.transaction();
        transaction
            .set_state(&entity.head(), &Attested::from(proto::EntityState { entity_id: entity.id(), state: prepared.clone() }))
            .await?;
        transaction.add_events(&[admitted.clone()]).await?;
        transaction.commit().await?.committed()?;
        cache(&storage, &[&parent]).await?;

        if reader_loads_snapshot {
            node.entities.with_state(&states, &events, entity.id(), prepared.clone()).await?;
        }
        let change = if reader_loads_snapshot {
            candidate.commit(&node.entities, &events).await?
        } else {
            candidate.commit(&node.entities, &NoEventReads).await?
        };
        if reader_loads_snapshot {
            assert!(change.events().is_empty(), "the committed event is already resident");
        } else {
            assert_eq!(change.events(), &[admitted]);
        }
        assert_eq!(entity.head(), admitted_head);
        assert_eq!(entity.to_state()?, prepared);
        assert_eq!(storage.get_state(entity.id()).await?.payload.state, prepared);
    }
    Ok(())
}

/// Counts reads of the resident tip: separate comparisons for each concurrent
/// incoming event would repeatedly read it, even though the history is shared.
struct CountedHistory<'a> {
    inner: &'a LocalEventGetter<TestStorage>,
    tip: EventId,
    tip_reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::retrieval::GetEvents for CountedHistory<'_> {
    async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> {
        if *id == self.tip {
            self.tip_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner.get_event(id).await
    }
    async fn event_stored(&self, id: &EventId) -> Result<bool, RetrievalError> { self.inner.event_stored(id).await }
}

impl SuspenseEvents for CountedHistory<'_> {
    fn stage_event(&self, event: Event) { self.inner.stage_event(event); }
}

#[tokio::test]
async fn batch_policy_checks_intermediate_states_and_stops_at_first_failure() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let resident = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(resident.clone()).await?;
    let accepted = Event::update(
        fixture.entity.id(),
        Clock::singleton(&fixture.genesis),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("allowed")?]),
    );
    let denied = Event::update(
        fixture.entity.id(),
        Clock::singleton(&accepted),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("forbidden")?]),
    );
    let later = Event::update(
        fixture.entity.id(),
        Clock::singleton(&denied),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("allowed again")?]),
    );
    let getter = CountedHistory { inner: &fixture.events, tip: resident.id(), tip_reads: 0.into() };
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let view = candidate.read();
    let mut events: Vec<_> = [&accepted, &denied, &later].into_iter().cloned().map(Attested::from).collect();
    let mut checked = Vec::new();
    let error = candidate
        .apply_events(&getter, &mut events, |event| {
            let value = view.property_value(&property()).unwrap();
            checked.push((event.id(), value.clone()));
            if value == Some(Value::String("forbidden".into())) {
                return Err(MutationError::InvalidUpdate("policy denied intermediate state"));
            }
            Ok(Some(proto::Attestation(vec![7])))
        })
        .await
        .unwrap_err();
    assert!(matches!(error, MutationError::InvalidUpdate("policy denied intermediate state")));
    assert_eq!(
        checked,
        vec![(accepted.id(), Some(Value::String("allowed".into()))), (denied.id(), Some(Value::String("forbidden".into()))),]
    );
    assert_eq!(events[0], Attested::opt(accepted.clone(), Some(proto::Attestation(vec![7]))));
    assert!(events[1].attestations.is_empty());
    assert!(events[2].attestations.is_empty());
    assert_eq!(getter.tip_reads.load(std::sync::atomic::Ordering::Relaxed), 1, "the batch uses one comparison");
    assert_eq!(fixture.entity.head(), Clock::singleton(&resident), "the failed fork was not published");
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
    assert!(fixture.storage.get_events(vec![accepted.id(), denied.id(), later.id()]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn unsorted_batch_is_rejected_before_application_or_policy_checks() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let parent =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("parent")?]));
    let child =
        Event::update(fixture.entity.id(), Clock::singleton(&parent), AuthorId::Unknown, OperationSet(vec![value_operation("child")?]));
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let before = candidate.to_state()?;
    let mut events = vec![Attested::from(child), Attested::from(parent)];
    let error =
        candidate.apply_events(&NoEventReads, &mut events, |_| panic!("an unsorted batch must not reach policy checks")).await.unwrap_err();
    assert!(matches!(error, MutationError::InvalidUpdate("event batch must be in causal order")));
    assert_eq!(candidate.to_state()?, before);
    assert!(events.iter().all(|event| event.attestations.is_empty()));
    Ok(())
}

#[tokio::test]
async fn batch_preparation_failure_changes_nothing_and_runs_no_policy_checks() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let accepted =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("first")?]));
    let missing = EventId::from_bytes([99; 32]);
    let failed = Event::update(
        fixture.entity.id(),
        Clock::new(vec![(2, accepted.id()), (2, missing.clone())])?,
        AuthorId::Unknown,
        OperationSet::default(),
    );
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let before = candidate.to_state()?;
    let mut events = vec![Attested::from(accepted), Attested::from(failed)];
    let error =
        candidate.apply_events(&fixture.events, &mut events, |_| panic!("preparation must finish before policy checks")).await.unwrap_err();
    assert!(matches!(error, MutationError::RetrievalError(RetrievalError::EventNotFound(id)) if id == missing));
    assert_eq!(candidate.to_state()?, before);
    assert!(events.iter().all(|event| event.attestations.is_empty()));
    Ok(())
}

#[tokio::test]
async fn one_comparison_prepares_and_persists_a_concurrent_batch() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let resident = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(resident.clone()).await?;
    let a = Event::update(
        fixture.entity.id(),
        Clock::singleton(&fixture.genesis),
        AuthorId::Unknown,
        OperationSet(vec![value_operation("first")?]),
    );
    let b = Event::update(fixture.entity.id(), Clock::singleton(&a), AuthorId::Unknown, OperationSet(vec![value_operation("second")?]));
    let getter = CountedHistory { inner: &fixture.events, tip: resident.id(), tip_reads: 0.into() };
    let update = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::EventBridge { events: vec![Attested::from(b.clone()).into(), Attested::from(a.clone()).into()] },
    };
    let change = NodeApplier::apply_delta(
        &fixture.node,
        &EntityId::from_bytes([5; 32]),
        update,
        Delivery::Live,
        &getter,
        &LocalStateGetter::new(fixture.storage.clone()),
    )
    .await?
    .change
    .expect("the batch applied");
    assert_eq!(getter.tip_reads.load(std::sync::atomic::Ordering::Relaxed), 1, "persistence must use the prepared fork");
    assert_eq!(fixture.entity.head(), Clock::from_events([&resident, &b]));
    assert_eq!(fixture.entity.property_value(&property())?, Some(Value::String("second".into())));
    assert_eq!(change.into_parts().1, vec![Attested::from(a), Attested::from(b)]);
    Ok(())
}

#[tokio::test]
async fn batch_checks_concurrent_events_before_later_operations_in_the_same_layer() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let ancestor = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(ancestor.clone()).await?;
    let resident = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    fixture.commit_event(resident.clone()).await?;
    let third_model = ModelId::EntityId(EntityId::from_bytes([18; 32]));
    let a = Event::update(
        fixture.entity.id(),
        fixture.entity.head(),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(model_b()))]),
    );
    let b = Event::update(
        fixture.entity.id(),
        fixture.entity.head(),
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(third_model))]),
    );
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let view = candidate.read();
    // Old inputs below the loaded history and duplicates still get policy checks without shifting the next event's layers.
    let mut events = vec![
        Attested::from(fixture.genesis.clone()),
        Attested::from(ancestor.clone()),
        Attested::from(resident.clone()),
        Attested::from(a.clone()),
        Attested::from(a.clone()),
        Attested::from(b.clone()),
    ];
    let mut checked = Vec::new();
    candidate
        .apply_events(&fixture.events, &mut events, |event| {
            checked.push((event.id(), view.memberships()));
            Ok(None)
        })
        .await?;
    assert_eq!(
        checked,
        vec![
            (fixture.genesis.id(), std::collections::BTreeSet::from([model_a()])),
            (ancestor.id(), std::collections::BTreeSet::from([model_a()])),
            (resident.id(), std::collections::BTreeSet::from([model_a()])),
            (a.id(), std::collections::BTreeSet::from([model_a(), model_b()])),
            (a.id(), std::collections::BTreeSet::from([model_a(), model_b()])),
            (b.id(), std::collections::BTreeSet::from([model_a(), model_b(), third_model])),
        ]
    );
    assert_eq!(candidate.head(), Clock::from_events([&a, &b]));
    Ok(())
}

#[tokio::test]
async fn batch_extension_does_not_fetch_the_existing_head() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let first = Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet::default());
    let second = Event::update(fixture.entity.id(), Clock::singleton(&first), AuthorId::Unknown, OperationSet::default());
    let expected_head = Clock::singleton(&second);
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let mut events = vec![Attested::from(first), Attested::from(second)];
    // An ephemeral node may have adopted this state without storing its head event.
    candidate.apply_events(&NoEventReads, &mut events, |_| Ok(None)).await?;
    assert_eq!(candidate.head(), expected_head);
    Ok(())
}

#[tokio::test]
async fn supplied_batch_can_exceed_the_single_event_history_budget() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut head = fixture.entity.head();
    let mut events = Vec::new();
    for _ in 0..crate::event_dag::DEFAULT_BUDGET * 4 + 1 {
        let event = Event::update(fixture.entity.id(), head, AuthorId::Unknown, OperationSet::default());
        head = Clock::singleton(&event);
        events.push(Attested::from(event));
    }
    let candidate = RemoteTrxEntity::edit(&fixture.entity)?;
    let count = events.len();
    let mut checks = 0;
    candidate
        .apply_events(&fixture.events, &mut events, |_| {
            checks += 1;
            Ok(None)
        })
        .await?;
    assert_eq!(checks, count);
    assert_eq!(candidate.head(), head);
    Ok(())
}

#[tokio::test]
async fn a_concurrent_snapshot_without_events_is_reported_not_adopted() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let remote =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
        // The comparison reads the sender's history through the getter, as a peer-backed getter would fetch it.
        fixture.events.stage_event(remote.clone());
        let snapshot = fixture.remote_snapshot(&remote).await?;
        let local = Event::update(
            fixture.entity.id(),
            fixture.entity.head(),
            AuthorId::Unknown,
            OperationSet(vec![set(other_property(), "local")?]),
        );
        fixture.commit_event(local.clone()).await?;
        let before = fixture.entity.to_state()?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let outcome = fixture.apply(proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) }, delivery).await?;
        let divergence = outcome.divergence.expect("a concurrent snapshot is not covered");
        assert_eq!(divergence.entity_id, fixture.entity.id());
        assert_eq!(divergence.target, snapshot.head);
        assert_eq!(divergence.actual, Clock::singleton(&local));
        assert!(outcome.change.is_none(), "{delivery:?}");

        assert_eq!(outcome.entity, Some(fixture.entity.clone()));
        assert_eq!(fixture.entity.to_state()?, before);
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
        assert!(fixture.storage.get_events(vec![remote.id()]).await?.is_empty());
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    Ok(())
}

/// A concurrent snapshot whose connecting events are carried merges those events: the local
/// edit survives, the sender's branch is incorporated, and the target counts as covered.
#[tokio::test]
async fn a_concurrent_snapshot_with_its_events_merges_and_keeps_the_local_edit() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let remote =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
        let snapshot = fixture.remote_snapshot(&remote).await?;
        let local = Event::update(
            fixture.entity.id(),
            fixture.entity.head(),
            AuthorId::Unknown,
            OperationSet(vec![set(other_property(), "local")?]),
        );
        fixture.commit_event(local.clone()).await?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let outcome = fixture
            .apply(proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([remote.clone()]) }, delivery)
            .await?;
        assert!(outcome.divergence.is_none(), "{delivery:?}");

        let change = outcome.change.expect("the merged branch is published");
        let published: &[Attested<Event>] = match delivery {
            Delivery::Initial => &[],
            Delivery::Live => &[Attested::from(remote.clone())],
        };
        assert_eq!(change.events(), published, "{delivery:?}");
        assert_eq!(fixture.entity.head(), Clock::from_events([&local, &remote]));
        assert_eq!(
            values(&fixture.entity)?,
            BTreeMap::from([(property(), Some(Value::String("remote".into()))), (other_property(), Some(Value::String("local".into()))),])
        );
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
        assert_eq!(fixture.storage.get_events(vec![remote.id()]).await?.len(), 1);
        assert_eq!(observed.try_recv()?, ());
        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    Ok(())
}

/// Equal and older snapshots are covered already: nothing changes, nothing is published, and
/// events carried beside them that the entity has applied are not applied again.
#[tokio::test]
async fn equal_and_older_snapshots_are_covered_without_change() -> anyhow::Result<()> {
    for delivery in [Delivery::Initial, Delivery::Live] {
        let fixture = Fixture::new().await?;
        let older = fixture.entity.to_state()?;
        let next =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("next")?]));
        fixture.commit_event(next.clone()).await?;
        let current = fixture.entity.to_state()?;
        let (notifications, observed) = mpsc::channel();
        let _listener = fixture.entity.broadcast().reference().listen(notifications);

        let shapes = [
            proto::DeltaContent::StateSnapshot { state: fragment(current.clone()) },
            proto::DeltaContent::StateSnapshot { state: fragment(older.clone()) },
            proto::DeltaContent::StateAndEvents { state: fragment(current.clone()), events: fragments([next.clone()]) },
            proto::DeltaContent::StateAndEvents { state: fragment(older.clone()), events: fragments([fixture.genesis.clone()]) },
        ];
        for content in shapes {
            let description = format!("{delivery:?}: {content:?}");
            let outcome = fixture.apply(content, delivery).await?;
            assert!(outcome.divergence.is_none(), "{description}");
            assert!(outcome.change.is_none(), "{description}");

            assert_eq!(outcome.entity, Some(fixture.entity.clone()), "{description}");
            assert_eq!(fixture.entity.to_state()?, current, "{description}");
            assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)), "{description}");
        }
        assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, current);
    }
    Ok(())
}

/// The same advance publishes differently by delivery: an initial transfer notifies an advanced entity
/// with no events, so another established query holding it sees no event update; a live update
/// publishes the events it applied.
#[tokio::test]
async fn delivery_decides_whether_applied_events_are_published() -> anyhow::Result<()> {
    #[derive(Clone, Copy, Debug)]
    enum Shape {
        Snapshot,
        StateAndEvents,
        Bridge,
    }
    for delivery in [Delivery::Initial, Delivery::Live] {
        for shape in [Shape::Snapshot, Shape::StateAndEvents, Shape::Bridge] {
            let fixture = Fixture::new().await?;
            let incoming = Event::update(
                fixture.entity.id(),
                fixture.entity.head(),
                AuthorId::Unknown,
                OperationSet(vec![value_operation("incoming")?]),
            );
            // A bare snapshot's comparison reads the sender's history through the getter.
            fixture.events.stage_event(incoming.clone());
            let snapshot = fixture.remote_snapshot(&incoming).await?;
            let content = match shape {
                Shape::Snapshot => proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) },
                Shape::StateAndEvents => {
                    proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) }
                }
                Shape::Bridge => proto::DeltaContent::EventBridge { events: fragments([incoming.clone()]) },
            };
            let (notifications, observed) = mpsc::channel();
            let _listener = fixture.entity.broadcast().reference().listen(notifications);

            let outcome = fixture.apply(content, delivery).await?;
            assert!(outcome.divergence.is_none(), "{delivery:?} {shape:?}");

            let change = outcome.change.expect("the entity advanced");
            let published: &[Attested<Event>] = match (delivery, shape) {
                (Delivery::Live, Shape::StateAndEvents | Shape::Bridge) => &[Attested::from(incoming.clone())],
                _ => &[],
            };
            assert_eq!(change.events(), published, "{delivery:?} {shape:?}");
            assert_eq!(change.entity(), &fixture.entity);
            assert_eq!(fixture.entity.to_state()?, snapshot, "{delivery:?} {shape:?}");
            assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
            let stored = fixture.storage.get_events(vec![incoming.id()]).await?.len();
            assert_eq!(stored, if matches!(shape, Shape::Snapshot) { 0 } else { 1 }, "{delivery:?} {shape:?}");
            assert_eq!(observed.try_recv()?, (), "{delivery:?} {shape:?}");
            assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn state_and_relation_is_refused() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let relation = proto::CausalAssertionFragment { relation: proto::CausalRelation::Equal, attestations: Default::default() };
    let content = proto::DeltaContent::StateAndRelation { state: fragment(fixture.entity.to_state()?), relation };
    let error = fixture.apply(content, Delivery::Initial).await.expect_err("unsupported");
    assert!(matches!(error, MutationError::InvalidUpdate(_)), "{error}");
    Ok(())
}

/// Two query streams carrying the same live state can both judge it applicable before either
/// persists it. The one that advances the resident publishes the events; the other, whose commit
/// conflicts and retries against the installed state, has nothing to publish.
#[tokio::test]
async fn concurrent_identical_live_states_publish_once() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let snapshot = fixture.remote_snapshot(&incoming).await?;
    let content = || proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) };
    let (notifications, observed) = mpsc::channel();
    let _listener = fixture.entity.broadcast().reference().listen(notifications);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *fixture.storage.hold_commit.lock().unwrap() = Some((entered, release_rx));

    // The first stream judged the snapshot applicable and is held inside its storage commit.
    let mut first = Box::pin(fixture.apply(content(), Delivery::Live));
    assert!(futures::poll!(&mut first).is_pending());
    entered_rx.await?;
    // The second stream forks the still-unchanged resident, judges the same, and waits for publication.
    let mut second = Box::pin(fixture.apply(content(), Delivery::Live));
    assert!(futures::poll!(&mut second).is_pending());
    release.send(()).unwrap();
    let first = first.await?;
    let second = second.await?;

    let published: Vec<_> = [first, second].into_iter().filter_map(|outcome| outcome.change).collect();
    assert_eq!(published.len(), 1, "the events are published exactly once");
    assert_eq!(published[0].events(), &[Attested::from(incoming.clone())]);
    assert_eq!(fixture.entity.to_state()?, snapshot);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
    assert_eq!(fixture.storage.get_events(vec![incoming.id()]).await?.len(), 1);
    assert_eq!(observed.try_recv()?, ());
    assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)));
    Ok(())
}

/// A sequenced stream applies under the credentials captured for its query, so it needs no
/// standing subscription with the peer; the unsequenced wrapper still refuses without one.
#[tokio::test]
async fn stream_updates_apply_under_captured_credentials_without_a_standing_subscription() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let snapshot = fixture.remote_snapshot(&incoming).await?;
    let before = fixture.entity.to_state()?;
    let item = || proto::SubscriptionUpdateItem {
        delta: proto::EntityDelta {
            entity_id: fixture.entity.id(),
            content: proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) },
        },
        predicate_relevance: vec![],
    };

    let error = NodeApplier::apply_updates(&fixture.node, &peer(), vec![item()]).await.expect_err("no standing subscription with the peer");
    assert!(matches!(&error, ApplyError::MutationError(cause) if matches!(**cause, MutationError::InvalidUpdate(_))), "{error}");
    assert_eq!(fixture.entity.to_state()?, before);
    assert!(fixture.storage.get_events(vec![incoming.id()]).await?.is_empty());

    NodeApplier::apply_stream_updates(&fixture.node, &peer(), &vec![DEFAULT_CONTEXT], vec![item()]).await?;
    assert_eq!(fixture.entity.to_state()?, snapshot);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, snapshot);
    assert_eq!(fixture.storage.get_events(vec![incoming.id()]).await?.len(), 1);
    Ok(())
}

/// A policy agent that trusts nothing a peer sends.
#[derive(Clone)]
struct RejectingAgent;

#[async_trait::async_trait]
impl PolicyAgent for RejectingAgent {
    type ContextData = &'static DefaultContext;

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &crate::node::NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(vec![])
    }

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        Ok(vec![])
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<Event>,
    ) -> Result<(), AccessDenied> {
        Err(AccessDenied::ByPolicy("the event attestation is not trusted"))
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Err(AccessDenied::ByPolicy("the state attestation is not trusted"))
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Err(AccessDenied::ByPolicy("the causal assertion is not trusted"))
    }
}

/// Every delta shape is validated against policy before anything is stored, applied, or published,
/// for a resident entity and for one this node has never seen.
#[tokio::test]
async fn untrusted_attestations_are_refused_before_anything_is_stored() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    let incoming =
        Event::update(source.entity.id(), source.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let snapshot = source.remote_snapshot(&incoming).await?;
    let shapes = || {
        [
            proto::DeltaContent::StateSnapshot { state: fragment(snapshot.clone()) },
            proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) },
            proto::DeltaContent::EventBridge { events: fragments([incoming.clone()]) },
        ]
    };

    for resident in [false, true] {
        for delivery in [Delivery::Initial, Delivery::Live] {
            let storage = Arc::new(TestStorage::default());
            let node = Node::new(storage.clone(), RejectingAgent);
            node.system.wait_loaded().await?;
            let events = LocalEventGetter::new(storage.clone(), false);
            let states = LocalStateGetter::new(storage.clone());
            let entity = if resident {
                // Local history is trusted; only what the peer sends is refused.
                events.stage_event(source.genesis.clone());
                let (entity, _) = NodeApplier::apply_and_commit_events(&node, &[Attested::from(source.genesis.clone())], &events)
                    .await?
                    .expect("the genesis creates the entity")
                    .into_parts();
                Some(entity)
            } else {
                None
            };
            let before = entity.as_ref().map(Entity::to_state).transpose()?;
            let (notifications, observed) = mpsc::channel();
            let _listener = entity.as_ref().map(|entity| entity.broadcast().reference().listen(notifications));

            for content in shapes() {
                let description = format!("resident {resident}, {delivery:?}: {content:?}");
                let delta = proto::EntityDelta { entity_id: source.entity.id(), content };
                let error = NodeApplier::apply_delta(&node, &peer(), delta, delivery, &events, &states).await.expect_err("refused");
                assert!(matches!(error, MutationError::AccessDenied(AccessDenied::ByPolicy(_))), "{description}: {error}");
                assert!(storage.get_events(vec![incoming.id()]).await?.is_empty(), "{description}");
                match (&entity, &before) {
                    (Some(entity), Some(before)) => {
                        assert_eq!(&entity.to_state()?, before, "{description}");
                        assert_eq!(&storage.get_state(entity.id()).await?.payload.state, before, "{description}");
                        assert!(matches!(observed.try_recv(), Err(mpsc::TryRecvError::Empty)), "{description}");
                    }
                    _ => {
                        assert!(node.entities.get(&source.entity.id()).is_none(), "{description}");
                        assert!(
                            matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))),
                            "{description}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn initial_batch_observes_actual_heads_without_retaining_entities() -> anyhow::Result<()> {
    let source = Fixture::new().await?;
    let older = source.entity.to_state()?;
    let next = Event::update(source.entity.id(), source.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("next")?]));
    source.commit_event(next).await?;
    let current = source.entity.to_state()?;

    let storage = Arc::new(TestStorage::default());
    let node = Node::new(storage.clone(), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    let applied = NodeApplier::apply_deltas(
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
    assert_eq!(applied.observed_head(&source.entity.id()), Some(current.head.clone()));
    assert!(!applied.diverged(&source.entity.id()));
    assert!(node.entities.get(&source.entity.id()).is_none(), "the batch result must not keep an entity resident");
    assert_eq!(storage.get_state(source.entity.id()).await?.payload.state, current);

    // An older offered snapshot must report the newer actual local head.
    let applied = NodeApplier::apply_deltas(
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
    assert_eq!(applied.observed_head(&source.entity.id()), Some(current.head.clone()));
    assert_ne!(applied.observed_head(&source.entity.id()), Some(older.head));
    assert!(!applied.diverged(&source.entity.id()));
    assert_eq!(source.entity.to_state()?, current);
    Ok(())
}

#[tokio::test]
async fn initial_batch_empty_bridge_leaves_the_storage_head_fallback_available() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let id = fixture.entity.id();
    let expected = fixture.entity.to_state()?;
    let Fixture { node, storage, events, entity, .. } = fixture;
    drop(entity);
    assert!(node.entities.get(&id).is_none());
    let applied = NodeApplier::apply_deltas(
        &node,
        &peer(),
        vec![proto::EntityDelta { entity_id: id, content: proto::DeltaContent::EventBridge { events: Vec::new() } }],
        &events,
        &LocalStateGetter::new(storage.clone()),
    )
    .await?;
    assert_eq!(applied.observed_head(&id), None);
    assert!(!applied.diverged(&id));
    assert!(node.entities.get(&id).is_none());
    assert_eq!(storage.get_state(id).await?.payload.state, expected);
    Ok(())
}

#[tokio::test]
async fn initial_batch_keeps_a_concurrent_snapshot_divergent() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let remote =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(property(), "remote")?]));
    fixture.events.stage_event(remote.clone());
    let snapshot = fixture.remote_snapshot(&remote).await?;
    let local =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![set(other_property(), "local")?]));
    fixture.commit_event(local).await?;
    let before = fixture.entity.to_state()?;
    let applied = NodeApplier::apply_deltas(
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
    assert!(applied.diverged(&fixture.entity.id()), "an observed head does not waive connecting-history repair");
    assert_eq!(applied.observed_head(&fixture.entity.id()), Some(before.head.clone()));
    assert_ne!(applied.observed_head(&fixture.entity.id()), Some(snapshot.head));
    assert_eq!(fixture.entity.to_state()?, before);
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, before);
    assert!(fixture.storage.get_events(vec![remote.id()]).await?.is_empty());
    Ok(())
}
