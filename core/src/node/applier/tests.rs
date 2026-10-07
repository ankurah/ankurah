use super::*;
use crate::{
    entity::Entity,
    policy::PermissiveAgent,
    property::backend::{lww::LWWBackend, PropertyBackend},
    retrieval::LocalEventGetter,
    test_utils::TestStorage,
    value::Value,
};
use proto::{
    AuthorId, Clock, EntityId, Event, EventId, EventStructureError, Membership, ModelId, Operation, OperationSet, PropertyId, State,
};
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
        let (entity, _) = NodeApplier::save_events(&node, genesis.entity_id, &[Attested::opt(genesis.clone(), None)], &events)
            .await?
            .unwrap()
            .into_parts();
        Ok(Self { node, storage, events, genesis, entity })
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

/// The ingresses that carry events from a peer.
#[derive(Clone, Copy, Debug)]
enum EventIngress {
    EventOnly,
    StateAndEvent,
    EventBridge,
}

impl Fixture {
    /// Deliver `events` for the fixture's entity through `ingress`, a state update carrying the current state.
    async fn deliver(&self, ingress: EventIngress, events: Vec<Event>) -> Result<(), MutationError> {
        let peer = EntityId::from_bytes([5; 32]);
        let states = LocalStateGetter::new(self.storage.clone());
        let fragments = events.into_iter().map(|event| Attested::opt(event, None).into()).collect();
        let content = match ingress {
            EventIngress::EventOnly => proto::UpdateContent::EventOnly(fragments),
            EventIngress::StateAndEvent => proto::UpdateContent::StateAndEvent(
                proto::StateFragment { state: self.entity.to_state()?, attestations: Default::default() },
                fragments,
            ),
            EventIngress::EventBridge => {
                let delta =
                    proto::EntityDelta { entity_id: self.entity.id(), content: proto::DeltaContent::EventBridge { events: fragments } };
                return NodeApplier::apply_delta_inner(&self.node, &peer, delta, &self.events, &states).await.map(|_| ());
            }
        };
        let update = proto::SubscriptionUpdateItem { entity_id: self.entity.id(), content, predicate_relevance: vec![] };
        NodeApplier::apply_update(&self.node, &peer, update, &self.events, &states, &mut Vec::new(), &mut ()).await
    }
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
        &node,
        &EntityId::from_bytes([5; 32]),
        delta.clone(),
        &LocalEventGetter::new(storage.clone(), false),
        &LocalStateGetter::new(storage.clone()),
    )
    .await;
    assert!(failed.is_err());
    assert!(node.entities.get(&source.entity.id()).is_none());
    assert!(matches!(storage.get_state(source.entity.id()).await, Err(RetrievalError::EntityNotFound(_))));

    let change = NodeApplier::apply_delta_inner(
        &node,
        &EntityId::from_bytes([5; 32]),
        delta,
        &LocalEventGetter::new(storage.clone(), false),
        &LocalStateGetter::new(storage.clone()),
    )
    .await?
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
        let error = fixture.apply_snapshot(SnapshotIngress::StateAndEvent, snapshot, forged.clone()).await.unwrap_err();
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
    fixture.apply_snapshot(SnapshotIngress::StateAndEvent, snapshot.clone(), honest.clone()).await?;
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
        let update = proto::SubscriptionUpdateItem {
            entity_id: source.entity.id(),
            content: proto::UpdateContent::StateAndEvent(
                proto::StateFragment { state: snapshot.clone(), attestations: Default::default() },
                cargo,
            ),
            predicate_relevance: vec![],
        };
        // A first snapshot must not ask its event getter for missing history; the generation check is local-only.
        let result = NodeApplier::apply_update(
            &node,
            &EntityId::from_bytes([5; 32]),
            update,
            &NoEventReads,
            &LocalStateGetter::new(storage.clone()),
            &mut Vec::new(),
            &mut (),
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
        let update = |carried: Vec<Attested<Event>>| proto::SubscriptionUpdateItem {
            entity_id: source.entity.id(),
            content: proto::UpdateContent::StateAndEvent(
                proto::StateFragment { state: snapshot.clone(), attestations: Default::default() },
                carried.into_iter().map(Into::into).collect(),
            ),
            predicate_relevance: vec![],
        };
        let mut changes = Vec::new();
        // The stray event is a sibling of first, so the snapshot does not include it.
        let error = NodeApplier::apply_update(
            &node,
            &EntityId::from_bytes([5; 32]),
            update(vec![tip.clone().into(), stray.clone().into(), first.clone().into()]),
            &events,
            &states,
            &mut changes,
            &mut (),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, MutationError::InvalidEvent), "{error}");
        assert!(changes.is_empty());
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
        NodeApplier::apply_update(
            &node,
            &EntityId::from_bytes([5; 32]),
            update(vec![tip.clone().into(), first.clone().into()]),
            &events,
            &states,
            &mut changes,
            &mut (),
        )
        .await?;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].events(), &[Attested::from(first.clone()), Attested::from(tip.clone())]);
        assert_eq!(changes[0].entity().to_state()?, snapshot);
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
    let accepted =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("accepted")?]));
    let failed = Event::update(
        fixture.entity.id(),
        Clock::singleton(&accepted),
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
        content: proto::UpdateContent::EventOnly(vec![
            Attested::opt(accepted.clone(), None).into(),
            Attested::opt(failed.clone(), None).into(),
        ]),
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
    assert_eq!(fixture.entity.head(), Clock::singleton(&accepted));
    assert_eq!(fixture.entity.values()?, vec![(property(), Some(Value::String("accepted".into())))]);
    assert_eq!(fixture.entity.memberships(), std::collections::BTreeSet::from([model_a()]));
    assert_eq!(fixture.storage.get_state(fixture.entity.id()).await?.payload.state, fixture.entity.to_state()?);
    assert_eq!(fixture.storage.get_events(vec![accepted.id(), failed.id()]).await?, vec![Attested::opt(accepted.clone(), None)]);
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

/// Each event ingress rejects false parent annotations before storage. EventOnly preserves the accepted prefix.
#[tokio::test]
async fn event_ingress_rejects_false_annotations_for_known_parents() -> anyhow::Result<()> {
    for ingress in [EventIngress::EventOnly, EventIngress::StateAndEvent, EventIngress::EventBridge] {
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
                matches!(error, MutationError::EventStructure(EventStructureError::GenerationMismatch { claimed: c, known: k, .. }) if (c, k) == (claimed, known)),
                "{ingress:?}, claimed {claimed}, known {known}: {error}"
            );
            let commits_prefix = parent_in_batch && matches!(ingress, EventIngress::EventOnly);
            let stored = fixture.storage.get_events(events.iter().map(Event::id).collect()).await?;
            assert_eq!(stored, if commits_prefix { vec![Attested::from(honest.clone())] } else { vec![] }, "{ingress:?}");
            let head = if commits_prefix { Clock::singleton(&honest) } else { genesis.clone() };
            assert_eq!(fixture.entity.head(), head, "{ingress:?}");

            fixture.deliver(ingress, vec![honest.clone()]).await?;
            assert_eq!(fixture.entity.head(), Clock::singleton(&honest), "{ingress:?}");
            assert_eq!(fixture.storage.get_events(vec![honest.id()]).await?.len(), 1, "{ingress:?}");
        }
    }
    Ok(())
}

/// Comparison's missing-parent error takes precedence over a known generation mismatch.
/// EventOnly still commits the accepted prefix.
#[tokio::test]
async fn an_update_naming_an_unreadable_parent_is_stopped_before_its_generation_is_judged() -> anyhow::Result<()> {
    for ingress in [EventIngress::EventOnly, EventIngress::StateAndEvent, EventIngress::EventBridge] {
        let fixture = Fixture::new().await?;
        let genesis = fixture.entity.head();
        let honest = Event::update(fixture.entity.id(), genesis.clone(), AuthorId::Unknown, OperationSet::default());
        let unreadable = EventId::from_bytes([9; 32]);
        // The known parent is generation 2, but the update claims 3.
        let over_both = Event::update(
            fixture.entity.id(),
            Clock::new(vec![(3, honest.id()), (1, unreadable.clone())])?,
            AuthorId::Unknown,
            OperationSet::default(),
        );

        let error = fixture.deliver(ingress, vec![honest.clone(), over_both.clone()]).await.unwrap_err();
        assert!(
            matches!(&error, MutationError::RetrievalError(RetrievalError::EventNotFound(id)) if *id == unreadable),
            "{ingress:?}: {error}"
        );
        let commits_prefix = matches!(ingress, EventIngress::EventOnly);
        let stored = fixture.storage.get_events(vec![honest.id(), over_both.id()]).await?;
        assert_eq!(stored, if commits_prefix { vec![Attested::opt(honest.clone(), None)] } else { vec![] }, "{ingress:?}");
        let head = if commits_prefix { Clock::singleton(&honest) } else { genesis.clone() };
        assert_eq!(fixture.entity.head(), head, "{ingress:?}");
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
    let change = NodeApplier::apply_delta_inner(&node, &peer, delta, &events, &states).await?.expect("the snapshot creates the entity");
    let (entity, _) = change.into_parts();
    assert!(storage.get_events(vec![newer.id(), older.id()]).await?.is_empty(), "a snapshot brings no events");

    // Only the newer branch is available. The comparison reaches the older tip from both sides
    // without its payload; admission must use that tip's annotation, not the head maximum.
    for event in newer_history.iter().chain([&genesis]) {
        events.stage_event(event.clone());
    }
    // Understate the older parent's known generation of 2.
    let under_claimed = Event::update(genesis.entity_id, Clock::new(vec![(1, older.id())])?, AuthorId::Unknown, OperationSet::default());
    let update = proto::SubscriptionUpdateItem {
        entity_id: genesis.entity_id,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(under_claimed.clone(), None).into()]),
        predicate_relevance: vec![],
    };
    let error = NodeApplier::apply_update(&node, &peer, update, &events, &states, &mut Vec::new(), &mut ()).await.unwrap_err();
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
    cache(&fixture.storage, &[&intermediate]).await?;
    let mut applied = vec![Attested::from(first), Attested::from(last)];
    for event in &mut applied {
        assert!(candidate.apply_event(&fixture.events, event, |_| Ok(None)).await?);
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
    // The earlier event is an ancestor via an event outside this batch; neither notification may be lost.
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
    assert!(candidate.apply_event(&fixture.events, &mut Attested::from(incoming.clone()), |_| Ok(None)).await?);
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

/// A candidate accepted an update while its parent was missing. Learning that parent's generation
/// before persistence must reject the dishonest update, even though the earlier candidate accepted it.
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
    // Falsify P's generation: its payload derives 2.
    let update = over(&parent, 99);

    let peer = EntityId::from_bytes([5; 32]);
    let events = LocalEventGetter::new(storage.clone(), false);
    let states = LocalStateGetter::new(storage.clone());
    let snapshot = State { memberships: [model_a()].into(), head: Clock::singleton(&tip), ..State::default() };
    let state = proto::StateFragment { state: snapshot, attestations: Default::default() };
    let delta = proto::EntityDelta { entity_id: genesis.entity_id, content: proto::DeltaContent::StateSnapshot { state } };
    let change = NodeApplier::apply_delta_inner(&node, &peer, delta, &events, &states).await?.expect("the snapshot creates the entity");
    let (entity, _) = change.into_parts();
    cache(&storage, &[&genesis, &tip]).await?;
    let admitted_head = Clock::from_events([&tip, &update]);

    // The EventOnly arm's first step: its candidate fork admits the update.
    let candidate = RemoteTrxEntity::edit(&entity)?;
    let mut admitted = Attested::opt(update.clone(), None);
    assert!(candidate.apply_event(&events, &mut admitted, |_| Ok(None)).await?);
    assert_eq!(candidate.head(), admitted_head);

    // A fetch caches P between the arm's two steps; deciding the update's generation again would now refuse it.
    cache(&storage, &[&parent]).await?;
    // The arm's persistence step must make the same check against the newly available evidence.
    let before = entity.to_state()?;
    let error = NodeApplier::save_events(&node, genesis.entity_id, &[admitted], &events).await.unwrap_err();
    assert!(
        matches!(error, MutationError::EventStructure(EventStructureError::GenerationMismatch { claimed: 99, known: 2, .. })),
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
        // Falsify P's generation: its payload derives 2.
        let update = over(&parent, 99);

        let peer = EntityId::from_bytes([5; 32]);
        let events = LocalEventGetter::new(storage.clone(), false);
        let states = LocalStateGetter::new(storage.clone());
        let snapshot = State { memberships: [model_a()].into(), head: Clock::singleton(&tip), ..State::default() };
        let state = proto::StateFragment { state: snapshot, attestations: Default::default() };
        let delta = proto::EntityDelta { entity_id: genesis.entity_id, content: proto::DeltaContent::StateSnapshot { state } };
        let change = NodeApplier::apply_delta_inner(&node, &peer, delta, &events, &states).await?.expect("the snapshot creates the entity");
        let (entity, _) = change.into_parts();
        cache(&storage, &[&genesis, &tip]).await?;
        let admitted_head = Clock::from_events([&tip, &update]);

        let candidate = RemoteTrxEntity::edit(&entity)?;
        let mut admitted = Attested::from(update.clone());
        assert!(candidate.apply_event(&events, &mut admitted, |_| Ok(None)).await?);
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
