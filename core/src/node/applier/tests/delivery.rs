//! What Initial and Live deliveries publish for an applied delta, and which deltas they refuse.
use super::*;
use crate::{
    error::ValidationError,
    policy::{AccessDenied, DefaultContext, PolicyAgent},
    util::Iterable,
};
use ankql::ast::{Predicate, Resolved};

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

/// A live state-and-events delta applies without any subscription relay state, because the state
/// stream that received it admitted it: its state and its event are stored and the resident advances.
#[tokio::test]
async fn a_live_state_and_events_delta_stores_its_state_and_event() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let incoming =
        Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("incoming")?]));
    let snapshot = fixture.remote_snapshot(&incoming).await?;
    let delta = proto::EntityDelta {
        entity_id: fixture.entity.id(),
        content: proto::DeltaContent::StateAndEvents { state: fragment(snapshot.clone()), events: fragments([incoming.clone()]) },
    };
    NodeApplier::apply_delta(
        &fixture.node,
        &peer(),
        delta,
        Delivery::Live,
        &fixture.events,
        &LocalStateGetter::new(fixture.storage.clone()),
    )
    .await?;
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
                let (entity, _) =
                    NodeApplier::apply_and_commit_events(&node, &[Attested::from(source.genesis.clone())], Delivery::Live, &events)
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
async fn initial_snapshot_does_not_consume_later_live_event_delivery() -> anyhow::Result<()> {
    for ingress in [EventIngress::LiveBridge, EventIngress::LiveStateAndEvents] {
        let fixture = Fixture::new().await?;
        let event =
            Event::update(fixture.entity.id(), fixture.entity.head(), AuthorId::Unknown, OperationSet(vec![value_operation("new")?]));
        fixture.events.stage_event(event.clone());
        let newer = fixture.remote_snapshot(&event).await?;
        let initial = fixture.apply(proto::DeltaContent::StateSnapshot { state: fragment(newer.clone()) }, Delivery::Initial).await?;
        assert!(initial.live_events.is_empty());
        assert!(initial.change.unwrap().events().is_empty());
        assert_eq!(fixture.entity.head(), newer.head);
        let live = fixture.deliver(ingress, vec![event.clone()]).await?;
        assert!(live.change.is_none(), "the data was already adopted by the initial snapshot");
        assert_eq!(live.live_events.iter().map(|event| event.payload.id()).collect::<Vec<_>>(), [event.id()]);
        let history = fixture.deliver(EventIngress::InitialBridge, vec![event]).await?;
        assert!(history.live_events.is_empty(), "historical repair is never a live event delivery");
    }
    Ok(())
}
