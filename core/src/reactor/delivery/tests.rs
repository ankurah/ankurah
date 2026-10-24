use super::*;
use crate::{
    retrieval::LocalEventGetter,
    storage::{StorageEngine, StorageTransaction},
    test_utils::TestStorage,
};
use std::sync::Arc;

#[tokio::test]
async fn descendant_notification_needs_connecting_history_but_not_the_baseline_event() -> anyhow::Result<()> {
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let baseline = proto::Clock::singleton(&genesis);
    let parent = proto::Event::update(genesis.entity_id, baseline.clone(), proto::AuthorId::Unknown, proto::OperationSet::default());
    let child =
        proto::Event::update(genesis.entity_id, proto::Clock::singleton(&parent), proto::AuthorId::Unknown, proto::OperationSet::default());
    let storage = Arc::new(TestStorage::default());
    let mut transaction = storage.transaction();
    transaction.add_events(&[parent.clone().into(), child.clone().into()]).await?;
    transaction.commit().await?.committed()?;
    // An initial snapshot establishes the baseline clock without supplying
    // its event payload. The later live child supplies the connecting history.
    assert!(storage.get_events(vec![genesis.id()]).await?.is_empty());
    let history = LocalEventGetter::new(storage, false);
    let (frontier, accepted) = undelivered_events(Some(&history), baseline, &[proto::Attested::from(child.clone())]).await?;
    assert_eq!(frontier, proto::Clock::singleton(&child));
    assert_eq!(accepted.len(), 1);
    let (frontier, accepted) =
        undelivered_events(Some(&history), frontier, &[proto::Attested::from(parent), proto::Attested::from(child.clone())]).await?;
    assert_eq!(frontier, proto::Clock::singleton(&child));
    assert!(accepted.is_empty(), "connecting history and duplicate live events remain silent");
    Ok(())
}

#[tokio::test]
async fn concurrent_notifications_keep_a_minimal_frontier_and_suppress_ancestor_replays() -> anyhow::Result<()> {
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let baseline = proto::Clock::singleton(&genesis);
    let branch = |model: u8| {
        proto::Event::update(
            genesis.entity_id,
            baseline.clone(),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(
                proto::EntityId::from_bytes([model; 32]),
            )))]),
        )
    };
    let left = branch(1);
    let right = branch(2);
    let merged = proto::Event::update(
        genesis.entity_id,
        proto::Clock::from_events([&left, &right]),
        proto::AuthorId::Unknown,
        proto::OperationSet::default(),
    );
    let storage = Arc::new(TestStorage::default());
    let mut transaction = storage.transaction();
    transaction
        .add_events(
            &[genesis.clone(), left.clone(), right.clone(), merged.clone()].into_iter().map(proto::Attested::from).collect::<Vec<_>>(),
        )
        .await?;
    transaction.commit().await?.committed()?;
    let history = LocalEventGetter::new(storage, false);
    let (frontier, accepted) =
        undelivered_events(Some(&history), baseline, &[proto::Attested::from(left.clone()), proto::Attested::from(right.clone())]).await?;
    assert_eq!(frontier, proto::Clock::from_events([&left, &right]));
    assert_eq!(accepted.len(), 2);
    let (frontier, accepted) = undelivered_events(
        Some(&history),
        frontier,
        &[proto::Attested::from(merged.clone()), proto::Attested::from(left), proto::Attested::from(right), proto::Attested::from(genesis)],
    )
    .await?;
    assert_eq!(frontier, proto::Clock::singleton(&merged));
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].payload.id(), merged.id());
    Ok(())
}

/// A change to a test entity carrying protocol events, whose parents place them in its history.
#[derive(Debug)]
struct EventChange(crate::reactor::tests::TestEntity, Vec<proto::Attested<proto::Event>>);
impl std::fmt::Display for EventChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "EventChange({})", self.0.id) }
}
impl crate::reactor::ChangeNotification for EventChange {
    type Entity = crate::reactor::tests::TestEntity;
    type Event = proto::Attested<proto::Event>;
    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { (self.0, self.1) }
    fn entity(&self) -> &Self::Entity { &self.0 }
    fn events(&self) -> &[Self::Event] { &self.1 }
}

#[tokio::test]
async fn an_unprovable_event_drops_only_its_entitys_subscription() {
    use crate::reactor::{tests::TestEntity, Reactor, ReactorUpdate};
    use ankurah_signals::Subscribe;
    let reactor = Reactor::<TestEntity, proto::Attested<proto::Event>>::new_test();
    let subscription = reactor.subscribe();
    let (failing, healthy) = (TestEntity::new("failing", "pending"), TestEntity::new("healthy", "pending"));
    subscription.add_entity_subscriptions([failing.id, healthy.id]);
    let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = delivered.clone();
    let _listener = subscription.subscribe(move |update: ReactorUpdate<TestEntity, proto::Attested<proto::Event>>| {
        recorded.lock().unwrap().extend(update.items.into_iter().flat_map(|item| item.events).map(|event| event.payload.id()));
    });
    let genesis = || proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let after = |parent: &proto::Event| {
        proto::Event::update(parent.entity_id, proto::Clock::singleton(parent), proto::AuthorId::Unknown, proto::OperationSet::default())
    };
    let (first, unrelated, other_first) = (genesis(), genesis(), genesis());
    let notify = |entity: &TestEntity, event: &proto::Event| EventChange(entity.clone(), vec![proto::Attested::from(event.clone())]);
    assert!(reactor.notify_change(vec![notify(&failing, &first), notify(&healthy, &other_first)]).await.is_empty());

    // The failing entity's next event descends from neither delivered head, and this reactor
    // has no history to trace it through. The healthy entity's event follows its head.
    let (unprovable, healthy_next) = (after(&unrelated), after(&other_first));
    let failures = reactor.notify_change(vec![notify(&failing, &unprovable), notify(&healthy, &healthy_next)]).await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].query, None);
    assert_eq!(*delivered.lock().unwrap(), [first.id(), other_first.id(), healthy_next.id()], "the healthy entity still delivers");

    let (failing_later, healthy_later) = (after(&unprovable), after(&healthy_next));
    assert!(reactor.notify_change(vec![notify(&failing, &failing_later), notify(&healthy, &healthy_later)]).await.is_empty());
    assert_eq!(delivered.lock().unwrap().last(), Some(&healthy_later.id()), "only the failing entity's subscription was dropped");
    assert!(!delivered.lock().unwrap().contains(&failing_later.id()));
}
