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
