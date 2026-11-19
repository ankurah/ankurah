//! Directional replication between independently stored durable nodes.

mod common;

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::core::{error::RetrievalError, storage::StorageTransaction};
use common::*;
use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

type Durable = Node<SledStorageEngine, PermissiveAgent>;

async fn within<T, E: Into<anyhow::Error>>(future: impl Future<Output = Result<T, E>>) -> anyhow::Result<T> {
    tokio::time::timeout(Duration::from_secs(15), future).await?.map_err(Into::into)
}

async fn seed() -> anyhow::Result<Durable> {
    let node = durable_sled_setup().await?;
    node.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Record>().await?;
    Ok(node)
}

/// Provision the existing System identity before constructing the new Node.
/// Only the root and catalog definitions are copied; user data must arrive
/// through the replication protocol exercised by each test.
async fn replica(seed: &Durable) -> anyhow::Result<Durable> { replica_in(seed, Arc::new(SledStorageEngine::new_test()?)).await }

async fn replica_in(seed: &Durable, storage: Arc<SledStorageEngine>) -> anyhow::Result<Durable> {
    assert!(!Arc::ptr_eq(&seed.storage, &storage));
    let root = seed.system.root().expect("seed has a persisted root");
    let mut states = BTreeMap::from([(root.payload.entity_id, root)]);
    for model in [proto::SystemModel::Model, proto::SystemModel::Property, proto::SystemModel::ModelProperty] {
        for state in seed.storage.fetch_states(&Predicate::MemberOf(proto::ModelId::System(model)).into()).await? {
            states.insert(state.payload.entity_id, state);
        }
    }
    let mut transaction = storage.transaction();
    for state in states.values() {
        let history = seed.storage.dump_entity_events(state.payload.entity_id).await?;
        assert!(!history.is_empty(), "provisioning must include supporting events");
        transaction.set_state(&proto::Clock::default(), state).await?;
        transaction.add_events(&history).await?;
    }
    transaction.commit().await?.committed()?;
    let node = Node::new_durable(storage, PermissiveAgent::new());
    within(node.wait_ready()).await?;
    assert_eq!(node.system.root_id(), seed.system.root_id());
    for id in states.keys() {
        assert_history(&node, *id, &event_ids(seed, *id).await?).await?;
    }
    Ok(node)
}

async fn selection(node: &Durable, predicate: &str) -> anyhow::Result<Selection<Resolved>> {
    let model = node.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<Record>().await?;
    Ok(node.catalog.resolve_selection(&model, ankql::parser::parse_selection(predicate)?)?.and_member_of(model))
}

async fn create(node: &Durable, title: &str, artist: &str) -> anyhow::Result<(EntityId, proto::Event)> {
    let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
    let id = transaction.create(&Record { title: title.into(), artist: artist.into() }).await?.id();
    let event = transaction.commit_and_return_events().await?.into_iter().find(|event| event.entity_id == id).unwrap();
    Ok((id, event))
}

async fn edit(node: &Durable, id: EntityId, title: Option<&str>, artist: Option<&str>) -> anyhow::Result<proto::Event> {
    let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
    let record = transaction.get::<Record>(&id).await?;
    if let Some(title) = title {
        record.title()?.set(&title.to_owned())?;
    }
    if let Some(artist) = artist {
        record.artist()?.set(&artist.to_owned())?;
    }
    Ok(transaction.commit_and_return_events().await?.into_iter().find(|event| event.entity_id == id).unwrap())
}

async fn event_ids(node: &Durable, id: EntityId) -> anyhow::Result<HashSet<proto::EventId>> {
    Ok(node.storage.dump_entity_events(id).await?.into_iter().map(|event| event.payload.id()).collect())
}

async fn assert_history(node: &Durable, id: EntityId, expected: &HashSet<proto::EventId>) -> anyhow::Result<()> {
    let events = node.storage.dump_entity_events(id).await?;
    assert_eq!(events.iter().map(|event| event.payload.id()).collect::<HashSet<_>>(), *expected);
    let generations: BTreeMap<_, _> = events.iter().map(|event| (event.payload.id(), event.payload.generation())).collect();
    for event in events {
        assert_eq!(event.payload.entity_id, id);
        for (generation, parent) in &event.payload.parent {
            assert_eq!(generations.get(parent), Some(generation), "every stored event has its complete parent history");
        }
    }
    for (generation, head) in &node.storage.get_state(id).await?.payload.state.head {
        assert_eq!(generations.get(head), Some(generation), "canonical state is justified by stored events");
    }
    Ok(())
}

async fn wait_for_head(node: &Durable, id: EntityId, expected: &proto::Clock) -> anyhow::Result<()> {
    within(async {
        loop {
            match node.storage.get_state(id).await {
                Ok(state) if state.payload.state.head == *expected => return Ok::<_, anyhow::Error>(()),
                Ok(_) | Err(RetrievalError::EntityNotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

async fn assert_absent(node: &Durable, id: EntityId) -> anyhow::Result<()> {
    assert!(matches!(node.storage.get_state(id).await, Err(RetrievalError::EntityNotFound(absent)) if absent == id));
    assert!(event_ids(node, id).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn cold_replication_keeps_complete_history_and_is_one_way() -> anyhow::Result<()> {
    let source = seed().await?;
    let (id, genesis) = create(&source, "first", "source").await?;
    let middle = edit(&source, id, Some("second"), None).await?;
    let last = edit(&source, id, Some("third"), None).await?;
    let target = replica(&source).await?;
    assert_absent(&target, id).await?;
    let (local, _) = create(&target, "local only", "target").await?;
    let _connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id, selection(&target, "true").await?, DEFAULT_CONTEXT)?;
    let covered = within(replication.wait_caught_up()).await?;
    assert_eq!(covered.digest.count(), 1);
    assert_eq!(target.storage.get_state(id).await?.payload.state, source.storage.get_state(id).await?.payload.state);
    assert_history(&target, id, &[genesis.id(), middle.id(), last.id()].into_iter().collect()).await?;
    assert_absent(&source, local).await?;

    let local_edit = edit(&target, local, Some("still local"), None).await?;
    assert_eq!(target.storage.get_state(local).await?.payload.state.head, proto::Clock::singleton(&local_edit));
    assert_absent(&source, local).await?;
    Ok(())
}

#[tokio::test]
async fn multiple_directional_interests_can_use_asymmetric_selections() -> anyhow::Result<()> {
    let left = seed().await?;
    let right = replica(&left).await?;
    let (to_right, rightward) = create(&left, "left record", "to right").await?;
    let (also_right, alternate) = create(&left, "another left record", "also right").await?;
    let (left_only, _) = create(&left, "left private", "stay left").await?;
    let (to_left, leftward) = create(&right, "to left", "right record").await?;
    let (right_only, _) = create(&right, "right private", "stay right").await?;
    let _connection = LocalProcessConnection::new(&left, &right).await?;
    let left_to_right = right.replicate_from(left.id, selection(&right, "artist = 'to right'").await?, DEFAULT_CONTEXT)?;
    let another_left_to_right = right.replicate_from(left.id, selection(&right, "artist = 'also right'").await?, DEFAULT_CONTEXT)?;
    let right_to_left = left.replicate_from(right.id, selection(&left, "title = 'to left'").await?, DEFAULT_CONTEXT)?;
    assert_eq!(within(left_to_right.wait_caught_up()).await?.digest.count(), 1);
    assert_eq!(within(another_left_to_right.wait_caught_up()).await?.digest.count(), 1);
    assert_eq!(within(right_to_left.wait_caught_up()).await?.digest.count(), 1);
    assert_history(&right, to_right, &[rightward.id()].into_iter().collect()).await?;
    assert_history(&right, also_right, &[alternate.id()].into_iter().collect()).await?;
    assert_history(&left, to_left, &[leftward.id()].into_iter().collect()).await?;
    assert_absent(&right, left_only).await?;
    assert_absent(&left, right_only).await?;
    Ok(())
}

#[tokio::test]
async fn reconnect_merges_offline_edits_and_recovers_a_missed_creation() -> anyhow::Result<()> {
    let left = seed().await?;
    let right = replica(&left).await?;
    let (id, genesis) = create(&left, "original title", "original artist").await?;
    let connection = LocalProcessConnection::new(&left, &right).await?;
    let selected = selection(&left, "true").await?;
    let left_to_right = right.replicate_from(left.id, selected.clone(), DEFAULT_CONTEXT)?;
    let right_to_left = left.replicate_from(right.id, selected, DEFAULT_CONTEXT)?;
    within(left_to_right.wait_caught_up()).await?;
    within(right_to_left.wait_caught_up()).await?;
    drop(connection);

    let left_edit = edit(&left, id, Some("left title"), None).await?;
    let right_edit = edit(&right, id, None, Some("right artist")).await?;
    assert_eq!(left_edit.parent, right_edit.parent);
    let (missed, missed_genesis) = create(&left, "missed creation", "left").await?;
    let missed_edit = edit(&left, missed, Some("later update"), None).await?;
    assert_absent(&right, missed).await?;

    let _connection = LocalProcessConnection::new(&left, &right).await?;
    let merged = proto::Clock::new(vec![(left_edit.generation(), left_edit.id()), (right_edit.generation(), right_edit.id())])?;
    wait_for_head(&left, id, &merged).await?;
    wait_for_head(&right, id, &merged).await?;
    wait_for_head(&right, missed, &proto::Clock::singleton(&missed_edit)).await?;
    for node in [&left, &right] {
        let record = node.context_async(DEFAULT_CONTEXT).await?.get::<RecordView>(id).await?;
        assert_eq!(record.title()?, "left title");
        assert_eq!(record.artist()?, "right artist");
        assert_history(node, id, &[genesis.id(), left_edit.id(), right_edit.id()].into_iter().collect()).await?;
    }
    assert_history(&right, missed, &[missed_genesis.id(), missed_edit.id()].into_iter().collect()).await?;
    Ok(())
}

#[tokio::test]
async fn departures_retain_history_and_reentry_recovers_intervening_events() -> anyhow::Result<()> {
    let source = seed().await?;
    let target = replica(&source).await?;
    let (id, genesis) = create(&source, "included", "first").await?;
    let connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id, selection(&target, "title = 'included'").await?, DEFAULT_CONTEXT)?;
    within(replication.wait_caught_up()).await?;
    drop(connection);

    let departed = edit(&source, id, Some("excluded"), None).await?;
    let connection = LocalProcessConnection::new(&source, &target).await?;
    wait_for_head(&target, id, &proto::Clock::singleton(&departed)).await?;
    assert!(target.storage.fetch_states(&selection(&target, "title = 'included'").await?).await?.is_empty());
    assert_history(&target, id, &[genesis.id(), departed.id()].into_iter().collect()).await?;
    drop(connection);

    let outside = edit(&source, id, None, Some("changed while outside")).await?;
    let reentered = edit(&source, id, Some("included"), None).await?;
    let _connection = LocalProcessConnection::new(&source, &target).await?;
    wait_for_head(&target, id, &proto::Clock::singleton(&reentered)).await?;
    assert_history(&target, id, &[genesis.id(), departed.id(), outside.id(), reentered.id()].into_iter().collect()).await?;
    assert_eq!(target.context_async(DEFAULT_CONTEXT).await?.get::<RecordView>(id).await?.artist()?, "changed while outside");
    Ok(())
}

#[tokio::test]
async fn reopening_storage_and_recreating_an_interest_repairs_only_missing_history() -> anyhow::Result<()> {
    let source = seed().await?;
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(SledStorageEngine::with_path(directory.path().to_path_buf())?);
    let target = replica_in(&source, storage.clone()).await?;
    let (id, genesis) = create(&source, "before restart", "source").await?;
    let selected = selection(&target, "true").await?;
    let connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id, selected.clone(), DEFAULT_CONTEXT)?;
    within(replication.wait_caught_up()).await?;
    assert_history(&target, id, &[genesis.id()].into_iter().collect()).await?;
    drop(replication);
    drop(connection);
    drop(target);
    within(async {
        while Arc::strong_count(&storage) != 1 {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?;
    drop(storage);

    let updated = edit(&source, id, Some("after restart"), None).await?;
    let reopened = Node::new_durable(Arc::new(SledStorageEngine::with_path(directory.path().to_path_buf())?), PermissiveAgent::new());
    within(reopened.wait_ready()).await?;
    assert_eq!(reopened.storage.get_state(id).await?.payload.state.head, proto::Clock::singleton(&genesis));
    assert_history(&reopened, id, &[genesis.id()].into_iter().collect()).await?;
    let sent = Arc::new(Mutex::new(Vec::new()));
    let recorded = sent.clone();
    let (_connection, _gate) = GatedConnection::new(&source, &reopened, move |message| {
        if let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. }) = message {
            for delta in deltas.iter().filter(|delta| delta.entity_id == id) {
                let proto::DeltaContent::EventBridge { events } = &delta.content else { panic!("durable repair must carry history") };
                recorded.lock().unwrap().extend(events.iter().map(|fragment| {
                    let event: proto::Attested<proto::Event> = (id, fragment.clone()).into();
                    event.payload.id()
                }));
            }
        }
        false
    })
    .await;
    let replication = reopened.replicate_from(source.id, selected, DEFAULT_CONTEXT)?;
    within(replication.wait_caught_up()).await?;
    assert_eq!(reopened.storage.get_state(id).await?.payload.state.head, proto::Clock::singleton(&updated));
    assert_history(&reopened, id, &[genesis.id(), updated.id()].into_iter().collect()).await?;
    assert_eq!(*sent.lock().unwrap(), vec![updated.id()], "restart retains the accepted ancestry and repairs only the missing suffix");
    Ok(())
}

#[tokio::test]
async fn a_slow_replica_does_not_delay_local_durable_commits() -> anyhow::Result<()> {
    let source = seed().await?;
    let target = replica(&source).await?;
    let (id, genesis) = create(&source, "initial", "source").await?;
    let hold = Arc::new(AtomicBool::new(true));
    let commits = Arc::new(AtomicUsize::new(0));
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let holding = hold.clone();
    let relayed_commits = commits.clone();
    let (_connection, gate) = GatedConnection::new(&source, &target, move |message| match message {
        proto::NodeMessage::Request {
            request: proto::NodeRequest { body: proto::NodeRequestBody::CommitTransaction { .. }, .. }, ..
        } => {
            relayed_commits.fetch_add(1, Ordering::SeqCst);
            true
        }
        proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. })
            if deltas.iter().any(|delta| delta.entity_id == id) && holding.load(Ordering::SeqCst) =>
        {
            sent.send(()).unwrap();
            true
        }
        _ => false,
    })
    .await;
    let replication = target.replicate_from(source.id, selection(&target, "true").await?, DEFAULT_CONTEXT)?;
    tokio::time::timeout(Duration::from_secs(5), received.recv()).await?.expect("a repair was held");
    assert_absent(&target, id).await?;
    let updated = tokio::time::timeout(Duration::from_secs(1), edit(&source, id, Some("committed locally"), None)).await??;
    assert_eq!(source.storage.get_state(id).await?.payload.state.head, proto::Clock::singleton(&updated));
    assert_eq!(commits.load(Ordering::SeqCst), 0, "durable writes do not use the synchronous commit relay");
    assert_absent(&target, id).await?;
    hold.store(false, Ordering::SeqCst);
    gate.release_held(&target).await;
    wait_for_head(&target, id, &proto::Clock::singleton(&updated)).await?;
    within(replication.wait_caught_up()).await?;
    assert_history(&target, id, &[genesis.id(), updated.id()].into_iter().collect()).await?;
    Ok(())
}
