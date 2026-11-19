//! Directional replication between independently stored durable nodes.

mod common;

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah::core::{
    entity::Entity,
    error::{RetrievalError, ValidationError},
    node::{NodeInner, ReplicationState},
    policy::{AccessDenied, DefaultContext, PolicyAgent},
    storage::StateScan,
    util::Iterable,
};
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

/// Import shared catalog identities into an independently initialized node.
/// User data must arrive through replication; matching labels alone do not
/// unify models minted independently by two nodes.
async fn replica(seed: &Durable) -> anyhow::Result<Durable> { replica_in(seed, Arc::new(SledStorageEngine::new_test()?)).await }

async fn replica_in(seed: &Durable, storage: Arc<SledStorageEngine>) -> anyhow::Result<Durable> {
    assert!(!Arc::ptr_eq(&seed.storage, &storage));
    let node = Node::new_durable(storage, PermissiveAgent::new());
    within(node.wait_ready()).await?;
    let mut states = BTreeMap::new();
    for model in [proto::SystemModel::Model, proto::SystemModel::Property, proto::SystemModel::ModelProperty] {
        for state in seed.storage.fetch_states(&Predicate::MemberOf(proto::ModelId::System(model)).into()).await? {
            states.insert(state.payload.entity_id, state);
        }
    }
    for state in states.values() {
        let mut history = seed.storage.dump_entity_events(state.payload.entity_id).await?;
        assert!(!history.is_empty(), "provisioning must include supporting events");
        history.sort_by_key(|event| event.payload.generation());
        node.import_events(&history).await?;
    }
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

async fn create_many(node: &Durable, count: usize) -> anyhow::Result<Vec<EntityId>> {
    let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
    let mut ids = Vec::new();
    for index in 0..count {
        ids.push(transaction.create(&Record { title: format!("record {index}"), artist: format!("{index:03}") }).await?.id());
    }
    transaction.commit().await?;
    Ok(ids)
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
    let replication = target.replicate_from(source.id().unwrap(), selection(&target, "true").await?, DEFAULT_CONTEXT)?;
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
    let left_to_right = right.replicate_from(left.id().unwrap(), selection(&right, "artist = 'to right'").await?, DEFAULT_CONTEXT)?;
    let another_left_to_right =
        right.replicate_from(left.id().unwrap(), selection(&right, "artist = 'also right'").await?, DEFAULT_CONTEXT)?;
    let right_to_left = left.replicate_from(right.id().unwrap(), selection(&left, "title = 'to left'").await?, DEFAULT_CONTEXT)?;
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
    let left_to_right = right.replicate_from(left.id().unwrap(), selected.clone(), DEFAULT_CONTEXT)?;
    let right_to_left = left.replicate_from(right.id().unwrap(), selected, DEFAULT_CONTEXT)?;
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
    let replication = target.replicate_from(source.id().unwrap(), selection(&target, "title = 'included'").await?, DEFAULT_CONTEXT)?;
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
    let replication = target.replicate_from(source.id().unwrap(), selected.clone(), DEFAULT_CONTEXT)?;
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
    let replication = reopened.replicate_from(source.id().unwrap(), selected, DEFAULT_CONTEXT)?;
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
    let replication = target.replicate_from(source.id().unwrap(), selection(&target, "true").await?, DEFAULT_CONTEXT)?;
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

async fn next_delivery(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) -> anyhow::Result<Vec<u8>> {
    within(async { receiver.recv().await.ok_or_else(|| anyhow::anyhow!("repair recorder closed")) }).await
}

fn repair_delivery(message: &[u8]) -> anyhow::Result<(proto::ComparisonId, Vec<EntityId>)> {
    let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { comparison, deltas }, .. }) =
        bincode::deserialize(message)?
    else {
        anyhow::bail!("expected comparison repair delivery")
    };
    Ok((comparison, deltas.into_iter().map(|delta| delta.entity_id).collect()))
}

#[tokio::test]
async fn cancelled_bootstrap_rejects_late_deliveries_and_resumes_committed_progress_after_restart() -> anyhow::Result<()> {
    let source = seed().await?;
    let ids: HashSet<_> = create_many(&source, 100).await?.into_iter().collect();
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(SledStorageEngine::with_path(directory.path().to_path_buf())?);
    let target = replica_in(&source, storage.clone()).await?;
    let selected = selection(&target, "true").await?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let data_ids = ids.clone();
    let (connection, gate) = GatedConnection::new(&source, &target, move |message| {
        if matches!(message, proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::Comparison { deltas, .. }, ..
        }) if deltas.iter().any(|delta| data_ids.contains(&delta.entity_id)))
        {
            sent.send(bincode::serialize(message).unwrap()).unwrap();
            return true;
        }
        false
    })
    .await;
    let replication = target.replicate_from(source.id().unwrap(), selected.clone(), DEFAULT_CONTEXT)?;
    let first = next_delivery(&mut received).await?;
    let completed: HashSet<_> = repair_delivery(&first)?.1.into_iter().collect();
    assert!(!completed.is_empty() && completed.len() < ids.len());
    assert!(matches!(replication.state().peek(), ReplicationState::Comparing));
    gate.release_first(&target, 1).await;
    let late = next_delivery(&mut received).await?;
    let (comparison, outstanding) = repair_delivery(&late)?;
    assert!(matches!(replication.state().peek(), ReplicationState::Comparing), "partial progress does not establish coverage");
    for id in &completed {
        assert_history(&target, *id, &event_ids(&source, *id).await?).await?;
    }
    for id in &outstanding {
        assert_absent(&target, *id).await?;
    }
    drop(replication);
    // The release is sent after dropping the receiver's admission. Observing
    // Expired at the source establishes cancellation without a scheduler sleep.
    within(async {
        loop {
            let reply = target
                .request(
                    source.id().unwrap(),
                    &DEFAULT_CONTEXT,
                    proto::NodeRequestBody::Compare {
                        comparison,
                        selection: selected.clone(),
                        request: proto::ComparisonRequest::Pages { spans: vec![ankurah_digest_tree::Span::all()] },
                    },
                )
                .await?;
            if matches!(reply, proto::NodeResponseBody::Compared(Err(proto::ComparisonFailure::Expired))) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let before_replay = storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?;
    target.handle_message(target.connections_of(source.id().unwrap())[0], bincode::deserialize(&first)?).await?;
    gate.release_held(&target).await;
    assert_eq!(storage.state_snapshot(StateScan::Changes(None)).await?.finish().await?, before_replay);
    for id in &outstanding {
        assert_absent(&target, *id).await?;
    }
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

    let changed = *completed.iter().min().unwrap();
    edit(&source, changed, Some("changed during restart"), None).await?;
    let target = Node::new_durable(Arc::new(SledStorageEngine::with_path(directory.path().to_path_buf())?), PermissiveAgent::new());
    within(target.wait_ready()).await?;
    assert_eq!(target.storage.fetch_states(&selected).await?.len(), completed.len());
    let repaired = Arc::new(Mutex::new(Vec::new()));
    let recorded = repaired.clone();
    let data_ids = ids.clone();
    let (_connection, _gate) = GatedConnection::new(&source, &target, move |message| {
        if let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. }) = message {
            recorded.lock().unwrap().extend(deltas.iter().map(|delta| delta.entity_id).filter(|id| data_ids.contains(id)));
        }
        false
    })
    .await;
    let replication = target.replicate_from(source.id().unwrap(), selected, DEFAULT_CONTEXT)?;
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count() as usize, ids.len());
    let expected: HashSet<_> = ids.difference(&completed).copied().chain([changed]).collect();
    let received = repaired.lock().unwrap().clone();
    assert_eq!(received.len(), expected.len());
    assert_eq!(received.into_iter().collect::<HashSet<_>>(), expected, "completed unchanged members are not transferred again");
    for id in ids {
        assert_eq!(target.storage.get_state(id).await?.payload.state.head, source.storage.get_state(id).await?.payload.state.head);
        assert_history(&target, id, &event_ids(&source, id).await?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn limited_replication_refills_order_gaps_without_discarding_departed_history() -> anyhow::Result<()> {
    let source = seed().await?;
    let target = replica(&source).await?;
    let (first, _) = create(&source, "included", "00").await?;
    let (second, _) = create(&source, "included", "10").await?;
    let (third, _) = create(&source, "included", "20").await?;
    let selected = selection(&target, "title = 'included' ORDER BY artist ASC LIMIT 2").await?;
    let _connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id().unwrap(), selected.clone(), DEFAULT_CONTEXT)?;
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count(), 2);
    assert_absent(&target, third).await?;

    edit(&source, first, None, Some("30")).await?;
    replication.retry();
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count(), 2);
    assert_eq!(
        target.storage.fetch_states(&selected).await?.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(),
        vec![second, third]
    );
    edit(&source, second, Some("excluded"), None).await?;
    replication.retry();
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count(), 2);
    assert_eq!(
        target.storage.fetch_states(&selected).await?.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(),
        vec![third, first]
    );
    let (entered, _) = create(&source, "included", "05").await?;
    replication.retry();
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count(), 2);
    assert_eq!(
        target.storage.fetch_states(&selected).await?.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(),
        vec![entered, third]
    );
    for id in [first, second, third, entered] {
        assert_history(&target, id, &event_ids(&source, id).await?).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_causally_newer_local_prefix_does_not_starve_a_missing_source_suffix() -> anyhow::Result<()> {
    let source = seed().await?;
    let ids = create_many(&source, 100).await?;
    let target = replica(&source).await?;
    let connection = LocalProcessConnection::new(&source, &target).await?;
    let initial =
        target.replicate_from(source.id().unwrap(), selection(&target, "true ORDER BY artist ASC LIMIT 80").await?, DEFAULT_CONTEXT)?;
    assert_eq!(within(initial.wait_caught_up()).await?.digest.count(), 80);
    drop(initial);
    drop(connection);
    let mut expected_histories = BTreeMap::new();
    for id in &ids[..80] {
        let local = edit(&target, *id, Some("locally advanced"), None).await?;
        let mut expected = event_ids(&source, *id).await?;
        expected.insert(local.id());
        expected_histories.insert(*id, expected);
    }
    for id in &ids[80..] {
        assert_absent(&target, *id).await?;
    }

    let repaired = Arc::new(Mutex::new(Vec::new()));
    let recorded = repaired.clone();
    let data_ids: HashSet<_> = ids.iter().copied().collect();
    let (_connection, _gate) = GatedConnection::new(&source, &target, move |message| {
        if let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::Comparison { deltas, .. }, .. }) = message {
            recorded.lock().unwrap().extend(deltas.iter().map(|delta| delta.entity_id).filter(|id| data_ids.contains(id)));
        }
        false
    })
    .await;
    let replication =
        target.replicate_from(source.id().unwrap(), selection(&target, "true ORDER BY artist ASC").await?, DEFAULT_CONTEXT)?;
    assert_eq!(within(replication.wait_caught_up()).await?.digest.count(), 100);
    let repaired = repaired.lock().unwrap().clone();
    assert_eq!(repaired.len(), 20, "covered local descendants need no history repair");
    assert_eq!(repaired.into_iter().collect::<HashSet<_>>(), ids[80..].iter().copied().collect());
    for (index, id) in ids.iter().enumerate() {
        let record = target.context_async(DEFAULT_CONTEXT).await?.get::<RecordView>(*id).await?;
        assert_eq!(record.title()?, if index < 80 { "locally advanced".to_owned() } else { format!("record {index}") });
        let expected = match expected_histories.get(id) {
            Some(events) => events.clone(),
            None => event_ids(&source, *id).await?,
        };
        assert_history(&target, *id, &expected).await?;
    }
    Ok(())
}

/// States are readable, but one required ancestor may not leave the source.
#[derive(Clone)]
struct HiddenAncestor {
    denied: proto::EventId,
    repairs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl PolicyAgent for HiddenAncestor {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &NodeInner<SE, Self>,
        credentials: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(credentials.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        auth: &A,
        request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        if matches!(request.body, proto::NodeRequestBody::Compare { request: proto::ComparisonRequest::Repair { .. }, .. }) {
            self.repairs.fetch_add(1, Ordering::SeqCst);
        }
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn state_reads_follow_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> bool { true }

    fn check_read_event<C: Iterable<Self::ContextData>>(&self, _: &C, event: &proto::Attested<proto::Event>) -> Result<(), AccessDenied> {
        if event.payload.id() == self.denied {
            Err(AccessDenied::ByPolicy("ancestor is not readable"))
        } else {
            Ok(())
        }
    }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

#[tokio::test]
async fn a_denied_required_ancestor_stops_replication_without_persisting_partial_history() -> anyhow::Result<()> {
    let original = seed().await?;
    let target = replica(&original).await?;
    let (id, genesis) = create(&original, "readable state", "source").await?;
    edit(&original, id, Some("later readable state"), None).await?;
    let storage = original.storage.clone();
    drop(original);
    within(async {
        while Arc::strong_count(&storage) != 1 {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?;
    let repairs = Arc::new(AtomicUsize::new(0));
    let source = Node::new_durable(storage, HiddenAncestor { denied: genesis.id(), repairs: repairs.clone() });
    within(source.wait_ready()).await?;
    let _connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id().unwrap(), selection(&target, "true").await?, DEFAULT_CONTEXT)?;
    let refusal = tokio::time::timeout(Duration::from_secs(5), replication.wait_caught_up()).await?.unwrap_err();
    assert!(matches!(refusal, RetrievalError::StorageError(error)
        if error.downcast_ref::<proto::ComparisonFailure>() == Some(&proto::ComparisonFailure::MemberUnavailable)));
    assert!(matches!(replication.state().peek(), ReplicationState::Failed(_)));
    assert_eq!(repairs.load(Ordering::SeqCst), 3, "one bounded reconciliation attempt owns all retries");
    assert_absent(&target, id).await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(repairs.load(Ordering::SeqCst), 3, "a permanent refusal must not start a fresh polling budget");
    assert_absent(&target, id).await?;
    Ok(())
}

#[tokio::test]
async fn independent_durable_nodes_with_the_same_key_can_replicate() -> anyhow::Result<()> {
    let initialized = seed().await?;
    let provisioned = replica(&initialized).await?;
    let key = ankurah::NodeKey::generate();
    let source = Node::new_durable_with_key(initialized.storage.clone(), PermissiveAgent::new(), key.clone());
    let target = Node::new_durable_with_key(provisioned.storage.clone(), PermissiveAgent::new(), key);
    drop(initialized);
    drop(provisioned);
    within(source.wait_ready()).await?;
    within(target.wait_ready()).await?;
    assert_eq!(source.id(), target.id());
    assert!(!Arc::ptr_eq(&source.storage, &target.storage));
    let (id, genesis) = create(&source, "shared identity", "independent storage").await?;
    let _connection = LocalProcessConnection::new(&source, &target).await?;
    let replication = target.replicate_from(source.id().unwrap(), selection(&target, "true").await?, DEFAULT_CONTEXT)?;
    within(replication.wait_caught_up()).await?;
    assert_history(&target, id, &[genesis.id()].into_iter().collect()).await?;
    assert_eq!(target.storage.get_state(id).await?.payload.state, source.storage.get_state(id).await?.payload.state);
    Ok(())
}
