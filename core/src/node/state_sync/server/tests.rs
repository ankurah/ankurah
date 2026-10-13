use super::*;
use crate::storage::StorageTransaction;
use crate::{
    connector::{PeerSender, SendError},
    policy::PermissiveAgent,
    test_utils::TestStorage,
};

type TestNode = Node<TestStorage, PermissiveAgent>;
#[derive(Clone)]
struct CaptureSender {
    peer: proto::EntityId,
    messages: Arc<Mutex<Vec<proto::NodeMessage>>>,
}
impl PeerSender for CaptureSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.peer }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}
async fn node() -> anyhow::Result<(TestNode, proto::EntityId, Arc<Mutex<Vec<proto::NodeMessage>>>)> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let peer = proto::EntityId::random();
    let messages = Arc::new(Mutex::new(Vec::new()));
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(CaptureSender { peer, messages: messages.clone() }),
    )
    .await?;
    Ok((node, peer, messages))
}
fn spec() -> proto::TreeSpec { proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True) }
async fn snapshot(states: Vec<proto::Attested<proto::EntityState>>) -> anyhow::Result<Snapshot> {
    super::super::QueryTree::default().refresh(spec(), states).await
}
fn states(count: usize, payload: usize) -> Vec<proto::Attested<proto::EntityState>> {
    (0..count)
        .map(|_| proto::Attested {
            payload: proto::EntityState {
                entity_id: proto::EntityId::random(),
                state: proto::State {
                    head: proto::Clock::genesis(proto::EventId::from_bytes([1; 32])),
                    state_buffers: proto::StateBuffers([("lww".into(), vec![7; payload])].into()),
                    ..Default::default()
                },
            },
            attestations: Default::default(),
        })
        .collect()
}
fn state_items(frame: &StateFrame) -> (&[StateItem], ComparisonProgress) {
    let StateFrameBody::State { items, progress } = &frame.body else { panic!("state frame") };
    (items, *progress)
}
fn live_item(bytes: usize) -> proto::SubscriptionUpdateItem {
    let state = states(1, bytes).pop().unwrap();
    proto::SubscriptionUpdateItem {
        delta: proto::EntityDelta {
            entity_id: state.payload.entity_id,
            content: proto::DeltaContent::StateSnapshot { state: state.into() },
        },
        predicate_relevance: vec![(proto::QueryId::new(), proto::MembershipChange::Add)],
    }
}

#[tokio::test]
async fn small_cold_and_inline_partial_inventories_complete_in_frame_zero() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let source = states(8, 16);
    let mut cached = snapshot(source[..4].to_vec()).await?;
    let known = cached.offer().await?;
    for (known, repairs) in [(None, 8), (Some(known), 4)] {
        let initial = node
            .start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(source.clone()).await?, known)
            .await?;
        let (items, progress) = state_items(&initial.frame);
        assert_eq!(initial.frame.sequence, 0);
        assert_eq!(progress, ComparisonProgress::Complete);
        assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Repair { .. })).count(), repairs);
        assert!(!items.iter().any(|item| matches!(item, StateItem::NeedHeads(_) | StateItem::NeedLeaves(_) | StateItem::Summary(_))));
    }
    Ok(())
}

#[tokio::test]
async fn empty_inventory_is_complete_without_questions() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(vec![]).await?, None).await?;
    let (items, progress) = state_items(&initial.frame);
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items, [StateItem::Leaves(page)] if page.leaves.is_empty()));
    Ok(())
}

#[tokio::test]
async fn initial_replay_is_identical_even_while_producer_is_locked_and_retires_after_credit() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let origin = proto::RequestId::new();
    let query = proto::QueryId::new();
    let initial =
        node.start_comparison(peer, origin.clone(), vec![11], vec![], Some((query, 7)), false, snapshot(vec![]).await?, None).await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let session = connection.comparisons.get(&initial.frame.comparison).unwrap();
    let guard = session.state.lock().await;
    let replay = connection.comparisons.replay_initial(&origin, &[11], &[])?.unwrap();
    let proto::NodeResponseBody::QuerySubscribed { query_id, state } = replay else { panic!("subscription replay") };
    assert_eq!(query_id, query);
    assert_eq!(bincode::serialize(&state)?, bincode::serialize(&initial)?);
    assert!(connection.comparisons.replay_initial(&origin, &[12], &[]).is_err());
    drop(guard);
    node.continue_state(peer, proto::RequestId::new(), initial.frame.comparison, 0, vec![], vec![]).await?;
    assert!(matches!(
        connection.comparisons.replay_initial(&origin, &[11], &[])?,
        Some(proto::NodeResponseBody::ComparisonUnavailable { .. })
    ));
    node.close_state(peer, initial.frame.comparison, proto::ComparisonOutcome::Abandoned, vec![]).await?;
    assert!(matches!(
        connection.comparisons.replay_initial(&origin, &[11], &[])?,
        Some(proto::NodeResponseBody::ComparisonUnavailable { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn cumulative_receipts_are_idempotent_and_cannot_grant_unsent_credit() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(vec![]).await?, None).await?;
    let id = initial.frame.comparison;
    let request = proto::RequestId::new();
    node.continue_state(peer, request.clone(), id, 0, vec![], vec![]).await?;
    node.continue_state(peer, request.clone(), id, 0, vec![], vec![]).await?;
    assert!(node.continue_state(peer, request, id, 1, vec![], vec![]).await.is_err());
    assert!(node.continue_state(peer, proto::RequestId::new(), id, 1, vec![], vec![]).await.is_err());
    node.close_state(peer, id, proto::ComparisonOutcome::Applied { processed_through: 0 }, vec![]).await?;
    assert!(matches!(node.continue_state(peer, proto::RequestId::new(), id, 0, vec![], vec![]).await?, proto::NodeResponseBody::Success));
    assert!(matches!(
        node.continue_state(peer, proto::RequestId::new(), id, 0, vec![StateAnswer::Heads { known: vec![], unknown: vec![] }], vec![])
            .await?,
        proto::NodeResponseBody::ComparisonUnavailable { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn cold_producer_stops_at_window_and_resumes_with_contiguous_frames() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let initial = node
        .start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(states(200, PAGE_BYTES / 2)).await?, None)
        .await?;
    let id = initial.frame.comparison;
    node.pump_comparison(peer, id).await?;
    let session = node.peer_connections.get(&peer).unwrap().comparisons.get(&id).unwrap();
    {
        let state = session.state.lock().await;
        assert!(state.bytes <= WINDOW_BYTES);
        assert!(state.retained.len() <= MAX_INFLIGHT_FRAMES - 2);
        assert!(!state.room(false));
        assert!(state.room(true), "ordinary output must leave a control/repair reserve");
        assert!(!state.comparison.as_ref().unwrap().complete);
    }
    loop {
        let (last, done) = {
            let state = session.state.lock().await;
            (state.last_sent, state.comparison.as_ref().unwrap().complete)
        };
        if done {
            break;
        }
        node.continue_state(peer, proto::RequestId::new(), id, last, vec![], vec![]).await?;
        node.pump_comparison(peer, id).await?;
    }
    let sequences = messages
        .lock()
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. })
                if frame.comparison == id =>
            {
                Some(frame.sequence)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(sequences.len() > MAX_INFLIGHT_FRAMES);
    assert!(sequences.iter().copied().eq(1..=sequences.len() as u64));
    Ok(())
}

#[tokio::test]
async fn evicted_initial_replay_details_never_allow_the_origin_to_create_a_second_stream() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let first = proto::RequestId::new();
    let mut first_id = None;
    for index in 0..MAX_COMPARISONS + 2 {
        let request = if index == 0 { first.clone() } else { proto::RequestId::new() };
        let initial = node.start_comparison(peer, request, vec![], vec![], None, false, snapshot(vec![]).await?, None).await?;
        first_id.get_or_insert(initial.frame.comparison);
        node.close_state(peer, initial.frame.comparison, proto::ComparisonOutcome::Applied { processed_through: 0 }, vec![]).await?;
    }
    let connection = node.peer_connections.get(&peer).unwrap();
    assert!(
        matches!(connection.comparisons.replay_initial(&first, &[], &[])?, Some(proto::NodeResponseBody::ComparisonUnavailable { comparison }) if Some(comparison) == first_id)
    );
    assert!(node.start_comparison(peer, first, vec![], vec![], None, false, snapshot(vec![]).await?, None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn complete_holds_live_until_applied_then_uses_same_sequence_and_typed_chunks() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let origin = proto::RequestId::new();
    let initial = node
        .start_comparison(peer, origin.clone(), vec![], vec![], Some((proto::QueryId::new(), 1)), false, snapshot(vec![]).await?, None)
        .await?;
    let id = initial.frame.comparison;
    let oversized = live_item(PAGE_BYTES * 2);
    let entity = oversized.delta.entity_id;
    let relevance = oversized.predicate_relevance.clone();
    let encoded = bincode::serialize(&oversized.delta.content)?;
    node.queue_live(peer, id, vec![oversized, live_item(1)])?;
    node.pump_comparison(peer, id).await?;
    assert!(!messages.lock().unwrap().iter().any(|message| matches!(message, proto::NodeMessage::Update(_))));
    node.close_state(peer, id, proto::ComparisonOutcome::Applied { processed_through: 0 }, vec![]).await?;
    node.pump_comparison(peer, id).await?;
    let mut assembled = Vec::new();
    let mut live_seen = false;
    for message in messages.lock().unwrap().iter() {
        let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { request_id, frame }, .. }) = message
        else {
            continue;
        };
        assert_eq!(request_id, &origin);
        assert_eq!(frame.comparison, id);
        match &frame.body {
            StateFrameBody::LiveChunk { chunk, predicate_relevance } => {
                assert!(!live_seen, "live item cannot interleave a chunk assembly");
                assert_eq!(chunk.entity_id, entity);
                assert_eq!(chunk.offset as usize, assembled.len());
                assert_eq!(predicate_relevance, &relevance);
                assembled.extend_from_slice(&chunk.bytes);
            }
            StateFrameBody::Live(_) => {
                assert_eq!(assembled, encoded);
                live_seen = true;
            }
            _ => panic!("unexpected post-Applied frame"),
        }
    }
    assert!(live_seen);
    let session = node.peer_connections.get(&peer).unwrap().comparisons.get(&id).unwrap();
    let state = session.state.lock().await;
    assert!(state.applied && state.comparison.is_none());
    assert!(state.bytes > 0, "live frames consume the same retained byte window");
    Ok(())
}

#[tokio::test]
async fn unavailable_late_repair_cannot_be_acknowledged_as_applied() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let source = states(1, 1);
    let initial =
        node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(source.clone()).await?, None).await?;
    let id = initial.frame.comparison;
    let entity = &source[0].payload;
    node.continue_state(
        peer,
        proto::RequestId::new(),
        id,
        0,
        vec![StateAnswer::NeedRepair { entity_id: entity.entity_id, target: entity.state.head.clone(), actual: proto::Clock::default() }],
        vec![],
    )
    .await?;
    node.pump_comparison(peer, id).await?;
    let session = node.peer_connections.get(&peer).unwrap().comparisons.get(&id).unwrap();
    let state = session.state.lock().await;
    let frame = &state.retained.back().unwrap().frame;
    assert!(state_items(frame).0.iter().any(|item| matches!(item, StateItem::RepairUnavailable { .. })));
    let last = state.last_sent;
    drop(state);
    assert!(node.close_state(peer, id, proto::ComparisonOutcome::Applied { processed_through: last }, vec![]).await.is_err());
    Ok(())
}

#[tokio::test]
async fn target_history_supplies_both_concurrent_branches_parent_first() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let left = proto::Event::update(
        genesis.entity_id,
        proto::Clock::singleton(&genesis),
        proto::AuthorId::Unknown,
        proto::OperationSet::default(),
    );
    let right = proto::Event::update(
        genesis.entity_id,
        proto::Clock::singleton(&genesis),
        proto::AuthorId::Unknown,
        proto::OperationSet::default(),
    );
    let events = vec![
        proto::Attested::opt(genesis.clone(), None),
        proto::Attested::opt(left.clone(), None),
        proto::Attested::opt(right.clone(), None),
    ];
    let mut trx = node.storage.transaction();
    trx.add_events(&events).await?;
    trx.commit().await?;
    let target = proto::Clock::from_events([&left, &right]);
    let source = proto::Attested::opt(
        proto::EntityState { entity_id: genesis.entity_id, state: proto::State { head: target.clone(), ..Default::default() } },
        None,
    );
    let initial =
        node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(vec![source]).await?, None).await?;
    node.continue_state(
        peer,
        proto::RequestId::new(),
        initial.frame.comparison,
        0,
        vec![StateAnswer::NeedRepair { entity_id: genesis.entity_id, target, actual: proto::Clock::singleton(&left) }],
        vec![],
    )
    .await?;
    node.pump_comparison(peer, initial.frame.comparison).await?;
    let session = node.peer_connections.get(&peer).unwrap().comparisons.get(&initial.frame.comparison).unwrap();
    let state = session.state.lock().await;
    let items = state_items(&state.retained.back().unwrap().frame).0;
    let StateItem::Repair { delta, .. } = &items[0] else { panic!("history repair") };
    let proto::DeltaContent::EventBridge { events } = &delta.content else { panic!("must not resend snapshot") };
    assert_eq!(events.len(), 3);
    assert!(events[0].parent.is_empty());
    let ids = events
        .iter()
        .map(|fragment| proto::Attested::<proto::Event>::from((genesis.entity_id, fragment.clone())).payload.id())
        .collect::<HashSet<_>>();
    assert!(ids.contains(&left.id()) && ids.contains(&right.id()) && ids.contains(&genesis.id()));
    Ok(())
}
