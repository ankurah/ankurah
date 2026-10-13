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
async fn mismatched_frontier_reuses_exact_bounds_then_refines_and_pages_terminal_buckets() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(256, 8);
    for (i, state) in source.iter_mut().enumerate() {
        let mut id = [0; proto::EntityId::BYTE_LEN];
        id[0] = if i < 128 { 0x20 } else { 0xa0 };
        id[31] = (i % 128) as u8;
        state.payload.entity_id = proto::EntityId::from_bytes(id);
    }
    let missing = source[0].payload.entity_id;
    let mut responder = snapshot(source.clone()).await?;
    let root = responder.head(super::super::whole()).await?;
    let frontier = responder.refine(&root).await?;
    let original =
        frontier.iter().find(|head| head.digest.count() > LEAF_PAGE as i64 && head.bounds.contains(&missing.to_bytes())).unwrap();
    let bounds = original.bounds.clone();
    let clipped = responder.head(bounds.clone()).await?;
    let refined = responder.refine(&clipped).await?;
    assert!(
        matches!(refined.as_slice(), [head] if head.bounds == bounds && head.prefix != clipped.prefix),
        "fixture must expose a same-bounds responder subtree"
    );
    let cached = source.iter().filter(|state| state.payload.entity_id != missing).cloned().collect::<Vec<_>>();
    let mut requester = snapshot(cached.clone()).await?;
    let mut regions = Vec::new();
    for head in frontier {
        regions.push(proto::KnownRegion::Summary(requester.head(head.bounds).await?));
    }
    let known = proto::KnownState { spec: spec(), regions };
    let mut comparison = Comparison::new(responder, Some(known))?;
    let mut repairs = Vec::new();
    let mut narrower_questions = 0;
    let mut leaf_pages = 0;
    let mut finished = false;
    for _ in 0..64 {
        let body = comparison.reply(&node, &vec![], false).await?;
        assert!(bincode::serialized_size(&body)? <= FRAME_BYTES as u64);
        let StateFrameBody::State { items, progress } = body else { panic!("state frame") };
        let mut answers = Vec::new();
        for item in items {
            match item {
                StateItem::Summary(head) => {
                    assert_ne!(head.bounds, bounds, "the requester already supplied this exact digest");
                    narrower_questions += 1;
                    answers.push(StateAnswer::Summary(requester.head(head.bounds).await?));
                }
                StateItem::NeedLeaves(bounds) => {
                    leaf_pages += 1;
                    answers.push(StateAnswer::Leaves(requester.page(bounds).await?));
                }
                StateItem::NeedHeads(ids) => {
                    let mut known = Vec::new();
                    let mut unknown = Vec::new();
                    for entity_id in ids {
                        if let Some(state) = cached.iter().find(|state| state.payload.entity_id == entity_id) {
                            known.push(proto::KnownEntity { entity_id, head: state.payload.state.head.clone() });
                        } else {
                            unknown.push(entity_id);
                        }
                    }
                    answers.push(StateAnswer::Heads { known, unknown });
                }
                StateItem::Repair { delta, .. } => repairs.push(delta.entity_id),
                StateItem::Equal(_) | StateItem::Leaves(_) => {}
                other => panic!("unexpected frontier comparison item: {other:?}"),
            }
        }
        if progress == ComparisonProgress::Complete {
            assert!(answers.is_empty());
            finished = true;
            break;
        }
        comparison.answer(answers).await?;
    }
    assert!(finished, "comparison must finish within bounded work turns");
    assert!(narrower_questions >= 2, "mixed children and terminal page bounds still require their own digests");
    assert!(leaf_pages >= 1, "terminal mismatch must still request the requester's bounded inventory");
    assert_eq!(repairs, [missing], "only the uncached entity needs repair");
    Ok(())
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
        let transferred = items
            .iter()
            .map(|item| match item {
                StateItem::Repair { .. } => 1,
                StateItem::Snapshots(page) => page.states.len(),
                _ => 0,
            })
            .sum::<usize>();
        assert_eq!(transferred, repairs);
        assert!(!items.iter().any(|item| matches!(item, StateItem::NeedHeads(_) | StateItem::NeedLeaves(_) | StateItem::Summary(_))));
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_pages_tile_gaps_split_by_bytes_and_resume_after_an_oversized_row() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(5, PAGE_BYTES / 4);
    for (index, state) in source.iter_mut().enumerate() {
        state.payload.entity_id = proto::EntityId::from_bytes([index as u8 + 1; 32]);
    }
    source[2].payload.state.state_buffers.0.insert("lww".into(), vec![9; PAGE_BYTES * 2]);
    let large = source[2].clone();
    let mut comparison = Comparison::new(snapshot(source.clone()).await?, None)?;
    let mut start = super::super::whole().start;
    let mut received = Vec::new();
    let mut chunks = Vec::new();
    let mut inline_after_chunks = false;
    while !comparison.complete {
        let body = comparison.reply(&node, &vec![], false).await?;
        assert!(bincode::serialized_size(&body)? <= FRAME_BYTES as u64);
        let StateFrameBody::State { items, .. } = body else { panic!("state frame") };
        for item in items {
            let bounds = match item {
                StateItem::Snapshots(page) => {
                    assert!(page.states.len() < super::super::MAX_SNAPSHOT_ROWS, "bytes, not row count, split these pages");
                    if !chunks.is_empty() {
                        inline_after_chunks = true;
                    }
                    received.extend(page.states.iter().map(|state| state.payload.entity_id));
                    Some(page.bounds)
                }
                StateItem::Leaves(page) => {
                    assert_eq!(page.leaves.len(), 1);
                    assert_eq!(page.leaves[0].entity.entity_id, large.payload.entity_id);
                    received.push(page.leaves[0].entity.entity_id);
                    Some(page.bounds)
                }
                StateItem::RepairChunk { target, chunk } => {
                    assert_eq!(chunk.entity_id, large.payload.entity_id);
                    assert_eq!(target, large.payload.state.head);
                    assert_eq!(chunk.offset as usize, chunks.len());
                    chunks.extend_from_slice(&chunk.bytes);
                    None
                }
                other => panic!("unexpected cold item: {other:?}"),
            };
            if let Some(bounds) = bounds {
                assert_eq!(bounds.start, start, "page coverage must have no gaps or overlap");
                start = opposite(&bounds.end);
            }
        }
    }
    assert_eq!(start, std::ops::Bound::Unbounded, "the final page includes the trailing gap");
    assert_eq!(received, source.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>());
    assert!(inline_after_chunks);
    let decoded: proto::DeltaContent = bincode::deserialize(&chunks)?;
    let proto::DeltaContent::StateSnapshot { state } = decoded else { panic!("chunked snapshot") };
    assert_eq!(state.state, large.payload.state);
    for state in &source {
        assert_eq!(comparison.offered.get(&state.payload.entity_id), Some(&state.payload.state.head));
    }
    comparison
        .answer(vec![StateAnswer::NeedRepair {
            entity_id: source[0].payload.entity_id,
            target: source[0].payload.state.head.clone(),
            actual: Default::default(),
        }])
        .await?;
    assert!(comparison.pending_repairs.contains(&source[0].payload.entity_id));
    Ok(())
}

#[tokio::test]
async fn snapshot_page_keeps_a_supplied_clock_for_a_member_that_moved_from_another_range() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let source = states(1, 10);
    let id = source[0].payload.entity_id;
    let mut comparison = Comparison::new(snapshot(source.clone()).await?, None)?;
    comparison.known_clocks.insert(id, source[0].payload.state.head.clone());
    let StateFrameBody::State { items, progress } = comparison.reply(&node, &vec![], false).await? else { panic!("state frame") };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items.as_slice(), [StateItem::Leaves(page)] if page.leaves[0].entity.entity_id == id));
    assert!(comparison.offered.is_empty(), "equal supplied state must not be sent again");
    let mut comparison = Comparison::new(snapshot(source).await?, None)?;
    assert!(comparison.reply(&node, &vec![], true).await.is_err(), "an exhaustive catalog page cannot skip an inadmissible row");
    Ok(())
}

#[tokio::test]
async fn empty_inventory_is_complete_without_questions() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = node.start_comparison(peer, proto::RequestId::new(), vec![], vec![], None, false, snapshot(vec![]).await?, None).await?;
    let (items, progress) = state_items(&initial.frame);
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items, [StateItem::Snapshots(page)] if page.states.is_empty()));
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

#[tokio::test]
async fn live_queue_exhaustion_sends_restart_without_emitting_partial_live_output() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let query = proto::QueryId::new();
    let initial = node
        .start_comparison(peer, proto::RequestId::new(), vec![], vec![], Some((query, 1)), false, snapshot(vec![]).await?, None)
        .await?;
    let id = initial.frame.comparison;
    let mut item = live_item(1);
    item.predicate_relevance = vec![(query, proto::MembershipChange::Add)];
    assert!(node.queue_live(peer, id, vec![item; MAX_LIVE_ITEMS + 1]).is_err());
    node.expire_comparison(peer, id).await?;
    let messages = messages.lock().unwrap();
    assert!(messages.iter().any(|message| matches!(message, proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame: StateFrame { comparison, body: StateFrameBody::RestartRequired, .. }, .. }, .. }) if *comparison == id)));
    assert!(!messages.iter().any(|message| matches!(
        message,
        proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::StateFrame {
                frame: StateFrame { body: StateFrameBody::Live(_) | StateFrameBody::LiveChunk { .. }, .. },
                ..
            },
            ..
        })
    )));
    drop(messages);
    assert!(matches!(node.close_state(peer, id, proto::ComparisonOutcome::Abandoned, vec![]).await?, proto::NodeResponseBody::Success));
    assert!(node.peer_connections.get(&peer).is_some(), "successful restart control preserves other streams on this connection");
    Ok(())
}

#[tokio::test]
async fn retired_controls_preserve_credential_checks_and_catalog_bypass() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    for catalog in [false, true] {
        let credentials = if catalog { vec![] } else { vec![crate::policy::DEFAULT_CONTEXT] };
        let initial = node
            .start_comparison(peer, proto::RequestId::new(), vec![], credentials.clone(), None, catalog, snapshot(vec![]).await?, None)
            .await?;
        let id = initial.frame.comparison;
        let receipt = || proto::NodeRequestBody::ContinueState { comparison: id, processed_through: 0, answers: vec![] };
        assert_eq!(node.comparison_bypasses_policy(peer, &receipt(), false), catalog);
        let session = node.peer_connections.get(&peer).unwrap().comparisons.get(&id).unwrap();
        assert!(session.permits(&credentials));
        let authenticated = [crate::policy::DEFAULT_CONTEXT];
        assert!(!session.permits(if catalog { &authenticated } else { &[] }));
        node.close_state(peer, id, proto::ComparisonOutcome::Applied { processed_through: 0 }, credentials).await?;
        assert!(node.peer_connections.get(&peer).unwrap().comparisons.get(&id).is_none());
        assert_eq!(node.comparison_bypasses_policy(peer, &receipt(), false), catalog);

        // Ordinary policy yields one nonempty credential for this auth entry.
        // Catalog controls must retain their original credentialless bypass.
        for (auth, succeeds) in [(vec![proto::AuthData(vec![])], true), (vec![], catalog)] {
            let request = proto::RequestId::new();
            node.handle_message(proto::NodeMessage::Request {
                auth,
                request: proto::NodeRequest { id: request.clone(), from: peer, to: node.id, body: receipt() },
            })
            .await?;
            let messages = messages.lock().unwrap();
            let response = messages
                .iter()
                .find_map(|message| match message {
                    proto::NodeMessage::Response(response) if response.request_id == request => Some(&response.body),
                    _ => None,
                })
                .unwrap();
            assert_eq!(matches!(response, proto::NodeResponseBody::Success), succeeds);
            if !succeeds {
                assert!(matches!(response, proto::NodeResponseBody::ComparisonUnavailable { comparison } if *comparison == id));
            }
        }
    }
    let unknown = proto::NodeRequestBody::ContinueState { comparison: proto::ComparisonId::new(), processed_through: 0, answers: vec![] };
    assert!(!node.comparison_bypasses_policy(peer, &unknown, false), "unknown controls must pass through policy");
    Ok(())
}
