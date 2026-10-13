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
async fn summary_aggregate_equality_requires_an_exact_summary_only_cover() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(4096, 8);
    for (index, state) in source.iter_mut().enumerate() {
        let mut id = [0; proto::EntityId::BYTE_LEN];
        id[0] = 0x20;
        id[1..5].copy_from_slice(&(index as u32).to_be_bytes());
        state.payload.entity_id = proto::EntityId::from_bytes(id);
    }
    let mut requester = snapshot(source.clone()).await?;
    let known = requester.offer().await?;
    assert!(known.regions.len() > 1);
    let mut equal = Comparison::new(snapshot(source.clone()).await?, Some(known.clone()))?;
    assert!(matches!(equal.work.front(), Some(Work::CompareFrontier(_))));
    let StateFrameBody::State { items, progress } = equal.reply(&node, &vec![], false, false).await? else { panic!("state") };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items.as_slice(), [StateItem::Equal(bounds)] if *bounds == super::super::whole()));

    let empty =
        known.regions.iter().position(|region| matches!(region, proto::KnownRegion::Summary(head) if head.digest.count() == 0)).unwrap();
    let mut withheld = known.clone();
    withheld.regions.remove(empty);
    let partial = Comparison::new(snapshot(source.clone()).await?, Some(withheld))?;
    assert!(!partial.work.iter().any(|work| matches!(work, Work::CompareFrontier(_))), "a withheld gap cannot claim aggregate equality");

    let mut mixed = known.clone();
    let proto::KnownRegion::Summary(head) = &mixed.regions[empty] else { unreachable!() };
    mixed.regions[empty] = proto::KnownRegion::Leaves(requester.page(head.bounds.clone()).await?);
    let mixed = Comparison::new(snapshot(source.clone()).await?, Some(mixed))?;
    assert!(!mixed.work.iter().any(|work| matches!(work, Work::CompareFrontier(_))));

    let mut overlap = known;
    overlap.regions.push(overlap.regions[0].clone());
    assert!(Comparison::new(snapshot(source).await?, Some(overlap)).is_err());
    Ok(())
}

#[tokio::test]
async fn a_128_leaf_mismatch_repairs_unknown_and_former_members_without_heads_questions() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(128, 8);
    source.sort_by_key(|state| state.payload.entity_id);
    let former = source[0].payload.entity_id;
    let former_clock = source[0].payload.state.head.clone();
    let mut requester = snapshot(source.clone()).await?;
    let known =
        proto::KnownState { spec: spec(), regions: vec![proto::KnownRegion::Summary(requester.head(super::super::whole()).await?)] };
    let mut current = source[1..].to_vec();
    let added = states(1, 8).pop().unwrap();
    let new_id = added.payload.entity_id;
    current.push(added);
    let mut comparison = Comparison::new(snapshot(current).await?, Some(known))?;
    let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false).await? else { panic!("state") };
    assert!(matches!(items.as_slice(), [StateItem::NeedLeaves(bounds)] if *bounds == super::super::whole()));
    comparison.answer(vec![StateAnswer::Leaves(requester.page(super::super::whole()).await?)]).await?;
    let Some(Work::AnswerLeaves { page, rest }) = comparison.work.pop_front() else { panic!("deferred leaf answer") };
    comparison.expand_leaf_answer(page, rest).await?;
    assert!(comparison.work.iter().any(|work| matches!(work, Work::Repair(id, None) if *id == new_id)));
    assert!(comparison.work.iter().any(|work| matches!(work, Work::Repair(id, Some(clock)) if *id == former && *clock == former_clock)));
    assert!(!comparison.work.iter().any(|work| matches!(work, Work::Item(StateItem::NeedHeads(_)))));
    for state in &source[1..] {
        assert!(comparison.work.iter().any(
            |work| matches!(work, Work::Repair(id, Some(clock)) if *id == state.payload.entity_id && *clock == state.payload.state.head)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn wider_responder_clocks_split_outbound_inventory_without_repairing_shared_ids_twice() -> anyhow::Result<()> {
    let mut source = states(128, 8);
    source.sort_by_key(|state| state.payload.entity_id);
    let mut requester = snapshot(source.clone()).await?;
    let request = requester.page(super::super::whole()).await?;
    assert_eq!(request.leaves.len(), 128);
    let mut current = source.clone();
    for state in &mut current {
        state.payload.state.head = proto::Clock::new(
            (0..200u32)
                .map(|n| {
                    let mut bytes = [0; 32];
                    bytes[..4].copy_from_slice(&n.to_be_bytes());
                    (1, proto::EventId::from_bytes(bytes))
                })
                .collect::<Vec<_>>(),
        )?;
    }
    let mut comparison = Comparison::new(snapshot(current).await?, None)?;
    comparison.work.clear();
    comparison.leaves.push(super::super::whole());
    comparison.answer(vec![StateAnswer::Leaves(request)]).await?;
    let Some(Work::AnswerLeaves { page, rest }) = comparison.work.pop_front() else { panic!("deferred leaf answer") };
    comparison.expand_leaf_answer(page, rest).await?;
    let pages = comparison
        .work
        .iter()
        .filter_map(|work| match work {
            Work::Item(StateItem::Leaves(page)) => Some(page),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(pages.len() > 1);
    let mut start = super::super::whole().start;
    let mut ids = Vec::new();
    for page in pages {
        assert_eq!(page.bounds.start, start);
        assert!(bincode::serialized_size(&StateAnswer::Leaves(page.clone()))? <= PAGE_BYTES as u64);
        ids.extend(page.leaves.iter().map(|leaf| leaf.entity.entity_id));
        start = opposite(&page.bounds.end);
    }
    assert_eq!(start, super::super::whole().end);
    assert_eq!(ids, source.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>());
    let repairs = comparison
        .work
        .iter()
        .filter_map(|work| match work {
            Work::Repair(id, clock) => Some((*id, clock)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(repairs.len(), source.len());
    for (id, clock) in repairs {
        assert_eq!(clock.as_ref(), Some(&source.iter().find(|state| state.payload.entity_id == id).unwrap().payload.state.head));
    }
    Ok(())
}

#[tokio::test]
async fn valid_batched_leaf_answers_cannot_expand_the_work_queue_past_its_limit() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut current = states(4096, 8);
    for (index, state) in current.iter_mut().enumerate() {
        let mut id = [0; proto::EntityId::BYTE_LEN];
        id[..4].copy_from_slice(&(index as u32).to_be_bytes());
        state.payload.entity_id = proto::EntityId::from_bytes(id);
        state.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([2; 32]));
    }
    let mut before = current.clone();
    for state in &mut before {
        state.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([1; 32]));
    }
    let mut requester = snapshot(before).await?;
    let mut regions = Vec::new();
    let mut start = super::super::whole().start;
    for (index, chunk) in current.chunks(LEAF_PAGE).enumerate() {
        let end = if (index + 1) * LEAF_PAGE == current.len() {
            super::super::whole().end
        } else {
            std::ops::Bound::Included(chunk.last().unwrap().payload.entity_id.to_bytes().to_vec())
        };
        let bounds = AddressRange::new(start, end);
        start = opposite(&bounds.end);
        regions.push(proto::KnownRegion::Summary(requester.head(bounds).await?));
    }
    let mut comparison = Comparison::new(snapshot(current).await?, Some(proto::KnownState { spec: spec(), regions }))?;
    let mut answers = Vec::new();
    while !comparison.work.is_empty() {
        let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false).await? else { panic!("state") };
        for item in items {
            let StateItem::NeedLeaves(bounds) = item else { panic!("each changed supplied region must ask for leaves directly") };
            answers.push(StateAnswer::Leaves(requester.page(bounds).await?));
        }
    }
    assert_eq!(answers.len(), 32, "all questions are legitimate bounded mismatch regions");
    // Several valid feedback requests can be accepted before the producer is polled.
    for batch in answers.chunks(4) {
        assert!(bincode::serialized_size(&(0u64, batch))? <= FRAME_BYTES as u64);
        comparison.answer(batch.to_vec()).await?;
    }
    assert_eq!(comparison.work.len(), 32, "one bounded work item must represent each accepted answer");
    let mut repaired = HashSet::new();
    while !comparison.complete {
        let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false).await? else { panic!("state") };
        assert!(comparison.work.len() <= 4096);
        for item in items {
            if let StateItem::Repair { delta, .. } = item {
                assert!(repaired.insert(delta.entity_id));
            }
        }
    }
    assert_eq!(repaired.len(), 4096, "bounded deferral must finish every changed entity exactly once");
    Ok(())
}

#[tokio::test]
async fn mismatched_frontier_reuses_exact_bounds_then_refines_and_pages_terminal_buckets() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(512, 8);
    for (i, state) in source.iter_mut().enumerate() {
        let mut id = [0; proto::EntityId::BYTE_LEN];
        id[0] = if i < 256 { 0x20 } else { 0xa0 };
        id[31] = (i % 256) as u8;
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
        let body = comparison.reply(&node, &vec![], false, false).await?;
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
        let body = comparison.reply(&node, &vec![], false, false).await?;
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
    let StateFrameBody::State { items, progress } = comparison.reply(&node, &vec![], false, false).await? else { panic!("state frame") };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items.as_slice(), [StateItem::Leaves(page)] if page.leaves[0].entity.entity_id == id));
    assert!(comparison.offered.is_empty(), "equal supplied state must not be sent again");
    let mut comparison = Comparison::new(snapshot(source).await?, None)?;
    assert!(comparison.reply(&node, &vec![], true, false).await.is_err(), "an exhaustive catalog page cannot skip an inadmissible row");
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

fn ordered_states(count: usize, spacing: u32) -> Vec<proto::Attested<proto::EntityState>> {
    let mut rows = states(count, 8);
    for (n, row) in rows.iter_mut().enumerate() {
        let mut id = [0; 32];
        id[..4].copy_from_slice(&((n as u32 + 1) * spacing).to_be_bytes());
        row.payload.entity_id = proto::EntityId::from_bytes(id);
    }
    rows
}
async fn initial_items(node: &TestNode, comparison: &mut Comparison) -> anyhow::Result<Vec<StateItem>> {
    let mut all = Vec::new();
    for _ in 0..512 {
        if comparison.complete {
            return Ok(all);
        }
        let StateFrameBody::State { items, .. } = comparison.reply(node, &vec![], false, false).await? else { panic!("state") };
        assert!(comparison.work.len() <= 4096);
        assert!(bincode::serialized_size(&items)? <= PAGE_BYTES as u64);
        assert!(!items.iter().any(|item| matches!(item, StateItem::NeedLeaves(_) | StateItem::NeedHeads(_) | StateItem::Summary(_))));
        all.extend(items);
    }
    anyhow::bail!("initial inventory did not complete")
}

#[tokio::test]
async fn complete_initial_inventory_retains_equal_points_and_repairs_only_changed_heads() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    for count in [256, 2048] {
        let source = ordered_states(count, 2);
        let mut requester = snapshot(source.clone()).await?;
        let known = requester.offer().await?;
        let mut comparison = Comparison::new(snapshot(source).await?, Some(known))?;
        assert_eq!(comparison.known_clocks.len(), count);
        assert!(comparison.work.iter().all(|work| matches!(work, Work::InitialLeaves(_))));
        let items = initial_items(&node, &mut comparison).await?;
        assert!(items.is_empty(), "equal initial pages need no response items");
        assert!(comparison.complete, "the full captured inventory was compared before Complete");
    }
    let before = ordered_states(128, 2);
    let mut after = before.clone();
    after[17].payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([2; 32]));
    let mut requester = snapshot(before).await?;
    let mut comparison = Comparison::new(snapshot(after.clone()).await?, Some(requester.offer().await?))?;
    let items = initial_items(&node, &mut comparison).await?;
    assert!(
        matches!(items.as_slice(), [StateItem::Repair { target, delta }] if delta.entity_id == after[17].payload.entity_id && *target == after[17].payload.state.head)
    );
    Ok(())
}

fn position_spec() -> proto::TreeSpec {
    proto::TreeSpec::new(
        ankurah_core_types::KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(proto::EntityId::from_bytes([42; 32])),
            ankurah_core_types::ValueType::I64,
        )]),
        ankql::ast::Predicate::True,
    )
}
fn positioned_state(id: u32, position: i64, head: u8) -> anyhow::Result<proto::Attested<proto::EntityState>> {
    use crate::property::backend::{LWWBackend, PropertyBackend};
    let mut row = ordered_states(1, 1).pop().unwrap();
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&id.to_be_bytes());
    row.payload.entity_id = proto::EntityId::from_bytes(bytes);
    let backend = LWWBackend::new();
    backend.set(proto::PropertyId::EntityId(proto::EntityId::from_bytes([42; 32])), Some(ankurah_core_types::Value::I64(position)));
    crate::test_helpers::commit_backend(&backend, head)?;
    row.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([head; 32]));
    row.payload.state.state_buffers = proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into());
    Ok(row)
}

#[tokio::test]
async fn initial_diff_keeps_cross_page_moves_and_readable_former_member_repairs() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let query = super::super::QueryTree::default();
    let before = (0..256).map(|id| positioned_state(id, id as i64, 1)).collect::<anyhow::Result<Vec<_>>>()?;
    let mut old = query.refresh(position_spec(), before).await?;
    let known = old.offer().await?;
    assert_eq!(known.regions.len(), 2);
    let saved_offer = bincode::serialize(&known)?;
    let mut members = old.members();
    let mut current = (0..256)
        .filter(|id| *id != 17)
        .map(|id| {
            positioned_state(
                id,
                match id {
                    0 => 10000,
                    250 => -20,
                    _ => id as i64,
                },
                if id == 0 || id == 250 { 2 } else { 1 },
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    current.push(positioned_state(1000, 40, 2)?);
    let former = positioned_state(17, -50, 3)?;
    let mut tx = node.storage.transaction();
    tx.set_state(&Default::default(), &former).await?;
    tx.commit().await?;
    let captured = query.refresh(position_spec(), current.clone()).await?;
    let expected = captured.members();
    let mut comparison = Comparison::new(captured, Some(known))?;
    assert_eq!(comparison.known_clocks.len(), 256, "later pages are loaded before the first moved key is visited");
    let items = initial_items(&node, &mut comparison).await?;
    let mut repairs = HashMap::new();
    for item in items {
        match item {
            StateItem::Leaves(page) => {
                members.retain(|address, _| !page.bounds.contains(address));
                for leaf in page.leaves {
                    members.insert(leaf_address(&leaf.key, leaf.entity.entity_id), leaf.entity.entity_id);
                }
            }
            StateItem::Snapshots(page) => {
                members.retain(|address, _| !page.bounds.contains(address));
                for state in page.states {
                    let key = super::super::tree::key(&position_spec(), &state)?;
                    members.insert(leaf_address(&key, state.payload.entity_id), state.payload.entity_id);
                }
            }
            StateItem::Repair { target, delta } => {
                assert!(repairs.insert(delta.entity_id, target).is_none());
            }
            StateItem::Equal(_) => {}
            other => panic!("unexpected diff item: {other:?}"),
        }
    }
    assert_eq!(members, expected, "repair dedup must not discard the new or removed key patch");
    for id in [0, 250] {
        let state = current.iter().find(|state| state.payload.entity_id.to_bytes()[..4] == (id as u32).to_be_bytes()).unwrap();
        assert_eq!(repairs.get(&state.payload.entity_id), Some(&state.payload.state.head));
    }
    assert_eq!(repairs.get(&former.payload.entity_id), Some(&former.payload.state.head));
    assert_eq!(bincode::serialize(&old.offer().await?)?, saved_offer, "captured inventory stays immutable");
    Ok(())
}

#[tokio::test]
async fn initial_diff_pages_many_entrants_and_coalesces_dense_removals() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let old = ordered_states(128, 10000);
    let mut requester = snapshot(old.clone()).await?;
    let mut current = ordered_states(1024, 1);
    current.extend(old);
    let expected = current.iter().take(1024).map(|state| state.payload.entity_id).collect::<HashSet<_>>();
    let mut comparison = Comparison::new(snapshot(current).await?, Some(requester.offer().await?))?;
    let items = initial_items(&node, &mut comparison).await?;
    let mut incoming = HashSet::new();
    for item in items {
        if let StateItem::Snapshots(page) = item {
            assert!(page.states.len() <= super::super::MAX_SNAPSHOT_ROWS);
            incoming.extend(page.states.into_iter().map(|state| state.payload.entity_id));
        }
    }
    assert_eq!(incoming, expected);

    let source = ordered_states(2048, 2);
    let mut tx = node.storage.transaction();
    for state in &source {
        let mut newer = state.clone();
        newer.payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([2; 32]));
        tx.set_state(&Default::default(), &newer).await?;
    }
    tx.commit().await?;
    let mut requester = snapshot(source).await?;
    let mut comparison = Comparison::new(snapshot(vec![]).await?, Some(requester.offer().await?))?;
    let items = initial_items(&node, &mut comparison).await?;
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Leaves(page) if page.leaves.is_empty())).count(), 16);
    assert_eq!(
        items.iter().filter(|item| matches!(item, StateItem::Repair { .. })).count(),
        2048,
        "coalescing must preserve every actual former clock repair"
    );
    Ok(())
}

#[tokio::test]
async fn alternating_initial_points_expand_one_bounded_page_at_a_time() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let before = ordered_states(2048, 2);
    let mut current = before.clone();
    for state in &mut current {
        let mut id = state.payload.entity_id.to_bytes();
        let number = u32::from_be_bytes(id[..4].try_into().unwrap()) - 1;
        id[..4].copy_from_slice(&number.to_be_bytes());
        state.payload.entity_id = proto::EntityId::from_bytes(id);
    }
    let mut requester = snapshot(before).await?;
    let mut comparison = Comparison::new(snapshot(current).await?, Some(requester.offer().await?))?;
    assert_eq!(comparison.work.len(), 16);
    let Some(Work::InitialLeaves(page)) = comparison.work.pop_front() else { panic!("initial page") };
    comparison.expand_initial_leaves(page)?;
    assert!(comparison.work.len() <= 385 + 15);
    let items = initial_items(&node, &mut comparison).await?;
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Leaves(page) if page.leaves.is_empty())).count(), 2048);
    assert_eq!(
        items.iter().filter_map(|item| if let StateItem::Snapshots(page) = item { Some(page.states.len()) } else { None }).sum::<usize>(),
        2048
    );
    Ok(())
}

#[tokio::test]
async fn initial_inventory_limits_and_exact_point_bounds_are_validated() -> anyhow::Result<()> {
    let before = ordered_states(2048, 2);
    let mut requester = snapshot(before.clone()).await?;
    let mut known = requester.offer().await?;
    let point = before[0].payload.entity_id.to_bytes().to_vec();
    let page = requester.page(AddressRange::new(std::ops::Bound::Included(point.clone()), std::ops::Bound::Included(point))).await?;
    let mut exact = Comparison::new(
        snapshot(before.clone()).await?,
        Some(proto::KnownState { spec: spec(), regions: vec![proto::KnownRegion::Leaves(page.clone())] }),
    )?;
    let point_work = exact.work.iter().position(|work| matches!(work, Work::InitialLeaves(_))).unwrap();
    let Some(Work::InitialLeaves(validated)) = exact.work.remove(point_work) else { panic!("validated exact point") };
    assert_eq!(validated.bounds, page.bounds);
    let unknown_gaps = exact.work.len();
    exact.expand_initial_leaves(validated)?;
    assert_eq!(exact.work.len(), unknown_gaps, "a validated equal point adds no equality echo");
    assert_eq!(exact.known_clocks.len(), 1);
    let (node, _, _) = node().await?;
    let items = initial_items(&node, &mut exact).await?;
    assert!(exact.complete);
    assert!(items.iter().all(|item| matches!(item, StateItem::Snapshots(_))), "unknown gaps still require membership output");
    assert_eq!(items.iter().map(|item| if let StateItem::Snapshots(page) = item { page.states.len() } else { 0 }).sum::<usize>(), 2047);
    known.regions.push(known.regions[0].clone());
    assert!(Comparison::new(snapshot(before.clone()).await?, Some(known)).is_err(), "overlapping pages must fail before comparison");
    let extra = positioned_state(3, 10000, 1)?;
    let mut keyed = super::super::QueryTree::default().refresh(position_spec(), vec![extra]).await?;
    let mut duplicate = keyed.offer().await?;
    let proto::KnownRegion::Leaves(first) = &mut duplicate.regions[0] else { unreachable!() };
    let mut second = first.clone();
    let key = CanonicalCodec.encode_prefix(position_spec().key_spec(), &[ankurah_core_types::Value::I64(10001)])?;
    second.leaves[0].key = key;
    let addr = leaf_address(&second.leaves[0].key, second.leaves[0].entity.entity_id);
    first.bounds.end = std::ops::Bound::Excluded(addr.clone());
    second.bounds.start = std::ops::Bound::Included(addr);
    duplicate.regions.push(proto::KnownRegion::Leaves(second));
    assert!(
        Comparison::new(super::super::QueryTree::default().refresh(position_spec(), vec![]).await?, Some(duplicate)).is_err(),
        "global duplicate IDs must fail even at distinct keys/pages"
    );
    let mut oversized = requester.offer().await?;
    let last = ordered_states(2049, 2).pop().unwrap();
    let proto::KnownRegion::Leaves(page) = oversized.regions.last_mut().unwrap() else { unreachable!() };
    page.bounds.end = std::ops::Bound::Excluded(last.payload.entity_id.to_bytes().to_vec());
    oversized.regions.push(proto::KnownRegion::Leaves(proto::LeafPage {
        bounds: AddressRange::new(std::ops::Bound::Included(last.payload.entity_id.to_bytes().to_vec()), super::super::whole().end),
        leaves: vec![proto::KnownLeaf {
            key: vec![],
            entity: proto::KnownEntity { entity_id: last.payload.entity_id, head: last.payload.state.head },
        }],
    }));
    assert!(Comparison::new(snapshot(before).await?, Some(oversized)).is_err(), "total leaf cap spans all pages");
    Ok(())
}

#[tokio::test]
async fn valid_long_key_inventory_falls_back_before_split_bounds_exceed_a_frame() -> anyhow::Result<()> {
    use crate::property::backend::{LWWBackend, PropertyBackend};
    let property = proto::PropertyId::EntityId(proto::EntityId::from_bytes([42; 32]));
    let spec = proto::TreeSpec::new(
        ankurah_core_types::KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(property, ankurah_core_types::ValueType::String)]),
        ankql::ast::Predicate::True,
    );
    let text_state = |text: String, head: u8| -> anyhow::Result<proto::Attested<proto::EntityState>> {
        let backend = LWWBackend::new();
        backend.set(property, Some(ankurah_core_types::Value::String(text)));
        crate::test_helpers::commit_backend(&backend, head)?;
        let mut row = positioned_state(1, 0, head)?;
        row.payload.state.state_buffers = proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into());
        Ok(row)
    };
    let before = text_state("z".repeat(40 * 1024), 1)?;
    let former = text_state("z".repeat(40 * 1024), 2)?;
    let mut requester = super::super::QueryTree::default().refresh(spec.clone(), vec![before]).await?;
    let known = requester.offer().await?;
    let [proto::KnownRegion::Leaves(page)] = known.regions.as_slice() else { panic!("one valid complete initial inventory") };
    assert!(bincode::serialized_size(&StateAnswer::Leaves(page.clone()))? <= PAGE_BYTES as u64);
    let supplied_clock = page.leaves[0].entity.head.clone();
    let address = leaf_address(&page.leaves[0].key, page.leaves[0].entity.entity_id);
    let removal = StateItem::Leaves(proto::LeafPage {
        bounds: AddressRange::new(std::ops::Bound::Included(address.clone()), std::ops::Bound::Included(address)),
        leaves: vec![],
    });
    assert!(bincode::serialized_size(&removal)? > PAGE_BYTES as u64);
    let (node, _, _) = node().await?;
    let mut tx = node.storage.transaction();
    tx.set_state(&Default::default(), &former).await?;
    tx.commit().await?;
    let empty = super::super::QueryTree::default().refresh(spec.clone(), vec![]).await?;
    let mut comparison = Comparison::new(empty, Some(known.clone()))?;
    let Some(Work::InitialLeaves(page)) = comparison.work.pop_front() else { panic!("initial region") };
    comparison.expand_initial_leaves(page)?;
    assert!(comparison.work.iter().any(|work| matches!(work, Work::SendLeaves(bounds) if *bounds == super::super::whole())));
    assert!(comparison
        .work
        .iter()
        .any(|work| matches!(work, Work::Repair(id, Some(clock)) if *id == former.payload.entity_id && *clock == supplied_clock)));
    let items = initial_items(&node, &mut comparison).await?;
    assert!(items
        .iter()
        .any(|item| matches!(item, StateItem::Leaves(page) if page.bounds == super::super::whole() && page.leaves.is_empty())));
    assert_eq!(comparison.offered.get(&former.payload.entity_id), Some(&former.payload.state.head));
    let moved = text_state("a".into(), 2)?;
    let captured = super::super::QueryTree::default().refresh(spec, vec![moved.clone()]).await?;
    let mut comparison = Comparison::new(captured, Some(known))?;
    let items = initial_items(&node, &mut comparison).await?;
    assert!(items.iter().any(|item| matches!(item, StateItem::Leaves(page) if page.leaves.len() == 1 && page.leaves[0].entity.entity_id == moved.payload.entity_id)));
    assert_eq!(comparison.offered.get(&moved.payload.entity_id), Some(&moved.payload.state.head));
    Ok(())
}

#[tokio::test]
async fn total_initial_bytes_are_checked_before_any_page_is_processed() -> anyhow::Result<()> {
    let mut source = ordered_states(64, 2);
    let head = proto::Clock::new(
        (0u32..200)
            .map(|n| {
                let mut bytes = [0; 32];
                bytes[..4].copy_from_slice(&n.to_be_bytes());
                (1, proto::EventId::from_bytes(bytes))
            })
            .collect::<Vec<_>>(),
    )?;
    for state in &mut source {
        state.payload.state.head = head.clone();
    }
    let mut requester = snapshot(source).await?;
    let mut remaining = super::super::whole();
    let mut regions = Vec::new();
    loop {
        let page = requester.page(remaining).await?;
        assert!(bincode::serialized_size(&StateAnswer::Leaves(page.clone()))? <= PAGE_BYTES as u64);
        let end = page.bounds.end.clone();
        regions.push(proto::KnownRegion::Leaves(page));
        if end == super::super::whole().end {
            break;
        }
        let std::ops::Bound::Included(last) = end else { panic!("progress") };
        remaining = super::super::whole().after(&last);
    }
    let known = proto::KnownState { spec: spec(), regions };
    assert!(bincode::serialized_size(&known)? > INITIAL_INVENTORY_BYTES as u64);
    assert!(Comparison::new(snapshot(vec![]).await?, Some(known)).is_err());
    Ok(())
}
