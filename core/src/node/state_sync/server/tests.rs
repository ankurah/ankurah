use super::*;
use crate::storage::StorageTransaction;
use crate::{
    connector::{PeerSender, SendError},
    policy::PermissiveAgent,
    test_utils::TestStorage,
};
use ankurah_digest_tree::CanonicalCodec;

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
fn unknown() -> proto::KnownState { proto::KnownState { spec: spec(), spans: vec![] } }
async fn snapshot(states: Vec<proto::Attested<proto::EntityState>>) -> anyhow::Result<Snapshot> {
    super::super::QueryTreeCache::default().refresh(spec(), states).await
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
    let StateFrameBody::State { items, progress, .. } = &frame.body else { panic!("state frame") };
    (items, *progress)
}
fn live_item(bytes: usize) -> proto::QueryChange {
    let state = states(1, bytes).pop().unwrap();
    proto::QueryChange {
        entity_id: state.payload.entity_id,
        member: Some(proto::QueryMember { key: vec![], head: state.payload.state.head.clone() }),
        delta: Some(proto::DeltaContent::StateSnapshot { state: state.into() }),
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
    assert!(known.spans.len() > 1);
    let mut equal = Comparison::new(snapshot(source.clone()).await?, known.clone())?;
    assert!(matches!(equal.work.front(), Some(Work::CompareFrontier(_))));
    let StateFrameBody::State { items, progress, .. } = equal.reply(&node, &vec![], false, false, 0).await? else { panic!("state") };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(items.is_empty(), "equal captured results need no response items");

    let empty =
        known.spans.iter().position(|region| matches!(region, proto::SpanContent::Digest(head) if head.digest.count() == 0)).unwrap();
    let mut withheld = known.clone();
    withheld.spans.remove(empty);
    let partial = Comparison::new(snapshot(source.clone()).await?, withheld)?;
    assert!(!partial.work.iter().any(|work| matches!(work, Work::CompareFrontier(_))), "a withheld gap cannot claim aggregate equality");

    let mut mixed = known.clone();
    let proto::SpanContent::Digest(head) = &mixed.spans[empty] else { unreachable!() };
    mixed.spans[empty] = proto::SpanContent::Heads(requester.page(head.bounds.clone())?);
    let mixed = Comparison::new(snapshot(source.clone()).await?, mixed)?;
    assert!(!mixed.work.iter().any(|work| matches!(work, Work::CompareFrontier(_))));

    let mut overlap = known;
    overlap.spans.push(overlap.spans[0].clone());
    assert!(Comparison::new(snapshot(source).await?, overlap).is_err());
    Ok(())
}

#[tokio::test]
async fn a_128_leaf_mismatch_repairs_unknown_and_former_members() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let mut source = states(128, 8);
    source.sort_by_key(|state| state.payload.entity_id);
    let former = source[0].payload.entity_id;
    let former_clock = source[0].payload.state.head.clone();
    let mut requester = snapshot(source.clone()).await?;
    let known =
        proto::KnownState { spec: spec(), spans: vec![proto::SpanContent::Digest(requester.span_digest(super::super::whole()).await?)] };
    let mut current = source[1..].to_vec();
    let added = states(1, 8).pop().unwrap();
    let new_id = added.payload.entity_id;
    current.push(added);
    let mut comparison = Comparison::new(snapshot(current).await?, known)?;
    let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false, 0).await? else { panic!("state") };
    assert!(matches!(items.as_slice(), [StateItem::NeedHeads(bounds)] if *bounds == super::super::whole()));
    comparison.answer(vec![StateAnswer::Heads(requester.page(super::super::whole())?)])?;
    let Some(Work::AnswerHeads { page, rest }) = comparison.work.pop_front() else { panic!("deferred leaf answer") };
    comparison.expand_head_answer(page, rest)?;
    assert!(comparison.work.iter().any(|work| matches!(work, Work::Repair(id, None) if *id == new_id)));
    assert!(comparison.work.iter().any(|work| matches!(work, Work::Repair(id, Some(clock)) if *id == former && *clock == former_clock)));
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
    let requester = snapshot(source.clone()).await?;
    let request = requester.page(super::super::whole())?;
    assert_eq!(request.heads.len(), 128);
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
    let mut comparison = Comparison::new(snapshot(current).await?, unknown())?;
    comparison.work.clear();
    comparison.head_requests.push(super::super::whole());
    comparison.answer(vec![StateAnswer::Heads(request)])?;
    let Some(Work::AnswerHeads { page, rest }) = comparison.work.pop_front() else { panic!("deferred leaf answer") };
    comparison.expand_head_answer(page, rest)?;
    let pages = comparison
        .work
        .iter()
        .filter_map(|work| match work {
            Work::Item(StateItem::Heads(page)) => Some(page),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(pages.len() > 1);
    let mut start = super::super::whole().start;
    let mut ids = Vec::new();
    for page in pages {
        assert_eq!(page.bounds.start, start);
        assert!(bincode::serialized_size(&StateAnswer::Heads(page.clone()))? <= PAGE_BYTES as u64);
        ids.extend(page.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id));
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
    let mut spans = Vec::new();
    let mut start = super::super::whole().start;
    for (index, chunk) in current.chunks(HEAD_PAGE).enumerate() {
        let end = if (index + 1) * HEAD_PAGE == current.len() {
            super::super::whole().end
        } else {
            std::ops::Bound::Included(chunk.last().unwrap().payload.entity_id.to_bytes().to_vec())
        };
        let bounds = TreeSpan::new(start, end);
        start = opposite(&bounds.end);
        spans.push(proto::SpanContent::Digest(requester.span_digest(bounds).await?));
    }
    let mut comparison = Comparison::new(snapshot(current).await?, proto::KnownState { spec: spec(), spans })?;
    let mut answers = Vec::new();
    while !comparison.work.is_empty() {
        let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false, 0).await? else { panic!("state") };
        for item in items {
            let StateItem::NeedHeads(bounds) = item else { panic!("each changed supplied region must ask for leaves directly") };
            answers.push(StateAnswer::Heads(requester.page(bounds)?));
        }
    }
    assert_eq!(answers.len(), 32, "all questions are legitimate bounded mismatch spans");
    // Several valid feedback requests can be accepted before the producer is polled.
    for batch in answers.chunks(4) {
        assert!(bincode::serialized_size(&(0u64, batch))? <= FRAME_BYTES as u64);
        comparison.answer(batch.to_vec())?;
    }
    assert_eq!(comparison.work.len(), 32, "one bounded work item must represent each accepted answer");
    let mut repaired = HashSet::new();
    while !comparison.complete {
        let StateFrameBody::State { items, .. } = comparison.reply(&node, &vec![], false, false, 0).await? else { panic!("state") };
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
    let root = responder.span_digest(super::super::whole()).await?;
    let frontier = responder.refine(&root).await?;
    let original =
        frontier.iter().find(|head| head.digest.count() > HEAD_PAGE as i64 && head.bounds.contains(&missing.to_bytes())).unwrap();
    let bounds = original.bounds.clone();
    let clipped = responder.span_digest(bounds.clone()).await?;
    let refined = responder.refine(&clipped).await?;
    assert!(
        matches!(refined.as_slice(), [head] if head.bounds == bounds && head.prefix != clipped.prefix),
        "fixture must expose a same-bounds responder subtree"
    );
    let cached = source.iter().filter(|state| state.payload.entity_id != missing).cloned().collect::<Vec<_>>();
    let mut requester = snapshot(cached.clone()).await?;
    let mut spans = Vec::new();
    for head in frontier {
        spans.push(proto::SpanContent::Digest(requester.span_digest(head.bounds).await?));
    }
    let known = proto::KnownState { spec: spec(), spans };
    let mut comparison = Comparison::new(responder, known)?;
    let mut repairs = Vec::new();
    let mut narrower_questions = 0;
    let mut leaf_pages = 0;
    let mut finished = false;
    for _ in 0..64 {
        let body = comparison.reply(&node, &vec![], false, false, 0).await?;
        assert!(bincode::serialized_size(&body)? <= FRAME_BYTES as u64);
        let StateFrameBody::State { items, progress, .. } = body else { panic!("state frame") };
        let mut answers = Vec::new();
        for item in items {
            match item {
                StateItem::Digest(head) => {
                    assert_ne!(head.bounds, bounds, "the requester already supplied this exact digest");
                    narrower_questions += 1;
                    answers.push(StateAnswer::Digest(requester.span_digest(head.bounds).await?));
                }
                StateItem::NeedHeads(bounds) => {
                    leaf_pages += 1;
                    answers.push(StateAnswer::Heads(requester.page(bounds)?));
                }
                StateItem::Repair { delta, .. } => repairs.push(delta.entity_id),
                StateItem::Heads(_) => {}
                other => panic!("unexpected frontier comparison item: {other:?}"),
            }
        }
        if progress == ComparisonProgress::Complete {
            assert!(answers.is_empty());
            finished = true;
            break;
        }
        comparison.answer(answers)?;
    }
    assert!(finished, "comparison must finish within bounded work turns");
    assert!(narrower_questions >= 2, "mixed children and terminal page bounds still require their own digests");
    assert!(leaf_pages >= 1, "terminal mismatch must still request the requester's bounded inventory");
    assert_eq!(repairs, [missing], "only the uncached entity needs repair");
    Ok(())
}

#[tokio::test]
async fn small_unknown_empty_and_partial_inventories_complete_in_frame_zero() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let source = states(8, 16);
    let mut cached = snapshot(source[..4].to_vec()).await?;
    let known = cached.offer().await?;
    let empty = snapshot(vec![]).await?.offer().await?;
    for (known, repairs) in [(unknown(), 8), (empty, 8), (known, 4)] {
        let initial = start_state_stream(
            &node,
            peer,
            proto::RequestId::new(),
            vec![],
            vec![],
            None,
            false,
            0,
            snapshot(source.clone()).await?,
            known,
        )
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
        assert!(!items.iter().any(|item| matches!(item, StateItem::NeedHeads(_) | StateItem::Digest(_))));
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
    let mut comparison = Comparison::new(snapshot(source.clone()).await?, unknown())?;
    let mut start = super::super::whole().start;
    let mut received = Vec::new();
    let mut chunks = Vec::new();
    let mut inline_after_chunks = false;
    while !comparison.complete {
        let body = comparison.reply(&node, &vec![], false, false, 0).await?;
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
                StateItem::Heads(page) => {
                    assert_eq!(page.heads.len(), 1);
                    assert_eq!(page.heads.to_vec()[0].entity.entity_id, large.payload.entity_id);
                    received.push(page.heads.to_vec()[0].entity.entity_id);
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
        .answer(vec![StateAnswer::NeedRepair { entity_id: source[0].payload.entity_id, target: source[0].payload.state.head.clone() }])?;
    assert!(comparison.pending_repairs.contains(&source[0].payload.entity_id));
    Ok(())
}

#[tokio::test]
async fn snapshot_page_keeps_a_supplied_clock_for_a_member_that_moved_from_another_range() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let source = states(1, 10);
    let id = source[0].payload.entity_id;
    let mut comparison = Comparison::new(snapshot(source.clone()).await?, unknown())?;
    let clock =
        proto::PackedHeads::from_rows(std::iter::once((&[][..], id, &source[0].payload.state.head))).cursor().next_clock().unwrap().1;
    comparison.known_clocks.insert(id, clock);
    let StateFrameBody::State { items, progress, .. } = comparison.reply(&node, &vec![], false, false, 0).await? else {
        panic!("state frame")
    };
    assert_eq!(progress, ComparisonProgress::Complete);
    assert!(matches!(items.as_slice(), [StateItem::Heads(page)] if page.heads.to_vec()[0].entity.entity_id == id));
    assert!(comparison.offered.is_empty(), "equal supplied state must not be sent again");
    let mut comparison = Comparison::new(snapshot(source).await?, unknown())?;
    assert!(comparison.reply(&node, &vec![], true, false, 0).await.is_err(), "an exhaustive catalog page cannot skip an inadmissible row");
    Ok(())
}

#[tokio::test]
async fn empty_inventory_is_complete_without_questions() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial =
        start_state_stream(&node, peer, proto::RequestId::new(), vec![], vec![], None, false, 0, snapshot(vec![]).await?, unknown())
            .await?;
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
        start_state_stream(&node, peer, origin.clone(), vec![11], vec![], Some((query, 7)), false, 0, snapshot(vec![]).await?, unknown())
            .await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let session = connection.state_streams.get(&initial.frame.stream).unwrap();
    let guard = session.state.lock().await;
    let replay = connection.state_streams.replay_initial(&origin, &[11], &[])?.unwrap();
    let proto::NodeResponseBody::QuerySubscribed { query_id, state } = replay else { panic!("subscription replay") };
    assert_eq!(query_id, query);
    assert_eq!(bincode::serialize(&state)?, bincode::serialize(&initial)?);
    assert!(connection.state_streams.replay_initial(&origin, &[12], &[]).is_err());
    drop(guard);
    continue_state(&node, peer, proto::RequestId::new(), initial.frame.stream, None, 0, None, vec![], vec![]).await?;
    assert!(matches!(
        connection.state_streams.replay_initial(&origin, &[11], &[])?,
        Some(proto::NodeResponseBody::StateStreamUnavailable { .. })
    ));
    abandon_state_stream(&node, peer, initial.frame.stream).await;
    assert!(matches!(
        connection.state_streams.replay_initial(&origin, &[11], &[])?,
        Some(proto::NodeResponseBody::StateStreamUnavailable { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn cumulative_receipts_are_idempotent_and_cannot_grant_unsent_credit() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial =
        start_state_stream(&node, peer, proto::RequestId::new(), vec![], vec![], None, false, 0, snapshot(vec![]).await?, unknown())
            .await?;
    let id = initial.frame.stream;
    let request = proto::RequestId::new();
    continue_state(&node, peer, request.clone(), id, None, 0, None, vec![], vec![]).await?;
    continue_state(&node, peer, request.clone(), id, None, 0, None, vec![], vec![]).await?;
    assert!(continue_state(&node, peer, request, id, None, 1, None, vec![], vec![]).await.is_err());
    assert!(continue_state(&node, peer, proto::RequestId::new(), id, None, 1, None, vec![], vec![]).await.is_err());
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![]).await?;
    assert!(matches!(
        continue_state(&node, peer, proto::RequestId::new(), id, None, 0, None, vec![], vec![]).await?,
        proto::NodeResponseBody::Success
    ));
    assert!(matches!(
        continue_state(
            &node,
            peer,
            proto::RequestId::new(),
            id,
            Some(0),
            0,
            None,
            vec![StateAnswer::Heads(proto::TreeSpanInventory { bounds: super::super::whole(), heads: vec![].into() })],
            vec![]
        )
        .await?,
        proto::NodeResponseBody::StateStreamUnavailable { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn cold_producer_stops_at_window_and_resumes_with_contiguous_frames() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        None,
        false,
        0,
        snapshot(states(200, PAGE_BYTES / 2)).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    pump_state_stream(&node, peer, id).await?;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
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
        continue_state(&node, peer, proto::RequestId::new(), id, None, last, None, vec![], vec![]).await?;
        pump_state_stream(&node, peer, id).await?;
    }
    let sequences = messages
        .lock()
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. })
                if frame.stream == id =>
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
    for index in 0..MAX_STATE_STREAMS + 2 {
        let request = if index == 0 { first.clone() } else { proto::RequestId::new() };
        let initial = start_state_stream(&node, peer, request, vec![], vec![], None, false, 0, snapshot(vec![]).await?, unknown()).await?;
        first_id.get_or_insert(initial.frame.stream);
        finish_state_round(&node, peer, initial.frame.stream, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![])
            .await?;
    }
    let connection = node.peer_connections.get(&peer).unwrap();
    assert!(
        matches!(connection.state_streams.replay_initial(&first, &[], &[])?, Some(proto::NodeResponseBody::StateStreamUnavailable { stream: comparison }) if Some(comparison) == first_id)
    );
    assert!(start_state_stream(&node, peer, first, vec![], vec![], None, false, 0, snapshot(vec![]).await?, unknown()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn live_before_applied_uses_the_same_sequence_and_typed_chunks() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let origin = proto::RequestId::new();
    let initial = start_state_stream(
        &node,
        peer,
        origin.clone(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    let oversized = live_item(PAGE_BYTES * 2);
    let entity = oversized.entity_id;
    let member = oversized.member.clone();
    let encoded = bincode::serialize(oversized.delta.as_ref().unwrap())?;
    queue_live(&node, peer, id, 1, vec![oversized, live_item(1)])?;
    pump_state_stream(&node, peer, id).await?;
    let mut assembled = Vec::new();
    let mut live_seen = false;
    for message in messages.lock().unwrap().iter() {
        let proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { request_id, frame }, .. }) = message
        else {
            continue;
        };
        assert_eq!(request_id, &origin);
        assert_eq!(frame.stream, id);
        match &frame.body {
            StateFrameBody::LiveChunk { revision, chunk, member: received, complete } => {
                assert_eq!(*revision, 1);
                assert!(!live_seen);
                assert_eq!(chunk.entity_id, entity);
                assert_eq!(chunk.offset as usize, assembled.len());
                assert_eq!(received, &member);
                assert!(!complete, "the following entity still belongs to this publication");
                assembled.extend_from_slice(&chunk.bytes);
            }
            StateFrameBody::Live { revision, complete, .. } => {
                assert_eq!(*revision, 1);
                assert!(*complete);
                assert_eq!(assembled, encoded);
                live_seen = true;
            }
            _ => panic!("unexpected live frame"),
        }
    }
    assert!(live_seen, "live data must flow before Applied for the initial round");
    {
        let state = session.state.lock().await;
        assert!(state.comparison.is_some(), "live output must not retire the pending round");
        assert!(state.bytes > 0, "live frames consume the retained window");
    }
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![]).await?;
    assert!(session.state.lock().await.comparison.is_none());
    assert!(node.peer_connections.get(&peer).unwrap().state_streams.get(&id).is_some());
    Ok(())
}

#[tokio::test]
async fn pending_round_marker_waits_for_the_whole_chunked_publication() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    // Drive the pump explicitly, including the cut between chunk fragments.
    session.pumping.store(true, Ordering::Release);
    queue_live(&node, peer, id, 1, vec![live_item(PAGE_BYTES * 2), live_item(1)])?;
    {
        let mut state = session.state.lock().await;
        let body = state.next_live(&session.live)?.unwrap();
        assert!(matches!(body, StateFrameBody::LiveChunk { complete: false, .. }));
        state.retain(StateFrame { stream: id, sequence: 1, body }, false)?;
    }
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![]).await?;
    let mut next = snapshot(vec![]).await?;
    let offer = next.offer().await?;
    schedule_round(&node, peer, id, 1, 1, next, offer).await?;
    // This later publication must follow BeginRound; the unfinished revision 1
    // must finish before it, including its non-chunked final entity.
    queue_live(&node, peer, id, 2, vec![live_item(1)])?;
    pump_state_stream(&node, peer, id).await?;
    let state = session.state.lock().await;
    let bodies = state.retained.iter().map(|retained| &retained.frame.body).collect::<Vec<_>>();
    let marker = bodies.iter().position(|body| matches!(body, StateFrameBody::BeginRound(round) if round.number == 1)).unwrap();
    assert!(bodies[..marker]
        .iter()
        .all(|body| matches!(body, StateFrameBody::LiveChunk { revision: 1, .. } | StateFrameBody::Live { revision: 1, .. })));
    assert!(matches!(bodies[marker - 1], StateFrameBody::Live { revision: 1, complete: true, .. }));
    assert!(bodies[marker + 1..].iter().any(|body| matches!(body, StateFrameBody::Live { revision: 2, complete: true, .. })));
    assert!(!bodies[..marker].iter().any(|body| matches!(body, StateFrameBody::Live { revision: 2, .. })));
    Ok(())
}

#[tokio::test]
async fn waiting_baseline_questions_do_not_starve_live_publications() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let source = states(128, 8);
    let mut requester = snapshot(source[..1].to_vec()).await?;
    let known =
        proto::KnownState { spec: spec(), spans: vec![proto::SpanContent::Digest(requester.span_digest(super::super::whole()).await?)] };
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(source).await?,
        known,
    )
    .await?;
    let id = initial.frame.stream;
    assert!(state_items(&initial.frame).0.iter().any(|item| matches!(item, StateItem::NeedHeads(_))));
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    for revision in 1..=8 {
        queue_live(&node, peer, id, revision, vec![live_item(1)])?;
    }
    pump_state_stream(&node, peer, id).await?;
    let state = session.state.lock().await;
    let comparison = state.comparison.as_ref().unwrap();
    assert!(!comparison.head_requests.is_empty(), "the unanswered baseline question must remain pending");
    assert!(!comparison.complete);
    let revisions = messages
        .lock()
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate {
                body:
                    proto::NodeUpdateBody::StateFrame {
                        frame: StateFrame { body: StateFrameBody::Live { revision, complete: true, .. }, .. },
                        ..
                    },
                ..
            }) => Some(*revision),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(revisions, (1..=8).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
async fn runnable_baseline_and_live_backlog_both_advance_before_the_window_fills() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(states(200, PAGE_BYTES / 2)).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    for revision in 1..=8 {
        queue_live(&node, peer, id, revision, vec![live_item(1)])?;
    }
    pump_state_stream(&node, peer, id).await?;
    let messages = messages.lock().unwrap();
    let bodies = messages
        .iter()
        .filter_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => {
                Some(&frame.body)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let first_live = bodies.iter().position(|body| matches!(body, StateFrameBody::Live { .. })).unwrap();
    let last_live = bodies.iter().rposition(|body| matches!(body, StateFrameBody::Live { .. })).unwrap();
    assert!(bodies[..first_live].iter().any(|body| matches!(body, StateFrameBody::State { .. })));
    assert!(
        bodies[first_live + 1..last_live].iter().any(|body| matches!(body, StateFrameBody::State { .. })),
        "a live backlog cannot starve baseline work"
    );
    let live_count = bodies.iter().filter(|body| matches!(body, StateFrameBody::Live { .. })).count();
    assert_eq!(live_count, 8, "baseline work cannot consume every available ordinary slot before live runs");
    drop(messages);
    let state = session.state.lock().await;
    assert!(!state.comparison.as_ref().unwrap().complete, "the fixture must leave a baseline backlog");
    assert!(!state.room(false));
    Ok(())
}

#[tokio::test]
async fn abandonment_is_one_way_and_scoped_to_the_peer_and_recipient() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let other_peer = proto::EntityId::random();
    node.register_peer(
        proto::Presence { node_id: other_peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(CaptureSender { peer: other_peer, messages: messages.clone() }),
    )
    .await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![crate::policy::DEFAULT_CONTEXT],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let stream = initial.frame.stream;
    let connection = node.peer_connections.get(&peer).unwrap();
    connection.state_streams.get(&stream).unwrap().pumping.store(true, Ordering::Release);
    messages.lock().unwrap().clear();
    for (from, to, id) in [(other_peer, node.id, stream), (peer, other_peer, stream), (peer, node.id, StateStreamId::new())] {
        node.handle_message(proto::NodeMessage::AbandonStateStream { from, to, stream: id }).await?;
        assert!(connection.state_streams.get(&stream).is_some(), "unrelated peer, recipient or handle cannot abandon this stream");
    }
    for _ in 0..2 {
        node.handle_message(proto::NodeMessage::AbandonStateStream { from: peer, to: node.id, stream }).await?;
        assert!(connection.state_streams.get(&stream).is_none());
    }
    assert!(connection.state_streams.was_closed(&stream));
    assert!(messages.lock().unwrap().is_empty(), "one-way cleanup must send no response or acknowledgment");
    Ok(())
}

#[tokio::test]
async fn cancelled_round_retires_fetch_but_keeps_live_stream_until_abandoned() -> anyhow::Result<()> {
    for query in [None, Some((proto::QueryId::new(), 1))] {
        let (node, peer, messages) = node().await?;
        let source = states(2, 8);
        let mut requester = snapshot(source[..1].to_vec()).await?;
        let known = proto::KnownState {
            spec: spec(),
            spans: vec![proto::SpanContent::Digest(requester.span_digest(super::super::whole()).await?)],
        };
        let initial = start_state_stream(
            &node,
            peer,
            proto::RequestId::new(),
            vec![],
            vec![crate::policy::DEFAULT_CONTEXT],
            query,
            false,
            0,
            snapshot(source).await?,
            known,
        )
        .await?;
        let id = initial.frame.stream;
        assert!(state_items(&initial.frame).0.iter().any(|item| matches!(item, StateItem::NeedHeads(_))));
        let connection = node.peer_connections.get(&peer).unwrap();
        let session = connection.state_streams.get(&id).unwrap();
        session.pumping.store(true, Ordering::Release);
        let cancelled = proto::StateRoundOutcome::Cancelled;
        let credentials = vec![crate::policy::DEFAULT_CONTEXT];
        assert!(finish_state_round(&node, peer, id, 0, cancelled, vec![]).await.is_err());
        assert!(finish_state_round(&node, peer, id, 1, cancelled, credentials.clone()).await.is_err());
        finish_state_round(&node, peer, id, 0, cancelled, credentials.clone()).await?;
        finish_state_round(&node, peer, id, 0, cancelled, credentials.clone()).await?;
        assert!(session.state.lock().await.comparison.is_none(), "cancellation releases the unfinished comparison");
        if query.is_none() {
            assert!(connection.state_streams.get(&id).is_none(), "Fetch has no ongoing stream after cancellation");
            continue;
        }
        assert!(connection.state_streams.get(&id).is_some());
        queue_live(&node, peer, id, 1, vec![live_item(1)])?;
        pump_state_stream(&node, peer, id).await?;
        assert!(
            messages.lock().unwrap().iter().any(|message| matches!(message,
                proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame {
                    frame: StateFrame { stream: comparison, body: StateFrameBody::Live { revision: 1, complete: true, .. }, .. }, ..
                }, .. }) if *comparison == id
            )),
            "a cancelled comparison must not stop live delivery"
        );
        schedule_round(&node, peer, id, 1, 1, snapshot(states(1, 8)).await?, unknown()).await?;
        pump_state_stream(&node, peer, id).await?;
        finish_state_round(&node, peer, id, 0, cancelled, credentials.clone()).await?;
        {
            let state = session.state.lock().await;
            assert_eq!(state.round.number, 1);
            assert!(state.comparison.is_some(), "a duplicate old cancellation cannot retire the new round");
        }
        abandon_state_stream(&node, peer, id).await;
        abandon_state_stream(&node, peer, id).await;
        assert!(connection.state_streams.get(&id).is_none());
        assert!(queue_live(&node, peer, id, 2, vec![live_item(1)]).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn applied_releases_only_its_round_snapshot_and_old_controls_cannot_retire_the_next_round() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let first = snapshot(states(1, 8)).await?;
    let captured = Arc::downgrade(first.states.values().next().unwrap());
    let old = first.states.values().next().unwrap().payload.clone();
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        first,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    assert!(captured.upgrade().is_some());
    let applied = proto::StateRoundOutcome::Applied { processed_through: 0 };
    finish_state_round(&node, peer, id, 0, applied, vec![]).await?;
    assert!(captured.upgrade().is_none(), "round retirement must release captured source rows");
    let next = snapshot(states(1, 8)).await?;
    let next_capture = Arc::downgrade(next.states.values().next().unwrap());
    let mut empty = snapshot(vec![]).await?;
    let offer = empty.offer().await?;
    schedule_round(&node, peer, id, 1, 1, next, offer).await?;
    pump_state_stream(&node, peer, id).await?;
    finish_state_round(&node, peer, id, 0, applied, vec![]).await?;
    assert!(next_capture.upgrade().is_some(), "repeating an old Applied cannot release the new snapshot");
    let (members, targets) = {
        let state = session.state.lock().await;
        let comparison = state.comparison.as_ref().unwrap();
        assert!(comparison.work.is_empty());
        assert!(comparison.pending_repairs.is_empty());
        (comparison.snapshot.members(), comparison.offered.clone())
    };
    for answers in [vec![], vec![StateAnswer::NeedRepair { entity_id: old.entity_id, target: old.state.head.clone() }]] {
        assert!(
            matches!(
                continue_state(&node, peer, proto::RequestId::new(), id, Some(0), 0, None, answers, vec![]).await?,
                proto::NodeResponseBody::Success
            ),
            "late round-zero feedback must be harmless after a newer live decision covered its target"
        );
        let state = session.state.lock().await;
        let comparison = state.comparison.as_ref().unwrap();
        assert_eq!(state.round.number, 1);
        assert_eq!(comparison.snapshot.members(), members);
        assert_eq!(comparison.offered, targets);
        assert!(comparison.work.is_empty() && comparison.pending_repairs.is_empty());
    }
    assert!(
        continue_state(&node, peer, proto::RequestId::new(), id, Some(2), 0, None, vec![], vec![]).await.is_err(),
        "future-round feedback is invalid"
    );
    let last = session.state.lock().await.required_through;
    assert_eq!(session.state.lock().await.round.number, 1);
    let applied = proto::StateRoundOutcome::Applied { processed_through: last };
    finish_state_round(&node, peer, id, 1, applied, vec![]).await?;
    finish_state_round(&node, peer, id, 1, applied, vec![]).await?;
    assert!(next_capture.upgrade().is_none());
    assert!(session.state.lock().await.comparison.is_none());
    assert!(node.peer_connections.get(&peer).unwrap().state_streams.get(&id).is_some(), "the standing stream survives");
    for round in 2..=32 {
        let next = snapshot(states(1, 8)).await?;
        let captured = Arc::downgrade(next.states.values().next().unwrap());
        schedule_round(&node, peer, id, round, round, next, empty.offer().await?).await?;
        pump_state_stream(&node, peer, id).await?;
        let last = session.state.lock().await.required_through;
        finish_state_round(&node, peer, id, round, proto::StateRoundOutcome::Applied { processed_through: last }, vec![]).await?;
        assert!(captured.upgrade().is_none(), "round {round} retained captured rows");
        let state = session.state.lock().await;
        assert!(state.comparison.is_none() && state.pending_round.is_none());
        assert!(state.retained.is_empty());
        assert_eq!(state.bytes, 0);
    }
    let connection = node.peer_connections.get(&peer).unwrap();
    assert_eq!(connection.state_streams.sessions.lock().unwrap().len(), 1);
    assert_eq!(connection.state_streams.origins.lock().unwrap().len(), 1, "new rounds do not allocate new stream origins");
    Ok(())
}

#[tokio::test]
async fn frame_receipts_and_round_applied_do_not_release_live_repair_targets() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    let first = live_item(1);
    let second = live_item(1);
    queue_live(&node, peer, id, 1, vec![first.clone()])?;
    queue_live(&node, peer, id, 3, vec![second])?;
    pump_state_stream(&node, peer, id).await?;
    {
        let mut state = session.state.lock().await;
        assert!(state.acknowledge_live(1).is_err(), "completed data is not yet processed");
        assert!(state.acknowledge_live(2).is_err(), "a skipped source revision is not a sent publication boundary");
        assert_eq!(state.live_targets.len(), 2);
    }
    let last = session.state.lock().await.last_sent;
    continue_state(&node, peer, proto::RequestId::new(), id, None, last, None, vec![], vec![]).await?;
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: last }, vec![]).await?;
    {
        let mut state = session.state.lock().await;
        assert_eq!(state.live_targets.len(), 2);
        state.request_live_repairs(vec![StateAnswer::NeedRepair { entity_id: first.entity_id, target: first.member.unwrap().head }])?;
        assert_eq!(state.live_repairs.len(), 1, "receipt alone must leave repair admissible");
    }
    continue_state(&node, peer, proto::RequestId::new(), id, None, last, Some(1), vec![], vec![]).await?;
    {
        let state = session.state.lock().await;
        assert_eq!(state.live_targets.len(), 1);
        assert_eq!(state.live_targets.front().unwrap().revision, 3);
    }
    continue_state(&node, peer, proto::RequestId::new(), id, None, last, Some(3), vec![], vec![]).await?;
    let state = session.state.lock().await;
    assert!(state.live_targets.is_empty());
    assert_eq!(state.target_bytes, 0);
    assert!(state.completed_live.is_empty());
    Ok(())
}

#[tokio::test]
async fn membership_only_publications_have_bounded_application_receipts() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    let mut state = session.state.lock().await;
    for sequence in 1..=MAX_LIVE_ITEMS as u64 {
        state.retain(
            StateFrame { stream: id, sequence, body: StateFrameBody::Live { revision: sequence * 2, items: vec![], complete: true } },
            false,
        )?;
        // Frame credit keeps flowing; semantic application still has its own cap.
        state.acknowledge(sequence)?;
    }
    assert!(state.live_targets.is_empty());
    assert_eq!(state.completed_live.len(), MAX_LIVE_ITEMS);
    assert!(state
        .retain(
            StateFrame {
                stream: id,
                sequence: MAX_LIVE_ITEMS as u64 + 1,
                body: StateFrameBody::Live { revision: MAX_LIVE_ITEMS as u64 * 2 + 2, items: vec![], complete: true },
            },
            false
        )
        .is_err());
    assert!(state.acknowledge_live(1).is_err(), "membership-only credit also requires an actual boundary");
    state.acknowledge_live(MAX_LIVE_ITEMS as u64 * 2)?;
    assert!(state.completed_live.is_empty());
    Ok(())
}

#[tokio::test]
async fn live_bridge_repair_uses_reserved_credit_when_ordinary_output_is_full() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let target = proto::Clock::singleton(&genesis);
    let mut trx = node.storage.transaction();
    trx.add_events(&[proto::Attested::opt(genesis.clone(), None)]).await?;
    trx.set_state(
        &Default::default(),
        &proto::Attested::from(proto::EntityState {
            entity_id: genesis.entity_id,
            state: proto::State { head: target.clone(), ..Default::default() },
        }),
    )
    .await?;
    trx.commit().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    queue_live(
        &node,
        peer,
        id,
        1,
        vec![proto::QueryChange {
            entity_id: genesis.entity_id,
            member: Some(proto::QueryMember { key: vec![], head: target.clone() }),
            delta: None,
        }],
    )?;
    pump_state_stream(&node, peer, id).await?;
    {
        let mut state = session.state.lock().await;
        while state.room(false) {
            let sequence = state.last_sent + 1;
            state.retain(
                StateFrame {
                    stream: id,
                    sequence,
                    body: StateFrameBody::State { round: 0, items: vec![], progress: ComparisonProgress::Continue },
                },
                false,
            )?;
        }
        assert!(state.room(true));
        state.request_live_repairs(vec![StateAnswer::NeedRepair { entity_id: genesis.entity_id, target: target.clone() }])?;
    }
    pump_state_stream(&node, peer, id).await?;
    let frames = messages.lock().unwrap();
    let repair = frames
        .iter()
        .find_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate {
                body: proto::NodeUpdateBody::StateFrame { frame: StateFrame { body: StateFrameBody::LiveRepair { items }, .. }, .. },
                ..
            }) => Some(items),
            _ => None,
        })
        .expect("causal repair must use the reserved lane without ordinary frame credit");
    let [StateItem::Repair { target: received, delta }] = repair.as_slice() else { panic!("connecting event repair") };
    assert_eq!(received, &target);
    assert_eq!(delta.entity_id, genesis.entity_id);
    let proto::DeltaContent::EventBridge { events } = &delta.content else { panic!("event bridge") };
    assert_eq!(events.len(), 1);
    let restored: proto::Attested<proto::Event> = (genesis.entity_id, events[0].clone()).into();
    assert_eq!(restored.payload, genesis);
    Ok(())
}

#[tokio::test]
async fn absent_event_bridge_is_rejected_before_it_can_create_an_unrepairable_target() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
    let mut item = proto::QueryChange {
        entity_id: genesis.entity_id,
        member: None,
        delta: Some(proto::DeltaContent::EventBridge { events: vec![proto::Attested::opt(genesis.clone(), None).into()] }),
    };
    assert!(queue_live(&node, peer, id, 1, vec![item.clone()]).is_err());
    assert!(session.live.lock().unwrap().publications.is_empty());
    item.member = Some(proto::QueryMember { key: vec![], head: proto::Clock::singleton(&genesis) });
    queue_live(&node, peer, id, 1, vec![item])?;
    pump_state_stream(&node, peer, id).await?;
    assert_eq!(session.state.lock().await.live_targets.len(), 1);
    Ok(())
}

#[tokio::test]
async fn independent_coverage_retires_the_round_before_a_late_unavailable_repair_receipt() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let source = states(1, 1);
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(source.clone()).await?,
        unknown(),
    )
    .await?;
    assert_eq!(state_items(&initial.frame).1, ComparisonProgress::Complete);
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    let entity = &source[0].payload;
    continue_state(
        &node,
        peer,
        proto::RequestId::new(),
        id,
        Some(0),
        0,
        None,
        vec![StateAnswer::NeedRepair { entity_id: entity.entity_id, target: entity.state.head.clone() }],
        vec![],
    )
    .await?;
    pump_state_stream(&node, peer, id).await?;
    let last = {
        let state = session.state.lock().await;
        let frame = &state.retained.back().unwrap().frame;
        assert!(state_items(frame).0.iter().any(|item| matches!(item, StateItem::RepairUnavailable { .. })));
        assert_eq!(state.required_through, 0, "late repair must not move the finite enumeration boundary");
        assert!(state.last_sent > 0);
        state.last_sent
    };
    // A live delivery may have covered the target independently. The receiver
    // proves that coverage; the sender accepts its claim through Complete.
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![]).await?;
    {
        let state = session.state.lock().await;
        assert!(state.comparison.is_none());
        assert!(
            state.retained.iter().any(|retained| retained.frame.sequence == last),
            "late repair still consumes receipt credit after the snapshot retires"
        );
    }
    let mut empty = snapshot(vec![]).await?;
    let offer = empty.offer().await?;
    schedule_round(&node, peer, id, 1, 1, empty, offer).await?;
    pump_state_stream(&node, peer, id).await?;
    continue_state(
        &node,
        peer,
        proto::RequestId::new(),
        id,
        Some(0),
        last,
        None,
        vec![StateAnswer::NeedRepair { entity_id: entity.entity_id, target: entity.state.head.clone() }],
        vec![],
    )
    .await?;
    let state = session.state.lock().await;
    let comparison = state.comparison.as_ref().unwrap();
    assert_eq!(state.round.number, 1);
    assert!(comparison.snapshot.members().is_empty());
    assert!(comparison.work.is_empty() && comparison.pending_repairs.is_empty());
    assert!(state.retained.iter().all(|retained| retained.frame.sequence > last));
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
        start_state_stream(&node, peer, proto::RequestId::new(), vec![], vec![], None, false, 0, snapshot(vec![source]).await?, unknown())
            .await?;
    continue_state(
        &node,
        peer,
        proto::RequestId::new(),
        initial.frame.stream,
        Some(0),
        0,
        None,
        vec![StateAnswer::NeedRepair { entity_id: genesis.entity_id, target }],
        vec![],
    )
    .await?;
    pump_state_stream(&node, peer, initial.frame.stream).await?;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&initial.frame.stream).unwrap();
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
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((query, 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let item = live_item(1);
    assert!(queue_live(&node, peer, id, 1, vec![item; MAX_LIVE_ITEMS + 1]).is_err());
    expire_state_stream(&node, peer, id).await?;
    let messages = messages.lock().unwrap();
    assert!(messages.iter().any(|message| matches!(message, proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame: StateFrame { stream: comparison, body: StateFrameBody::RestartRequired, .. }, .. }, .. }) if *comparison == id)));
    assert!(!messages.iter().any(|message| matches!(
        message,
        proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::StateFrame {
                frame: StateFrame { body: StateFrameBody::Live { .. } | StateFrameBody::LiveChunk { .. }, .. },
                ..
            },
            ..
        })
    )));
    drop(messages);
    abandon_state_stream(&node, peer, id).await;
    assert!(node.peer_connections.get(&peer).is_some(), "successful restart control preserves other streams on this connection");
    Ok(())
}

#[tokio::test]
async fn retired_controls_preserve_credential_checks_and_catalog_bypass() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    for catalog in [false, true] {
        let credentials = if catalog { vec![] } else { vec![crate::policy::DEFAULT_CONTEXT] };
        let initial = start_state_stream(
            &node,
            peer,
            proto::RequestId::new(),
            vec![],
            credentials.clone(),
            None,
            catalog,
            0,
            snapshot(vec![]).await?,
            unknown(),
        )
        .await?;
        let id = initial.frame.stream;
        let receipt = || proto::NodeRequestBody::ContinueState {
            stream: id,
            round: None,
            applied_through: None,
            processed_through: 0,
            answers: vec![],
        };
        assert_eq!(state_stream_bypasses_policy(&node, peer, &receipt(), false), catalog);
        let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
        assert!(session.permits(&credentials));
        let authenticated = [crate::policy::DEFAULT_CONTEXT];
        assert!(!session.permits(if catalog { &authenticated } else { &[] }));
        finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, credentials).await?;
        assert!(node.peer_connections.get(&peer).unwrap().state_streams.get(&id).is_none());
        assert_eq!(state_stream_bypasses_policy(&node, peer, &receipt(), false), catalog);

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
                assert!(matches!(response, proto::NodeResponseBody::StateStreamUnavailable { stream: comparison } if *comparison == id));
            }
        }
    }
    let unknown = proto::NodeRequestBody::ContinueState {
        stream: proto::StateStreamId::new(),
        round: None,
        applied_through: None,
        processed_through: 0,
        answers: vec![],
    };
    assert!(!state_stream_bypasses_policy(&node, peer, &unknown, false), "unknown controls must pass through policy");
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
        let StateFrameBody::State { items, .. } = comparison.reply(node, &vec![], false, false, 0).await? else { panic!("state") };
        assert!(comparison.work.len() <= 4096);
        assert!(bincode::serialized_size(&items)? <= PAGE_BYTES as u64);
        assert!(!items.iter().any(|item| matches!(item, StateItem::NeedHeads(_) | StateItem::Digest(_))));
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
        let mut comparison = Comparison::new(snapshot(source).await?, known)?;
        assert_eq!(comparison.known_clocks.len(), count);
        assert!(comparison.work.iter().all(|work| matches!(work, Work::InitialHeads(_))));
        let items = initial_items(&node, &mut comparison).await?;
        assert!(items.is_empty(), "equal initial pages need no response items");
        assert!(comparison.complete, "the full captured inventory was compared before Complete");
    }
    let before = ordered_states(128, 2);
    let mut after = before.clone();
    after[17].payload.state.head = proto::Clock::genesis(proto::EventId::from_bytes([2; 32]));
    let mut requester = snapshot(before).await?;
    let mut comparison = Comparison::new(snapshot(after.clone()).await?, requester.offer().await?)?;
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
async fn initial_diff_keeps_cross_batch_moves_and_readable_former_member_repairs() -> anyhow::Result<()> {
    let (node, _, _) = node().await?;
    let query = super::super::QueryTreeCache::default();
    let before = (0..256).map(|id| positioned_state(id, id as i64, 1)).collect::<anyhow::Result<Vec<_>>>()?;
    let mut old = query.refresh(position_spec(), before).await?;
    let known = old.offer().await?;
    assert_eq!(known.spans.len(), 1);
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
    let mut comparison = Comparison::new(captured, known)?;
    assert_eq!(comparison.known_clocks.len(), 256, "all clocks are loaded before the first moved key is visited");
    let items = initial_items(&node, &mut comparison).await?;
    let mut repairs = HashMap::new();
    for item in items {
        match item {
            StateItem::Heads(page) => {
                members.retain(|address, _| !page.bounds.contains(address));
                for leaf in page.heads.to_vec() {
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
    let mut comparison = Comparison::new(snapshot(current).await?, requester.offer().await?)?;
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
    let mut comparison = Comparison::new(snapshot(vec![]).await?, requester.offer().await?)?;
    let items = initial_items(&node, &mut comparison).await?;
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Heads(page) if page.heads.is_empty())).count(), 16);
    assert_eq!(
        items.iter().filter(|item| matches!(item, StateItem::Repair { .. })).count(),
        2048,
        "coalescing must preserve every actual former clock repair"
    );
    Ok(())
}

#[tokio::test]
async fn one_initial_inventory_expands_one_bounded_batch_at_a_time() -> anyhow::Result<()> {
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
    let mut comparison = Comparison::new(snapshot(current).await?, requester.offer().await?)?;
    assert_eq!(comparison.work.len(), 1);
    let Some(Work::InitialHeads(page)) = comparison.work.pop_front() else { panic!("initial page") };
    comparison.expand_initial_heads(page)?;
    // One batch can add 128 point removals, 128 repairs, and 129 gaps;
    // the rest of the single request inventory remains deferred.
    assert!(comparison.work.len() <= 385 + 1);
    assert!(comparison.work.iter().any(|work| matches!(work,
        Work::InitialHeads(rest) if rest.heads.len() == 2048 - HEAD_PAGE)));
    let items = initial_items(&node, &mut comparison).await?;
    assert_eq!(items.iter().filter(|item| matches!(item, StateItem::Heads(page) if page.heads.is_empty())).count(), 2048);
    assert_eq!(
        items.iter().filter_map(|item| if let StateItem::Snapshots(page) = item { Some(page.states.len()) } else { None }).sum::<usize>(),
        2048
    );
    Ok(())
}

#[tokio::test]
async fn prefix_encoded_initial_inventory_keeps_its_cursor_across_work_batches() -> anyhow::Result<()> {
    let states = (0..1024).map(|n| positioned_state(n, n as i64, 1)).collect::<anyhow::Result<Vec<_>>>()?;
    let tree = super::super::QueryTreeCache::default();
    let mut requester = tree.refresh(position_spec(), states.clone()).await?;
    let known = requester.offer().await?;
    let mut comparison = Comparison::new(tree.refresh(position_spec(), states).await?, known)?;
    let mut remaining = 1024;
    let mut start = super::super::whole().start;
    while let Some(Work::InitialHeads(page)) = comparison.work.pop_front() {
        assert_eq!(page.bounds.start, start);
        assert_eq!(page.heads.len(), remaining);
        comparison.expand_initial_heads(page)?;
        remaining -= HEAD_PAGE;
        if remaining > 0 {
            let Some(Work::InitialHeads(next)) = comparison.work.front() else { panic!("only the deferred equal batch remains") };
            assert_ne!(next.bounds.start, start);
            start = next.bounds.start.clone();
        }
    }
    assert_eq!(remaining, 0);
    assert!(comparison.work.is_empty(), "equal rows require neither repairs nor membership echoes");
    Ok(())
}

#[tokio::test]
async fn initial_inventory_limits_and_exact_point_bounds_are_validated() -> anyhow::Result<()> {
    let before = ordered_states(2048, 2);
    let mut requester = snapshot(before.clone()).await?;
    let mut known = requester.offer().await?;
    let point = before[0].payload.entity_id.to_bytes().to_vec();
    let page = requester.page(TreeSpan::new(std::ops::Bound::Included(point.clone()), std::ops::Bound::Included(point)))?;
    let mut exact = Comparison::new(
        snapshot(before.clone()).await?,
        proto::KnownState { spec: spec(), spans: vec![proto::SpanContent::Heads(page.clone())] },
    )?;
    let point_work = exact.work.iter().position(|work| matches!(work, Work::InitialHeads(_))).unwrap();
    let Some(Work::InitialHeads(validated)) = exact.work.remove(point_work) else { panic!("validated exact point") };
    assert_eq!(validated.bounds, page.bounds);
    let unknown_gaps = exact.work.len();
    exact.expand_initial_heads(validated)?;
    assert_eq!(exact.work.len(), unknown_gaps, "a validated equal point adds no equality echo");
    assert_eq!(exact.known_clocks.len(), 1);
    let (node, _, _) = node().await?;
    let items = initial_items(&node, &mut exact).await?;
    assert!(exact.complete);
    assert!(items.iter().all(|item| matches!(item, StateItem::Snapshots(_))), "unknown gaps still require membership output");
    assert_eq!(items.iter().map(|item| if let StateItem::Snapshots(page) = item { page.states.len() } else { 0 }).sum::<usize>(), 2047);
    known.spans.push(known.spans[0].clone());
    assert!(Comparison::new(snapshot(before.clone()).await?, known).is_err(), "overlapping pages must fail before comparison");
    let extra = positioned_state(3, 10000, 1)?;
    let mut keyed = super::super::QueryTreeCache::default().refresh(position_spec(), vec![extra]).await?;
    let mut duplicate = keyed.offer().await?;
    let proto::SpanContent::Heads(first) = &mut duplicate.spans[0] else { unreachable!() };
    let mut second = first.clone();
    let key = CanonicalCodec.encode_prefix(position_spec().key_spec(), &[ankurah_core_types::Value::I64(10001)])?;
    let mut rows = second.heads.to_vec();
    rows[0].key = key;
    let addr = leaf_address(&rows[0].key, rows[0].entity.entity_id);
    second.heads = rows.into();
    first.bounds.end = std::ops::Bound::Excluded(addr.clone());
    second.bounds.start = std::ops::Bound::Included(addr);
    duplicate.spans.push(proto::SpanContent::Heads(second));
    assert!(
        Comparison::new(super::super::QueryTreeCache::default().refresh(position_spec(), vec![]).await?, duplicate).is_err(),
        "global duplicate IDs must fail even at distinct keys/pages"
    );
    let mut oversized = requester.offer().await?;
    let last = ordered_states(2049, 2).pop().unwrap();
    let proto::SpanContent::Heads(page) = oversized.spans.last_mut().unwrap() else { unreachable!() };
    page.bounds.end = std::ops::Bound::Excluded(last.payload.entity_id.to_bytes().to_vec());
    oversized.spans.push(proto::SpanContent::Heads(proto::TreeSpanInventory {
        bounds: TreeSpan::new(std::ops::Bound::Included(last.payload.entity_id.to_bytes().to_vec()), super::super::whole().end),
        heads: vec![proto::KeyedEntityHead {
            key: vec![],
            entity: proto::EntityHead { entity_id: last.payload.entity_id, head: last.payload.state.head },
        }]
        .into(),
    }));
    assert!(Comparison::new(snapshot(before).await?, oversized).is_err(), "total leaf cap spans all pages");
    Ok(())
}

#[tokio::test]
async fn compressed_initial_spans_share_one_uncompressed_work_budget() -> anyhow::Result<()> {
    let page = proto::TreeSpanInventory {
        bounds: super::super::whole(),
        heads: (0..128)
            .map(|n| proto::KeyedEntityHead {
                key: vec![42; 256],
                entity: proto::EntityHead { entity_id: proto::EntityId::from_bytes([n; 32]), head: proto::Clock::default() },
            })
            .collect::<Vec<_>>()
            .into(),
    };
    let known = proto::KnownState { spec: spec(), spans: vec![proto::SpanContent::Heads(page); 8] };
    assert!(bincode::serialized_size(&known)? < INITIAL_INVENTORY_BYTES as u64);
    let error = Comparison::new(snapshot(vec![]).await?, known).err().expect("aggregate work must be rejected before span validation");
    assert!(error.to_string().contains("initial inventory exceeds limit"));
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
    let mut requester = super::super::QueryTreeCache::default().refresh(spec.clone(), vec![before]).await?;
    let known = requester.offer().await?;
    let [proto::SpanContent::Heads(page)] = known.spans.as_slice() else { panic!("one valid complete initial inventory") };
    assert!(bincode::serialized_size(&StateAnswer::Heads(page.clone()))? <= PAGE_BYTES as u64);
    let supplied_clock = page.heads.to_vec()[0].entity.head.clone();
    let address = leaf_address(&page.heads.to_vec()[0].key, page.heads.to_vec()[0].entity.entity_id);
    let removal = StateItem::Heads(proto::TreeSpanInventory {
        bounds: TreeSpan::new(std::ops::Bound::Included(address.clone()), std::ops::Bound::Included(address)),
        heads: vec![].into(),
    });
    assert!(bincode::serialized_size(&removal)? > PAGE_BYTES as u64);
    let (node, _, _) = node().await?;
    let mut tx = node.storage.transaction();
    tx.set_state(&Default::default(), &former).await?;
    tx.commit().await?;
    let empty = super::super::QueryTreeCache::default().refresh(spec.clone(), vec![]).await?;
    let mut comparison = Comparison::new(empty, known.clone())?;
    let Some(Work::InitialHeads(page)) = comparison.work.pop_front() else { panic!("initial region") };
    comparison.expand_initial_heads(page)?;
    assert!(comparison.work.iter().any(|work| matches!(work, Work::SendHeads(bounds) if *bounds == super::super::whole())));
    assert!(comparison
        .work
        .iter()
        .any(|work| matches!(work, Work::Repair(id, Some(clock)) if *id == former.payload.entity_id && *clock == supplied_clock)));
    let items = initial_items(&node, &mut comparison).await?;
    assert!(items
        .iter()
        .any(|item| matches!(item, StateItem::Heads(page) if page.bounds == super::super::whole() && page.heads.is_empty())));
    assert_eq!(comparison.offered.get(&former.payload.entity_id), Some(&former.payload.state.head));
    let moved = text_state("a".into(), 2)?;
    let captured = super::super::QueryTreeCache::default().refresh(spec, vec![moved.clone()]).await?;
    let mut comparison = Comparison::new(captured, known)?;
    let items = initial_items(&node, &mut comparison).await?;
    assert!(items.iter().any(|item| matches!(item, StateItem::Heads(page) if page.heads.len() == 1 && page.heads.to_vec()[0].entity.entity_id == moved.payload.entity_id)));
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
    let requester = snapshot(source).await?;
    let mut remaining = super::super::whole();
    let mut spans = Vec::new();
    loop {
        let page = requester.page(remaining)?;
        assert!(bincode::serialized_size(&StateAnswer::Heads(page.clone()))? <= PAGE_BYTES as u64);
        let end = page.bounds.end.clone();
        spans.push(proto::SpanContent::Heads(page));
        if end == super::super::whole().end {
            break;
        }
        let std::ops::Bound::Included(last) = end else { panic!("progress") };
        remaining = super::super::whole().after(&last);
    }
    let known = proto::KnownState { spec: spec(), spans };
    assert!(bincode::serialized_size(&known)? > INITIAL_INVENTORY_BYTES as u64);
    assert!(Comparison::new(snapshot(vec![]).await?, known).is_err());
    Ok(())
}

#[test]
fn comparison_credentials_require_the_same_order_independent_bag() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    struct Credential(u8);
    impl ContextData for Credential {}
    let reader = Credential(1);
    let admin = Credential(2);
    assert!(same_credentials(&[reader.clone(), admin.clone()], &[admin.clone(), reader.clone()]));
    assert!(same_credentials(&[reader.clone(), reader.clone(), admin.clone()], &[admin.clone(), reader.clone(), reader.clone()]));
    assert!(!same_credentials(&[reader.clone(), reader.clone()], &[reader.clone(), admin.clone()]));
    assert!(!same_credentials(&[reader.clone(), admin], &[reader.clone(), reader]));
}

#[tokio::test]
async fn shared_live_target_survives_until_every_offering_publication_is_applied() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 0)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&initial.frame.stream).unwrap();
    let mut state = session.state.lock().await;
    let item = live_item(1);
    let head = item.member.as_ref().unwrap().head.clone();
    let key = bincode::serialize(&(item.entity_id, &head))?;
    state.offer_live_target(1, &item)?;
    state.offer_live_target(1, &item)?;
    state.offer_live_target(2, &item)?;
    assert_eq!(state.live_targets.len(), 2, "duplicate targets within a publication share one reference");
    assert_eq!(state.live_target_index.len(), 1);
    for revision in 1..=2 {
        state.retain(
            StateFrame {
                stream: initial.frame.stream,
                sequence: revision,
                body: StateFrameBody::Live { revision, items: vec![], complete: true },
            },
            false,
        )?;
    }
    state.acknowledge(2)?;
    state.acknowledge_live(1)?;
    assert_eq!(state.live_target_index.get(key.as_slice()).unwrap().publications, 1);
    state.request_live_repairs(vec![StateAnswer::NeedRepair { entity_id: item.entity_id, target: head.clone() }])?;
    state.acknowledge_live(2)?;
    assert!(state.live_target_index.is_empty() && state.live_targets.is_empty());
    assert_eq!(state.target_bytes, 0);
    assert!(state.request_live_repairs(vec![StateAnswer::NeedRepair { entity_id: item.entity_id, target: head }]).is_err());
    Ok(())
}

#[tokio::test]
async fn first_live_frame_after_idle_starts_a_fresh_progress_timeout() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 0)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&initial.frame.stream).unwrap();
    let mut state = session.state.lock().await;
    state.last_activity = Instant::now() - STREAM_TTL;
    state.retain(
        StateFrame {
            stream: initial.frame.stream,
            sequence: 1,
            body: StateFrameBody::Live { revision: 1, items: vec![live_item(1)], complete: true },
        },
        false,
    )?;
    assert!(state.last_activity.elapsed() < STREAM_TTL, "ordinary idle time is not an unacknowledged-frame timeout");
    Ok(())
}

#[tokio::test]
async fn registered_stream_queues_live_changes_before_initial_frame_is_ready() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let pending = prepare_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        10,
        snapshot(vec![]).await?,
        unknown(),
    )?;
    let id = pending.id();
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    queue_live(&node, peer, id, 11, vec![live_item(8)])?;
    pump_state_stream(&node, peer, id).await?;
    assert!(messages.lock().unwrap().is_empty(), "no output before frame 0 is prepared");
    assert_eq!(session.live.lock().unwrap().publications.len(), 1);
    let initial = pending.initialize(&node).await?;
    assert_eq!(initial.frame.sequence, 0);
    assert_eq!(initial.round.revision, 10);
    assert!(matches!(initial.frame.body, StateFrameBody::State { progress: ComparisonProgress::Complete, .. }));
    session.pumping.store(true, Ordering::Release);
    pump_state_stream(&node, peer, id).await?;
    assert!(messages.lock().unwrap().iter().any(|message| matches!(
        message,
        proto::NodeMessage::Update(proto::NodeUpdate {
            body: proto::NodeUpdateBody::StateFrame {
                frame: StateFrame { sequence: 1, body: StateFrameBody::Live { revision: 11, complete: true, .. }, .. },
                ..
            },
            ..
        })
    )));
    Ok(())
}

#[tokio::test]
async fn dropping_uninitialized_stream_releases_its_route_and_snapshot() -> anyhow::Result<()> {
    let (node, peer, _) = node().await?;
    let pending = prepare_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )?;
    let id = pending.id();
    queue_live(&node, peer, id, 1, vec![live_item(8)])?;
    drop(pending);
    let connection = node.peer_connections.get(&peer).unwrap();
    assert!(connection.state_streams.get(&id).is_none());
    assert!(connection.state_streams.was_closed(&id));
    Ok(())
}

#[tokio::test]
async fn refresh_cut_is_queued_without_waiting_for_stream_io_and_blocks_later_live_data() -> anyhow::Result<()> {
    let (node, peer, messages) = node().await?;
    let initial = start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        snapshot(vec![]).await?,
        unknown(),
    )
    .await?;
    let id = initial.frame.stream;
    let session = node.peer_connections.get(&peer).unwrap().state_streams.get(&id).unwrap();
    session.pumping.store(true, Ordering::Release);
    finish_state_round(&node, peer, id, 0, proto::StateRoundOutcome::Applied { processed_through: 0 }, vec![]).await?;
    let mut next = snapshot(vec![]).await?;
    let offer = next.offer().await?;
    {
        // The pump holds this lock while preparing frames, including history I/O.
        let mut state = session.state.lock().await;
        queue_round(&node, peer, id, 1, 1, next, offer)?;
        queue_live(&node, peer, id, 2, vec![live_item(8)])?;
        assert!(state.next_live(&session.live)?.is_none(), "the queued marker fences this racing live read");
    }
    pump_state_stream(&node, peer, id).await?;
    let messages = messages.lock().unwrap();
    let bodies: Vec<_> = messages
        .iter()
        .filter_map(|message| match message {
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => {
                Some(&frame.body)
            }
            _ => None,
        })
        .collect();
    let marker = bodies
        .iter()
        .position(|body| matches!(body, StateFrameBody::BeginRound(round) if round.number == 1 && round.revision == 1))
        .unwrap();
    let live = bodies.iter().position(|body| matches!(body, StateFrameBody::Live { revision: 2, .. })).unwrap();
    assert!(marker < live);
    Ok(())
}
