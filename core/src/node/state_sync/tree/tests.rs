use super::*;
mod ordering;
use std::ops::Bound;

fn spec() -> TreeSpec { TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True) }

fn state(id: u8, head: u8) -> Attested<EntityState> {
    Attested {
        payload: EntityState {
            entity_id: EntityId::from_bytes([id; EntityId::BYTE_LEN]),
            state: proto::State { head: proto::Clock::genesis(proto::EventId::from_bytes([head; 32])), ..Default::default() },
        },
        attestations: Default::default(),
    }
}

fn numbered_state(id: u32, head: u8) -> Attested<EntityState> {
    let mut state = state(0, head);
    let mut bytes = [0; EntityId::BYTE_LEN];
    bytes[..4].copy_from_slice(&id.to_be_bytes());
    state.payload.entity_id = EntityId::from_bytes(bytes);
    state
}

fn offered_digest(offer: &proto::KnownState) -> anyhow::Result<ankurah_digest_tree::Digest> {
    ankurah_digest_tree::Digest::checked_sum(offer.spans.iter().map(|region| {
        let proto::SpanContent::Digest(head) = region else { panic!("expected only summaries") };
        head.digest
    }))
    .map_err(Into::into)
}

fn keyed_spec() -> TreeSpec {
    TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN])),
            ValueType::I64,
        )]),
        ankql::ast::Predicate::True,
    )
}

fn keyed_state(id: u32, head: u8, value: Option<Value>) -> anyhow::Result<(Attested<EntityState>, Vec<Option<Value>>)> {
    use crate::property::backend::{LWWBackend, PropertyBackend};
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN]));
    let backend = LWWBackend::new();
    backend.set(property, value);
    crate::test_helpers::commit_backend(&backend, head)?;
    let values = vec![backend.get(&property)];
    let mut state = numbered_state(id, head);
    state.payload.state.state_buffers = proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into());
    Ok((state, values))
}

#[tokio::test]
async fn captured_keys_match_decoded_states_across_sparse_movement_and_dense_refresh() -> anyhow::Result<()> {
    let initial =
        (0..256).map(|id| keyed_state(id, 1, (id != 11).then_some(Value::I64(id as i64 - 128)))).collect::<anyhow::Result<Vec<_>>>()?;
    let query = QueryTreeCache::default();
    let mut old = query.refresh_captured(keyed_spec(), initial.clone()).await?;
    let original = old.span_digest(super::super::whole()).await?;
    let original_members = old.members();
    let oracle = QueryTreeCache::default();
    let mut decoded = oracle.refresh(keyed_spec(), initial.iter().map(|(state, _)| state.clone()).collect()).await?;
    assert_eq!(original, decoded.span_digest(super::super::whole()).await?);
    assert_eq!(original_members, decoded.members());

    let mut sparse = initial;
    sparse[7] = keyed_state(7, 2, Some(Value::I64(9999)))?;
    sparse.retain(|(state, _)| state.payload.entity_id != numbered_state(21, 1).payload.entity_id);
    sparse.push(keyed_state(256, 2, Some(Value::I64(-5000)))?);
    let dense = sparse
        .iter()
        .enumerate()
        .map(|(n, (state, _))| {
            let id = u32::from_be_bytes(state.payload.entity_id.to_bytes()[..4].try_into().unwrap());
            keyed_state(id, 3, Some(Value::I64(n as i64 + 12345)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (rows, built) in [(sparse, true), (dense, false)] {
        let mut captured = query.refresh_captured(keyed_spec(), rows.clone()).await?;
        assert_eq!(captured.tree.is_some(), built, "sparse movement stays incremental; dense refresh defers reconstruction");
        let oracle = QueryTreeCache::default();
        let mut decoded = oracle.refresh(keyed_spec(), rows.into_iter().map(|(state, _)| state).collect()).await?;
        assert_eq!(captured.members(), decoded.members());
        assert_eq!(captured.span_digest(super::super::whole()).await?, decoded.span_digest(super::super::whole()).await?);
        assert_eq!(old.members(), original_members);
        assert_eq!(old.span_digest(super::super::whole()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn captured_key_encoding_failure_preserves_the_existing_cache() -> anyhow::Result<()> {
    let rows = (0..80).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let query = QueryTreeCache::default();
    let mut snapshot = query.refresh_captured(keyed_spec(), rows).await?;
    let original = snapshot.span_digest(super::super::whole()).await?;
    let (generation, members, states) = {
        let cache = query.0.lock().await;
        let cache = cache.as_ref().unwrap();
        (cache.generation.clone(), cache.members.clone(), cache.rows.iter().map(|(id, row)| (*id, row.state.clone())).collect::<States>())
    };
    // Both value vectors match their serialized states. The second state has
    // a property type incompatible with the retained integer key layout.
    let invalid = vec![keyed_state(0, 2, Some(Value::I64(77)))?, keyed_state(1, 2, Some(Value::String("wrong type".into())))?];
    assert!(query.refresh_captured(keyed_spec(), invalid).await.is_err());
    let mut tree = {
        let cache = query.0.lock().await;
        let cache = cache.as_ref().unwrap();
        assert!(Arc::ptr_eq(&generation, &cache.generation));
        assert!(Arc::ptr_eq(&members, &cache.members));
        assert_eq!(cache.rows.len(), states.len());
        for (id, row) in &cache.rows {
            assert!(Arc::ptr_eq(&row.state, &states[id]));
        }
        cache.tree.as_ref().expect("the existing tree must remain built").begin()
    };
    let scope = tree.scope_ranges([super::super::whole()]);
    assert_eq!(tree.digest(&scope).await?, original.digest);
    assert_eq!(snapshot.span_digest(super::super::whole()).await?, original);
    Ok(())
}

#[tokio::test]
async fn dense_refresh_rebuilds_lazily_and_preserves_captured_snapshots() -> anyhow::Result<()> {
    let query = QueryTreeCache::default();
    let initial = (0..256).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial).await?;
    assert_eq!(old.offer().await?.spans.len(), 1, "the complete inventory is one region");
    let original = old.span_digest(super::super::whole()).await?;

    // Addition, replacement, and removal each cross the measured dense threshold.
    for states in [
        (0..512).map(|id| numbered_state(id, 1)).collect::<Vec<_>>(),
        (0..512).map(|id| numbered_state(id, 2)).collect(),
        (0..256).map(|id| numbered_state(id, 2)).collect(),
    ] {
        let mut refreshed = query.refresh(spec(), states.clone()).await?;
        assert!(refreshed.tree.is_none(), "dense refresh must defer digest construction");
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        let recomputed_query = QueryTreeCache::default();
        let mut recomputed = recomputed_query.refresh(spec(), states).await?;
        assert_eq!(refreshed.members(), recomputed.members());
        let offer = refreshed.offer().await?;
        assert_eq!(offer.spans.len(), 1, "a retained bulk rebuild offers one complete inventory");
        assert!(offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Heads(_))));
        assert!(refreshed.tree.is_none(), "offering a complete inventory stays lazy");
        assert_eq!(refreshed.span_digest(super::super::whole()).await?, recomputed.span_digest(super::super::whole()).await?);
        assert_eq!(old.span_digest(super::super::whole()).await?, original);
    }

    let sparse = (0..256).map(|id| numbered_state(id, if id == 17 { 3 } else { 2 })).collect::<Vec<_>>();
    let mut refreshed = query.refresh(spec(), sparse.clone()).await?;
    assert!(refreshed.tree.is_some(), "a sparse refresh must retain incremental construction");
    let recomputed_query = QueryTreeCache::default();
    let mut recomputed = recomputed_query.refresh(spec(), sparse).await?;
    assert_eq!(refreshed.span_digest(super::super::whole()).await?, recomputed.span_digest(super::super::whole()).await?);
    assert_eq!(old.span_digest(super::super::whole()).await?, original);
    Ok(())
}

#[tokio::test]
async fn retained_unbuilt_cache_offers_a_complete_inventory_after_initial_membership_fill() -> anyhow::Result<()> {
    let query = QueryTreeCache::default();
    let mut initial = query.refresh(spec(), vec![]).await?;
    assert!(matches!(initial.offer().await?.spans.as_slice(), [proto::SpanContent::Heads(_)]));
    let mut refreshed = query.refresh(spec(), (0..256).map(|id| numbered_state(id, 1)).collect()).await?;
    assert!(refreshed.tree.is_none());
    let offer = refreshed.offer().await?;
    assert_eq!(offer.spans.len(), 1);
    assert_eq!(
        offer
            .spans
            .iter()
            .map(|region| match region {
                proto::SpanContent::Heads(page) => page.heads.len(),
                _ => panic!("complete leaves"),
            })
            .sum::<usize>(),
        256
    );
    assert!(refreshed.tree.is_none());
    Ok(())
}

async fn comparison_node() -> anyhow::Result<(crate::node::Node<crate::test_utils::TestStorage, crate::policy::PermissiveAgent>, EntityId)>
{
    use crate::{
        connector::{PeerSender, SendError},
        node::Node,
        policy::PermissiveAgent,
        test_utils::TestStorage,
    };
    #[derive(Clone)]
    struct Sender(EntityId);
    impl PeerSender for Sender {
        fn send_message(&self, _: proto::NodeMessage) -> Result<(), SendError> { Ok(()) }
        fn recipient_node_id(&self) -> EntityId { self.0 }
        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let peer = EntityId::random();
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    Ok((node, peer))
}

#[tokio::test]
async fn initial_complete_keeps_complete_inventory_digests_lazy() -> anyhow::Result<()> {
    let (node, peer) = comparison_node().await?;
    for (standing, count) in [(false, 64), (true, 32), (true, 33), (true, 64), (true, 256), (true, 2048)] {
        let query = QueryTreeCache::default();
        let snapshot = query.refresh(spec(), (0..count).map(|id| numbered_state(id as u32, 1)).collect()).await?;
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        let known = if count > 64 {
            let mut requester =
                QueryTreeCache::default().refresh(spec(), (0..count).map(|id| numbered_state(id as u32, 1)).collect()).await?;
            requester.offer().await?
        } else {
            proto::KnownState { spec: spec(), spans: vec![] }
        };
        let reply = crate::node::state_sync::start_state_stream(
            &node,
            peer,
            proto::RequestId::new(),
            vec![],
            vec![],
            standing.then(|| (proto::QueryId::new(), 1)),
            false,
            0,
            snapshot,
            known,
        )
        .await?;
        assert!(matches!(reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none(), "a complete initial inventory needs no retained digest");
    }
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn a_moderate_first_refresh_repairs_changed_heads_without_building_a_digest() -> anyhow::Result<()> {
    let (node, peer) = comparison_node().await?;
    let query = QueryTreeCache::default();
    let rows = (0..64).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), rows.clone()).await?;
    let known = old.offer().await?;
    let original = bincode::serialize(&known)?;
    let subscription = proto::QueryId::new();
    let reply = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 1)),
        false,
        0,
        query.refresh(spec(), rows.clone()).await?,
        proto::KnownState { spec: spec(), spans: vec![] },
    )
    .await?;
    assert!(matches!(reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());

    let mut changed = rows;
    changed[17] = numbered_state(17, 2);
    changed.push(numbered_state(64, 2));
    let captured = query.refresh(spec(), changed.clone()).await?;
    let expected = QueryTreeCache::default().refresh(spec(), changed.clone()).await?;
    assert_eq!(captured.members(), expected.members());
    let reply = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 2)),
        false,
        0,
        captured,
        known,
    )
    .await?;
    let proto::StateFrameBody::State { items, progress, .. } = reply.frame.body else { panic!("state") };
    assert_eq!(progress, proto::ComparisonProgress::Complete);
    assert!(items.iter().any(|item| matches!(item, proto::StateItem::Repair { target, delta }
        if delta.entity_id == changed[17].payload.entity_id && *target == changed[17].payload.state.head)));
    assert!(items.iter().any(|item| matches!(item, proto::StateItem::Snapshots(page)
        if page.states.iter().any(|state| state.payload.entity_id == changed[64].payload.entity_id))));
    assert!(!items.iter().any(|item| matches!(item, proto::StateItem::Digest(_) | proto::StateItem::NeedHeads(_))));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none(), "the first refresh stays on the inventory diff path");
    assert_eq!(bincode::serialize(&old.offer().await?)?, original);
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn crossing_the_inventory_count_limit_builds_and_shrinking_preserves_the_tree() -> anyhow::Result<()> {
    let (node, peer) = comparison_node().await?;
    let query = QueryTreeCache::default();
    let rows = (0..super::super::INITIAL_INVENTORY_HEADS as u32).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), rows.clone()).await?;
    let known = old.offer().await?;
    let saved = bincode::serialize(&known)?;
    let subscription = proto::QueryId::new();
    let initial = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 1)),
        false,
        0,
        query.refresh(spec(), rows.clone()).await?,
        known.clone(),
    )
    .await?;
    assert!(matches!(initial.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());

    let mut larger = rows.clone();
    larger.push(numbered_state(super::super::INITIAL_INVENTORY_HEADS as u32, 1));
    let crossing = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 2)),
        false,
        0,
        query.refresh(spec(), larger.clone()).await?,
        known.clone(),
    )
    .await?;
    assert!(matches!(crossing.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_some(), "build the 2,049-row digest before Complete");
    let mut current = query.refresh(spec(), larger.clone()).await?;
    let mut oracle = QueryTreeCache::default().refresh(spec(), larger).await?;
    assert_eq!(current.span_digest(super::super::whole()).await?, oracle.span_digest(super::super::whole()).await?);

    let shrunk = query.refresh(spec(), rows.clone()).await?;
    assert!(shrunk.tree.is_some(), "sparse removal must retain the existing tree below the threshold");
    let final_reply = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 3)),
        false,
        0,
        shrunk,
        known,
    )
    .await?;
    assert!(matches!(final_reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    let mut current = query.refresh(spec(), rows.clone()).await?;
    let mut oracle = QueryTreeCache::default().refresh(spec(), rows).await?;
    assert!(current.tree.is_some());
    assert_eq!(current.span_digest(super::super::whole()).await?, oracle.span_digest(super::super::whole()).await?);
    assert_eq!(bincode::serialize(&old.offer().await?)?, saved, "the pre-crossing snapshot remains immutable");
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn small_and_empty_inventories_offer_exact_leaves_without_building_a_tree() -> anyhow::Result<()> {
    for count in [0, 32] {
        let query = QueryTreeCache::default();
        let states = (0..count).rev().map(|id| state(id as u8, 1)).collect();
        let mut snapshot = query.refresh(spec(), states).await?;
        let inventory = snapshot.offer().await?;
        let [proto::SpanContent::Heads(page)] = inventory.spans.as_slice() else { panic!("expected one exact inventory") };
        assert_eq!(page.bounds, super::super::whole());
        assert_eq!(page.heads.len(), count);
        assert_eq!(snapshot.members().len(), count);
        for (id, leaf) in page.heads.to_vec().iter().enumerate() {
            assert!(leaf.key.is_empty());
            assert_eq!(leaf.entity.entity_id, EntityId::from_bytes([id as u8; EntityId::BYTE_LEN]));
        }
        assert!(snapshot.tree.is_none());
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn small_inventory_exceeding_the_byte_budget_falls_back_to_a_summary() -> anyhow::Result<()> {
    let wide_clock = |count: u32| {
        proto::Clock::new(
            (0..count)
                .map(|id| {
                    let mut bytes = [0; 32];
                    bytes[..4].copy_from_slice(&id.to_be_bytes());
                    (1, proto::EventId::from_bytes(bytes))
                })
                .collect::<Vec<_>>(),
        )
    };
    let mut state = state(1, 1);
    state.payload.state.head = wide_clock(3000)?;
    let mut within_request = QueryTreeCache::default().refresh(spec(), vec![state.clone()]).await?;
    let offer = within_request.offer().await?;
    assert!(matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]));
    assert!(bincode::serialized_size(&offer)? > super::super::PAGE_BYTES as u64);
    assert!(within_request.page(super::super::whole()).is_err(), "follow-up pages retain their smaller limit");
    assert!(within_request.tree.is_none());

    state.payload.state.head = wide_clock(10000)?;
    let query = QueryTreeCache::default();
    let mut snapshot = query.refresh(spec(), vec![state.clone()]).await?;
    assert!(snapshot.page(super::super::whole()).is_err(), "an oversized single leaf must fail explicitly");
    assert!(snapshot.tree.is_none(), "preparing the candidate inventory must not build a tree");
    let inventory = snapshot.offer().await?;
    let [proto::SpanContent::Digest(head)] = inventory.spans.as_slice() else { panic!("oversized inventory must use a summary") };
    assert_eq!(head.digest.count(), 1);
    assert_eq!(head.bounds, super::super::whole());
    assert!(bincode::serialized_size(&inventory)? <= super::super::FRAME_BYTES as u64);
    assert!(snapshot.tree.is_some());

    // A small result that cannot fit the inventory byte policy still builds
    // on demand when a later requester offers its Digest. The count guard
    // avoids eager construction, not the work needed by this comparison.
    let (node, peer) = comparison_node().await?;
    let responder = QueryTreeCache::default();
    let captured = responder.refresh(spec(), vec![state]).await?;
    assert!(responder.0.lock().await.as_ref().unwrap().tree.is_none());
    let reply = crate::node::state_sync::start_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        captured,
        inventory,
    )
    .await?;
    let proto::StateFrameBody::State { items, progress, .. } = reply.frame.body else { panic!("state") };
    assert_eq!(progress, proto::ComparisonProgress::Complete);
    assert!(items.is_empty(), "equal retained frontiers need no response items");
    assert!(responder.0.lock().await.as_ref().unwrap().tree.is_some());
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn leaf_pages_budget_the_exact_answer_and_preserve_old_wide_key_snapshots() -> anyhow::Result<()> {
    let wide_spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(
            proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN])),
            ValueType::String,
        )]),
        ankql::ast::Predicate::True,
    );
    let rows = (0..32)
        .map(|id| {
            let mut row = keyed_state(id as u32, 1, Some(Value::String("key".repeat(160))))?;
            row.0.payload.state.head = proto::Clock::new(
                (0..100u32)
                    .map(|n| {
                        let mut bytes = [0; 32];
                        bytes[..4].copy_from_slice(&n.to_be_bytes());
                        (1, proto::EventId::from_bytes(bytes))
                    })
                    .collect::<Vec<_>>(),
            )?;
            Ok(row)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let query = QueryTreeCache::default();
    let mut old = query.refresh_captured(wide_spec.clone(), rows).await?;
    let mut remaining = super::super::whole();
    let mut pages = Vec::new();
    let mut ids = Vec::new();
    loop {
        let page = old.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(bincode::serialized_size(&proto::StateAnswer::Heads(page.clone()))? <= super::super::PAGE_BYTES as u64);
        ids.extend(page.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id));
        let complete = page.bounds.end == super::super::whole().end;
        if !complete {
            let Bound::Included(last) = &page.bounds.end else { panic!("a byte-bounded page must advance") };
            remaining = super::super::whole().after(last);
        }
        pages.push(page);
        if complete {
            break;
        }
    }
    assert!(pages.len() > 1, "clock and key bytes must split an ordinary inline-sized inventory");
    assert_eq!(ids, old.members().values().copied().collect::<Vec<_>>());
    assert!(old.tree.is_none());
    let offer = old.offer().await?;
    assert!(
        matches!(offer.spans.as_slice(), [proto::SpanContent::Heads(_)]),
        "one initial inventory has its own budget, independent of follow-up pagination"
    );

    let changed = (0..32).map(|id| keyed_state(id as u32, 2, Some(Value::String("changed".into())))).collect::<anyhow::Result<Vec<_>>>()?;
    query.refresh_captured(wide_spec, changed).await?;
    assert_eq!(bincode::serialize(&old.page(super::super::whole())?)?, bincode::serialize(&pages[0])?);
    Ok(())
}

#[tokio::test]
async fn canonical_pages_cover_requested_bounds_without_building_a_tree() -> anyhow::Result<()> {
    let spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId)]),
        ankql::ast::Predicate::True,
    );
    let query = QueryTreeCache::default();
    let snapshot = query.refresh(spec, (0..70).rev().map(|id| state(id, 1)).collect()).await?;
    let members = snapshot.members();
    let addresses = members.keys().cloned().collect::<Vec<_>>();
    let requested = proto::TreeSpan::new(Bound::Excluded(addresses[2].clone()), Bound::Included(addresses[67].clone()));
    let mut remaining = requested.clone();
    let mut received = Vec::new();
    loop {
        let page = snapshot.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(page.heads.len() <= super::super::HEAD_PAGE);
        for leaf in page.heads.to_vec() {
            let address = leaf_address(&leaf.key, leaf.entity.entity_id);
            assert!(requested.contains(&address));
            assert!(page.bounds.contains(&address));
            received.push(address);
        }
        if page.bounds.end == requested.end {
            break;
        }
        let Bound::Included(last) = page.bounds.end else { panic!("page must end at its last leaf") };
        remaining = requested.after(&last);
    }
    assert_eq!(received, addresses[3..68]);
    assert!(snapshot.tree.is_none());
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
    Ok(())
}

#[tokio::test]
async fn an_unbuilt_snapshot_stays_stable_after_refresh() -> anyhow::Result<()> {
    let query = QueryTreeCache::default();
    let before = vec![state(1, 1), state(2, 1)];
    let mut old = query.refresh(spec(), before.clone()).await?;
    let mut current = query.refresh(spec(), vec![state(1, 2), state(3, 1)]).await?;
    let expected_query = QueryTreeCache::default();
    let mut expected = expected_query.refresh(spec(), before).await?;
    assert_eq!(old.span_digest(super::super::whole()).await?, expected.span_digest(super::super::whole()).await?);
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none(), "building an old snapshot cannot populate the current cache");
    let page = old.page(super::super::whole())?;
    assert_eq!(
        page.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>(),
        old.members().values().copied().collect::<Vec<_>>()
    );
    assert_ne!(old.span_digest(super::super::whole()).await?, current.span_digest(super::super::whole()).await?);
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_some());
    Ok(())
}

#[tokio::test]
async fn retained_tree_changes_match_recomputation_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let query = QueryTreeCache::default();
    let initial = (0..70).map(|id| state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial.clone()).await?;
    let inventory = old.offer().await?;
    assert!(inventory.spans.iter().all(|region| matches!(region, proto::SpanContent::Heads(_))));
    assert!(old.tree.is_none());
    let original = old.span_digest(super::super::whole()).await?;
    let generation = query.0.lock().await.as_ref().unwrap().generation.clone();
    let mut unchanged = query.refresh(spec(), initial).await?;
    assert!(Arc::ptr_eq(&generation, &query.0.lock().await.as_ref().unwrap().generation));
    assert_eq!(unchanged.span_digest(super::super::whole()).await?, original);
    for states in [
        (10..80).map(|id| state(id, if id == 30 { 2 } else { 1 })).collect::<Vec<_>>(),
        (20..40).map(|id| state(id, 3)).collect(),
        Vec::new(),
    ] {
        let mut incremental = query.refresh(spec(), states.clone()).await?;
        assert!(incremental.tree.is_some(), "refresh must retain the already-built tree");
        let recomputed_query = QueryTreeCache::default();
        let mut recomputed = recomputed_query.refresh(spec(), states).await?;
        assert_eq!(incremental.members(), recomputed.members());
        assert_eq!(incremental.span_digest(super::super::whole()).await?, recomputed.span_digest(super::super::whole()).await?);
        assert_eq!(old.span_digest(super::super::whole()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn a_new_summary_frontier_is_bounded_and_covers_the_entire_snapshot_including_empty_gaps() -> anyhow::Result<()> {
    let states = (0..4096u32)
        .map(|n| {
            let mut state = state(0, 1);
            let mut bytes = [0; EntityId::BYTE_LEN];
            bytes[..4].copy_from_slice(&n.to_be_bytes());
            state.payload.entity_id = EntityId::from_bytes(bytes);
            state
        })
        .collect::<Vec<_>>();
    let query = QueryTreeCache::default();
    let mut snapshot = query.refresh(spec(), states.clone()).await?;
    let inventory = snapshot.offer().await?;
    assert!(inventory.spans.len() > 1, "a new large tree must offer its bounded cached descendants");
    assert!(inventory.spans.len() <= OFFER_SPANS);
    assert!(bincode::serialized_size(&inventory)? <= OFFER_BYTES);
    let mut next = super::super::whole().start;
    let mut digests = Vec::new();
    let mut empty = false;
    for region in inventory.spans {
        let proto::SpanContent::Digest(head) = region else { panic!("large offer must contain only summaries") };
        assert_eq!(head.bounds.start, next, "spans must be disjoint and exactly adjacent");
        assert_eq!(snapshot.span_digest(head.bounds.clone()).await?.digest, head.digest, "each head describes its exact captured range");
        empty |= head.digest.count() == 0;
        digests.push(head.digest);
        next = match head.bounds.end {
            Bound::Included(end) => Bound::Excluded(end),
            Bound::Excluded(end) => Bound::Included(end),
            Bound::Unbounded => Bound::Unbounded,
        };
    }
    assert_eq!(next, super::super::whole().end);
    assert!(empty, "gaps before and after the populated addresses must remain explicit");
    let root = snapshot.span_digest(super::super::whole()).await?;
    assert_eq!(ankurah_digest_tree::Digest::checked_sum(digests.iter().copied())?, root.digest);

    let mut warm = query.refresh(spec(), states).await?;
    let retained = warm.offer().await?;
    assert!(retained.spans.len() > 1, "retained offers must also identify bounded changed spans");
    assert!(retained.spans.len() <= OFFER_SPANS);
    assert!(bincode::serialized_size(&retained)? <= OFFER_BYTES);
    assert!(retained
        .spans
        .iter()
        .all(|region| matches!(region, proto::SpanContent::Digest(head) if head.digest.count() <= super::super::HEAD_PAGE as i64)));
    assert_eq!(offered_digest(&retained)?, root.digest);
    Ok(())
}

#[tokio::test]
async fn complete_initial_inventories_are_bounded_lazy_and_immutable() -> anyhow::Result<()> {
    for count in [256, super::super::INITIAL_INVENTORY_HEADS] {
        let query = QueryTreeCache::default();
        let mut old = query.refresh(spec(), (0..count as u32).map(|id| numbered_state(id, 1)).collect()).await?;
        let offer = old.offer().await?;
        assert!(bincode::serialized_size(&offer)? <= super::super::INITIAL_INVENTORY_BYTES as u64);
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("one complete initial inventory") };
        assert_eq!(inventory.bounds, super::super::whole());
        assert_eq!(inventory.heads.len(), count);
        let received = inventory.heads.to_vec().iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>();
        let wire = bincode::serialize(&offer)?;
        if count == super::super::INITIAL_INVENTORY_HEADS {
            assert!(wire.len() > super::super::PAGE_BYTES, "the request has a separate byte budget");
        }
        let decoded: proto::KnownState = bincode::deserialize(&wire)?;
        assert_eq!(bincode::serialize(&decoded)?, wire);
        assert_eq!(received, old.members().values().copied().collect::<Vec<_>>());
        assert!(old.tree.is_none());
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        query.refresh(spec(), vec![numbered_state(17, 2)]).await?;
        assert_eq!(bincode::serialize(&old.offer().await?)?, bincode::serialize(&offer)?);
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_initial_inventory_falls_back_for_count_or_total_bytes() -> anyhow::Result<()> {
    let query = QueryTreeCache::default();
    let mut too_many = query.refresh(spec(), (0..2049).map(|id| numbered_state(id, 1)).collect()).await?;
    let offer = too_many.offer().await?;
    assert!(offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))));
    assert_eq!(offered_digest(&offer)?.count(), 2049);

    let head = proto::Clock::new(
        (0u32..200)
            .map(|n| {
                let mut bytes = [0; 32];
                bytes[..4].copy_from_slice(&n.to_be_bytes());
                (1, proto::EventId::from_bytes(bytes))
            })
            .collect::<Vec<_>>(),
    )?;
    let mut wide = query
        .refresh(
            spec(),
            (0..64)
                .map(|id| {
                    let mut state = numbered_state(id, 1);
                    state.payload.state.head = head.clone();
                    state
                })
                .collect(),
        )
        .await?;
    let mut pages = Vec::new();
    let mut remaining = super::super::whole();
    loop {
        let page = wide.page(remaining)?;
        let end = page.bounds.end.clone();
        pages.push(proto::SpanContent::Heads(page));
        if end == super::super::whole().end {
            break;
        }
        let Bound::Included(last) = end else { panic!("progress") };
        remaining = super::super::whole().after(&last);
    }
    let oversized = proto::KnownState { spec: spec(), spans: pages };
    assert!(bincode::serialized_size(&oversized)? > super::super::INITIAL_INVENTORY_BYTES as u64);
    let offer = wide.offer().await?;
    assert!(
        offer.spans.iter().all(|region| matches!(region, proto::SpanContent::Digest(_))),
        "no partial leaf cover may escape the initial budget"
    );
    assert_eq!(offered_digest(&offer)?.count(), 64);
    Ok(())
}

#[tokio::test]
async fn source_row_deltas_preserve_frozen_snapshots_across_sparse_and_dense_changes() -> anyhow::Result<()> {
    let initial = (0..192).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let tree = QueryTreeCache::default();
    let mut old = tree.refresh_captured(keyed_spec(), initial.clone()).await?;
    let old_head = old.span_digest(super::super::whole()).await?;
    let old_members = old.members();
    let removed = initial[11].0.payload.entity_id;
    let sparse = keyed_state(10, 2, Some(Value::I64(-100)))?;
    tree.apply_captured(vec![sparse.clone()], vec![removed]).await?;
    let mut expected = initial
        .into_iter()
        .filter(|(state, _)| state.payload.entity_id != removed && state.payload.entity_id != sparse.0.payload.entity_id)
        .collect::<Vec<_>>();
    expected.push(sparse);
    for dense in [false, true] {
        if dense {
            let changes = (0..128)
                .filter(|id| *id != 11)
                .map(|id| keyed_state(id, 3, Some(Value::I64(id as i64 + 1000))))
                .collect::<anyhow::Result<Vec<_>>>()?;
            expected.retain(|(state, _)| !changes.iter().any(|(after, _)| after.payload.entity_id == state.payload.entity_id));
            expected.extend(changes.clone());
            // Include one new source row so this really crosses the dense threshold.
            let added = keyed_state(256, 3, Some(Value::I64(2000)))?;
            expected.push(added.clone());
            let mut changes = changes;
            changes.push(added);
            tree.apply_captured(changes, Vec::new()).await?;
        }
        let mut current = tree.current_snapshot().await?;
        let oracle = QueryTreeCache::default();
        let mut rebuilt = oracle.refresh(keyed_spec(), expected.iter().map(|(state, _)| state.clone()).collect()).await?;
        assert_eq!(current.members(), rebuilt.members());
        assert_eq!(current.span_digest(super::super::whole()).await?, rebuilt.span_digest(super::super::whole()).await?);
        assert_eq!(old.members(), old_members);
        assert_eq!(old.span_digest(super::super::whole()).await?, old_head);
    }
    let before = tree.current_snapshot().await?.members();
    let invalid = keyed_state(10, 4, Some(Value::String("wrong type".into())))?;
    assert!(tree.apply_captured(vec![invalid], vec![expected[0].0.payload.entity_id]).await.is_err());
    assert_eq!(tree.current_snapshot().await?.members(), before, "failed encoding cannot apply even the removals");
    Ok(())
}

#[tokio::test]
async fn catalog_arrival_preserves_an_accepted_entity_id_fallback() -> anyhow::Result<()> {
    use crate::{node::Node, policy::PermissiveAgent, schema::catalog::SysPropertyRow, test_utils::TestStorage};
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};

    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    let context = node.privileged_context();
    let transaction = context.begin();
    let property = transaction
        .create(&SysPropertyRow {
            name: "score".into(),
            backend: "lww".into(),
            value_type: "i64".into(),
            minted_for: None,
            target_model: None,
        })
        .await?
        .id();
    transaction.commit().await?;
    let path = PropertyPath::from(proto::PropertyId::EntityId(property));
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(path.clone())),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::I64(7))),
        },
        order_by: Some(vec![OrderByItem { path, direction: OrderDirection::Asc }]),
        limit: None,
    };
    let fallback = QueryTreeCache::spec(&selection, &CatalogManager::default());
    assert!(fallback.key_spec().keyparts.is_empty(), "missing catalog types require entity-ID keys");
    assert_eq!(QueryTreeCache::spec(&selection, &node.catalog).key_spec().keyparts.len(), 1, "catalog types now permit a keyed layout");
    let offer = proto::KnownState { spec: fallback.clone(), spans: vec![] };
    assert_eq!(
        QueryTreeCache::offered_spec(&selection, &node.catalog, &offer)?,
        fallback,
        "the accepted fallback remains a distinct tree specification"
    );
    Ok(())
}

#[tokio::test]
async fn different_spec_creates_a_new_tree_and_preserves_old_snapshots() -> anyhow::Result<()> {
    let rows = (0..80).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let states = rows.iter().map(|(state, _)| state.clone()).collect::<Vec<_>>();
    let oracle = QueryTreeCache::default();
    let expected = oracle.refresh(spec(), states.clone()).await?.span_digest(super::super::whole()).await?;
    for build_old_first in [false, true] {
        let cache = QueryTreeCache::default();
        let mut old = cache.refresh(spec(), states.clone()).await?;
        if build_old_first {
            assert_eq!(old.span_digest(super::super::whole()).await?, expected);
        }
        let mut replacement = cache.refresh_captured(keyed_spec(), rows.clone()).await?;
        let replacement_digest = replacement.span_digest(super::super::whole()).await?;
        assert_ne!(replacement_digest.digest, expected.digest);
        assert_eq!(old.spec, spec());
        assert_eq!(old.span_digest(super::super::whole()).await?, expected, "old snapshots retain their original spec and content");
        assert_eq!(cache.0.lock().await.as_ref().unwrap().tree.as_ref().unwrap().spec(), &keyed_spec());
        assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?, replacement_digest);
    }
    Ok(())
}

fn range_request(lower: i64, limit: Option<u64>, descending: bool) -> TreeRequest {
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};
    let property = keyed_spec().key_spec().keyparts[0].key;
    let path = PropertyPath::from(property);
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(path.clone())),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::I64(lower))),
        },
        order_by: Some(vec![OrderByItem { path, direction: if descending { OrderDirection::Desc } else { OrderDirection::Asc } }]),
        limit,
    };
    TreeRequest::for_selection(&selection, &std::collections::HashMap::from([(property, ValueType::I64)])).unwrap()
}

#[tokio::test]
async fn missing_key_counts_track_updates_removals_and_frozen_snapshots() -> anyhow::Result<()> {
    use ankql::ast::{OrderByItem, OrderDirection, Predicate};
    let spec = keyed_spec();
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(spec.key_spec().keyparts[0].key), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let cache = QueryTreeCache::default();
    let initial = cache.refresh_captured(spec.clone(), vec![keyed_state(1, 1, Some(Value::I64(1)))?, keyed_state(2, 1, None)?]).await?;
    assert_eq!(initial.missing_counts, [1]);
    assert!(initial.has_descending_nulls(&selection)?);
    cache.apply_captured(vec![keyed_state(2, 2, Some(Value::I64(2)))?], vec![]).await?;
    let present = cache.current_snapshot().await?;
    assert_eq!(present.missing_counts, [0]);
    assert!(!present.has_descending_nulls(&selection)?);
    cache.apply_captured(vec![keyed_state(1, 2, None)?, keyed_state(3, 1, None)?], vec![]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [2]);
    cache.apply_captured(vec![], vec![numbered_state(1, 2).payload.entity_id]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [1]);
    cache.apply_captured(vec![], vec![numbered_state(3, 1).payload.entity_id]).await?;
    assert_eq!(cache.current_snapshot().await?.missing_counts, [0]);
    let rebuilt = cache.refresh_captured(spec, vec![keyed_state(4, 1, None)?]).await?;
    assert_eq!(rebuilt.missing_counts, [1]);
    assert_eq!(initial.missing_counts, [1], "later changes cannot rewrite captured null metadata");
    assert_eq!(present.missing_counts, [0]);
    assert!(initial.has_descending_nulls(&selection)?);
    assert!(!present.has_descending_nulls(&selection)?);
    Ok(())
}

#[tokio::test]
async fn limited_inventory_does_not_hash_or_retain_the_complete_snapshot() -> anyhow::Result<()> {
    let rows = (0..4096).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTreeCache::default();
    let first = cache.refresh_captured(keyed_spec(), rows.clone()).await?;
    let second = cache.current_snapshot().await?;
    assert!(Arc::ptr_eq(&first.states, &second.states), "capturing unchanged rows must not clone the complete map");
    let mut view = second.select_request(&range_request(-1, Some(10), false)).await?;
    drop(first);
    assert_eq!(view.states.len(), 10);
    assert!(view.tree.is_none(), "selecting a small view must not build hashes");
    {
        let cached = cache.0.lock().await;
        let cached = cached.as_ref().unwrap();
        assert!(cached.tree.is_none());
        assert_eq!(Arc::strong_count(&cached.states), 1, "the selected view must not retain all 4096 captured rows");
    }
    assert!(matches!(view.offer().await?.spans.as_slice(), [proto::SpanContent::Heads(inventory)] if inventory.heads.len() == 10));
    assert!(cache.0.lock().await.as_ref().unwrap().tree.is_none());

    cache
        .apply_captured(
            vec![keyed_state(5, 2, Some(Value::I64(5)))?, keyed_state(4096, 2, Some(Value::I64(-1)))?],
            vec![numbered_state(0, 1).payload.entity_id],
        )
        .await?;
    let oracle = QueryTreeCache::default();
    let expected = oracle.refresh_captured(keyed_spec(), rows[..10].to_vec()).await?.span_digest(super::super::whole()).await?;
    assert_eq!(view.span_digest(super::super::whole()).await?, expected, "late hashes must describe the captured selection");
    assert!(cache.0.lock().await.as_ref().unwrap().tree.is_none(), "a private view digest must not populate the full resident cache");
    let mut full = cache.current_snapshot().await?;
    assert_eq!(full.span_digest(super::super::whole()).await?.digest.count(), 4096);
    assert_eq!(view.span_digest(super::super::whole()).await?, expected);
    Ok(())
}

#[tokio::test]
async fn scoped_snapshots_keep_one_full_tree_and_hide_unselected_rows() -> anyhow::Result<()> {
    let rows = (0..256).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTreeCache::default();
    let mut view = cache.refresh_captured(keyed_spec(), rows.clone()).await?.select_request(&range_request(200, None, false)).await?;
    assert_eq!(view.states.len(), 55);
    assert_eq!(
        view.members.values().copied().collect::<Vec<_>>(),
        (201..256).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let offer = view.offer().await?;
    let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("small scoped inventory") };
    assert_eq!(inventory.bounds, super::super::whole(), "scope gaps remain known-empty query addresses");
    assert_eq!(inventory.heads.len(), 55);

    let oracle = QueryTreeCache::default();
    let mut selected = oracle.refresh_captured(keyed_spec(), rows[201..].to_vec()).await?;
    let expected = selected.span_digest(super::super::whole()).await?;
    let digest = view.span_digest(super::super::whole()).await?;
    assert_eq!(digest, expected);
    let children = view.refine(&digest).await?;
    assert_eq!(Digest::checked_sum(children.iter().map(|child| child.digest))?, digest.digest);
    for child in &children {
        assert_eq!(
            child.digest,
            selected.span_digest(child.bounds.clone()).await?.digest,
            "refinement must never expose the full resident digest"
        );
    }
    let mut full = cache.current_snapshot().await?;
    assert_eq!(full.span_digest(super::super::whole()).await?.digest.count(), 256, "selecting a view must not cache a partial tree");
    assert_eq!(full.states.len(), 256);
    Ok(())
}

#[tokio::test]
async fn limits_take_independent_cuts_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let rows = (0..8).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTreeCache::default();
    cache.refresh_captured(keyed_spec(), rows).await?;
    let mut low = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(2), false)).await?;
    let mut high = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(3), true)).await?;
    assert_eq!(
        low.states.keys().copied().collect::<Vec<_>>(),
        (0..2).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    assert_eq!(
        high.states.keys().copied().collect::<Vec<_>>(),
        (5..8).map(|id| numbered_state(id, 1).payload.entity_id).collect::<Vec<_>>()
    );
    let low_before = low.span_digest(super::super::whole()).await?;
    let high_before = high.span_digest(super::super::whole()).await?;
    assert_eq!(low_before.digest.count(), 2);
    assert_eq!(high_before.digest.count(), 3);
    let high_point = high.members.keys().next().unwrap().clone();
    assert_eq!(
        low.span_digest(proto::TreeSpan::new(Bound::Included(high_point.clone()), Bound::Included(high_point))).await?.digest,
        Digest::empty()
    );

    cache.apply_captured(vec![keyed_state(99, 2, Some(Value::I64(-10)))?], vec![]).await?;
    let after = cache.current_snapshot().await?.select_request(&range_request(-1000, Some(2), false)).await?;
    assert_eq!(after.states.keys().copied().collect::<Vec<_>>(), [0, 99].map(|id| numbered_state(id, 1).payload.entity_id));
    assert_eq!(low.span_digest(super::super::whole()).await?, low_before);
    assert_eq!(high.span_digest(super::super::whole()).await?, high_before);
    assert_eq!(cache.current_snapshot().await?.states.len(), 9);
    Ok(())
}

#[tokio::test]
async fn limited_peers_answer_bounds_using_their_own_cut() -> anyhow::Result<()> {
    let requester = QueryTreeCache::default();
    let responder = QueryTreeCache::default();
    let request = range_request(-1, Some(2), false);
    let rows = |ids: &[u32]| ids.iter().map(|id| keyed_state(*id, 1, Some(Value::I64(*id as i64)))).collect::<anyhow::Result<Vec<_>>>();
    let mut left = requester.refresh_captured(keyed_spec(), rows(&[0, 3, 4])?).await?.select_request(&request).await?;
    let mut right = responder.refresh_captured(keyed_spec(), rows(&[1, 2, 4])?).await?.select_request(&request).await?;
    for view in [&mut left, &mut right] {
        let offer = view.offer().await?;
        let [proto::SpanContent::Heads(inventory)] = offer.spans.as_slice() else { panic!("limited inventory") };
        assert_eq!(inventory.bounds, super::super::whole());
        assert_eq!(inventory.heads.len(), 2);
    }
    let left_last = left.members.keys().last().unwrap().clone();
    let point = proto::TreeSpan::new(Bound::Included(left_last.clone()), Bound::Included(left_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 1);
    assert_eq!(right.span_digest(point).await?.digest.count(), 0);
    let right_last = right.members.keys().last().unwrap().clone();
    let point = proto::TreeSpan::new(Bound::Included(right_last.clone()), Bound::Included(right_last));
    assert_eq!(left.span_digest(point.clone()).await?.digest.count(), 0);
    assert_eq!(right.span_digest(point).await?.digest.count(), 1);
    assert_eq!(left.span_digest(super::super::whole()).await?.digest.count(), 2);
    assert_eq!(right.span_digest(super::super::whole()).await?.digest.count(), 2);
    Ok(())
}

#[tokio::test]
async fn peer_spec_changes_only_the_view_description_not_resident_identity() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, Predicate};
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Value::EntityId(state(0, 1).payload.entity_id))),
    };
    let selection = Selection::from(predicate.clone());
    let wire_spec = TreeSpec::new(KeySpec::new(vec![]), predicate);
    let catalog = CatalogManager::default();
    let cache = QueryTreeCache::default();
    let full = cache.refresh(spec(), (0..3).map(|id| state(id, 1)).collect()).await?;
    assert!(full.tree.is_none());
    let mut view = full.with_spec(wire_spec.clone(), &selection, &catalog)?.select(&selection, &catalog).await?;
    assert_eq!(view.spec, wire_spec);
    assert_eq!(view.span_digest(super::super::whole()).await?.digest.count(), 2);
    {
        let resident = cache.0.lock().await;
        let resident = resident.as_ref().unwrap();
        assert_eq!(resident.spec, spec());
        assert!(resident.tree.is_none(), "hashing a scoped view must leave the full cache lazy");
        assert_eq!(resident.rows.len(), 3);
    }
    assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?.digest.count(), 3);
    assert_eq!(cache.0.lock().await.as_ref().unwrap().tree.as_ref().unwrap().spec(), &spec());
    Ok(())
}

#[tokio::test]
async fn selected_fallback_ids_keep_unselected_addresses_empty() -> anyhow::Result<()> {
    let cache = QueryTreeCache::default();
    let full = cache.refresh(spec(), (0..5).map(|id| state(id, 1)).collect()).await?;
    let ids = [state(1, 1).payload.entity_id, state(3, 1).payload.entity_id].into();
    let mut view = full.select_ids(&ids).await?;
    assert_eq!(view.span_digest(super::super::whole()).await?.digest.count(), 2);
    assert_eq!(view.query_members().keys().copied().collect::<BTreeSet<_>>(), ids);
    let point = state(2, 1).payload.entity_id.to_bytes().to_vec();
    assert_eq!(
        view.span_digest(proto::TreeSpan::new(Bound::Included(point.clone()), Bound::Included(point))).await?.digest,
        Digest::empty()
    );
    assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?.digest.count(), 5);
    assert!(cache.current_snapshot().await?.select_ids(&[state(99, 1).payload.entity_id].into()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn zero_limit_view_does_not_build_or_empty_the_resident_tree() -> anyhow::Result<()> {
    let cache = QueryTreeCache::default();
    let full = cache.refresh(spec(), (0..8).map(|id| state(id, 1)).collect()).await?;
    let mut selection = Selection::from(ankql::ast::Predicate::True);
    selection.limit = Some(0);
    let mut view = full.select(&selection, &CatalogManager::default()).await?;
    assert!(view.states.is_empty());
    assert_eq!(view.span_digest(super::super::whole()).await?.digest, Digest::empty());
    assert!(cache.0.lock().await.as_ref().unwrap().tree.is_none());
    assert_eq!(cache.current_snapshot().await?.span_digest(super::super::whole()).await?.digest.count(), 8);
    Ok(())
}

#[test]
fn compatible_offered_spec_accepts_broader_admission_but_rejects_invalid_keys() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};
    let catalog = CatalogManager::default();
    let selection = Selection::from(Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Value::EntityId(state(1, 1).payload.entity_id))),
    });
    let offer = proto::KnownState { spec: spec(), spans: vec![] };
    assert_eq!(QueryTreeCache::offered_spec(&selection, &catalog, &offer)?, spec());
    let mut differently_ordered = selection.clone();
    differently_ordered.order_by = Some(vec![OrderByItem {
        path: PropertyPath::from(proto::PropertyId::EntityId(EntityId::from_bytes([42; EntityId::BYTE_LEN]))),
        direction: OrderDirection::Desc,
    }]);
    assert!(QueryTreeCache::serves(&spec(), &differently_ordered, &catalog), "an unlimited order does not change the selected span");
    let invalid =
        TreeSpec::new(KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::I64)]), Predicate::True);
    assert!(!QueryTreeCache::serves(&invalid, &Selection::from(Predicate::True), &catalog));
    let mut invalid = ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId);
    invalid.collation = Some("unsupported".into());
    assert!(!QueryTreeCache::serves(&TreeSpec::new(KeySpec::new(vec![invalid]), Predicate::True), &selection, &catalog));
    Ok(())
}
