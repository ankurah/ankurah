use super::*;
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
    ankurah_digest_tree::Digest::checked_sum(offer.regions.iter().map(|region| {
        let proto::KnownRegion::Summary(head) = region else { panic!("expected only summaries") };
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
    let query = QueryTree::default();
    let mut old = query.refresh_captured(keyed_spec(), initial.clone()).await?;
    let original = old.head(super::super::whole()).await?;
    let original_members = old.members();
    let oracle = QueryTree::default();
    let mut decoded = oracle.refresh(keyed_spec(), initial.iter().map(|(state, _)| state.clone()).collect()).await?;
    assert_eq!(original, decoded.head(super::super::whole()).await?);
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
        let oracle = QueryTree::default();
        let mut decoded = oracle.refresh(keyed_spec(), rows.into_iter().map(|(state, _)| state).collect()).await?;
        assert_eq!(captured.members(), decoded.members());
        assert_eq!(captured.head(super::super::whole()).await?, decoded.head(super::super::whole()).await?);
        assert_eq!(old.members(), original_members);
        assert_eq!(old.head(super::super::whole()).await?, original);
    }
    Ok(())
}

#[tokio::test]
async fn captured_key_encoding_failure_preserves_the_existing_cache() -> anyhow::Result<()> {
    let rows = (0..80).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let query = QueryTree::default();
    let mut snapshot = query.refresh_captured(keyed_spec(), rows).await?;
    let original = snapshot.head(super::super::whole()).await?;
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
    assert_eq!(snapshot.head(super::super::whole()).await?, original);
    Ok(())
}

#[tokio::test]
async fn dense_refresh_rebuilds_lazily_and_preserves_captured_snapshots() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let initial = (0..256).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial).await?;
    assert!(old.offer().await?.regions.len() > 1, "the complete inventory spans bounded pages");
    let original = old.head(super::super::whole()).await?;

    // Addition, replacement, and removal each cross the measured dense threshold.
    for states in [
        (0..512).map(|id| numbered_state(id, 1)).collect::<Vec<_>>(),
        (0..512).map(|id| numbered_state(id, 2)).collect(),
        (0..256).map(|id| numbered_state(id, 2)).collect(),
    ] {
        let mut refreshed = query.refresh(spec(), states.clone()).await?;
        assert!(refreshed.tree.is_none(), "dense refresh must defer digest construction");
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        let recomputed_query = QueryTree::default();
        let mut recomputed = recomputed_query.refresh(spec(), states).await?;
        assert_eq!(refreshed.members(), recomputed.members());
        let offer = refreshed.offer().await?;
        assert!(offer.regions.len() > 1, "a retained bulk rebuild must still offer bounded regions");
        assert!(offer.regions.iter().all(|region| matches!(region, proto::KnownRegion::Leaves(_))));
        assert!(refreshed.tree.is_none(), "offering a complete inventory stays lazy");
        assert_eq!(refreshed.head(super::super::whole()).await?, recomputed.head(super::super::whole()).await?);
        assert_eq!(old.head(super::super::whole()).await?, original);
    }

    let sparse = (0..256).map(|id| numbered_state(id, if id == 17 { 3 } else { 2 })).collect::<Vec<_>>();
    let mut refreshed = query.refresh(spec(), sparse.clone()).await?;
    assert!(refreshed.tree.is_some(), "a sparse refresh must retain incremental construction");
    let recomputed_query = QueryTree::default();
    let mut recomputed = recomputed_query.refresh(spec(), sparse).await?;
    assert_eq!(refreshed.head(super::super::whole()).await?, recomputed.head(super::super::whole()).await?);
    assert_eq!(old.head(super::super::whole()).await?, original);
    Ok(())
}

#[tokio::test]
async fn retained_unbuilt_cache_offers_a_complete_inventory_after_initial_membership_fill() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let mut initial = query.refresh(spec(), vec![]).await?;
    assert!(matches!(initial.offer().await?.regions.as_slice(), [proto::KnownRegion::Leaves(_)]));
    let mut refreshed = query.refresh(spec(), (0..256).map(|id| numbered_state(id, 1)).collect()).await?;
    assert!(refreshed.tree.is_none());
    let offer = refreshed.offer().await?;
    assert!(offer.regions.len() > 1);
    assert_eq!(
        offer
            .regions
            .iter()
            .map(|region| match region {
                proto::KnownRegion::Leaves(page) => page.leaves.len(),
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
        let query = QueryTree::default();
        let snapshot = query.refresh(spec(), (0..count).map(|id| numbered_state(id as u32, 1)).collect()).await?;
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        let known = if count > 64 {
            let mut requester = QueryTree::default().refresh(spec(), (0..count).map(|id| numbered_state(id as u32, 1)).collect()).await?;
            Some(requester.offer().await?)
        } else {
            None
        };
        let reply = crate::node::state_sync::start_comparison(
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
    let query = QueryTree::default();
    let rows = (0..64).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), rows.clone()).await?;
    let known = old.offer().await?;
    let original = bincode::serialize(&known)?;
    let subscription = proto::QueryId::new();
    let reply = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 1)),
        false,
        0,
        query.refresh(spec(), rows.clone()).await?,
        None,
    )
    .await?;
    assert!(matches!(reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());

    let mut changed = rows;
    changed[17] = numbered_state(17, 2);
    changed.push(numbered_state(64, 2));
    let captured = query.refresh(spec(), changed.clone()).await?;
    let expected = QueryTree::default().refresh(spec(), changed.clone()).await?;
    assert_eq!(captured.members(), expected.members());
    let reply = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 2)),
        false,
        0,
        captured,
        Some(known),
    )
    .await?;
    let proto::StateFrameBody::State { items, progress, .. } = reply.frame.body else { panic!("state") };
    assert_eq!(progress, proto::ComparisonProgress::Complete);
    assert!(items.iter().any(|item| matches!(item, proto::StateItem::Repair { target, delta }
        if delta.entity_id == changed[17].payload.entity_id && *target == changed[17].payload.state.head)));
    assert!(items.iter().any(|item| matches!(item, proto::StateItem::Snapshots(page)
        if page.states.iter().any(|state| state.payload.entity_id == changed[64].payload.entity_id))));
    assert!(!items.iter().any(|item| matches!(item, proto::StateItem::Summary(_) | proto::StateItem::NeedLeaves(_))));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none(), "the first refresh stays on the inventory diff path");
    assert_eq!(bincode::serialize(&old.offer().await?)?, original);
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn crossing_the_inventory_count_limit_builds_and_shrinking_preserves_the_tree() -> anyhow::Result<()> {
    let (node, peer) = comparison_node().await?;
    let query = QueryTree::default();
    let rows = (0..super::super::INITIAL_INVENTORY_LEAVES as u32).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), rows.clone()).await?;
    let known = old.offer().await?;
    let saved = bincode::serialize(&known)?;
    let subscription = proto::QueryId::new();
    let initial = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 1)),
        false,
        0,
        query.refresh(spec(), rows.clone()).await?,
        Some(known.clone()),
    )
    .await?;
    assert!(matches!(initial.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());

    let mut larger = rows.clone();
    larger.push(numbered_state(super::super::INITIAL_INVENTORY_LEAVES as u32, 1));
    let crossing = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 2)),
        false,
        0,
        query.refresh(spec(), larger.clone()).await?,
        Some(known.clone()),
    )
    .await?;
    assert!(matches!(crossing.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_some(), "build the 2,049-row digest before Complete");
    let mut current = query.refresh(spec(), larger.clone()).await?;
    let mut oracle = QueryTree::default().refresh(spec(), larger).await?;
    assert_eq!(current.head(super::super::whole()).await?, oracle.head(super::super::whole()).await?);

    let shrunk = query.refresh(spec(), rows.clone()).await?;
    assert!(shrunk.tree.is_some(), "sparse removal must retain the existing tree below the threshold");
    let final_reply = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((subscription, 3)),
        false,
        0,
        shrunk,
        Some(known),
    )
    .await?;
    assert!(matches!(final_reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
    let mut current = query.refresh(spec(), rows.clone()).await?;
    let mut oracle = QueryTree::default().refresh(spec(), rows).await?;
    assert!(current.tree.is_some());
    assert_eq!(current.head(super::super::whole()).await?, oracle.head(super::super::whole()).await?);
    assert_eq!(bincode::serialize(&old.offer().await?)?, saved, "the pre-crossing snapshot remains immutable");
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn small_and_empty_inventories_offer_exact_leaves_without_building_a_tree() -> anyhow::Result<()> {
    for count in [0, 32] {
        let query = QueryTree::default();
        let states = (0..count).rev().map(|id| state(id as u8, 1)).collect();
        let mut snapshot = query.refresh(spec(), states).await?;
        let inventory = snapshot.offer().await?;
        let [proto::KnownRegion::Leaves(page)] = inventory.regions.as_slice() else { panic!("expected one exact inventory") };
        assert_eq!(page.bounds, super::super::whole());
        assert_eq!(page.leaves.len(), count);
        assert_eq!(snapshot.members().len(), count);
        for (id, leaf) in page.leaves.iter().enumerate() {
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
    let mut state = state(1, 1);
    state.payload.state.head = proto::Clock::new(
        (0..3000u32)
            .map(|id| {
                let mut bytes = [0; 32];
                bytes[..4].copy_from_slice(&id.to_be_bytes());
                (1, proto::EventId::from_bytes(bytes))
            })
            .collect::<Vec<_>>(),
    )?;
    let query = QueryTree::default();
    let mut snapshot = query.refresh(spec(), vec![state.clone()]).await?;
    assert!(snapshot.page(super::super::whole()).is_err(), "an oversized single leaf must fail explicitly");
    assert!(snapshot.tree.is_none(), "preparing the candidate inventory must not build a tree");
    let inventory = snapshot.offer().await?;
    let [proto::KnownRegion::Summary(head)] = inventory.regions.as_slice() else { panic!("oversized inventory must use a summary") };
    assert_eq!(head.digest.count(), 1);
    assert_eq!(head.bounds, super::super::whole());
    assert!(bincode::serialized_size(&inventory)? <= super::super::FRAME_BYTES as u64);
    assert!(snapshot.tree.is_some());

    // A small result that cannot fit the inventory byte policy still builds
    // on demand when a later requester offers its Summary. The count guard
    // avoids eager construction, not the work needed by this comparison.
    let (node, peer) = comparison_node().await?;
    let responder = QueryTree::default();
    let captured = responder.refresh(spec(), vec![state]).await?;
    assert!(responder.0.lock().await.as_ref().unwrap().tree.is_none());
    let reply = crate::node::state_sync::start_comparison(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![],
        Some((proto::QueryId::new(), 1)),
        false,
        0,
        captured,
        Some(inventory),
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
    let query = QueryTree::default();
    let mut old = query.refresh_captured(wide_spec.clone(), rows).await?;
    let mut remaining = super::super::whole();
    let mut pages = Vec::new();
    let mut ids = Vec::new();
    loop {
        let page = old.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(bincode::serialized_size(&proto::StateAnswer::Leaves(page.clone()))? <= super::super::PAGE_BYTES as u64);
        ids.extend(page.leaves.iter().map(|leaf| leaf.entity.entity_id));
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
        offer.regions.iter().all(|region| matches!(region, proto::KnownRegion::Leaves(_))),
        "multiple bounded pages may form one complete initial inventory"
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
    let query = QueryTree::default();
    let snapshot = query.refresh(spec, (0..70).rev().map(|id| state(id, 1)).collect()).await?;
    let members = snapshot.members();
    let addresses = members.keys().cloned().collect::<Vec<_>>();
    let requested = proto::AddressRange::new(Bound::Excluded(addresses[2].clone()), Bound::Included(addresses[67].clone()));
    let mut remaining = requested.clone();
    let mut received = Vec::new();
    loop {
        let page = snapshot.page(remaining.clone())?;
        assert_eq!(page.bounds.start, remaining.start);
        assert!(page.leaves.len() <= super::super::LEAF_PAGE);
        for leaf in page.leaves {
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
    let query = QueryTree::default();
    let before = vec![state(1, 1), state(2, 1)];
    let mut old = query.refresh(spec(), before.clone()).await?;
    let mut current = query.refresh(spec(), vec![state(1, 2), state(3, 1)]).await?;
    let expected_query = QueryTree::default();
    let mut expected = expected_query.refresh(spec(), before).await?;
    assert_eq!(old.head(super::super::whole()).await?, expected.head(super::super::whole()).await?);
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_none(), "building an old snapshot cannot populate the current cache");
    let page = old.page(super::super::whole())?;
    assert_eq!(
        page.leaves.iter().map(|leaf| leaf.entity.entity_id).collect::<Vec<_>>(),
        old.members().values().copied().collect::<Vec<_>>()
    );
    assert_ne!(old.head(super::super::whole()).await?, current.head(super::super::whole()).await?);
    assert!(query.0.lock().await.as_ref().unwrap().tree.is_some());
    Ok(())
}

#[tokio::test]
async fn retained_tree_changes_match_recomputation_and_preserve_old_snapshots() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let initial = (0..70).map(|id| state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial.clone()).await?;
    let inventory = old.offer().await?;
    assert!(inventory.regions.iter().all(|region| matches!(region, proto::KnownRegion::Leaves(_))));
    assert!(old.tree.is_none());
    let original = old.head(super::super::whole()).await?;
    let generation = query.0.lock().await.as_ref().unwrap().generation.clone();
    let mut unchanged = query.refresh(spec(), initial).await?;
    assert!(Arc::ptr_eq(&generation, &query.0.lock().await.as_ref().unwrap().generation));
    assert_eq!(unchanged.head(super::super::whole()).await?, original);
    for states in [
        (10..80).map(|id| state(id, if id == 30 { 2 } else { 1 })).collect::<Vec<_>>(),
        (20..40).map(|id| state(id, 3)).collect(),
        Vec::new(),
    ] {
        let mut incremental = query.refresh(spec(), states.clone()).await?;
        assert!(incremental.tree.is_some(), "refresh must retain the already-built tree");
        let recomputed_query = QueryTree::default();
        let mut recomputed = recomputed_query.refresh(spec(), states).await?;
        assert_eq!(incremental.members(), recomputed.members());
        assert_eq!(incremental.head(super::super::whole()).await?, recomputed.head(super::super::whole()).await?);
        assert_eq!(old.head(super::super::whole()).await?, original);
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
    let query = QueryTree::default();
    let mut snapshot = query.refresh(spec(), states.clone()).await?;
    let inventory = snapshot.offer().await?;
    assert!(inventory.regions.len() > 1, "a new large tree must offer its bounded cached descendants");
    assert!(inventory.regions.len() <= OFFER_REGIONS);
    assert!(bincode::serialized_size(&inventory)? <= OFFER_BYTES);
    let mut next = super::super::whole().start;
    let mut digests = Vec::new();
    let mut empty = false;
    for region in inventory.regions {
        let proto::KnownRegion::Summary(head) = region else { panic!("large offer must contain only summaries") };
        assert_eq!(head.bounds.start, next, "regions must be disjoint and exactly adjacent");
        assert_eq!(snapshot.head(head.bounds.clone()).await?.digest, head.digest, "each head describes its exact captured range");
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
    let root = snapshot.head(super::super::whole()).await?;
    assert_eq!(ankurah_digest_tree::Digest::checked_sum(digests.iter().copied())?, root.digest);

    let mut warm = query.refresh(spec(), states).await?;
    let retained = warm.offer().await?;
    assert!(retained.regions.len() > 1, "retained offers must also identify bounded changed regions");
    assert!(retained.regions.len() <= OFFER_REGIONS);
    assert!(bincode::serialized_size(&retained)? <= OFFER_BYTES);
    assert!(retained
        .regions
        .iter()
        .all(|region| matches!(region, proto::KnownRegion::Summary(head) if head.digest.count() <= super::super::LEAF_PAGE as i64)));
    assert_eq!(offered_digest(&retained)?, root.digest);
    Ok(())
}

#[tokio::test]
async fn complete_initial_inventories_are_bounded_lazy_and_immutable() -> anyhow::Result<()> {
    for count in [256, super::super::INITIAL_INVENTORY_LEAVES] {
        let query = QueryTree::default();
        let mut old = query.refresh(spec(), (0..count as u32).map(|id| numbered_state(id, 1)).collect()).await?;
        let offer = old.offer().await?;
        assert!(bincode::serialized_size(&offer)? <= super::super::INITIAL_INVENTORY_BYTES as u64);
        let mut start = super::super::whole().start;
        let mut received = Vec::new();
        for region in &offer.regions {
            let proto::KnownRegion::Leaves(page) = region else { panic!("complete bounded leaf inventory") };
            assert_eq!(page.bounds.start, start);
            assert!(page.leaves.len() <= super::super::LEAF_PAGE);
            assert!(bincode::serialized_size(&proto::StateAnswer::Leaves(page.clone()))? <= super::super::PAGE_BYTES as u64);
            received.extend(page.leaves.iter().map(|leaf| leaf.entity.entity_id));
            start = match &page.bounds.end {
                Bound::Included(end) => Bound::Excluded(end.clone()),
                Bound::Unbounded => Bound::Unbounded,
                _ => panic!("exhaustive page advances at its last point"),
            };
        }
        assert_eq!(start, super::super::whole().end);
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
    let query = QueryTree::default();
    let mut too_many = query.refresh(spec(), (0..2049).map(|id| numbered_state(id, 1)).collect()).await?;
    let offer = too_many.offer().await?;
    assert!(offer.regions.iter().all(|region| matches!(region, proto::KnownRegion::Summary(_))));
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
        pages.push(proto::KnownRegion::Leaves(page));
        if end == super::super::whole().end {
            break;
        }
        let Bound::Included(last) = end else { panic!("progress") };
        remaining = super::super::whole().after(&last);
    }
    let oversized = proto::KnownState { spec: spec(), regions: pages };
    assert!(bincode::serialized_size(&oversized)? > super::super::INITIAL_INVENTORY_BYTES as u64);
    let offer = wide.offer().await?;
    assert!(
        offer.regions.iter().all(|region| matches!(region, proto::KnownRegion::Summary(_))),
        "no partial leaf cover may escape the initial budget"
    );
    assert_eq!(offered_digest(&offer)?.count(), 64);
    Ok(())
}

#[tokio::test]
async fn source_row_deltas_preserve_frozen_snapshots_across_sparse_and_dense_changes() -> anyhow::Result<()> {
    let initial = (0..192).map(|id| keyed_state(id, 1, Some(Value::I64(id as i64)))).collect::<anyhow::Result<Vec<_>>>()?;
    let tree = QueryTree::default();
    let mut old = tree.refresh_captured(keyed_spec(), initial.clone()).await?;
    let old_head = old.head(super::super::whole()).await?;
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
        let oracle = QueryTree::default();
        let mut rebuilt = oracle.refresh(keyed_spec(), expected.iter().map(|(state, _)| state.clone()).collect()).await?;
        assert_eq!(current.members(), rebuilt.members());
        assert_eq!(current.head(super::super::whole()).await?, rebuilt.head(super::super::whole()).await?);
        assert_eq!(old.members(), old_members);
        assert_eq!(old.head(super::super::whole()).await?, old_head);
    }
    let before = tree.current_snapshot().await?.members();
    let invalid = keyed_state(10, 4, Some(Value::String("wrong type".into())))?;
    assert!(tree.apply_captured(vec![invalid], vec![expected[0].0.payload.entity_id]).await.is_err());
    assert_eq!(tree.current_snapshot().await?.members(), before, "failed encoding cannot apply even the removals");
    Ok(())
}
