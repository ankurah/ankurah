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

#[tokio::test]
async fn dense_refresh_rebuilds_lazily_and_preserves_captured_snapshots() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let initial = (0..256).map(|id| numbered_state(id, 1)).collect::<Vec<_>>();
    let mut old = query.refresh(spec(), initial).await?;
    assert!(old.offer().await?.regions.len() > 1, "a fresh tree offers its bounded frontier");
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
        let [proto::KnownRegion::Summary(root)] = offer.regions.as_slice() else {
            panic!("a retained query must offer its root even after a bulk rebuild")
        };
        assert_eq!(*root, recomputed.head(super::super::whole()).await?);
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
async fn retained_unbuilt_cache_offers_a_root_after_initial_membership_fill() -> anyhow::Result<()> {
    let query = QueryTree::default();
    let mut initial = query.refresh(spec(), vec![]).await?;
    assert!(matches!(initial.offer().await?.regions.as_slice(), [proto::KnownRegion::Leaves(_)]));
    let mut refreshed = query.refresh(spec(), (0..256).map(|id| numbered_state(id, 1)).collect()).await?;
    assert!(refreshed.tree.is_none());
    let offer = refreshed.offer().await?;
    let [proto::KnownRegion::Summary(root)] = offer.regions.as_slice() else {
        panic!("same-query lineage must use a root even when no previous digest was built")
    };
    assert_eq!(root.digest.count(), 256);
    Ok(())
}

#[tokio::test]
async fn initial_complete_retains_only_large_standing_query_digests() -> anyhow::Result<()> {
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
    for (standing, count) in [(false, 64), (true, super::super::LEAF_PAGE), (true, 64)] {
        let query = QueryTree::default();
        let snapshot = query.refresh(spec(), (0..count).map(|id| numbered_state(id as u32, 1)).collect()).await?;
        assert!(query.0.lock().await.as_ref().unwrap().tree.is_none());
        let reply = node
            .start_comparison(
                peer,
                proto::RequestId::new(),
                vec![],
                vec![],
                standing.then(|| (proto::QueryId::new(), 1)),
                false,
                snapshot,
                None,
            )
            .await?;
        assert!(matches!(reply.frame.body, proto::StateFrameBody::State { progress: proto::ComparisonProgress::Complete, .. }));
        assert_eq!(
            query.0.lock().await.as_ref().unwrap().tree.is_some(),
            standing && count > super::super::LEAF_PAGE,
            "only a large standing query must build its retained digest before Complete"
        );
    }
    node.deregister_peer(peer);
    Ok(())
}

#[tokio::test]
async fn small_and_empty_inventories_offer_exact_leaves_without_building_a_tree() -> anyhow::Result<()> {
    for count in [0, super::super::LEAF_PAGE] {
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
    let mut snapshot = query.refresh(spec(), vec![state]).await?;
    let candidate = proto::KnownState {
        spec: snapshot.spec.clone(),
        regions: vec![proto::KnownRegion::Leaves(snapshot.page(super::super::whole()).await?)],
    };
    assert!(bincode::serialized_size(&candidate)? > super::super::FRAME_BYTES as u64);
    assert!(snapshot.tree.is_none(), "preparing the candidate inventory must not build a tree");
    let inventory = snapshot.offer().await?;
    let [proto::KnownRegion::Summary(head)] = inventory.regions.as_slice() else { panic!("oversized inventory must use a summary") };
    assert_eq!(head.digest.count(), 1);
    assert_eq!(head.bounds, super::super::whole());
    assert!(bincode::serialized_size(&inventory)? <= super::super::FRAME_BYTES as u64);
    assert!(snapshot.tree.is_some());
    Ok(())
}

#[tokio::test]
async fn canonical_pages_cover_requested_bounds_without_building_a_tree() -> anyhow::Result<()> {
    let spec = TreeSpec::new(
        KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(proto::PropertyId::Id, ValueType::EntityId)]),
        ankql::ast::Predicate::True,
    );
    let query = QueryTree::default();
    let mut snapshot = query.refresh(spec, (0..70).rev().map(|id| state(id, 1)).collect()).await?;
    let members = snapshot.members();
    let addresses = members.keys().cloned().collect::<Vec<_>>();
    let requested = proto::AddressRange::new(Bound::Excluded(addresses[2].clone()), Bound::Included(addresses[67].clone()));
    let mut remaining = requested.clone();
    let mut received = Vec::new();
    loop {
        let page = snapshot.page(remaining.clone()).await?;
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
    let page = old.page(super::super::whole()).await?;
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
    assert!(inventory.regions.iter().all(|region| matches!(region, proto::KnownRegion::Summary(_))));
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
    let [proto::KnownRegion::Summary(retained_root)] = retained.regions.as_slice() else {
        panic!("retained equality must use its cheap root offer")
    };
    assert_eq!(*retained_root, root);
    Ok(())
}
