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
    let [proto::KnownRegion::Summary(original)] = inventory.regions.as_slice() else { panic!("large inventories need a summary") };
    let original = original.clone();
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
