//! Which requests share a resident tree, and which need their own.
use super::*;

#[tokio::test]
async fn compatible_selections_share_a_complete_tree_with_independent_scopes() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "08", "10", "12", "20"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let broad = acquire(&node.resident_trees, &node, above("07", None)).await?;
    let full = broad.identity();
    let scans = node.storage.fetches.load(Ordering::Relaxed);
    let cases = [
        (above("07", None), vec![1, 2, 3, 4]),
        (Selection { order_by: None, ..above("07", None) }, vec![1, 2, 3, 4]),
        (above("10", None), vec![3, 4]),
        (above("07", Some(2)), vec![1, 2]),
        (
            Selection {
                order_by: Some(vec![OrderByItem { path: PropertyPath::from(property()), direction: OrderDirection::Desc }]),
                ..above("10", Some(1))
            },
            vec![4],
        ),
    ];
    for (selected, expected) in cases {
        let lease = acquire(&node.resident_trees, &node, selected).await?;
        assert!(Weak::ptr_eq(&full, &lease.identity()), "compatible selections use the same physical tree");
        let snapshot = lease.snapshot().await?.snapshot;
        let expected: BTreeSet<_> = expected.into_iter().map(|index| events[index].entity_id).collect();
        assert_eq!(snapshot.entries.keys().copied().collect::<BTreeSet<_>>(), expected);
    }
    assert_eq!(node.resident_trees.len(), 1);
    assert_eq!(node.storage.fetches.load(Ordering::Relaxed), scans, "scopes do not scan storage again");
    assert_eq!(broad.snapshot().await?.snapshot.entries.len(), 4, "query limits never truncate the tree");
    let before = acquire(&node.resident_trees, &node, above("10", None)).await?.snapshot().await?.snapshot;
    let changed = update(&events[1], "15")?;
    apply(&node, &changed).await?;
    let after = acquire(&node.resident_trees, &node, above("10", None)).await?.snapshot().await?.snapshot;
    assert!(!before.entries.contains_key(&events[1].entity_id));
    assert!(after.entries.contains_key(&events[1].entity_id));
    assert_eq!(broad.snapshot().await?.snapshot.entries.len(), 4);
    Ok(())
}

#[tokio::test]
async fn limited_coverage_is_explicit_and_complete_coverage_is_preferred() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let first = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    let tree = first.identity();
    let frozen = first.snapshot().await?.snapshot;
    assert_eq!(frozen.entries.len(), 1);
    let exact = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(Weak::ptr_eq(&tree, &exact.identity()));
    let all = node.resident_trees.acquire(&node, selection(None), Some(frozen.spec.clone()), vec![DEFAULT_CONTEXT]).await?;
    assert!(!Weak::ptr_eq(&tree, &all.identity()), "bounded coverage cannot satisfy the unlimited request");
    let preferred = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(Weak::ptr_eq(&all.identity(), &preferred.identity()), "prefer a compatible complete tree");
    assert_eq!(all.snapshot().await?.snapshot.entries.len(), 3);
    apply(&node, &update(&events[0], "09")?).await?;
    let after = first.snapshot().await?.snapshot;
    assert_eq!(after.entries.keys().copied().collect::<Vec<_>>(), vec![events[1].entity_id]);
    assert_eq!(frozen.entries.keys().copied().collect::<Vec<_>>(), vec![events[0].entity_id]);
    assert_eq!(after.entries.len(), 1, "a refill keeps the bounded tree at its cut");
    assert_eq!(node.resident_trees.len(), 2);
    Ok(())
}

#[tokio::test]
async fn missing_coverage_or_unaddressable_order_requires_another_tree() -> anyhow::Result<()> {
    let node = node().await?;
    for value in ["01", "08", "12"] {
        apply(&node, &genesis(value)?).await?;
    }
    let initial = acquire(&node.resident_trees, &node, above("07", None)).await?;
    let wider = acquire(&node.resident_trees, &node, above("03", None)).await?;
    assert!(!Weak::ptr_eq(&initial.identity(), &wider.identity()), "narrow admission cannot cover a wider predicate");
    let all = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(!Weak::ptr_eq(&initial.identity(), &all.identity()));
    assert!(!Weak::ptr_eq(&wider.identity(), &all.identity()));
    let ordered_limit = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(!Weak::ptr_eq(&all.identity(), &ordered_limit.identity()), "an entity-ID tree cannot provide a label-ordered limit");
    assert_eq!(node.resident_trees.len(), 4);
    Ok(())
}

#[tokio::test]
async fn limited_coverage_rejects_other_selections_and_shares_exact_responder_requests() -> anyhow::Result<()> {
    let node = node().await?;
    for value in ["01", "02", "03"] {
        apply(&node, &genesis(value)?).await?;
    }
    let selected = selection(Some(1));
    let limited = acquire(&node.resident_trees, &node, selected.clone()).await?;
    let tree = limited.identity();
    let spec = limited.snapshot().await?.snapshot.spec;
    let serving = node.resident_trees.acquire(&node, selected, Some(spec), vec![DEFAULT_CONTEXT]).await?;
    assert!(Weak::ptr_eq(&tree, &serving.identity()), "requesting and serving use the same bounded tree");
    let mut opposite = selection(Some(1));
    opposite.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
    let mut narrower = above("01", None);
    narrower.limit = Some(1);
    for other in [selection(None), selection(Some(2)), selection(Some(0)), opposite, narrower] {
        let other = acquire(&node.resident_trees, &node, other).await?;
        assert!(!Weak::ptr_eq(&tree, &other.identity()), "bounded coverage never proves another selection complete");
    }
    assert_eq!(limited.snapshot().await?.snapshot.entries.len(), 1);
    Ok(())
}

#[tokio::test]
async fn replacing_a_limit_reuses_the_full_resident_tree() -> anyhow::Result<()> {
    let node = node().await?;
    let mut initial = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        initial.push(event);
    }
    // Complete coverage can serve several limits without a bounded refill.
    let limited = |limit| (selection(Some(limit)), Some(spec_for(&selection(Some(limit)), &node.catalog)));
    let full = node.resident_trees.acquire(&node, selection(None), limited(2).1, vec![DEFAULT_CONTEXT]).await?;
    let (two, spec) = limited(2);
    let original = node.resident_trees.acquire(&node, two, spec, vec![DEFAULT_CONTEXT]).await?;
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    apply(&node, &update(&initial[0], "09")?).await?;
    let (one, spec) = limited(1);
    let reused = node.resident_trees.acquire(&node, one, spec, vec![DEFAULT_CONTEXT]).await?;
    assert!(Weak::ptr_eq(&original.identity(), &reused.identity()), "compatible limits select scopes of the same resident tree");
    assert!(Weak::ptr_eq(&full.identity(), &reused.identity()), "both limits reuse the complete tree");
    let snapshot = full.snapshot().await?.snapshot;
    assert_eq!(snapshot.entries.len(), 3, "the tree keeps the full admission");
    let snapshot = snapshot.select(&selection(Some(1)), &node.catalog)?;
    assert_eq!(snapshot.entries.keys().copied().collect::<Vec<_>>(), vec![initial[1].entity_id]);
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed), scans);
    Ok(())
}
