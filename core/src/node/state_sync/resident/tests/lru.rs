//! The LRU: retention, eviction, pins and idle maintenance.
use super::*;

#[tokio::test]
async fn idle_resident_updates_then_exact_reuse_avoids_another_scan() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let tree = resident.identity();
    let frozen = resident.snapshot().await?.snapshot;
    let scans = node.storage.fetches.load(Ordering::Relaxed);
    drop(resident);
    let next = update(&initial, "02")?;
    apply(&node, &next).await?;
    let reused = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(Weak::ptr_eq(&tree, &reused.identity()));
    let current = reused.snapshot().await?;
    assert_eq!(current.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&next));
    assert_eq!(frozen.entries[&initial.entity_id].head, proto::Clock::singleton(&initial));
    assert_eq!(node.storage.fetches.load(Ordering::Relaxed), scans, "ordinary updates and exact reuse never rescan storage");
    Ok(())
}

#[tokio::test]
async fn count_limit_evicts_idle_trees_but_pins_preserve_active_owners() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let cache = ResidentTrees::with_limits(1, 1024 * 1024);
    let first = acquire(&cache, &node, selection(None)).await?;
    let tree = first.identity();
    let snapshot = first.snapshot().await?.snapshot;
    let second = acquire(&cache, &node, selection(Some(1))).await?;
    assert_eq!(cache.len(), 2, "active operations do not acquire a new artificial query limit");
    assert!(Weak::ptr_eq(&tree, &acquire(&cache, &node, selection(None)).await?.identity()));
    drop(first);
    drop(second);
    assert_eq!(cache.len(), 2, "the admitted active comparison also pins its tree");
    assert_eq!(snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&initial));
    drop(snapshot);
    assert_eq!(cache.len(), 1);
    let second = acquire(&cache, &node, selection(Some(1))).await?;
    assert!(!Weak::ptr_eq(&tree, &second.identity()));
    Ok(())
}

#[tokio::test]
async fn oversized_miss_does_not_evict_a_useful_small_entry() -> anyhow::Result<()> {
    let node = node().await?;
    for value in ["01", "02", "03", "04"] {
        apply(&node, &genesis(value)?).await?;
    }
    let small = above("03", None);
    let small_resident = complete_resident(&node, spec_for(&small, &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(small_resident.tree.entries().len(), 1);
    let small_bytes = small_resident.accounted_bytes();
    let budget = small_bytes + 1;
    assert!(small_bytes <= budget);
    let all = selection(None);
    let full_resident = complete_resident(&node, spec_for(&all, &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(full_resident.tree.entries().len(), 4);
    assert!(full_resident.accounted_bytes() > budget);
    let cache = ResidentTrees::with_limits(2, budget);
    let resident = acquire(&cache, &node, above("03", None)).await?;
    let tree = resident.identity();
    drop(resident);
    let oversized = acquire(&cache, &node, selection(None)).await?;
    assert_eq!(oversized.snapshot().await?.snapshot.entries.len(), 4);
    drop(oversized);
    assert_eq!(cache.len(), 1);
    assert!(Weak::ptr_eq(&tree, &acquire(&cache, &node, above("03", None)).await?.identity()));
    Ok(())
}

#[tokio::test]
async fn canceled_build_never_becomes_reusable_and_halt_stops_idle_entries() -> anyhow::Result<()> {
    let node = node().await?;
    let (entered, release) = hold_fetch(&node);
    {
        let admission = acquire(&node.resident_trees, &node, selection(None));
        tokio::pin!(admission);
        assert!(futures::poll!(&mut admission).is_pending());
        entered.await?;
    }
    assert!(release.send(()).is_err());
    assert_eq!(node.resident_trees.len(), 0);
    drop(acquire(&node.resident_trees, &node, selection(None)).await?);
    assert_eq!(node.resident_trees.len(), 1);
    node.system.halt(crate::error::NodeHaltReason::SystemLoad("fixture halt".into()));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while node.resident_trees.len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(acquire(&node.resident_trees, &node, selection(None)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn an_idle_entry_that_grows_beyond_retention_is_evicted() -> anyhow::Result<()> {
    let node = node().await?;
    apply(&node, &genesis("01")?).await?;
    // Retain exactly what one member costs, so that more members cannot fit.
    let budget = complete_resident(&node, spec_for(&selection(None), &node.catalog), vec![DEFAULT_CONTEXT]).await?.accounted_bytes();
    let cache = ResidentTrees::with_limits(8, budget);
    let resident = acquire(&cache, &node, selection(None)).await?;
    let frozen = resident.snapshot().await?.snapshot;
    assert_eq!(frozen.entries.len(), 1);
    drop(frozen);
    drop(resident);
    for value in ["02", "03", "04"] {
        apply(&node, &genesis(value)?).await?;
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while cache.len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let current = acquire(&cache, &node, selection(None)).await?;
    assert_eq!(current.snapshot().await?.snapshot.entries.len(), 4);
    drop(current);
    assert_eq!(cache.len(), 0);
    Ok(())
}

#[tokio::test]
async fn active_snapshot_lease_pins_tree_until_last_raii_owner_drops() -> anyhow::Result<()> {
    let node = node().await?;
    apply(&node, &genesis("01")?).await?;
    let cache = ResidentTrees::with_limits(0, 0);
    let lease = acquire(&cache, &node, selection(None)).await?;
    let another = lease.clone();
    let snapshot = lease.snapshot().await?.snapshot;
    drop(lease);
    cache.evict_idle();
    assert_eq!(cache.len(), 1);
    drop(another);
    cache.evict_idle();
    assert_eq!(cache.len(), 1, "an active comparison owns a lease independent of its caller");
    drop(snapshot);
    assert_eq!(cache.len(), 0, "dropping the last lease automatically releases idle retention");
    Ok(())
}

#[tokio::test]
async fn a_new_limited_resident_retains_only_its_cut_and_a_zero_limit_reads_nothing() -> anyhow::Result<()> {
    let node = node().await?;
    for value in 0..128 {
        apply(&node, &genesis(&format!("{value:03}"))?).await?;
    }
    let rows_read = node.storage.returned_rows.load(Ordering::Relaxed);
    let limited = acquire(&node.resident_trees, &node, selection(Some(3))).await?;
    assert_eq!(limited.snapshot().await?.snapshot.entries.len(), 3);
    assert_eq!(
        node.storage.returned_rows.load(Ordering::Relaxed) - rows_read,
        3,
        "a cold limited resident reads, and so holds, only the selected rows"
    );

    let scans = node.storage.fetches.load(Ordering::Relaxed);
    let rows_read = node.storage.returned_rows.load(Ordering::Relaxed);
    let empty = acquire(&node.resident_trees, &node, selection(Some(0))).await?;
    assert!(empty.snapshot().await?.snapshot.entries.is_empty());
    assert_eq!(node.storage.fetches.load(Ordering::Relaxed), scans, "LIMIT 0 must not start an application scan");
    assert_eq!(node.storage.returned_rows.load(Ordering::Relaxed), rows_read);
    Ok(())
}

#[tokio::test]
async fn catalog_changes_invalidate_ordinary_entries_but_maintain_catalog_entries() -> anyhow::Result<()> {
    let node = node().await?;
    let ordinary = acquire(&node.resident_trees, &node, selection(None)).await?;
    let catalog_selection =
        Selection { predicate: Predicate::MemberOf(proto::ModelId::System(proto::SystemModel::Model)), order_by: None, limit: None };
    let catalog = acquire(&node.resident_trees, &node, catalog_selection).await?;
    let event = proto::Event::genesis(
        None,
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![
            proto::Operation::Membership(proto::Membership::Add(proto::ModelId::System(proto::SystemModel::Model))),
            operation("catalog fixture")?,
        ]),
    );
    apply(&node, &event).await?;
    assert!(ordinary.snapshot().await.is_err(), "old catalog interpretation cannot serve another admission");
    assert!(catalog.snapshot().await?.snapshot.entries.contains_key(&event.entity_id));
    assert_eq!(node.resident_trees.len(), 1);
    Ok(())
}

/// Apply `source`'s stored state of `event`'s entity to `node` as a state
/// snapshot, staging `event`, the snapshot's head, for the causal check a
/// snapshot of an existing entity needs.
async fn apply_snapshot(node: &TestNode, source: &TestNode, event: &proto::Event) -> anyhow::Result<()> {
    let stored = source.storage.get_state(event.entity_id).await?;
    let events = LocalEventGetter::new(node.storage.clone(), true);
    events.stage_event(event.clone());
    NodeApplier::apply_states(
        node,
        &proto::EntityId::from_bytes([24; 32]),
        vec![stored],
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn state_snapshots_update_a_resident_tree_for_new_and_existing_entities() -> anyhow::Result<()> {
    let (node, source) = (node().await?, node().await?);
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let created = genesis("01")?;
    apply(&source, &created).await?;
    apply_snapshot(&node, &source, &created).await?;
    assert_eq!(resident.snapshot().await?.snapshot.entries[&created.entity_id].head, proto::Clock::singleton(&created));
    let changed = update(&created, "02")?;
    apply(&source, &changed).await?;
    apply_snapshot(&node, &source, &changed).await?;
    assert_eq!(resident.snapshot().await?.snapshot.entries[&created.entity_id].head, proto::Clock::singleton(&changed));
    Ok(())
}

#[tokio::test]
async fn an_idle_tree_does_not_require_event_delivery_permission() -> anyhow::Result<()> {
    let initial = genesis("01")?;
    let denied = update(&initial, "02")?;
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { denied_event: Some(denied.id()), ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    apply(&node, &initial).await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let tree = resident.identity();
    drop(resident);
    apply(&node, &denied).await?;
    let reused = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(Weak::ptr_eq(&tree, &reused.identity()));
    assert_eq!(reused.snapshot().await?.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&denied));
    Ok(())
}
