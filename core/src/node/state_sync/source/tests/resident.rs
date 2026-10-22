use super::*;
use crate::node::state_sync::ResidentTrees;
use std::sync::atomic::Ordering;

async fn acquire<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    cache: &ResidentTrees<PA::ContextData>,
    node: &Node<OrderedStorage, PA>,
    selection: Selection<Resolved>,
) -> anyhow::Result<crate::node::state_sync::resident::ResidentLease<PA::ContextData>> {
    cache.acquire(node, selection, None, vec![DEFAULT_CONTEXT]).await
}

#[tokio::test]
async fn idle_resident_updates_then_exact_reuse_avoids_another_scan() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let tree = resident.tree();
    let frozen = resident.snapshot().await?.snapshot;
    let scans = node.storage.2.load(Ordering::Relaxed);
    drop(resident);
    let next = update(&initial, "02")?;
    apply(&node, &next).await?;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let snapshot = tree.current_snapshot().await.unwrap();
            if snapshot.entries[&initial.entity_id].head == proto::Clock::singleton(&next) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let reused = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(Arc::ptr_eq(&tree, &reused.tree()));
    let current = reused.snapshot().await?;
    assert_eq!(current.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&next));
    assert_eq!(frozen.entries[&initial.entity_id].head, proto::Clock::singleton(&initial));
    assert_eq!(node.storage.2.load(Ordering::Relaxed), scans, "ordinary updates and exact reuse never rescan storage");
    Ok(())
}

#[tokio::test]
async fn count_limit_evicts_idle_trees_but_pins_preserve_active_owners() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let cache = ResidentTrees::with_limits(1, 1024 * 1024);
    let first = acquire(&cache, &node, selection(None)).await?;
    let tree = first.tree();
    let snapshot = first.snapshot().await?.snapshot;
    let second = acquire(&cache, &node, selection(Some(1))).await?;
    assert_eq!(cache.len(), 2, "active operations do not acquire a new artificial query limit");
    assert!(Arc::ptr_eq(&tree, &acquire(&cache, &node, selection(None)).await?.tree()));
    drop(first);
    drop(second);
    assert_eq!(cache.len(), 2, "the captured active comparison also pins its tree");
    assert_eq!(snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&initial));
    drop(snapshot);
    assert_eq!(cache.len(), 1);
    let second = acquire(&cache, &node, selection(Some(1))).await?;
    assert!(!Arc::ptr_eq(&tree, &second.tree()));
    Ok(())
}

#[tokio::test]
async fn oversized_miss_does_not_evict_a_useful_small_entry() -> anyhow::Result<()> {
    let node = node().await?;
    for value in ["01", "02", "03", "04"] {
        apply(&node, &genesis(value)?).await?;
    }
    let small = above("03", None);
    let capture =
        capture_query(&node, small.clone(), QueryTree::spec(&small, &node.catalog), vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default()))
            .await?;
    assert_eq!(capture.projection.tree.current_snapshot().await?.entries.len(), 1);
    let small_bytes = QuerySource::from_capture(vec![DEFAULT_CONTEXT], capture).accounted_bytes();
    let budget = small_bytes + 1;
    assert!(small_bytes <= budget);
    let all = selection(None);
    let full_capture =
        capture_query(&node, all.clone(), QueryTree::spec(&all, &node.catalog), vec![DEFAULT_CONTEXT], Arc::new(QueryTree::default()))
            .await?;
    assert_eq!(full_capture.projection.tree.current_snapshot().await?.entries.len(), 4);
    assert!(QuerySource::from_capture(vec![DEFAULT_CONTEXT], full_capture).accounted_bytes() > budget);
    let cache = ResidentTrees::with_limits(2, budget);
    let resident = acquire(&cache, &node, above("03", None)).await?;
    let tree = resident.tree();
    drop(resident);
    let oversized = acquire(&cache, &node, selection(None)).await?;
    assert_eq!(oversized.snapshot().await?.snapshot.entries.len(), 4);
    drop(oversized);
    assert_eq!(cache.len(), 1);
    assert!(Arc::ptr_eq(&tree, &acquire(&cache, &node, above("03", None)).await?.tree()));
    Ok(())
}

#[tokio::test]
async fn shared_responder_routes_reuse_the_same_tree_and_detach_independently() -> anyhow::Result<()> {
    let node = node().await?;
    let event = genesis("01")?;
    apply(&node, &event).await?;
    let selection = selection(None);
    let resident = acquire(&node.resident_trees, &node, selection.clone()).await?;
    let original = resident.tree();
    drop(resident);
    let scans = node.storage.2.load(Ordering::Relaxed);
    let mut subscriptions = Vec::new();
    for number in [61, 62] {
        let peer = proto::EntityId::from_bytes([number; 32]);
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
            Box::new(Sender(peer)),
        )
        .await?;
        let query = proto::QueryId::new();
        let handler = node.peer_connections.get(&peer).unwrap();
        handler
            .subscription_handler
            .subscribe_query(
                &node,
                proto::RequestId::new(),
                Vec::new(),
                query,
                selection.clone(),
                Some(&DEFAULT_CONTEXT),
                1,
                proto::KnownState { spec: QueryTree::spec(&selection, &node.catalog), spans: vec![] },
            )
            .await?;
        let (_, tree) = handler.subscription_handler.query_tree_for_test(query).await.unwrap();
        assert!(Arc::ptr_eq(&original, &tree));
        subscriptions.push((peer, query));
    }
    assert_eq!(node.resident_trees.len(), 1);
    assert_eq!(node.storage.2.load(Ordering::Relaxed), scans);
    let (peer, query) = subscriptions.remove(0);
    node.peer_connections.get(&peer).unwrap().subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    let next = update(&event, "02")?;
    apply(&node, &next).await?;
    let resident = acquire(&node.resident_trees, &node, selection).await?;
    assert_eq!(resident.snapshot().await?.snapshot.entries[&event.entity_id].head, proto::Clock::singleton(&next));
    let (peer, query) = subscriptions.remove(0);
    node.peer_connections.get(&peer).unwrap().subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
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
    let tree = resident.tree();
    drop(resident);
    apply(&node, &denied).await?;
    let reused = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(Arc::ptr_eq(&tree, &reused.tree()));
    assert_eq!(reused.snapshot().await?.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&denied));
    Ok(())
}

#[tokio::test]
async fn permissions_are_rechecked_and_new_grants_are_not_lost() -> anyhow::Result<()> {
    let allowed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let discovery = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let policy = RestrictedDiscovery {
        read_allowed: Some(allowed.clone()),
        discover_all: Some(discovery.clone()),
        deny_empty: true,
        ..Default::default()
    };
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), policy);
    node.system.create().await?;
    node.wait_ready().await?;
    for value in ["01", "09"] {
        apply(&node, &genesis(value)?).await?;
    }
    let selection = selection(None);
    let spec = QueryTree::spec(&selection, &node.catalog);
    let capture = node.resident_trees.capture(&node, selection.clone(), Some(spec.clone()), vec![DEFAULT_CONTEXT]).await?;
    assert!(capture.snapshot.entries.is_empty());
    allowed.store(true, Ordering::SeqCst);
    let capture = node.resident_trees.capture(&node, selection.clone(), Some(spec.clone()), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(capture.snapshot.entries.len(), 1, "a newly granted cached candidate is discovered");
    discovery.store(true, Ordering::SeqCst);
    let capture = node.resident_trees.capture(&node, selection.clone(), Some(spec.clone()), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(capture.snapshot.entries.len(), 2, "a broader discovery scope requires a fresh capture");
    let denied = node.resident_trees.capture(&node, selection.clone(), Some(spec.clone()), vec![]).await?;
    assert!(denied.snapshot.entries.is_empty(), "empty credentials are not privileged and never borrow an authorized view");
    allowed.store(false, Ordering::SeqCst);
    let denied = node.resident_trees.capture(&node, selection, Some(spec), vec![DEFAULT_CONTEXT]).await?;
    assert!(denied.snapshot.entries.is_empty(), "revoked rows cannot be disclosed by a cache hit");
    assert_eq!(capture.snapshot.entries.len(), 2, "the old comparison snapshot is frozen independently");
    Ok(())
}

#[tokio::test]
async fn canceled_build_never_becomes_reusable_and_halt_stops_idle_entries() -> anyhow::Result<()> {
    let node = node().await?;
    let (entered, release) = hold_fetch(&node);
    {
        let capture = acquire(&node.resident_trees, &node, selection(None));
        tokio::pin!(capture);
        assert!(futures::poll!(&mut capture).is_pending());
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
    let cache = ResidentTrees::with_limits(8, 7_000);
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
async fn slow_initial_history_does_not_block_the_shared_tree() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let mut snapshot = resident.snapshot().await?.snapshot;
    let known = snapshot.offer().await?;
    let later = update(&initial, "02")?;
    apply(&node, &later).await?;
    let peer = proto::EntityId::from_bytes([71; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let (entered, waiting) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *node.storage.3.lock().unwrap() = Some((entered, resume));
    let subscription = resident.subscribe(&node, peer, proto::RequestId::new(), vec![], proto::QueryId::new(), 1, false, known);
    tokio::pin!(subscription);
    tokio::select! {
        result = &mut subscription => { result?; anyhow::bail!("fixture did not need history"); }
        result = tokio::time::timeout(std::time::Duration::from_secs(3), waiting) => { result??; }
    }
    let newest = update(&later, "03")?;
    apply(&node, &newest).await?;
    let current = tokio::time::timeout(std::time::Duration::from_secs(3), resident.snapshot()).await??;
    assert_eq!(current.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&newest));
    release.send(()).unwrap();
    let (_, route) = subscription.await?;
    drop(route);
    node.deregister_peer(peer);
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
    assert!(ordinary.snapshot().await.is_err(), "old catalog interpretation cannot serve another capture");
    assert!(catalog.snapshot().await?.snapshot.entries.contains_key(&event.entity_id));
    assert_eq!(node.resident_trees.len(), 1);
    Ok(())
}

#[tokio::test]
async fn initial_state_transfer_also_updates_a_resident_tree() -> anyhow::Result<()> {
    let node = node().await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let event = genesis("01")?;
    let events = LocalEventGetter::new(node.storage.clone(), true);
    events.stage_event(event.clone());
    NodeApplier::apply_delta(
        &node,
        &proto::EntityId::from_bytes([24; 32]),
        proto::EntityDelta {
            entity_id: event.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![Attested::from(event.clone()).into()] },
        },
        Delivery::Initial,
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    let capture = resident.snapshot().await?;
    assert_eq!(capture.snapshot.entries[&event.entity_id].head, proto::Clock::singleton(&event));
    Ok(())
}

#[tokio::test]
async fn invalidation_reaches_another_peer_while_first_initial_reply_waits_for_history() -> anyhow::Result<()> {
    #[derive(Clone)]
    struct RecordingSender {
        peer: proto::EntityId,
        messages: Arc<std::sync::Mutex<Vec<proto::NodeMessage>>>,
    }
    impl PeerSender for RecordingSender {
        fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
        fn recipient_node_id(&self) -> proto::EntityId { self.peer }
        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }

    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let selected = selection(None);
    let resident = acquire(&node.resident_trees, &node, selected.clone()).await?;
    let mut previous = resident.snapshot().await?.snapshot;
    let previous_offer = previous.offer().await?;
    apply(&node, &update(&initial, "02")?).await?;

    let messages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let slow_peer = proto::EntityId::from_bytes([81; 32]);
    let healthy_peer = proto::EntityId::from_bytes([82; 32]);
    for peer in [slow_peer, healthy_peer] {
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
            Box::new(RecordingSender { peer, messages: messages.clone() }),
        )
        .await?;
    }
    let slow_connection = node.peer_connections.get(&slow_peer).unwrap();
    let healthy_connection = node.peer_connections.get(&healthy_peer).unwrap();
    let (entered, waiting) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *node.storage.3.lock().unwrap() = Some((entered, resume));
    let mut slow = Box::pin(slow_connection.subscription_handler.subscribe_query(
        &node,
        proto::RequestId::new(),
        vec![],
        proto::QueryId::new(),
        selected.clone(),
        Some(&DEFAULT_CONTEXT),
        1,
        previous_offer,
    ));
    tokio::select! {
        result = &mut slow => { result?; anyhow::bail!("fixture did not need history"); }
        result = tokio::time::timeout(std::time::Duration::from_secs(3), waiting) => { result??; }
    }

    let mut current = resident.snapshot().await?.snapshot;
    let query = proto::QueryId::new();
    let response = healthy_connection
        .subscription_handler
        .subscribe_query(&node, proto::RequestId::new(), vec![], query, selected, Some(&DEFAULT_CONTEXT), 1, current.offer().await?)
        .await?;
    let proto::NodeResponseBody::QuerySubscribed { state, .. } = response else { panic!("expected healthy subscription") };
    let (_, shared) = healthy_connection.subscription_handler.query_tree_for_test(query).await.unwrap();
    assert!(Arc::ptr_eq(&shared, &resident.tree()));

    let catalog_change = proto::Event::genesis(
        None,
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![
            proto::Operation::Membership(proto::Membership::Add(proto::ModelId::System(proto::SystemModel::Model))),
            operation("catalog invalidation fixture")?,
        ]),
    );
    apply(&node, &catalog_change).await?;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if messages.lock().unwrap().iter().any(|message| matches!(message,
                proto::NodeMessage::Update(proto::NodeUpdate { to, body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. })
                    if *to == healthy_peer && frame.stream == state.frame.stream && matches!(frame.body, proto::StateFrameBody::RestartRequired)
            )) { break; }
            tokio::task::yield_now().await;
        }
    }).await?;
    assert!(healthy_connection.subscription_handler.query_tree_for_test(query).await.is_none());
    assert_eq!(node.resident_trees.len(), 0);
    // The first peer's history read is still blocked. Cancel it only after the
    // healthy peer has received the independent invalidation.
    drop(slow);
    assert!(release.send(()).is_err());
    node.deregister_peer(slow_peer);
    node.deregister_peer(healthy_peer);
    Ok(())
}

fn above(value: &str, limit: Option<u64>) -> Selection<Resolved> {
    use ankql::ast::{ComparisonOperator, Expr};
    let mut selected = selection(limit);
    selected.predicate = Predicate::And(
        Box::new(selected.predicate),
        Box::new(Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(property()))),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::String(value.into()))),
        }),
    );
    selected
}

#[tokio::test]
async fn compatible_selections_share_full_resident_with_independent_scopes() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for value in ["01", "08", "10", "12", "20"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let broad = acquire(&node.resident_trees, &node, above("07", None)).await?;
    let full = broad.tree();
    let scans = node.storage.2.load(Ordering::Relaxed);
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
        assert!(Arc::ptr_eq(&full, &lease.tree()), "compatible selections use the same physical tree");
        let snapshot = lease.snapshot().await?.snapshot;
        let expected: BTreeSet<_> = expected.into_iter().map(|index| events[index].entity_id).collect();
        assert_eq!(snapshot.entries.keys().copied().collect::<BTreeSet<_>>(), expected);
    }
    assert_eq!(node.resident_trees.len(), 1);
    assert_eq!(node.storage.2.load(Ordering::Relaxed), scans, "scopes do not scan storage again");
    assert_eq!(full.current_snapshot().await?.entries.len(), 4, "query limits never truncate the resident");
    let before = acquire(&node.resident_trees, &node, above("10", None)).await?.snapshot().await?.snapshot;
    let changed = update(&events[1], "15")?;
    apply(&node, &changed).await?;
    let after = acquire(&node.resident_trees, &node, above("10", None)).await?.snapshot().await?.snapshot;
    assert!(!before.entries.contains_key(&events[1].entity_id));
    assert!(after.entries.contains_key(&events[1].entity_id));
    assert_eq!(full.current_snapshot().await?.entries.len(), 4);
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
    let tree = first.tree();
    let frozen = first.snapshot().await?.snapshot;
    assert_eq!(frozen.entries.len(), 1);
    assert_eq!(tree.current_snapshot().await?.entries.len(), 1);
    let exact = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(Arc::ptr_eq(&tree, &exact.tree()));
    let all = node.resident_trees.acquire(&node, selection(None), Some(frozen.spec.clone()), vec![DEFAULT_CONTEXT]).await?;
    assert!(!Arc::ptr_eq(&tree, &all.tree()), "bounded coverage cannot satisfy the unlimited request");
    let preferred = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(Arc::ptr_eq(&all.tree(), &preferred.tree()), "prefer a compatible complete resident");
    assert_eq!(all.snapshot().await?.snapshot.entries.len(), 3);
    apply(&node, &update(&events[0], "09")?).await?;
    let after = first.snapshot().await?.snapshot;
    assert_eq!(after.entries.keys().copied().collect::<Vec<_>>(), vec![events[1].entity_id]);
    assert_eq!(frozen.entries.keys().copied().collect::<Vec<_>>(), vec![events[0].entity_id]);
    assert_eq!(tree.current_snapshot().await?.entries.len(), 1);
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
    assert!(!Arc::ptr_eq(&initial.tree(), &wider.tree()), "narrow admission cannot cover a wider predicate");
    let all = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(!Arc::ptr_eq(&initial.tree(), &all.tree()));
    assert!(!Arc::ptr_eq(&wider.tree(), &all.tree()));
    let ordered_limit = acquire(&node.resident_trees, &node, selection(Some(1))).await?;
    assert!(!Arc::ptr_eq(&all.tree(), &ordered_limit.tree()), "an entity-ID tree cannot provide a label-ordered limit");
    assert_eq!(node.resident_trees.len(), 4);
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
async fn cold_limit_bounds_physical_retention_and_zero_limit_reads_nothing() -> anyhow::Result<()> {
    let node = node().await?;
    for value in 0..128 {
        apply(&node, &genesis(&format!("{value:03}"))?).await?;
    }
    let rows_read = node.storage.4.load(Ordering::Relaxed);
    let limited = acquire(&node.resident_trees, &node, selection(Some(3))).await?;
    assert_eq!(limited.snapshot().await?.snapshot.entries.len(), 3);
    assert_eq!(limited.tree().current_snapshot().await?.entries.len(), 3, "the physical resident, not only its view, must be bounded");
    assert_eq!(node.storage.4.load(Ordering::Relaxed) - rows_read, 3, "cold capture requests only the selected rows from storage");

    let scans = node.storage.2.load(Ordering::Relaxed);
    let rows_read = node.storage.4.load(Ordering::Relaxed);
    let empty = acquire(&node.resident_trees, &node, selection(Some(0))).await?;
    assert!(empty.snapshot().await?.snapshot.entries.is_empty());
    assert!(empty.tree().current_snapshot().await?.entries.is_empty());
    assert_eq!(node.storage.2.load(Ordering::Relaxed), scans, "LIMIT 0 must not start an application scan");
    assert_eq!(node.storage.4.load(Ordering::Relaxed), rows_read);
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
    let tree = limited.tree();
    let mut original = limited.snapshot().await?.snapshot;
    let peer = proto::EntityId::from_bytes([90; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let query = proto::QueryId::new();
    connection
        .subscription_handler
        .subscribe_query(&node, proto::RequestId::new(), vec![], query, selected, Some(&DEFAULT_CONTEXT), 1, original.offer().await?)
        .await?;
    let (_, serving) = connection.subscription_handler.query_tree_for_test(query).await.unwrap();
    assert!(Arc::ptr_eq(&tree, &serving), "requesting and serving use the same bounded resident");
    let mut opposite = selection(Some(1));
    opposite.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
    let mut narrower = above("01", None);
    narrower.limit = Some(1);
    for other in [selection(None), selection(Some(2)), selection(Some(0)), opposite, narrower] {
        let other = acquire(&node.resident_trees, &node, other).await?;
        assert!(!Arc::ptr_eq(&tree, &other.tree()), "bounded coverage never proves another selection complete");
    }
    assert_eq!(tree.current_snapshot().await?.entries.len(), 1);
    connection.subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    Ok(())
}
