use super::*;
use crate::node::state_sync::ResidentTrees;
use std::sync::atomic::Ordering;

async fn acquire<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    cache: &ResidentTrees<PA::ContextData>,
    node: &Node<OrderedStorage, PA>,
    selection: Selection<Resolved>,
) -> anyhow::Result<crate::node::state_sync::resident::ResidentLease<PA::ContextData>> {
    let spec = QueryTreeCache::spec(&selection, &node.catalog);
    cache.acquire(node, selection, spec, vec![DEFAULT_CONTEXT]).await
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
            if snapshot.states[&initial.entity_id].payload.state.head == proto::Clock::singleton(&next) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let reused = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(Arc::ptr_eq(&tree, &reused.tree()));
    let current = reused.snapshot().await?;
    assert_eq!(current.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&next));
    assert_eq!(frozen.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&initial));
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
    assert_eq!(cache.len(), 1);
    let second = acquire(&cache, &node, selection(Some(1))).await?;
    assert!(!Arc::ptr_eq(&tree, &second.tree()));
    assert_eq!(snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&initial));
    Ok(())
}

#[tokio::test]
async fn oversized_miss_does_not_evict_a_useful_small_entry() -> anyhow::Result<()> {
    let node = node().await?;
    for value in ["01", "02", "03", "04"] {
        apply(&node, &genesis(value)?).await?;
    }
    let cache = ResidentTrees::with_limits(2, 10_000);
    let resident = acquire(&cache, &node, selection(Some(1))).await?;
    let tree = resident.tree();
    drop(resident);
    let oversized = acquire(&cache, &node, selection(None)).await?;
    assert_eq!(oversized.snapshot().await?.snapshot.states.len(), 4);
    drop(oversized);
    assert_eq!(cache.len(), 1);
    assert!(Arc::ptr_eq(&tree, &acquire(&cache, &node, selection(Some(1))).await?.tree()));
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
                proto::KnownState { spec: QueryTreeCache::spec(&selection, &node.catalog), spans: vec![] },
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
    assert_eq!(resident.snapshot().await?.snapshot.states[&event.entity_id].payload.state.head, proto::Clock::singleton(&next));
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
    assert_eq!(reused.snapshot().await?.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&denied));
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
    let spec = QueryTreeCache::spec(&selection, &node.catalog);
    let capture = node.resident_trees.capture(&node, selection.clone(), spec.clone(), vec![DEFAULT_CONTEXT]).await?;
    assert!(capture.snapshot.states.is_empty());
    allowed.store(true, Ordering::SeqCst);
    let capture = node.resident_trees.capture(&node, selection.clone(), spec.clone(), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(capture.snapshot.states.len(), 1, "a newly granted cached candidate is discovered");
    discovery.store(true, Ordering::SeqCst);
    let capture = node.resident_trees.capture(&node, selection.clone(), spec.clone(), vec![DEFAULT_CONTEXT]).await?;
    assert_eq!(capture.snapshot.states.len(), 2, "a broader discovery scope requires a fresh capture");
    let denied = node.resident_trees.capture(&node, selection.clone(), spec.clone(), vec![]).await?;
    assert!(denied.snapshot.states.is_empty(), "empty credentials are not privileged and never borrow an authorized view");
    allowed.store(false, Ordering::SeqCst);
    let denied = node.resident_trees.capture(&node, selection, spec, vec![DEFAULT_CONTEXT]).await?;
    assert!(denied.snapshot.states.is_empty(), "revoked rows cannot be disclosed by a cache hit");
    assert_eq!(capture.snapshot.states.len(), 2, "the old comparison snapshot is frozen independently");
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
    let cache = ResidentTrees::with_limits(8, 10_000);
    let resident = acquire(&cache, &node, selection(None)).await?;
    let frozen = resident.snapshot().await?.snapshot;
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
    assert_eq!(frozen.states.len(), 1);
    let current = acquire(&cache, &node, selection(None)).await?;
    assert_eq!(current.snapshot().await?.snapshot.states.len(), 4);
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
    assert_eq!(current.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&newest));
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
    assert!(catalog.snapshot().await?.snapshot.states.contains_key(&event.entity_id));
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
    assert_eq!(capture.snapshot.states[&event.entity_id].payload.state.head, proto::Clock::singleton(&event));
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
