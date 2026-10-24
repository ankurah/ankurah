//! Which requests share a resident, and which need their own.
use super::*;

#[tokio::test]
async fn shared_responder_routes_reuse_the_same_tree_and_detach_independently() -> anyhow::Result<()> {
    let node = node().await?;
    let event = genesis("01")?;
    apply(&node, &event).await?;
    let selection = selection(None);
    let resident = acquire(&node.resident_trees, &node, selection.clone()).await?;
    let original = resident.identity();
    drop(resident);
    let scans = node.storage.fetches.load(Ordering::Relaxed);
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
                proto::KnownState { spec: spec_for(&selection, &node.catalog), spans: vec![] },
            )
            .await?;
        let (_, tree) = handler.subscription_handler.query_resident_for_test(query).await.unwrap();
        assert!(Weak::ptr_eq(&original, &tree));
        subscriptions.push((peer, query));
    }
    assert_eq!(node.resident_trees.len(), 1);
    assert_eq!(node.storage.fetches.load(Ordering::Relaxed), scans);
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
    *node.storage.events_pause.lock().unwrap() = Some((entered, resume));
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
    *node.storage.events_pause.lock().unwrap() = Some((entered, resume));
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
    let (_, shared) = healthy_connection.subscription_handler.query_resident_for_test(query).await.unwrap();
    assert!(Weak::ptr_eq(&shared, &resident.identity()));

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
                proto::NodeMessage::StateFrame { to, frame, .. }
                    if *to == healthy_peer && frame.stream == state.frame.stream && matches!(frame.body, proto::StateFrameBody::RestartRequired)
            )) { break; }
            tokio::task::yield_now().await;
        }
    }).await?;
    assert!(healthy_connection.subscription_handler.query_resident_for_test(query).await.is_none());
    assert_eq!(node.resident_trees.len(), 0);
    // The first peer's history read is still blocked. Cancel it only after the
    // healthy peer has received the independent invalidation.
    drop(slow);
    assert!(release.send(()).is_err());
    node.deregister_peer(slow_peer);
    node.deregister_peer(healthy_peer);
    Ok(())
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
    assert_eq!(broad.snapshot().await?.snapshot.entries.len(), 4, "query limits never truncate the resident");
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
    assert!(Weak::ptr_eq(&all.identity(), &preferred.identity()), "prefer a compatible complete resident");
    assert_eq!(all.snapshot().await?.snapshot.entries.len(), 3);
    apply(&node, &update(&events[0], "09")?).await?;
    let after = first.snapshot().await?.snapshot;
    assert_eq!(after.entries.keys().copied().collect::<Vec<_>>(), vec![events[1].entity_id]);
    assert_eq!(frozen.entries.keys().copied().collect::<Vec<_>>(), vec![events[0].entity_id]);
    assert_eq!(after.entries.len(), 1, "a refill keeps the bounded resident at its cut");
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
    let (_, serving) = connection.subscription_handler.query_resident_for_test(query).await.unwrap();
    assert!(Weak::ptr_eq(&tree, &serving), "requesting and serving use the same bounded resident");
    let mut opposite = selection(Some(1));
    opposite.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
    let mut narrower = above("01", None);
    narrower.limit = Some(1);
    for other in [selection(None), selection(Some(2)), selection(Some(0)), opposite, narrower] {
        let other = acquire(&node.resident_trees, &node, other).await?;
        assert!(!Weak::ptr_eq(&tree, &other.identity()), "bounded coverage never proves another selection complete");
    }
    assert_eq!(limited.snapshot().await?.snapshot.entries.len(), 1);
    connection.subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
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
    let peer = proto::EntityId::from_bytes([26; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    // Complete coverage can serve several limits without a bounded refill.
    let full = node
        .resident_trees
        .acquire(&node, selection(None), Some(spec_for(&selection(Some(2)), &node.catalog)), vec![DEFAULT_CONTEXT])
        .await?;
    let handler = crate::peer_subscription::SubscriptionHandler::new(peer);
    let query = proto::QueryId::new();
    handler
        .subscribe_query(
            &node,
            proto::RequestId::new(),
            Vec::new(),
            query,
            selection(Some(2)),
            Some(&DEFAULT_CONTEXT),
            1,
            proto::KnownState { spec: spec_for(&selection(Some(2)), &node.catalog), spans: vec![] },
        )
        .await?;
    let (_, original) = handler.query_resident_for_test(query).await.unwrap();
    let scans = node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed);
    apply(&node, &update(&initial[0], "09")?).await?;
    handler
        .subscribe_query(
            &node,
            proto::RequestId::new(),
            Vec::new(),
            query,
            selection(Some(1)),
            Some(&DEFAULT_CONTEXT),
            2,
            proto::KnownState { spec: spec_for(&selection(Some(1)), &node.catalog), spans: vec![] },
        )
        .await?;
    let (version, reused) = handler.query_resident_for_test(query).await.unwrap();
    assert_eq!(version, 2);
    assert!(Weak::ptr_eq(&original, &reused), "compatible limits select scopes of the same resident tree");
    assert!(Weak::ptr_eq(&full.identity(), &reused), "both limits reuse the complete resident");
    let snapshot = full.snapshot().await?.snapshot;
    assert_eq!(snapshot.entries.len(), 3, "the resident keeps the full admission");
    let snapshot = snapshot.select(&selection(Some(1)), &node.catalog)?;
    assert_eq!(snapshot.entries.keys().copied().collect::<Vec<_>>(), vec![initial[1].entity_id]);
    assert_eq!(node.storage.fetches.load(std::sync::atomic::Ordering::Relaxed), scans);
    handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    Ok(())
}
