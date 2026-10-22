use super::*;

#[tokio::test]
async fn fallback_view_never_reads_storage_or_blocks_the_shared_subscription() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let second = genesis("02")?;
    apply(&node, &second).await?;
    let broad_selection = selection(None);
    let broad = node.resident_trees.acquire(&node, broad_selection.clone(), None, vec![DEFAULT_CONTEXT]).await?;
    let tree = broad.tree();
    let mut snapshot = broad.snapshot().await?.snapshot;
    let by_id = snapshot.spec.clone();
    assert!(by_id.key_spec().keyparts.is_empty());

    let peer = proto::EntityId::from_bytes([89; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let connection = node.peer_connections.get(&peer).unwrap();
    let query = proto::QueryId::new();
    connection
        .subscription_handler
        .subscribe_query(&node, proto::RequestId::new(), vec![], query, broad_selection, Some(&DEFAULT_CONTEXT), 1, snapshot.offer().await?)
        .await?;

    // Arm a storage read pause. Captured-state selection must finish without
    // entering it, while the resident continues to publish live updates.
    let limited = node.resident_trees.acquire(&node, selection(Some(1)), Some(by_id), vec![DEFAULT_CONTEXT]).await?;
    assert!(Arc::ptr_eq(&tree, &limited.tree()));
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let (mut entered, release) = hold_fetch(&node);
    let before = tokio::time::timeout(std::time::Duration::from_secs(2), limited.snapshot()).await??;
    assert_eq!(before.snapshot.states.keys().copied().collect::<Vec<_>>(), vec![initial.entity_id]);
    let changed = update(&initial, "09")?;
    apply(&node, &changed).await?;
    let after = tokio::time::timeout(std::time::Duration::from_secs(2), limited.snapshot()).await??;
    assert_eq!(after.snapshot.states.keys().copied().collect::<Vec<_>>(), vec![second.entity_id]);
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans);
    assert_eq!(entered.try_recv(), Err(oneshot::error::TryRecvError::Empty), "the storage pause remains unused");
    node.storage.0.hold_fetch.lock().unwrap().remove(&model());
    drop(release);

    let current = broad.snapshot().await?;
    assert_eq!(current.snapshot.states[&initial.entity_id].payload.state.head, proto::Clock::singleton(&changed));
    assert_eq!(node.resident_trees.len(), 1);
    let (_, serving) = connection.subscription_handler.query_tree_for_test(query).await.expect("the other subscription remains active");
    assert!(Arc::ptr_eq(&tree, &serving));
    assert!(Arc::ptr_eq(&tree, &limited.tree()));
    connection.subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    Ok(())
}
