use super::*;

#[tokio::test]
async fn an_unindexed_limit_builds_independently_without_blocking_the_shared_tree() -> anyhow::Result<()> {
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

    // ORDER BY is not represented in this tree's entity-ID keys, so the
    // limited selection needs its own bounded resident and storage read.
    let (entered, release) = hold_fetch(&node);
    let pending = node.resident_trees.acquire(&node, selection(Some(1)), Some(by_id), vec![DEFAULT_CONTEXT]);
    tokio::pin!(pending);
    assert!(futures::poll!(&mut pending).is_pending());
    entered.await?;
    let changed = update(&initial, "09")?;
    apply(&node, &changed).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if tree.current_snapshot().await?.entries[&initial.entity_id].head == proto::Clock::singleton(&changed) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    release.send(()).unwrap();
    let limited = pending.await?;
    assert!(!Arc::ptr_eq(&tree, &limited.tree()), "unindexed ordering cannot borrow the broad resident");
    let after = limited.snapshot().await?;
    assert_eq!(after.snapshot.entries.keys().copied().collect::<Vec<_>>(), vec![second.entity_id]);

    let current = broad.snapshot().await?;
    assert_eq!(current.snapshot.entries[&initial.entity_id].head, proto::Clock::singleton(&changed));
    assert_eq!(node.resident_trees.len(), 2);
    let (_, serving) = connection.subscription_handler.query_tree_for_test(query).await.expect("the other subscription remains active");
    assert!(Arc::ptr_eq(&tree, &serving));
    connection.subscription_handler.remove_predicate(&node, query).await?;
    node.deregister_peer(peer);
    Ok(())
}
