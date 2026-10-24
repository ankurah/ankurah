use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[tokio::test]
async fn reactor_publication_evaluates_the_state_free_read_proof_for_each_change() -> anyhow::Result<()> {
    let allowed = Arc::new(AtomicBool::new(true));
    let checks = Arc::new(AtomicUsize::new(0));
    let proofs = Arc::new(AtomicUsize::new(0));
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery {
            read_allowed: Some(allowed.clone()),
            state_free_reads: true,
            state_free_proofs: Some(proofs.clone()),
            state_checks: Some(checks.clone()),
            ..Default::default()
        },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    let mut current = genesis("01")?;
    apply(&node, &current).await?;
    let selected = selection(None);
    let spec = spec_for(&selected, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    assert!(view(&actor, &selected, &node.catalog).await?.entries.contains_key(&current.entity_id));

    for (readable, value) in [(true, "02"), (false, "03"), (true, "04")] {
        allowed.store(readable, Ordering::SeqCst);
        let before = checks.load(Ordering::Relaxed);
        let before_proofs = proofs.load(Ordering::Relaxed);
        current = update(&current, value)?;
        apply(&node, &current).await?;
        let batch = actor.cursor.next().await?;
        assert_eq!(checks.load(Ordering::Relaxed) - before, usize::from(!readable), "only an unavailable proof needs a state check");
        assert_eq!(proofs.load(Ordering::Relaxed) - before_proofs, 1, "each publication evaluates permission afresh");
        actor.apply(batch.changes, false).await?;
        let snapshot = actor.tree.snapshot();
        assert_eq!(snapshot.entries.contains_key(&current.entity_id), readable);
        if readable {
            assert_eq!(snapshot.entries[&current.entity_id].head, proto::Clock::singleton(&current));
        }
        assert!(actor.candidates.contains(&current.entity_id), "revoked candidates remain watched for reentry");
    }
    Ok(())
}

#[tokio::test]
async fn unrestricted_reads_reuse_a_complete_tree_for_a_limit_without_reading_states() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = Vec::new();
    for row in 0..64 {
        let event = genesis(&format!("row-{row:08}"))?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let full = acquire(&node.resident_trees, &node, above("", None)).await?;
    let original = full.identity();
    assert!(node.entities.get(&events[0].entity_id).is_none(), "the proof must not depend on resident payloads");
    let reads = node.storage.state_reads.load(Ordering::Relaxed);
    let scans = node.storage.fetches.load(Ordering::Relaxed);
    let limited = acquire(&node.resident_trees, &node, above("row-00000031", Some(10))).await?;
    assert!(Weak::ptr_eq(&original, &limited.identity()));
    assert_eq!(
        limited.snapshot().await?.snapshot.entries.keys().copied().collect::<BTreeSet<_>>(),
        events[32..42].iter().map(|event| event.entity_id).collect(),
    );
    assert_eq!(
        node.storage.state_reads.load(Ordering::Relaxed),
        reads,
        "unchanged authorization does not reload the complete tree's states"
    );
    assert_eq!(node.storage.fetches.load(Ordering::Relaxed), scans);

    let moved = update(&events[0], "row-99999999")?;
    apply(&node, &moved).await?;
    let reads = node.storage.state_reads.load(Ordering::Relaxed);
    let snapshot = full.snapshot().await?.snapshot;
    assert_eq!(snapshot.entries[&moved.entity_id].head, proto::Clock::singleton(&moved), "the proof never bypasses reactor maintenance");
    assert_eq!(node.storage.state_reads.load(Ordering::Relaxed), reads);
    Ok(())
}

#[tokio::test]
async fn state_free_read_proofs_are_fresh_and_do_not_hide_new_grants_or_revocations() -> anyhow::Result<()> {
    let allowed = Arc::new(AtomicBool::new(false));
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { read_allowed: Some(allowed.clone()), state_free_reads: true, deny_empty: true, ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    let first = genesis("01")?;
    let second = genesis("02")?;
    for event in [&first, &second] {
        apply(&node, event).await?;
    }
    let denied = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(denied.snapshot().await?.snapshot.entries.is_empty());
    let reads = node.storage.state_reads.load(Ordering::Relaxed);
    allowed.store(true, Ordering::SeqCst);
    assert!(denied.snapshot().await.is_err(), "the old empty admission cannot become an all-readable answer");
    assert_eq!(node.storage.state_reads.load(Ordering::Relaxed), reads, "the universal grant detects omitted candidates from metadata");
    let granted = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert_eq!(
        granted.snapshot().await?.snapshot.entries.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([first.entity_id, second.entity_id]),
    );
    assert_eq!(node.storage.state_reads.load(Ordering::Relaxed), reads);

    let no_credentials = node.resident_trees.acquire(&node, selection(None), None, vec![]).await?;
    assert!(!Weak::ptr_eq(&granted.identity(), &no_credentials.identity()), "a credential change cannot borrow the earlier grant");
    assert!(no_credentials.snapshot().await?.snapshot.entries.is_empty());
    let reads = node.storage.state_reads.load(Ordering::Relaxed);
    allowed.store(false, Ordering::SeqCst);
    assert!(granted.snapshot().await.is_err(), "the proof is not retained after revocation");
    assert!(node.storage.state_reads.load(Ordering::Relaxed) > reads, "a false proof falls back to actual state checks");
    let revoked = acquire(&node.resident_trees, &node, selection(None)).await?;
    assert!(revoked.snapshot().await?.snapshot.entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn restricted_or_failed_retrieval_never_uses_a_state_free_read_proof() -> anyhow::Result<()> {
    for retrieval_error in [false, true] {
        let proofs = Arc::new(AtomicUsize::new(0));
        let node = Node::new_durable(
            Arc::new(OrderedStorage::default()),
            RestrictedDiscovery {
                retrieval: Some(Predicate::Comparison {
                    left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
                    operator: ankql::ast::ComparisonOperator::LessThan,
                    right: Box::new(ankql::ast::Expr::Literal(Value::String("03".into()))),
                }),
                state_free_reads: true,
                state_free_proofs: Some(proofs.clone()),
                retrieval_error,
                ..Default::default()
            },
        );
        node.system.create().await?;
        node.wait_ready().await?;
        let readable = genesis("01")?;
        for event in [&readable, &genesis("04")?] {
            apply(&node, event).await?;
        }
        let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
        let reads = node.storage.state_reads.load(Ordering::Relaxed);
        let proof_calls = proofs.load(Ordering::Relaxed);
        let snapshot = resident.snapshot().await?.snapshot;
        assert_eq!(snapshot.entries.keys().copied().collect::<Vec<_>>(), if retrieval_error { vec![] } else { vec![readable.entity_id] });
        assert_eq!(
            node.storage.state_reads.load(Ordering::Relaxed) - reads,
            2,
            "restricted retrieval rechecks every candidate's actual state"
        );
        assert_eq!(proofs.load(Ordering::Relaxed), proof_calls, "a non-True retrieval predicate never invokes the proof hook");
    }
    Ok(())
}

#[tokio::test]
async fn policies_without_a_state_free_proof_keep_their_state_checks() -> anyhow::Result<()> {
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { read_allowed: Some(Arc::new(AtomicBool::new(false))), ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    apply(&node, &genesis("01")?).await?;
    let resident = acquire(&node.resident_trees, &node, selection(None)).await?;
    let reads = node.storage.state_reads.load(Ordering::Relaxed);
    assert!(resident.snapshot().await?.snapshot.entries.is_empty());
    assert_eq!(node.storage.state_reads.load(Ordering::Relaxed) - reads, 1);
    Ok(())
}

#[tokio::test]
async fn one_publication_evaluates_the_state_free_read_proof_once_for_all_its_changes() -> anyhow::Result<()> {
    let proofs = Arc::new(AtomicUsize::new(0));
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { state_free_reads: true, state_free_proofs: Some(proofs.clone()), ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    let mut actor = complete_resident(&node, spec_for(&selection(None), &node.catalog), vec![DEFAULT_CONTEXT]).await?;
    let mut changes = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        let label = LWWBackend::new();
        label.set(property(), Some(Value::String(value.into())));
        crate::test_helpers::commit_backend(&label, 1)?;
        let state = proto::State {
            memberships: [model()].into_iter().collect(),
            head: proto::Clock::singleton(&event),
            state_buffers: proto::StateBuffers([("lww".into(), label.to_state_buffer()?)].into()),
        };
        let (_, entity) = node
            .entities
            .with_state(
                &LocalStateGetter::new(node.storage.clone()),
                &LocalEventGetter::new(node.storage.clone(), false),
                event.entity_id,
                state,
            )
            .await?;
        changes.push(crate::changes::EntityChange::new(entity, vec![Attested::from(event)]));
    }
    let before = proofs.load(Ordering::Relaxed);
    node.reactor.begin_commit().await.publish(&changes, true);
    let batch = actor.cursor.next().await?;
    assert_eq!(batch.changes.len(), 3);
    assert_eq!(proofs.load(Ordering::Relaxed) - before, 1, "a publication's projection evaluates the proof once");
    actor.apply(batch.changes, false).await?;
    assert_eq!(actor.tree.entries().len(), 3);
    Ok(())
}

#[tokio::test]
async fn a_command_reads_its_candidates_states_in_one_storage_call() -> anyhow::Result<()> {
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { read_allowed: Some(Arc::new(AtomicBool::new(true))), ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    let mut events = Vec::new();
    for value in ["01", "02", "03"] {
        let event = genesis(value)?;
        apply(&node, &event).await?;
        events.push(event);
    }
    let resident = node.resident_trees.acquire(&node, selection(None), None, vec![DEFAULT_CONTEXT]).await?;
    assert!(events.iter().all(|event| node.entities.get(&event.entity_id).is_none()), "every candidate's state is in storage alone");
    let batches = node.storage.state_batches.load(Ordering::Relaxed);
    assert_eq!(resident.snapshot().await?.snapshot.entries.len(), 3);
    assert_eq!(node.storage.state_batches.load(Ordering::Relaxed) - batches, 1, "the per-candidate read check reads every state at once");
    Ok(())
}
