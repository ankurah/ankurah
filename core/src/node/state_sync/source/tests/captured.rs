use super::*;

#[tokio::test]
async fn captures_share_payloads_across_admission_attestation_and_frozen_snapshots() -> anyhow::Result<()> {
    let node = node().await?;
    let original = genesis("01")?;
    apply(&node, &original).await?;
    let selection = selection(None);
    let spec = QueryTreeCache::spec(&selection, &node.catalog);
    let mut captured = capture_query(&node, selection, spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTreeCache::default())).await?;
    let id = original.entity_id;
    let admitted = &captured.projection.rows[&id];
    assert!(Arc::ptr_eq(admitted, &captured.projection.candidates[&id]), "authorization must not duplicate row keys/values");
    assert!(Arc::ptr_eq(&admitted.state, &captured.snapshot.states[&id].payload));
    let frozen = captured.snapshot;
    let moved = update(&original, "02")?;
    apply(&node, &moved).await?;
    let batch = captured.cursor.next().await?;
    let published = &batch.changes.iter().find(|change| change.state.entity_id == id).unwrap().state;
    let credentials = vec![DEFAULT_CONTEXT];
    captured.projection.apply(&node, &credentials, &batch, true).await?;
    let current = captured.projection.tree.current_snapshot().await?;
    let admitted = &captured.projection.rows[&id];
    assert!(Arc::ptr_eq(admitted, &captured.projection.candidates[&id]));
    assert!(Arc::ptr_eq(published, &admitted.state));
    assert!(Arc::ptr_eq(published, &current.states[&id].payload));
    assert!(!Arc::ptr_eq(published, &frozen.states[&id].payload));
    assert_eq!(current.states[&id].payload.state.head, proto::Clock::singleton(&moved));
    assert_eq!(current.states[&id].payload.read()?.value(&property()), Some(Value::String("02".into())));
    assert_eq!(frozen.states[&id].payload.state.head, proto::Clock::singleton(&original));
    assert_eq!(frozen.states[&id].payload.read()?.value(&property()), Some(Value::String("01".into())));
    let wire_bytes: usize =
        batch.changes.iter().map(|change| bincode::serialized_size(&(&*change.state, &change.events)).unwrap() as usize).sum();
    let lookup_bytes: usize = batch.changes.iter().map(|change| change.state.lookup_bytes()).sum();
    assert!(lookup_bytes > 0);
    assert_eq!(batch.bytes, wire_bytes + lookup_bytes, "the source backlog accounts for retained compact values");
    assert!(admitted.bytes >= admitted.state.lookup_bytes());
    let mut shared = current.states[&id].as_ref().clone();
    shared.attestations.push(proto::Attestation(vec![1, 3, 5]));
    let owned: Attested<proto::EntityState> = shared.to_owned();
    assert_eq!(bincode::serialize(&shared)?, bincode::serialize(&owned)?);
    Ok(())
}

#[tokio::test]
async fn shared_values_do_not_cache_state_or_event_permissions() -> anyhow::Result<()> {
    let node = node().await?;
    let event = genesis("01")?;
    apply(&node, &event).await?;
    let state = CapturedEntity::new(node.storage.get_state(event.entity_id).await?.payload);
    assert_eq!(state.read()?.value(&property()), Some(Value::String("01".into())));
    let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let agent = RestrictedDiscovery {
        read_allowed: Some(allowed.clone()),
        denied_event: Some(event.id()),
        retrieval: Some(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
            operator: ankql::ast::ComparisonOperator::LessThan,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("05".into()))),
        }),
        ..Default::default()
    };
    let credentials = vec![DEFAULT_CONTEXT];
    let policy = ContextPolicy::from_credentials(&agent, &credentials);
    policy.check_read_captured(&state)?;
    let outside = genesis("09")?;
    apply(&node, &outside).await?;
    let outside = CapturedEntity::new(node.storage.get_state(outside.entity_id).await?.payload);
    let old_error = policy.check_read(&outside.entity_id, &outside.state).unwrap_err();
    assert_eq!(policy.check_read_captured(&outside).unwrap_err().to_string(), old_error.to_string());
    let mut malformed = state.to_entity_state();
    malformed.state.state_buffers.0.insert("yrs".into(), vec![255]);
    let old_error = policy.check_read(&malformed.entity_id, &malformed.state).unwrap_err();
    assert_eq!(policy.check_read_captured(&CapturedEntity::new(malformed)).unwrap_err().to_string(), old_error.to_string());
    let attested = Attested::from(event);
    assert!(policy.check_read_captured_event(&attested, &state).is_err(), "cached values cannot bypass event authorization");
    allowed.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(policy.check_read_captured(&state).is_err(), "cached values cannot preserve an obsolete authorization");
    allowed.store(true, std::sync::atomic::Ordering::SeqCst);
    policy.check_read_captured(&state)?;
    let mismatched = Attested::from(genesis("03")?);
    assert_eq!(
        policy.check_read_captured_event(&mismatched, &state).unwrap_err().to_string(),
        crate::policy::AccessDenied::ByPolicy("Event and captured entity do not match").to_string(),
    );
    Ok(())
}
