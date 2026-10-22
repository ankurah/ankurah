use super::*;

async fn route<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    projection: &QueryProjection,
    selection: Selection<Resolved>,
) -> anyhow::Result<Route> {
    let snapshot = projection.tree.current_snapshot().await?.select(&selection, &node.catalog).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let mut filtered = selection.clone();
    filtered.predicate = policy.filter_predicate(filtered.predicate)?;
    Ok(Route {
        peer: proto::EntityId::from_bytes([97; 32]),
        stream: proto::StateStreamId::new(),
        owner: Weak::new(),
        selection,
        filtered,
        members: snapshot.query_members(),
    })
}

fn above(value: &str) -> Selection<Resolved> {
    let mut selection = selection(None);
    selection.predicate = Predicate::And(
        Box::new(selection.predicate),
        Box::new(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(PropertyPath::from(property()))),
            operator: ankql::ast::ComparisonOperator::GreaterThan,
            right: Box::new(ankql::ast::Expr::Literal(Value::String(value.into()))),
        }),
    );
    selection
}

#[tokio::test]
async fn shared_tree_routes_keep_independent_limits_and_supply_unchanged_entrants() -> anyhow::Result<()> {
    let node = node().await?;
    let events = [genesis("01")?, genesis("02")?, genesis("03")?];
    for event in &events {
        apply(&node, event).await?;
    }
    let spec = QueryTreeCache::spec(&selection(Some(2)), &node.catalog);
    let tree = Arc::new(QueryTreeCache::default());
    let mut capture = capture_query(&node, selection(None), spec, vec![DEFAULT_CONTEXT], tree.clone()).await?;
    let frozen = tree.current_snapshot().await?.select(&selection(Some(1)), &node.catalog).await?;
    let mut one = route(&node, &capture.projection, selection(Some(1))).await?;
    let mut two = route(&node, &capture.projection, selection(Some(2))).await?;
    let moved = update(&events[0], "09")?;
    apply(&node, &moved).await?;
    let batch = capture.cursor.next().await?;
    let sources = capture.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let changes_one = one.changes(&node, &capture.projection, &policy, &sources).await?;
    let changes_two = two.changes(&node, &capture.projection, &policy, &sources).await?;
    assert_eq!(one.members.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[1].entity_id]));
    assert_eq!(two.members.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([events[1].entity_id, events[2].entity_id]));
    for changes in [&changes_one, &changes_two] {
        let leaving = changes.iter().find(|change| change.entity_id == events[0].entity_id).unwrap();
        assert!(leaving.member.is_none());
        match leaving.delta.as_ref().unwrap() {
            proto::DeltaContent::StateAndEvents { events, .. } => assert_eq!(events.len(), 1),
            _ => panic!("the changed former member must retain its causal event"),
        }
    }
    for (changes, entrant) in [(&changes_one, events[1].entity_id), (&changes_two, events[2].entity_id)] {
        assert_eq!(changes.len(), 2, "only the changed former member and unchanged entrant are sent");
        let added = changes.iter().find(|change| change.entity_id == entrant).unwrap();
        assert!(added.member.is_some());
        match added.delta.as_ref().unwrap() {
            proto::DeltaContent::StateAndEvents { events, .. } => assert!(events.is_empty()),
            _ => panic!("unchanged entrant must carry its captured state"),
        }
    }
    assert_eq!(frozen.states[&events[0].entity_id].payload.state.head, proto::Clock::singleton(&events[0]));
    assert_eq!(tree.current_snapshot().await?.states.len(), 3, "neither limit truncates the resident tree");
    Ok(())
}

#[tokio::test]
async fn shared_tree_routes_apply_different_membership_predicates() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let mut admitted = above("00");
    admitted.order_by = None;
    let spec = QueryTreeCache::spec(&admitted, &node.catalog);
    let mut capture = capture_query(&node, admitted.clone(), spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTreeCache::default())).await?;
    let mut all = route(&node, &capture.projection, admitted).await?;
    let mut narrow = route(&node, &capture.projection, above("05")).await?;
    assert!(narrow.members.is_empty());
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let mut current = initial.clone();
    for (value, narrow_present) in [("02", false), ("09", true), ("03", false)] {
        let next = update(&current, value)?;
        apply(&node, &next).await?;
        let batch = capture.cursor.next().await?;
        let sources = capture.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch, true).await?;
        let all_changes = all.changes(&node, &capture.projection, &policy, &sources).await?;
        let narrow_changes = narrow.changes(&node, &capture.projection, &policy, &sources).await?;
        assert_eq!(all_changes.len(), 1);
        assert!(all_changes[0].member.is_some());
        if value == "02" {
            assert!(narrow_changes.is_empty(), "changes outside a route do not become its live updates");
        } else {
            assert_eq!(narrow_changes.len(), 1);
            assert_eq!(narrow_changes[0].member.is_some(), narrow_present);
            assert!(narrow_changes[0].delta.is_some());
        }
        assert_eq!(narrow.members.contains_key(&initial.entity_id), narrow_present);
        current = next;
    }
    Ok(())
}

#[tokio::test]
async fn a_denied_event_only_fails_routes_that_would_deliver_it() -> anyhow::Result<()> {
    let initial = genesis("01")?;
    let denied = update(&initial, "02")?;
    let node = Node::new_durable(
        Arc::new(OrderedStorage::default()),
        RestrictedDiscovery { denied_event: Some(denied.id()), ..Default::default() },
    );
    node.system.create().await?;
    node.wait_ready().await?;
    apply(&node, &initial).await?;
    let admitted = above("00");
    let spec = QueryTreeCache::spec(&admitted, &node.catalog);
    let mut capture = capture_query(&node, admitted.clone(), spec, vec![DEFAULT_CONTEXT], Arc::new(QueryTreeCache::default())).await?;
    let mut includes = route(&node, &capture.projection, admitted).await?;
    let mut excludes = route(&node, &capture.projection, above("03")).await?;
    let old = includes.members.clone();
    apply(&node, &denied).await?;
    let batch = capture.cursor.next().await?;
    let sources = capture.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    assert!(includes.changes(&node, &capture.projection, &policy, &sources).await.is_err());
    assert_eq!(includes.members, old, "a failed publication never partially advances route membership");
    assert!(excludes.changes(&node, &capture.projection, &policy, &sources).await?.is_empty());
    assert_eq!(
        capture.projection.tree.current_snapshot().await?.states[&initial.entity_id].payload.state.head,
        proto::Clock::singleton(&denied)
    );
    Ok(())
}

#[tokio::test]
async fn fallback_limit_uses_frozen_rows_and_folds_queued_updates_without_reads() -> anyhow::Result<()> {
    let node = node().await?;
    let events = [genesis("01")?, genesis("02")?, genesis("03")?];
    for event in &events {
        apply(&node, event).await?;
    }
    let admitted = selection(None);
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), admitted.predicate.clone());
    let tree = Arc::new(QueryTreeCache::default());
    let mut capture = capture_query(&node, admitted, spec, vec![DEFAULT_CONTEXT], tree.clone()).await?;
    let scans = node.storage.2.load(std::sync::atomic::Ordering::Relaxed);
    let frozen = tree.current_snapshot().await?;
    let mut limited = route(&node, &capture.projection, selection(Some(1))).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);

    // Both commits are visible to storage before either publication is folded.
    // Selecting this older capture must neither reread storage nor reject it.
    for event in &events[..2] {
        apply(&node, &update(event, "09")?).await?;
    }
    let frozen = frozen.select(&selection(Some(1)), &node.catalog).await?;
    assert_eq!(frozen.states.keys().copied().collect::<Vec<_>>(), vec![events[0].entity_id]);
    for entrant in &events[1..] {
        let batch = capture.cursor.next().await?;
        let sources = capture.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch, true).await?;
        let changes = limited.changes(&node, &capture.projection, &policy, &sources).await?;
        assert_eq!(limited.members.keys().copied().collect::<Vec<_>>(), vec![entrant.entity_id]);
        let entering = changes.iter().find(|change| change.entity_id == entrant.entity_id).unwrap();
        assert!(entering.member.is_some());
        let Some(proto::DeltaContent::StateAndEvents { events, .. }) = &entering.delta else {
            panic!("unchanged entrant needs a snapshot")
        };
        assert!(events.is_empty());
    }
    assert_eq!(frozen.states[&events[0].entity_id].payload.state.head, proto::Clock::singleton(&events[0]));
    assert_eq!(tree.current_snapshot().await?.states.len(), 3, "limits leave resident admission intact");
    assert_eq!(node.storage.2.load(std::sync::atomic::Ordering::Relaxed), scans, "scoping and publication never reread storage");
    Ok(())
}

#[tokio::test]
async fn fallback_limit_ranks_only_authorized_captured_rows() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(OrderedStorage::default()), RestrictedDiscovery::default());
    node.system.create().await?;
    node.wait_ready().await?;
    let events = [genesis("01")?, genesis("02")?, genesis("09")?];
    for event in &events {
        apply(&node, event).await?;
    }
    let mut limited = selection(Some(1));
    limited.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), limited.predicate.clone());
    let tree = Arc::new(QueryTreeCache::default());
    let capture = capture_query(&node, limited, spec, vec![DEFAULT_CONTEXT], tree).await?;
    assert_eq!(capture.snapshot.states.keys().copied().collect::<Vec<_>>(), vec![events[1].entity_id]);
    assert_eq!(capture.projection.rows.len(), 2, "policy filtering precedes ranking");
    Ok(())
}

#[tokio::test]
async fn unchanged_replacements_follow_query_order_after_commit_changes() -> anyhow::Result<()> {
    for fallback in [false, true] {
        let node = node().await?;
        let mut events = [genesis("01")?, genesis("02")?, genesis("03")?, genesis("04")?];
        events.sort_by_key(|event| event.entity_id);
        for (index, event) in events.iter_mut().enumerate() {
            apply(&node, event).await?;
            *event = update(event, &format!("{:02}", index + 1))?;
            apply(&node, event).await?;
        }
        let mut limited = selection(Some(2));
        limited.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
        let spec = if fallback {
            proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), limited.predicate.clone())
        } else {
            QueryTreeCache::spec(&limited, &node.catalog)
        };
        let tree = Arc::new(QueryTreeCache::default());
        let mut capture = capture_query(&node, selection(None), spec, vec![DEFAULT_CONTEXT], tree).await?;
        let mut route = route(&node, &capture.projection, limited).await?;
        for event in &events[2..] {
            apply(&node, &update(event, "00")?).await?;
        }
        let first = capture.cursor.next().await?;
        let second = capture.cursor.next().await?;
        let batch = SourceBatch {
            revision: second.revision,
            changes: first.changes.iter().chain(&second.changes).cloned().collect(),
            bytes: first.bytes + second.bytes,
        };
        let sources = capture.projection.apply(&node, &vec![DEFAULT_CONTEXT], &batch, true).await?;
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
        let changes = route.changes(&node, &capture.projection, &policy, &sources).await?;
        assert_eq!(
            changes.iter().map(|change| (change.entity_id, change.member.is_some())).collect::<Vec<_>>(),
            vec![(events[2].entity_id, false), (events[3].entity_id, false), (events[1].entity_id, true), (events[0].entity_id, true)],
            "commit order is preserved; replacement query order deliberately opposes EntityId order"
        );
        for change in &changes[2..] {
            let Some(proto::DeltaContent::StateAndEvents { events, .. }) = &change.delta else { panic!("replacement needs its state") };
            assert!(events.is_empty());
        }
    }
    Ok(())
}
