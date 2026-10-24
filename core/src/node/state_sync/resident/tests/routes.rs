use super::*;

pub(super) async fn route<PA: PolicyAgent<ContextData = &'static crate::policy::DefaultContext>>(
    node: &Node<OrderedStorage, PA>,
    actor: &ResidentActor<PA::ContextData>,
    selection: Selection<Resolved>,
) -> anyhow::Result<Route<PA::ContextData>> {
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let snapshot = view(actor, &selection, &node.catalog).await?;
    let mut filtered = selection.clone();
    filtered.predicate = policy.filter_predicate(filtered.predicate)?;
    Ok(Route {
        peer: proto::EntityId::from_bytes([97; 32]),
        stream: proto::StateStreamId::new(),
        owner: Weak::new(),
        selection,
        filtered,
        credentials: vec![DEFAULT_CONTEXT],
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
    let spec = spec_for(&selection(Some(2)), &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let frozen = view(&actor, &selection(Some(1)), &node.catalog).await?;
    let mut one = route(&node, &actor, selection(Some(1))).await?;
    let mut two = route(&node, &actor, selection(Some(2))).await?;
    let moved = update(&events[0], "09")?;
    apply(&node, &moved).await?;
    let batch = actor.cursor.next().await?;
    let sources = actor.apply(batch.changes, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let changes_one = one.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?;
    let changes_two = two.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?;
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
    assert_eq!(frozen.entries[&events[0].entity_id].head, proto::Clock::singleton(&events[0]));
    assert_eq!(actor.tree.entries().len(), 3, "neither limit truncates the resident tree");
    Ok(())
}

#[tokio::test]
async fn shared_tree_routes_apply_different_membership_predicates() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = genesis("01")?;
    apply(&node, &initial).await?;
    let mut admitted = above("00");
    admitted.order_by = None;
    let spec = spec_for(&admitted, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let mut all = route(&node, &actor, admitted).await?;
    let mut narrow = route(&node, &actor, above("05")).await?;
    assert!(narrow.members.is_empty());
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let mut current = initial.clone();
    for (value, narrow_present) in [("02", false), ("09", true), ("03", false)] {
        let next = update(&current, value)?;
        apply(&node, &next).await?;
        let batch = actor.cursor.next().await?;
        let sources = actor.apply(batch.changes, true).await?;
        let all_changes = all.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?;
        let narrow_changes = narrow.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?;
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
    let spec = spec_for(&admitted, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let mut includes = route(&node, &actor, admitted).await?;
    let mut excludes = route(&node, &actor, above("03")).await?;
    let old = includes.members.clone();
    apply(&node, &denied).await?;
    let batch = actor.cursor.next().await?;
    let sources = actor.apply(batch.changes, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    assert!(includes.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await.is_err());
    assert_eq!(includes.members, old, "a failed publication never partially advances route membership");
    assert!(excludes.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?.is_empty());
    assert_eq!(actor.tree.snapshot().entries[&initial.entity_id].head, proto::Clock::singleton(&denied));
    Ok(())
}

#[tokio::test]
async fn fallback_limit_ranks_only_authorized_current_rows() -> anyhow::Result<()> {
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
    let actor = resident(&node, limited, spec).await?;
    assert_eq!(actor.tree.entries().keys().copied().collect::<Vec<_>>(), vec![events[1].entity_id]);
    assert_eq!(actor.tree.snapshot().entries.len(), 1, "only the authorized bounded result is retained");
    Ok(())
}

#[tokio::test]
async fn unchanged_replacements_follow_query_order_after_commit_changes() -> anyhow::Result<()> {
    let node = node().await?;
    let mut events = [genesis("01")?, genesis("02")?, genesis("03")?, genesis("04")?];
    events.sort_by_key(|event| event.entity_id);
    for (index, event) in events.iter_mut().enumerate() {
        apply(&node, event).await?;
        *event = update(event, &format!("{:02}", index + 1))?;
        apply(&node, event).await?;
    }
    // Ruling missing labels out lets a complete tree rank a descending limit by its keys.
    let mut limited = above("");
    limited.limit = Some(2);
    limited.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
    let spec = spec_for(&limited, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let mut route = route(&node, &actor, limited).await?;
    for event in &events[2..] {
        apply(&node, &update(event, "00")?).await?;
    }
    let batch = actor.cursor.next().await?;
    assert_eq!(batch.changes.iter().map(|change| change.entity_id).collect::<Vec<_>>(), vec![events[2].entity_id, events[3].entity_id]);
    let sources = actor.apply(batch.changes, true).await?;
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &vec![DEFAULT_CONTEXT]);
    let changes = route.changes(&node, &actor.identity.coverage, &actor.tree, &policy, &sources).await?;
    assert_eq!(
        changes.iter().map(|change| (change.entity_id, change.member.is_some())).collect::<Vec<_>>(),
        vec![(events[2].entity_id, false), (events[3].entity_id, false), (events[1].entity_id, true), (events[0].entity_id, true)],
        "commit order is preserved; replacement query order deliberately opposes EntityId order"
    );
    for change in &changes[2..] {
        let Some(proto::DeltaContent::StateAndEvents { events, .. }) = &change.delta else { panic!("replacement needs its state") };
        assert!(events.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn active_bounded_route_refills_after_queued_commits() -> anyhow::Result<()> {
    let node = node().await?;
    let initial = [genesis("01")?, genesis("02")?, genesis("03")?];
    for event in &initial {
        apply(&node, event).await?;
    }
    let admitted = selection(None);
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), admitted.predicate.clone());
    let selected = selection(Some(1));
    let mut actor = resident(&node, selected.clone(), spec).await?;
    let mut snapshot = actor.tree.snapshot();
    let members = snapshot.query_members();
    let peer = proto::EntityId::from_bytes([92; 32]);
    node.register_peer(
        proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
        Box::new(Sender(peer)),
    )
    .await?;
    let known = snapshot.offer().await?;
    let pending = crate::node::state_sync::prepare_state_stream(
        &node,
        peer,
        proto::RequestId::new(),
        vec![],
        vec![DEFAULT_CONTEXT],
        Some((proto::QueryId::new(), 1)),
        false,
        actor.revision,
        snapshot,
        known,
    )?;
    let stream = pending.id();
    pending.initialize(&node).await?;
    let owner = Arc::new(());
    actor.routes.push(Route {
        peer,
        stream,
        owner: Arc::downgrade(&owner),
        selection: selected.clone(),
        filtered: selected,
        credentials: vec![DEFAULT_CONTEXT],
        members,
    });
    let first = update(&initial[0], "09")?;
    let second = update(&initial[1], "08")?;
    apply(&node, &first).await?;
    apply(&node, &second).await?;
    let current = actor.refresh(&node).await?;
    assert_eq!(current.entries.keys().copied().collect::<Vec<_>>(), vec![initial[2].entity_id]);
    assert_eq!(actor.routes.len(), 1, "storage ahead of a queued intermediate revision must not expire the route");
    assert_eq!(actor.routes[0].members.keys().copied().collect::<Vec<_>>(), vec![initial[2].entity_id]);
    node.deregister_peer(peer);
    Ok(())
}
