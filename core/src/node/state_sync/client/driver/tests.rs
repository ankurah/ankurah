use super::*;

fn id(n: u8) -> proto::EntityId { proto::EntityId::from_bytes([n; 32]) }
fn head(n: u8) -> proto::Clock { proto::Clock::genesis(proto::EventId::from_bytes([n; 32])) }
fn member(n: u8) -> proto::QueryMember { proto::QueryMember { key: vec![], head: head(n) } }
fn state(n: u8) -> proto::Attested<proto::EntityState> {
    proto::Attested::opt(proto::EntityState { entity_id: id(n), state: proto::State { head: head(n), ..Default::default() } }, None)
}
type TestNode = Node<crate::test_utils::TestStorage, crate::policy::PermissiveAgent>;
async fn fixture(
    rows: Vec<proto::Attested<proto::EntityState>>,
) -> anyhow::Result<(TestNode, Driver, Arc<super::super::super::IncomingStates>)> {
    let node = TestNode::new(Arc::new(crate::test_utils::TestStorage::default()), crate::policy::PermissiveAgent::new());
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let snapshot = super::super::super::QueryTree::default().refresh(spec, rows).await?;
    let registry = Arc::new(super::super::super::IncomingStates::default());
    let stream = registry.register(proto::RequestId::new(), None, None)?;
    Ok((node, Driver::new(snapshot, proto::StateRound { number: 0, revision: 10 }, stream, proto::ComparisonId::new())?, registry))
}

#[tokio::test]
async fn finite_round_finishes_while_newer_publication_target_remains_uncovered() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.goals.add(id(1), head(1), true)?;
    driver.projection.round.as_mut().unwrap().complete = true;
    driver.projection.publish(11, vec![(id(1), Some(member(2)))])?;
    let mut goals = Goals::default();
    goals.add(id(1), head(2), false)?;
    driver.publications.push_back(Publication { revision: 11, changes: BTreeMap::new(), change_bytes: 0, goals, jobs: 0, complete: true });
    assert!(driver.complete_round(&node, None).await?, "a later hot target must not move the captured round goal");
    assert!(driver.projection.round.is_none());
    assert_eq!(driver.projection.members[&id(1)].head, head(2), "closing a round retains the newer source decision");
    assert!(!driver.publications[0].goals.covered(), "the round does not certify the later target");
    assert_eq!(driver.applied_through, None, "round readiness is not a publication coverage receipt");
    Ok(())
}

#[tokio::test]
async fn repair_after_absence_and_overlay_gc_cannot_restore_membership() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.projection.publish(11, vec![(id(1), None)])?;
    driver.projection.round.as_mut().unwrap().complete = true;
    assert!(driver.complete_round(&node, None).await?);
    assert_eq!(driver.projection.retained_bytes(), 0, "round tombstones can be reclaimed immediately");
    driver.observe(&node, Applied { owner: Owner::Repair, id: id(1), head: Some(head(1)), change: None, bytes: 0, repair: None }).await?;
    assert!(!driver.projection.members.contains_key(&id(1)));
    assert!(!driver.eligible.contains(&id(1)));
    assert!(!driver.observed.contains_key(&id(1)), "an unneeded historical identity must not accumulate");
    Ok(())
}

#[tokio::test]
async fn cancelled_round_jobs_do_not_complete_or_write_the_next_round_membership() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.projection.publish(11, vec![(id(1), None)])?;
    driver.cancel_round(&node, id(90), &[])?;
    let snapshot = super::super::super::QueryTree::default().refresh(driver.spec.clone(), vec![state(2)]).await?;
    driver.projection.begin(Round::new(proto::StateRound { number: 1, revision: 11 }, snapshot))?;
    driver.round_number = 1;
    driver.round_jobs = 1;
    driver.observe(&node, Applied { owner: Owner::Round(0), id: id(1), head: Some(head(1)), change: None, bytes: 0, repair: None }).await?;
    assert_eq!(driver.round_jobs, 1, "old completion does not decrement a new round's work");
    assert!(!driver.projection.members.contains_key(&id(1)));
    assert!(!driver.eligible.contains(&id(1)));
    assert!(driver.projection.round.as_ref().unwrap().members.contains_key(&id(2)));
    Ok(())
}

#[test]
fn observed_entity_lookup_visits_only_its_distinct_targets() -> anyhow::Result<()> {
    let mut goals = Goals::default();
    for n in 0..=255 {
        goals.add(id(n), head(n), false)?;
    }
    goals.add(id(7), head(8), false)?;
    goals.add(id(7), head(8), false)?;
    assert_eq!(goals.count, 257);
    assert_eq!(goals.for_id_mut(&id(7)).count(), 2);
    assert_eq!(goals.for_id_mut(&id(9)).count(), 1);
    assert_eq!(goals.values().count(), 257);
    Ok(())
}

#[tokio::test]
async fn overlay_coalesces_reentry_and_has_an_aggregate_byte_bound() -> anyhow::Result<()> {
    let (_, mut driver, _registry) = fixture(vec![]).await?;
    for revision in 11..1011 {
        driver.projection.publish(revision, vec![(id(1), Some(member(1)))])?;
    }
    let one_decision = bincode::serialized_size(&(id(1), Some(member(1))))? as usize;
    assert_eq!(driver.projection.retained_bytes(), one_decision, "coalescing does not retain intermediate revisions");
    driver.check_retained()?;
    let huge = proto::QueryMember { key: vec![1; super::super::super::WINDOW_BYTES], head: head(1) };
    driver.projection.publish(1011, vec![(id(1), Some(huge))])?;
    assert!(driver.check_retained().is_err(), "one wide key cannot escape the aggregate overlay limit");
    Ok(())
}

#[tokio::test]
async fn live_coverage_can_finish_round_with_queued_repair_and_ignore_late_unavailability() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.goals.add(id(1), head(1), false)?;
    driver.goals.for_id_mut(&id(1)).next().unwrap().requested = true;
    driver.projection.round.as_mut().unwrap().complete = true;
    // A live application completes while that comparison's connecting-history
    // answer is still outstanding. The actual head, not its revision, covers R.
    driver.observe(&node, Applied { owner: Owner::Live(11), id: id(1), head: Some(head(1)), change: None, bytes: 0, repair: None }).await?;
    assert!(driver.complete_round(&node, None).await?);
    driver.items(&node, id(90), &[], Owner::Repair, vec![StateItem::RepairUnavailable { entity_id: id(1), target: head(1) }]).await?;
    assert!(driver.projection.members.contains_key(&id(1)));
    let mut required = Goals::default();
    required.add(id(2), head(2), false)?;
    driver.publications.push_back(Publication {
        revision: 12,
        changes: BTreeMap::new(),
        change_bytes: 0,
        goals: required,
        jobs: 0,
        complete: true,
    });
    assert!(
        driver
            .items(&node, id(90), &[], Owner::Repair, vec![StateItem::RepairUnavailable { entity_id: id(2), target: head(2) }])
            .await
            .is_err(),
        "an actually uncovered target still fails"
    );
    Ok(())
}

async fn committed_entity(node: &TestNode) -> anyhow::Result<(proto::Event, crate::entity::Entity)> {
    node.system.wait_loaded().await?;
    let genesis = proto::Event::genesis(
        Some(id(90)),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(id(91))))]),
    );
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let events = CachedEventGetter::new(node, &credentials);
    let states = LocalStateGetter::new(node.storage.clone());
    let outcome = NodeApplier::apply_delta(
        node,
        &id(90),
        proto::EntityDelta {
            entity_id: genesis.entity_id,
            content: proto::DeltaContent::EventBridge { events: vec![proto::Attested::opt(genesis.clone(), None).into()] },
        },
        Delivery::Initial,
        &events,
        &states,
    )
    .await?;
    Ok((genesis, outcome.entity.unwrap()))
}
fn edit(entity: proto::EntityId, parent: proto::Clock, model: u8) -> proto::Event {
    proto::Event::update(
        entity,
        parent,
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(id(model))))]),
    )
}
fn bridge(event: proto::Event) -> proto::EntityDelta {
    proto::EntityDelta {
        entity_id: event.entity_id,
        content: proto::DeltaContent::EventBridge { events: vec![proto::Attested::opt(event, None).into()] },
    }
}

#[tokio::test]
async fn held_live_application_cannot_be_overtaken_by_refresh_snapshot() -> anyhow::Result<()> {
    use crate::{retrieval::SuspenseEvents, storage::StorageTransaction};
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let event = edit(entity.id(), entity.head(), 92);
    let getter = LocalEventGetter::new(node.storage.clone(), false);
    getter.stage_event(event.clone());
    let fork = crate::entity::RemoteTrxEntity::edit(&entity)?;
    fork.apply_events(&getter, &mut [proto::Attested::opt(event.clone(), None)], |_| Ok(None)).await?;
    let newer = fork.to_state()?;
    // History can already be cached independently of the entity's application.
    // Without the owner FIFO, this later snapshot could adopt that head first.
    let mut transaction = node.storage.transaction();
    transaction.add_events(&[proto::Attested::opt(event.clone(), None)]).await?;
    transaction.commit().await?.committed()?;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *node.storage.hold_set_state.lock().unwrap() = Some((entered, release_rx));
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Live(11), bridge(event.clone()), None)?;
    driver.enqueue(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        proto::EntityDelta {
            entity_id: entity.id(),
            content: proto::DeltaContent::StateSnapshot { state: proto::StateFragment { state: newer, attestations: Default::default() } },
        },
        None,
    )?;
    {
        let next = driver.jobs.next();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
    }
    entered_rx.await?;
    {
        let next = driver.jobs.next();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending(), "the later snapshot must not run while the earlier live commit is held");
    }
    release.send(()).unwrap();
    let live = driver.jobs.next().await.unwrap()?;
    assert_eq!(live.change.as_ref().unwrap().events().iter().map(|event| event.payload.id()).collect::<Vec<_>>(), [event.id()]);
    driver.observe(&node, live).await?;
    let snapshot = driver.jobs.next().await.unwrap()?;
    assert!(snapshot.change.as_ref().is_none_or(|change| change.events().is_empty()));
    driver.observe(&node, snapshot).await?;
    assert_eq!(entity.head(), proto::Clock::singleton(&event));
    assert_eq!(driver.application_jobs, 0);
    assert_eq!(driver.application_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn completed_live_jobs_for_one_entity_keep_both_event_notifications() -> anyhow::Result<()> {
    use crate::{context::Context, reactor::fetch_gap::QueryGapFetcher};
    use ankurah_signals::Subscribe;
    let (node, _, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let context = Context::new(node.clone(), crate::policy::DEFAULT_CONTEXT);
    let subscription = node.reactor.subscribe();
    let query = proto::QueryId::new();
    node.reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None },
            context.0.clone(),
            crate::resultset::EntityResultSet::empty(),
            Arc::new(QueryGapFetcher::new(context.clone())),
            1,
            (),
            vec![entity.clone()],
            crate::reactor::PeerQueryMembership::default(),
        )
        .await?;
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = received.clone();
    let _listener = subscription.subscribe(move |update: crate::reactor::ReactorUpdate| {
        observed.lock().unwrap().extend(update.items.into_iter().flat_map(|item| item.events.into_iter().map(|event| event.payload.id())));
    });
    let first = edit(entity.id(), entity.head(), 92);
    let second = edit(entity.id(), proto::Clock::singleton(&first), 93);
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let getter = CachedEventGetter::new(&node, &credentials);
    let states = LocalStateGetter::new(node.storage.clone());
    let mut changes = Vec::new();
    for event in [first.clone(), second.clone()] {
        let outcome = NodeApplier::apply_delta(&node, &id(90), bridge(event), Delivery::Live, &getter, &states).await?;
        changes.extend(outcome.change);
    }
    assert_eq!(changes.len(), 2);
    notify_changes(&node, None, changes).await;
    assert_eq!(*received.lock().unwrap(), [first.id(), second.id()], "a ready batch cannot coalesce away the second event vector");
    Ok(())
}

#[tokio::test]
async fn first_chunk_reservation_can_receive_rest_after_later_live_fills_window() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let target = entity.head();
    let earlier =
        proto::DeltaContent::StateSnapshot { state: proto::StateFragment { state: entity.to_state()?, attestations: Default::default() } };
    let bytes = bincode::serialize(&earlier)?;
    driver.chunk(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        target.clone(),
        None,
        proto::EntityDeltaChunk { entity_id: entity.id(), offset: 0, bytes: bytes[..1].to_vec().into(), last: false },
    )?;
    let later = proto::DeltaContent::StateSnapshot {
        state: proto::StateFragment {
            state: entity.to_state()?,
            attestations: proto::AttestationSet(vec![proto::Attestation(vec![7; super::super::super::WINDOW_BYTES])]),
        },
    };
    let later = bincode::serialize(&later)?;
    for (index, part) in later.chunks(super::super::super::PAGE_BYTES / 2).enumerate() {
        let offset = index * (super::super::super::PAGE_BYTES / 2);
        driver.chunk(
            &node,
            id(90),
            &[crate::policy::DEFAULT_CONTEXT],
            Owner::Live(11),
            proto::Clock::default(),
            Some(proto::QueryMember { key: vec![], head: target.clone() }),
            proto::EntityDeltaChunk {
                entity_id: entity.id(),
                offset: offset as u64,
                bytes: part.to_vec().into(),
                last: offset + part.len() == later.len(),
            },
        )?;
    }
    assert!(driver.application_bytes >= super::super::super::WINDOW_BYTES);
    assert_eq!(driver.application_jobs, 2);
    assert_eq!(driver.queued_jobs[&entity.id()].len(), 1);
    assert!(driver.can_receive(), "the earlier reserved job requires more bytes: application pressure cannot stop that reception");
    driver.chunk(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        target,
        None,
        proto::EntityDeltaChunk { entity_id: entity.id(), offset: 1, bytes: bytes[1..].to_vec().into(), last: true },
    )?;
    assert!(driver.assemblies.is_empty());
    for _ in 0..2 {
        let applied = tokio::time::timeout(std::time::Duration::from_secs(5), driver.jobs.next()).await?.unwrap()?;
        driver.observe(&node, applied).await?;
    }
    assert_eq!(driver.application_bytes, 0);
    assert_eq!(driver.application_jobs, 0);
    assert!(driver.can_receive());
    Ok(())
}

#[tokio::test]
async fn independently_covered_round_chunk_releases_reservation_without_waiting_for_its_rest() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let target = entity.head();
    driver.goals.add(entity.id(), target.clone(), true)?;
    let content =
        proto::DeltaContent::StateSnapshot { state: proto::StateFragment { state: entity.to_state()?, attestations: Default::default() } };
    let bytes = bincode::serialize(&content)?;
    driver.chunk(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        target.clone(),
        None,
        proto::EntityDeltaChunk { entity_id: entity.id(), offset: 0, bytes: bytes[..1].to_vec().into(), last: false },
    )?;
    driver.projection.round.as_mut().unwrap().complete = true;
    driver.discard_covered_round_chunks();
    let released = driver.jobs.next().await.unwrap()?;
    assert!(released.head.is_none());
    driver.observe(&node, released).await?;
    // Late bytes can arrive before Applied, without recreating an assembly.
    driver.chunk(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        target,
        None,
        proto::EntityDeltaChunk { entity_id: entity.id(), offset: 1, bytes: bytes[1..].to_vec().into(), last: true },
    )?;
    assert!(driver.assemblies.is_empty());
    assert!(driver.complete_round(&node, None).await?);
    assert_eq!(driver.assembly_bytes, 0);
    assert_eq!(driver.application_jobs, 0);
    Ok(())
}

#[tokio::test]
async fn older_actual_head_does_not_fail_when_later_target_history_is_not_local_yet() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let later = edit(entity.id(), entity.head(), 92);
    let target = proto::Clock::singleton(&later);
    assert!(node.storage.get_events(vec![later.id()]).await?.is_empty());
    assert!(!covers_target(&node, &entity.head(), &target).await?, "missing future history is uncovered, not a stream application failure");
    driver.goals.add(entity.id(), target, false)?;
    driver.observe_head(&node, entity.id(), entity.head()).await?;
    assert!(!driver.goals.covered(), "an older successful application must not certify that later target");
    assert_eq!(entity.head().entries().len(), 1);
    Ok(())
}
