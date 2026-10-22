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
    Ok((node, Driver::new(snapshot, proto::StateRound { number: 0, revision: 10 }, stream, proto::StateStreamId::new())?, registry))
}

#[tokio::test]
async fn malformed_inventories_do_not_change_the_round_projection() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(2), state(4)]).await?;
    let original = driver.projection.round.as_ref().unwrap().members.clone();
    let head = |n| proto::KeyedEntityHead { key: vec![], entity: proto::EntityHead { entity_id: id(n), head: head(n) } };
    let address = |n| ankurah_digest_tree::leaf_address(&[], id(n));
    let invalid = [
        StateItem::Heads(proto::TreeSpanInventory { bounds: proto::TreeSpan::all(), heads: vec![head(4), head(2)].into() }),
        StateItem::Heads(proto::TreeSpanInventory {
            bounds: proto::TreeSpan::new(std::ops::Bound::Included(address(4)), std::ops::Bound::Included(address(2))),
            heads: Default::default(),
        }),
    ];
    for item in invalid {
        assert!(driver.items(&node, id(90), &[], Owner::Round(0), vec![item]).await.is_err());
        assert_eq!(driver.projection.round.as_ref().unwrap().members, original);
        assert!(driver.goals.rows.is_empty());
        assert!(driver.jobs.is_empty());
    }
    Ok(())
}

fn application_genesis(seed: u8) -> proto::Event {
    proto::Event::genesis(
        Some(id(seed)),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(id(91))))]),
    )
}

fn pause_state_read(node: &TestNode, id: proto::EntityId) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    node.storage.hold_state_reads.lock().unwrap().insert(id, (entered, release_rx));
    (entered_rx, release)
}

async fn wait_for_paused_reads(driver: &mut Driver, entered: &mut FuturesUnordered<tokio::sync::oneshot::Receiver<()>>, count: usize) {
    let all_entered = async {
        for _ in 0..count {
            entered.next().await.unwrap().expect("state read barrier remains registered");
        }
    };
    tokio::select! {
        _ = driver.jobs.next() => panic!("an application passed its held read"),
        reached = tokio::time::timeout(std::time::Duration::from_secs(2), all_entered) => {
            reached.expect("active applications reach their pre-commit read barriers");
        }
    }
    // Give every remaining runnable future an opportunity to attempt admission;
    // assertions below count actual paused reads, independent of which IDs won.
    assert!(driver.jobs.next().now_or_never().is_none());
}

#[tokio::test]
async fn active_application_window_does_not_start_the_next_storage_read_until_a_slot_releases() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    node.system.wait_loaded().await?;
    let mut entered = FuturesUnordered::new();
    let mut releases = Vec::new();
    for seed in 0..=128 {
        let event = application_genesis(seed);
        let (reached, release) = pause_state_read(&node, event.entity_id);
        entered.push(reached);
        releases.push(release);
        driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event), None)?;
    }
    wait_for_paused_reads(&mut driver, &mut entered, 128).await;
    assert_eq!(node.storage.hold_state_reads.lock().unwrap().len(), 1, "exactly128 of129 round applications may start reads");
    for release in releases {
        release.send(()).unwrap();
    }
    let mut completed = 0;
    while let Some(applied) = driver.jobs.next().await {
        let applied = applied?;
        assert!(applied.change.is_some());
        drop(driver.observe(&node, applied).await?);
        completed += 1;
    }
    assert_eq!(completed, 129);
    assert_eq!(driver.application_jobs, 0);
    assert_eq!(driver.application_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn incomplete_chunk_inputs_do_not_occupy_live_application_slots() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    node.system.wait_loaded().await?;
    let mut partial_ids = HashSet::new();
    for seed in 0..128 {
        let event = application_genesis(seed);
        assert!(partial_ids.insert(event.entity_id));
        driver.chunk(
            &node,
            id(90),
            &[crate::policy::DEFAULT_CONTEXT],
            Owner::Round(0),
            proto::Clock::singleton(&event),
            None,
            proto::EntityDeltaChunk { entity_id: event.entity_id, offset: 0, bytes: vec![1].into(), last: false },
        )?;
    }
    let ready_round = application_genesis(128);
    let ready_round_id = ready_round.entity_id;
    let live = application_genesis(129);
    let live_id = live.entity_id;
    assert!(!partial_ids.contains(&ready_round_id));
    assert!(!partial_ids.contains(&live_id));
    assert_ne!(ready_round_id, live_id);
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(ready_round), None)?;
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Live(11), bridge(live), None)?;
    let mut finished = HashSet::new();
    for _ in 0..2 {
        let applied = tokio::time::timeout(std::time::Duration::from_secs(2), driver.jobs.next()).await?.unwrap()?;
        assert!(applied.head.is_some() && applied.change.is_some(), "both ready payloads actually apply");
        assert!(applied.id == ready_round_id || applied.id == live_id);
        finished.insert(applied.id);
        drop(driver.observe(&node, applied).await?);
    }
    assert_eq!(
        finished,
        [ready_round_id, live_id].into_iter().collect(),
        "incomplete round input cannot occupy either a ready round's slot or live admission"
    );
    assert_eq!(driver.assemblies.len(), 128);
    driver.cancel_round(&node, id(90), &[])?;
    while let Some(applied) = driver.jobs.next().await {
        let applied = applied?;
        assert!(applied.head.is_none(), "cancelled incomplete input never applies");
        drop(driver.observe(&node, applied).await?);
    }
    assert_eq!(driver.application_jobs, 0);
    assert_eq!(driver.application_bytes, 0);
    assert!(driver.assemblies.is_empty());
    Ok(())
}

#[tokio::test]
async fn application_lanes_reserve_live_and_repair_progress_while_round_reads_are_stalled() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    node.system.wait_loaded().await?;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let repair_entity = NodeApplier::apply_delta(
        &node,
        &id(90),
        bridge(application_genesis(201)),
        Delivery::Initial,
        &CachedEventGetter::new(&node, &credentials),
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?
    .entity
    .unwrap();
    let repair = edit(repair_entity.id(), repair_entity.head(), 92);
    let repair_id = repair.entity_id;
    let repair_head = proto::Clock::singleton(&repair);
    let mut holds = Vec::new();
    let mut entered_reads = FuturesUnordered::new();
    let mut round_ids = HashSet::new();
    for seed in 0..=128 {
        let event = application_genesis(seed);
        assert_ne!(event.entity_id, repair_id);
        assert!(round_ids.insert(event.entity_id));
        let (entered, release) = pause_state_read(&node, event.entity_id);
        entered_reads.push(entered);
        holds.push(release);
        driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event), None)?;
    }
    wait_for_paused_reads(&mut driver, &mut entered_reads, 128).await;
    assert_eq!(node.storage.hold_state_reads.lock().unwrap().len(), 1, "one of129 round inputs remains queued");
    let live = application_genesis(200);
    let live_id = live.entity_id;
    assert!(!round_ids.contains(&live_id));
    assert_ne!(live_id, repair_id);
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Live(11), bridge(live), None)?;
    driver.publication(11)?.goals.add(repair_id, repair_head.clone(), false)?;
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Repair, bridge(repair), None)?;
    let mut finished = HashSet::new();
    for _ in 0..2 {
        let applied = tokio::time::timeout(std::time::Duration::from_secs(2), driver.jobs.next())
            .await
            .expect("round read stalls must not exhaust unrelated live/repair admission")
            .unwrap()?;
        assert!(applied.id == live_id || applied.id == repair_id);
        if applied.id == repair_id {
            assert_eq!(applied.head, Some(repair_head.clone()));
        }
        finished.insert(applied.id);
        drop(driver.observe(&node, applied).await?);
    }
    assert_eq!(finished, [live_id, repair_id].into_iter().collect());
    assert!(driver.publication(11)?.goals.covered(), "the repair actually covers its waiting target");
    assert_eq!(driver.round_jobs, 129, "no blocked round application completed to make this progress possible");
    for release in holds {
        release.send(()).unwrap();
    }
    while let Some(applied) = driver.jobs.next().await {
        drop(driver.observe(&node, applied?).await?);
    }
    assert_eq!(driver.application_jobs, 0);
    assert_eq!(driver.application_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn application_lanes_limit_live_and_repair_to_sixteen_active_reads() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    node.system.wait_loaded().await?;
    let mut entered = FuturesUnordered::new();
    let mut releases = Vec::new();
    for seed in 0..=16 {
        let event = application_genesis(seed);
        let (reached, release) = pause_state_read(&node, event.entity_id);
        entered.push(reached);
        releases.push(release);
        let owner = if seed % 2 == 0 { Owner::Live(11) } else { Owner::Repair };
        driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], owner, bridge(event), None)?;
    }
    wait_for_paused_reads(&mut driver, &mut entered, 16).await;
    assert_eq!(node.storage.hold_state_reads.lock().unwrap().len(), 1, "exactly16 of17 live/repair applications may start reads");
    for release in releases {
        release.send(()).unwrap();
    }
    let mut completed = 0;
    while let Some(applied) = driver.jobs.next().await {
        let applied = applied?;
        assert!(applied.change.is_some());
        drop(driver.observe(&node, applied).await?);
        completed += 1;
    }
    assert_eq!(completed, 17);
    assert_eq!(driver.application_jobs, 0);
    assert_eq!(driver.application_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn finite_round_finishes_while_newer_publication_target_remains_uncovered() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.goals.add(id(1), head(1), true)?;
    driver.projection.round.as_mut().unwrap().complete = true;
    let mut goals = Goals::default();
    goals.add(id(1), head(2), false)?;
    driver.publications.push_back(Publication {
        revision: 11,
        changes: [(id(1), Some(member(2)))].into_iter().collect(),
        applied: BTreeMap::new(),
        retained_bytes: 0,
        change_bytes: 0,
        goals,
        jobs: 0,
        complete: true,
    });
    assert!(driver.complete_round(&node, None).await?, "a later hot target must not move the captured round goal");
    assert!(driver.projection.round.is_none());
    assert_eq!(driver.projection.members[&id(1)].head, head(1), "an uncovered later decision is not publicly committed");
    assert!(!driver.publications[0].goals.covered(), "the round does not certify the later target");
    assert_eq!(driver.applied_through, None, "round readiness is not a publication coverage receipt");
    Ok(())
}

#[tokio::test]
async fn finite_round_waits_its_pre_cut_publications_but_not_later_data() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![state(1)]).await?;
    driver.projection.round.as_mut().unwrap().descriptor.revision = 20;
    driver.projection.round.as_mut().unwrap().complete = true;
    let mut goals = Goals::default();
    goals.add(id(2), head(2), false)?;
    driver.publications.push_back(Publication {
        revision: 15,
        changes: IndexMap::new(),
        applied: BTreeMap::new(),
        retained_bytes: 0,
        change_bytes: 0,
        goals,
        jobs: 0,
        complete: true,
    });
    assert!(!driver.complete_round(&node, None).await?, "pre-cut application/notification is part of the finite boundary");
    driver.publications[0].goals.values_mut().for_each(|goal| goal.covered = true);
    driver.commit_publications(&node, None).await?;
    assert_eq!(driver.applied_through, Some(15));
    assert!(driver.projection.round.as_ref().unwrap().absent_ids().next().is_none(), "pre-cut publications do not overlay R");
    assert!(driver.complete_round(&node, None).await?);
    assert_eq!(driver.projection.revision, 20);
    Ok(())
}

async fn local_query(node: &TestNode) -> anyhow::Result<crate::livequery::EntityLiveQuery> {
    use crate::{
        context::Context,
        livequery::{EntityLiveQuery, QueryResolution},
        node::CachePolicy,
    };
    let query = EntityLiveQuery::new(
        Context::new(node.clone(), crate::policy::DEFAULT_CONTEXT).0,
        CachePolicy::Local,
        QueryResolution::Resolved(ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }),
    )?;
    tokio::time::timeout(std::time::Duration::from_secs(2), query.wait_initialized()).await??;
    Ok(query)
}

async fn bind_query_driver(
    node: &TestNode,
    entity: &crate::entity::Entity,
    query: crate::livequery::EntityLiveQuery,
) -> anyhow::Result<(Driver, Arc<super::super::super::IncomingStates>, crate::livequery::EntityLiveQuery)> {
    use crate::peer_subscription::RemoteQuerySubscriber;
    let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True);
    let snapshot = super::super::super::QueryTree::default()
        .refresh(spec, vec![proto::Attested::opt(proto::EntityState { entity_id: entity.id(), state: entity.to_state()? }, None)])
        .await?;
    let registry = Arc::new(super::super::super::IncomingStates::default());
    let stream = registry.register(proto::RequestId::new(), Some((query.query_id(), 1)), None)?;
    query.weak().stream_bound(1, stream.request.clone(), Arc::new(|| false));
    let mut driver = Driver::new(snapshot, proto::StateRound { number: 0, revision: 10 }, stream, proto::StateStreamId::new())?;
    driver.prepare(node, Some((query.query_id(), 1))).await?;
    Ok((driver, registry, query))
}

async fn query_driver(
    node: &TestNode,
    entity: &crate::entity::Entity,
) -> anyhow::Result<(Driver, Arc<super::super::super::IncomingStates>, crate::livequery::EntityLiveQuery)> {
    bind_query_driver(node, entity, local_query(node).await?).await
}
#[tokio::test]
async fn live_present_for_known_but_ineligible_row_requires_an_install() -> anyhow::Result<()> {
    let (node, _, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let (mut driver, _registry, query) = query_driver(&node, &entity).await?;
    query.resultset().clear();
    driver.eligible.clear();
    query.set_remote_candidates(1, &driver.stream.request, []);
    let owner = Some((query.query_id(), 1));
    assert!(driver.eligible.is_empty());
    driver
        .live(
            &node,
            id(90),
            &[],
            11,
            vec![proto::QueryChange {
                entity_id: entity.id(),
                member: Some(proto::QueryMember { key: vec![], head: entity.head() }),
                delta: None,
            }],
            true,
        )
        .await?;
    assert!(!driver.eligible.contains(&entity.id()), "metadata receipt alone cannot publish membership");
    assert_eq!(driver.projection.revision, 10);
    driver.commit_publications(&node, owner).await?;
    assert!(driver.eligible.contains(&entity.id()));
    assert!(
        query.resultset().contains_key(&entity.id()),
        "new eligibility must install the candidate window even when the captured key is unchanged"
    );
    assert_eq!(driver.projection.revision, 11);
    assert_eq!(driver.applied_through, Some(11));
    Ok(())
}

#[tokio::test]
async fn actual_owner_publication_retires_round_update_but_metadata_only_does_not() -> anyhow::Result<()> {
    for actual_live in [true, false] {
        let (node, _, _registry) = fixture(vec![]).await?;
        let (_, entity) = committed_entity(&node).await?;
        let (mut driver, _registry, query) = query_driver(&node, &entity).await?;
        let owner = Some((query.query_id(), 1));
        let updates = Arc::new(std::sync::Mutex::new(Vec::<crate::reactor::ReactorUpdate>::new()));
        let recorded = updates.clone();
        let _guard = query.listen_updates(move |update| recorded.lock().unwrap().push(update));
        let initial = edit(entity.id(), entity.head(), 92);
        driver.projection.round.as_mut().unwrap().members.get_mut(&entity.id()).unwrap().head = proto::Clock::singleton(&initial);
        driver.goals.add(entity.id(), proto::Clock::singleton(&initial), false)?;
        driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(initial), None)?;
        let applied = driver.jobs.next().await.unwrap()?;
        driver.observe(&node, applied).await?;
        assert!(driver.round_changed.contains(&entity.id()));
        let event = edit(entity.id(), entity.head(), 93);
        let target = if actual_live { proto::Clock::singleton(&event) } else { entity.head() };
        driver
            .live(
                &node,
                id(90),
                &[crate::policy::DEFAULT_CONTEXT],
                11,
                vec![proto::QueryChange {
                    entity_id: entity.id(),
                    member: Some(proto::QueryMember { key: vec![], head: target }),
                    delta: actual_live.then(|| bridge(event.clone()).content),
                }],
                true,
            )
            .await?;
        if actual_live {
            let applied = driver.jobs.next().await.unwrap()?;
            let deferred = driver.deferred_notification(applied.owner, applied.id);
            let change = driver.observe(&node, applied).await?.unwrap();
            notify_changes(
                &node,
                Some(crate::reactor::QueryOrigin { query: query.query_id(), version: 1, request: driver.stream.request.clone() }),
                vec![(change, deferred)],
            )
            .await?;
        }
        assert!(updates.lock().unwrap().is_empty(), "raw data application is not an owner publication");
        driver.commit_publications(&node, owner).await?;
        assert_eq!(driver.round_changed.contains(&entity.id()), !actual_live);
        assert_eq!(updates.lock().unwrap().len(), usize::from(actual_live));
        driver.projection.round.as_mut().unwrap().complete = true;
        assert!(driver.complete_round(&node, owner).await?);
        let updates = updates.lock().unwrap();
        assert_eq!(
            updates.len(),
            1,
            "finite completion cannot duplicate a real prior owner update, but still reports a metadata-only no-op's pending state change"
        );
        assert_eq!(updates[0].items.len(), 1);
        assert!(updates[0].items[0].predicate_relevance.is_empty());
        assert_eq!(updates[0].items[0].events.len(), usize::from(actual_live));
        if actual_live {
            assert_eq!(updates[0].items[0].events[0].payload.id(), event.id());
        }
    }
    Ok(())
}

#[tokio::test]
async fn query_round_pins_applied_residents_through_install_without_storage_reload() -> anyhow::Result<()> {
    let (node, _, _registry) = fixture(vec![]).await?;
    let query = local_query(&node).await?;
    let (_, entity) = committed_entity(&node).await?;
    let entity_id = entity.id();
    let (mut driver, _registry, query) = bind_query_driver(&node, &entity, query).await?;
    query.resultset().clear();
    driver.eligible.clear();
    let event = edit(entity_id, entity.head(), 92);
    driver.projection.round.as_mut().unwrap().members.get_mut(&entity_id).unwrap().head = proto::Clock::singleton(&event);
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event), None)?;
    drop(entity);
    let applied = driver.jobs.next().await.unwrap()?;
    drop(driver.observe(&node, applied).await?);
    assert_eq!(driver.round_pins.len(), 1);
    let resident = node.entities.get(&entity_id).expect("the finite round owns this resident");
    assert_eq!(driver.round_pin_bytes, bincode::serialized_size(&resident.to_state()?)? as usize + proto::EntityId::BYTE_LEN);
    drop(resident);
    *node.storage.reject_state_read.lock().unwrap() = Some(entity_id);
    driver.projection.round.as_mut().unwrap().complete = true;
    assert!(driver.complete_round(&node, Some((query.query_id(), 1))).await?);
    assert!(query.resultset().contains_key(&entity_id));
    assert!(driver.round_pins.is_empty());
    assert_eq!(driver.round_pin_bytes, 0);
    drop(query);
    assert!(node.entities.get(&entity_id).is_none(), "round completion leaves no hidden residency");
    Ok(())
}

#[tokio::test]
async fn query_round_skips_warm_residents_and_releases_cancelled_pins() -> anyhow::Result<()> {
    let (node, _, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let (mut warm_driver, _registry, warm_query) = query_driver(&node, &entity).await?;
    warm_driver.pin_round_entity(&node, &entity);
    assert!(warm_driver.round_pins.is_empty(), "warm query members already keep their resident alive");
    drop(warm_driver);
    drop(warm_query);
    drop(entity);

    let query = local_query(&node).await?;
    // Use a new row so the query's original local result cannot own the
    // resident whose cancellation lifetime is being tested.
    let genesis = proto::Event::genesis(
        Some(id(89)),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(id(88))))]),
    );
    let entity_id = genesis.entity_id;
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let entity = NodeApplier::apply_delta(
        &node,
        &id(90),
        bridge(genesis),
        Delivery::Initial,
        &CachedEventGetter::new(&node, &credentials),
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?
    .entity
    .unwrap();
    let (mut driver, _registry, query) = bind_query_driver(&node, &entity, query).await?;
    let event = edit(entity_id, entity.head(), 92);
    driver.enqueue(&node, id(90), &credentials, Owner::Round(0), bridge(event), None)?;
    drop(entity);
    let applied = driver.jobs.next().await.unwrap()?;
    drop(driver.observe(&node, applied).await?);
    assert!(driver.round_pins.contains_key(&entity_id));
    driver.cancel_round(&node, id(90), &[])?;
    assert!(driver.round_pins.is_empty());
    assert_eq!(driver.round_pin_bytes, 0);
    assert!(node.entities.get(&entity_id).is_none());
    drop(query);
    Ok(())
}

#[tokio::test]
async fn round_pin_refresh_evicts_large_merged_resident_and_query_still_installs() -> anyhow::Result<()> {
    use crate::{
        property::backend::{LWWBackend, PropertyBackend},
        value::Value,
    };
    let (node, _, _registry) = fixture(vec![]).await?;
    let query = local_query(&node).await?;
    let (_, entity) = committed_entity(&node).await?;
    let entity_id = entity.id();
    let (mut driver, _registry, query) = bind_query_driver(&node, &entity, query).await?;
    query.resultset().clear();
    driver.eligible.clear();
    let initial = edit(entity_id, entity.head(), 92);
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(initial), None)?;
    let applied = driver.jobs.next().await.unwrap()?;
    drop(driver.observe(&node, applied).await?);
    assert!(driver.round_pins.contains_key(&entity_id));

    let backend = LWWBackend::new();
    backend.set(proto::PropertyId::EntityId(id(94)), Some(Value::String("x".repeat(super::super::super::WINDOW_BYTES))));
    let large = proto::Event::update(
        entity_id,
        entity.head(),
        proto::AuthorId::Unknown,
        proto::OperationSet(vec![proto::Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() }]),
    );
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    // An independent application grows the canonical resident. The next tiny
    // round delta must be sized by the merged state, not its own wire bytes.
    NodeApplier::apply_delta(
        &node,
        &id(90),
        bridge(large),
        Delivery::Initial,
        &CachedEventGetter::new(&node, &credentials),
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    let tiny = edit(entity_id, entity.head(), 95);
    let target = proto::Clock::singleton(&tiny);
    driver.projection.round.as_mut().unwrap().members.get_mut(&entity_id).unwrap().head = target.clone();
    driver.goals.add(entity_id, target.clone(), false)?;
    let tiny = bridge(tiny);
    assert!(bincode::serialized_size(&tiny)? < 1024);
    driver.enqueue(&node, id(90), &credentials, Owner::Round(0), tiny, None)?;
    let applied = driver.jobs.next().await.unwrap()?;
    drop(driver.observe(&node, applied).await?);
    assert!(driver.round_pins.is_empty(), "the actual large resident exceeds the optional admission budget");
    assert_eq!(driver.round_pin_bytes, 0);
    drop(entity);
    assert!(node.entities.get(&entity_id).is_none(), "fallback permits the resident to expire");
    driver.projection.round.as_mut().unwrap().complete = true;
    assert!(driver.complete_round(&node, Some((query.query_id(), 1))).await?);
    assert_eq!(query.resultset().read().iter_entities().find(|(id, _)| *id == entity_id).unwrap().1.head(), target);
    assert!(driver.round_pins.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_round_does_not_retain_or_reload_changes_for_a_nonexistent_query_owner() -> anyhow::Result<()> {
    let (node, mut driver, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let entity_id = entity.id();
    let event = edit(entity_id, entity.head(), 92);
    let target = proto::Clock::singleton(&event);
    driver.projection.round.as_mut().unwrap().members.insert(entity_id, proto::QueryMember { key: vec![], head: target.clone() });
    driver.goals.add(entity_id, target, false)?;
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event), None)?;
    drop(entity);
    let applied = driver.jobs.next().await.unwrap()?;
    drop(driver.observe(&node, applied).await?);
    assert!(driver.round_changed.is_empty());
    assert!(driver.round_pins.is_empty());
    assert!(node.entities.get(&entity_id).is_none());
    *node.storage.reject_state_read.lock().unwrap() = Some(entity_id);
    driver.projection.round.as_mut().unwrap().complete = true;
    assert!(driver.complete_round(&node, None).await?, "Fetch completion has no owner callback to reload");
    assert_eq!(driver.ids(), [entity_id]);
    Ok(())
}

#[tokio::test]
async fn round_pins_retire_only_after_actual_owner_state_notification() -> anyhow::Result<()> {
    for actual_live in [true, false] {
        let (node, _, _registry) = fixture(vec![]).await?;
        let query = local_query(&node).await?;
        let (_, entity) = committed_entity(&node).await?;
        let entity_id = entity.id();
        let (mut driver, _registry, query) = bind_query_driver(&node, &entity, query).await?;
        let owner = Some((query.query_id(), 1));
        let updates = Arc::new(std::sync::Mutex::new(0));
        let recorded = updates.clone();
        let _guard = query.listen_updates(move |_| *recorded.lock().unwrap() += 1);
        let initial = edit(entity_id, entity.head(), 92);
        driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(initial), None)?;
        let applied = driver.jobs.next().await.unwrap()?;
        drop(driver.observe(&node, applied).await?);
        assert!(driver.round_pins.contains_key(&entity_id));
        let event = edit(entity_id, entity.head(), 93);
        let target = if actual_live { proto::Clock::singleton(&event) } else { entity.head() };
        driver
            .live(
                &node,
                id(90),
                &[crate::policy::DEFAULT_CONTEXT],
                11,
                vec![proto::QueryChange {
                    entity_id,
                    member: Some(proto::QueryMember { key: vec![], head: target }),
                    delta: actual_live.then(|| bridge(event).content),
                }],
                true,
            )
            .await?;
        if actual_live {
            let applied = driver.jobs.next().await.unwrap()?;
            drop(driver.observe(&node, applied).await?);
        }
        driver.commit_publications(&node, owner).await?;
        assert_eq!(driver.round_changed.contains(&entity_id), !actual_live);
        assert_eq!(driver.round_pins.contains_key(&entity_id), !actual_live);
        let before = *updates.lock().unwrap();
        driver.projection.round.as_mut().unwrap().complete = true;
        assert!(driver.complete_round(&node, owner).await?);
        assert_eq!(
            *updates.lock().unwrap(),
            before + usize::from(!actual_live),
            "a metadata-only publication cannot retire the pending finite state update"
        );
        assert!(driver.round_pins.is_empty());
    }
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
        changes: IndexMap::new(),
        applied: BTreeMap::new(),
        retained_bytes: 0,
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
async fn initial_state_advance_is_retained_for_finite_owner_notification_without_historical_events() -> anyhow::Result<()> {
    let (node, _, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let (mut driver, _registry, query) = query_driver(&node, &entity).await?;
    let event = edit(entity.id(), entity.head(), 92);
    driver.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event), None)?;
    let applied = driver.jobs.next().await.unwrap()?;
    let change = applied.change.as_ref().expect("a real initial delivery advances state");
    assert!(change.events().is_empty(), "historical repair is not a new live event batch");
    driver.observe(&node, applied).await?;
    assert_eq!(driver.round_changed, [entity.id()].into_iter().collect());
    driver.check_retained()?;
    driver.projection.round.as_mut().unwrap().complete = true;
    assert!(driver.complete_round(&node, Some((query.query_id(), 1))).await?);
    assert!(driver.round_changed.is_empty(), "finite retirement releases changed identities");
    Ok(())
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
            vec![],
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
    notify_changes(&node, None, changes.into_iter().map(|change| (change, true)).collect()).await?;
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

#[tokio::test]
async fn snapshot_on_one_stream_does_not_consume_live_events_on_another() -> anyhow::Result<()> {
    use crate::retrieval::SuspenseEvents;
    let (node, mut initial, _initial_registry) = fixture(vec![]).await?;
    let (_, mut live, _live_registry) = fixture(vec![]).await?;
    let (genesis, entity) = committed_entity(&node).await?;
    let query = local_query(&node).await?;
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = events.clone();
    let _listener = query.listen_updates(move |update| {
        recorded.lock().unwrap().extend(update.items.iter().flat_map(|item| item.events.iter().map(|event| event.payload.id())));
    });
    let event = edit(entity.id(), entity.head(), 92);
    let getter = LocalEventGetter::new(node.storage.clone(), false);
    getter.stage_event(event.clone());
    let fork = crate::entity::RemoteTrxEntity::edit(&entity)?;
    fork.apply_events(&getter, &mut [proto::Attested::opt(event.clone(), None)], |_| Ok(None)).await?;
    let newer = fork.to_state()?;
    initial.enqueue(
        &node,
        id(90),
        &[crate::policy::DEFAULT_CONTEXT],
        Owner::Round(0),
        proto::EntityDelta {
            entity_id: entity.id(),
            content: proto::DeltaContent::StateAndEvents {
                state: proto::StateFragment { state: newer, attestations: Default::default() },
                events: vec![proto::Attested::from(event.clone()).into()],
            },
        },
        None,
    )?;
    let adopted = initial.jobs.next().await.unwrap()?;
    assert!(adopted.change.as_ref().unwrap().events().is_empty());
    assert_eq!(entity.head(), proto::Clock::singleton(&event));
    live.enqueue(&node, id(91), &[crate::policy::DEFAULT_CONTEXT], Owner::Live(11), bridge(event.clone()), None)?;
    let delivered = live.jobs.next().await.unwrap()?;
    let change = delivered.change.unwrap();
    assert_eq!(change.events().iter().map(|event| event.payload.id()).collect::<Vec<_>>(), [event.id()]);
    assert!(node.reactor.notify_change(vec![change]).await.is_empty());
    // Another source can carry the same live event after the first source.
    let (_, mut second_live, _second_registry) = fixture(vec![]).await?;
    second_live.enqueue(&node, id(92), &[crate::policy::DEFAULT_CONTEXT], Owner::Live(11), bridge(event.clone()), None)?;
    let replay = second_live.jobs.next().await.unwrap()?;
    assert!(node.reactor.notify_change(vec![replay.change.unwrap()]).await.is_empty());
    // A pre-subscription ancestor is also covered by this query's event frontier.
    assert!(node.reactor.notify_change(vec![EntityChange::new(entity, vec![proto::Attested::from(genesis)])]).await.is_empty());
    assert_eq!(*events.lock().unwrap(), [event.id()], "live delivery is independent of data adoption and deduplicated across sources");
    Ok(())
}

#[tokio::test]
async fn first_peer_install_uses_captured_head_for_event_baseline_not_newer_entity_state() -> anyhow::Result<()> {
    use crate::{context::Context, reactor::fetch_gap::QueryGapFetcher};
    use ankurah_signals::Subscribe;
    let (node, mut initial, _registry) = fixture(vec![]).await?;
    let (_, entity) = committed_entity(&node).await?;
    let baseline = entity.head();
    let event = edit(entity.id(), baseline.clone(), 92);
    initial.enqueue(&node, id(90), &[crate::policy::DEFAULT_CONTEXT], Owner::Round(0), bridge(event.clone()), None)?;
    initial.jobs.next().await.unwrap()?;
    let subscription = node.reactor.subscribe();
    let query = proto::QueryId::new();
    let context = Context::new(node.clone(), crate::policy::DEFAULT_CONTEXT);
    node.reactor
        .upsert_query_from_peer(
            subscription.id(),
            query,
            ankql::ast::Selection::from(ankql::ast::Predicate::True),
            context.0.clone(),
            crate::resultset::EntityResultSet::empty(),
            Arc::new(QueryGapFetcher::new(context)),
            1,
            (),
            vec![entity.clone()],
            crate::reactor::PeerQueryMembership { baseline: [(entity.id(), baseline)].into_iter().collect(), ..Default::default() },
            vec![],
        )
        .await?;
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = received.clone();
    let _guard = subscription.subscribe(move |update: crate::reactor::ReactorUpdate| {
        recorded.lock().unwrap().extend(update.items.into_iter().flat_map(|item| item.events.into_iter().map(|event| event.payload.id())));
    });
    assert!(node.reactor.notify_change(vec![EntityChange::new(entity, vec![proto::Attested::from(event.clone())])]).await.is_empty());
    assert_eq!(*received.lock().unwrap(), [event.id()]);
    Ok(())
}

#[tokio::test]
async fn missing_notification_history_fails_the_query_without_claiming_the_committed_state_failed() -> anyhow::Result<()> {
    let (node, _, _registry) = fixture(vec![]).await?;
    node.system.wait_loaded().await?;
    let genesis = application_genesis(55);
    let unknown = proto::Clock::new(vec![(2, proto::EventId::from_bytes([99; 32]))])?;
    let state = proto::State { head: unknown.clone(), memberships: [proto::ModelId::EntityId(id(91))].into(), ..Default::default() };
    let credentials = vec![crate::policy::DEFAULT_CONTEXT];
    let outcome = NodeApplier::apply_delta(
        &node,
        &id(90),
        proto::EntityDelta {
            entity_id: genesis.entity_id,
            content: proto::DeltaContent::StateSnapshot { state: proto::StateFragment { state, attestations: Default::default() } },
        },
        Delivery::Initial,
        &CachedEventGetter::new(&node, &credentials),
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    let entity = outcome.entity.unwrap();
    let query = local_query(&node).await?;
    assert!(query.resultset().is_loaded());
    let failures = node.reactor.notify_change(vec![EntityChange::new(entity.clone(), vec![proto::Attested::from(genesis)])]).await;
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].query, Some(query.query_id()));
    node.live_queries.fail_notifications(&failures);
    assert!(!query.resultset().is_loaded());
    assert!(query.error().value().is_some());
    assert_eq!(entity.head(), unknown, "notification failure cannot undo or misreport an already committed entity");
    Ok(())
}
