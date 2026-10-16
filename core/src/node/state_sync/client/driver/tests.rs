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
