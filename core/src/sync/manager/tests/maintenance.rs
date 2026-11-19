use super::*;

#[tokio::test]
async fn missing_predicate_values_are_nonmatches_during_opening_and_maintenance() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(4)?;
    let present = version(1, 10, true);
    let mut missing = version(2, 20, true);
    missing.active = None;
    fixture.seed(&[present.clone(), missing.clone()])?;
    let tree = fixture.manager.open(selection(true, None, false)).await?;
    assert_eq!(ids(&tree.view()).await?, vec![present.id]);

    let mut removed_property = present.clone();
    removed_property.active = None;
    let mut added_property = missing.clone();
    added_property.active = Some(true);
    fixture.commit(&[(Some(present), Some(removed_property)), (Some(missing), Some(added_property.clone()))]).await?;
    assert_eq!(ids(&tree.view()).await?, vec![added_property.id]);
    Ok(())
}

#[tokio::test]
async fn missing_values_preserve_not_and_or_predicate_semantics() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(4)?;
    let mut missing = version(1, 10, true);
    missing.active = None;
    let inactive = version(2, 20, false);
    fixture.seed(&[missing.clone(), inactive.clone()])?;
    let active = Predicate::Comparison {
        left: Box::new(Expr::Path(active().into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::Bool(true))),
    };
    let mut selection = selection(false, None, false);
    selection.predicate = Predicate::And(Box::new(Predicate::MemberOf(model())), Box::new(Predicate::Not(Box::new(active.clone()))));
    let tree = fixture.manager.open(selection.clone()).await?;
    assert_eq!(ids(&tree.view()).await?, vec![inactive.id], "NOT UNKNOWN is still not true");

    selection.predicate =
        Predicate::And(Box::new(Predicate::MemberOf(model())), Box::new(Predicate::Or(Box::new(active), Box::new(Predicate::True))));
    let tree = fixture.manager.open(selection).await?;
    assert_eq!(ids(&tree.view()).await?, vec![missing.id, inactive.id], "a true OR branch includes the entity");
    Ok(())
}

async fn maintains_idle_trees<G: DigestGroup>() -> anyhow::Result<()> {
    let fixture = Fixture::<G>::new(4)?;
    let all = selection(false, None, false);
    let active = selection(true, Some(1), false);
    let before = [version(1, 10, true), version(2, 20, true), version(3, 30, false)];
    fixture.seed(&before)?;
    drop(fixture.manager.open(all.clone()).await?);
    drop(fixture.manager.open(active.clone()).await?);
    let all_view = fixture.manager.view(all).await?;
    let mut leaves = before[1].clone();
    leaves.active = Some(false);
    let mut enters = before[2].clone();
    enters.active = Some(true);
    enters.score = 5;
    enters.head = Clock::new(vec![(2, EventId::from_bytes([44; 32]))])?;
    let inserted = version(4, 40, true);
    let changes = [
        (Some(before[0].clone()), None),
        (Some(before[1].clone()), Some(leaves.clone())),
        (Some(before[2].clone()), Some(enters.clone())),
        (None, Some(inserted.clone())),
    ];
    fixture.commit(&changes).await?;

    assert_eq!(ids(&all_view).await?, vec![enters.id, leaves.id, inserted.id]);
    assert_eq!(ids(&fixture.manager.view(active.clone()).await?).await?, vec![enters.id]);
    let resident = fixture.manager.tree(&selection_key(&active)?).unwrap();
    let members: Vec<_> = resident.view().range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, enters.head);
    assert_eq!(resident.audit().await?.members, 1);
    Ok(())
}

#[tokio::test]
async fn maintenance_reaches_idle_trees_and_limited_membership_in_ristretto() -> anyhow::Result<()> {
    maintains_idle_trees::<Ristretto255>().await
}

#[tokio::test]
async fn maintenance_reaches_idle_trees_and_limited_membership_in_lthash() -> anyhow::Result<()> {
    maintains_idle_trees::<LtHash16>().await
}

#[tokio::test]
async fn a_rolled_back_canonical_write_reconciles_to_unchanged_results() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let view = fixture.manager.view(selection(false, None, false)).await?;
    let mut after = before.clone();
    after.head = Clock::genesis(EventId::from_bytes([55; 32]));
    let changes = [(Some(before.clone()), Some(after))];
    {
        let _publication = fixture.publication.lock().await;
        let mut trx = fixture.storage.transaction();
        write_versions(&mut trx, &changes).await?;
        fixture.manager.invalidate();
        drop(trx);
    }
    assert!(view.count(..).await.is_err());
    fixture.manager.maintain().await?;
    let members: Vec<_> = view.range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, before.head);
    assert_eq!(fixture.storage.get_state(before.id).await?.payload.state.head, before.head);
    Ok(())
}

#[tokio::test]
async fn unrelated_and_repeated_states_leave_the_membership_digest_unchanged() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    let member = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&member))?;
    let tree = fixture.manager.open(selection(true, None, false)).await?;
    let before = tree.view().span_digest(..).await?;
    fixture.commit(&[]).await?;
    fixture.commit(&[(None, Some(version(2, 5, false)))]).await?;
    fixture.commit(&[(Some(member.clone()), Some(member))]).await?;
    assert_eq!(tree.view().span_digest(..).await?, before);
    tree.audit().await?;
    Ok(())
}

#[tokio::test]
async fn an_unreadable_tree_does_not_stop_maintenance_of_the_other_trees() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let by_score = selection(false, None, false);
    let mut by_active = by_score.clone();
    by_active.order_by.as_mut().unwrap()[0].path = active().into();
    fixture.manager.open(by_score).await?;
    fixture.manager.open(by_active).await?;

    // Break the first tree's ordering value, whichever order the registry uses.
    // The other selection still has a valid ordering value and must catch up.
    let trees = TreeManager::live(&mut fixture.manager.trees.lock().unwrap());
    let broken_property = trees[0].0.order_by.as_ref().unwrap()[0].path.property_id();
    let broken = trees[0].1.view();
    let healthy = trees[1].1.view();
    let mut values = vec![(score(), Value::I64(10)), (active(), Value::Bool(true))];
    values.iter_mut().find(|(property, _)| *property == broken_property).unwrap().1 = Value::String("wrong type".into());
    let mut after = state(before.id, model(), values)?;
    after.payload.state.head = Clock::genesis(EventId::from_bytes([99; 32]));
    {
        let _publication = fixture.publication.lock().await;
        let mut trx = fixture.storage.transaction();
        trx.set_state(&before.head, &after).await?;
        fixture.manager.invalidate();
        trx.commit().await?.committed()?;
    }
    assert!(fixture.manager.maintain().await.is_err());
    assert!(broken.count(..).await.is_err(), "a failed reconciliation remains invalidated");
    let leaves: Vec<_> = healthy.range(..)?.try_collect().await?;
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].payload.0, after.payload.state.head);
    Ok(())
}
