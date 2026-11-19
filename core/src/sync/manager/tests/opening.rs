use std::pin::pin;

use ankurah_digest_tree::test_support::poll_once;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn exact_selections_share_one_tree_with_limited_membership() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(4)?;
    let selection = selection(true, Some(1), false);
    let entities = [version(1, 30, true), version(2, 10, true), version(3, 5, false)];
    fixture.seed(&entities)?;
    let first = fixture.manager.open(selection.clone()).await?;
    let second = fixture.manager.open(selection.clone()).await?;
    assert!(std::ptr::eq(first.store().inner(), second.store().inner()));
    assert_eq!(fixture.manager.trees().len(), 1);
    let view = fixture.manager.view(selection).await?;
    assert_eq!(view.count(..).await?, 1, "membership includes LIMIT");
    assert_eq!(ids(&view).await?, vec![entities[1].id]);
    Ok(())
}

#[tokio::test]
async fn display_labels_do_not_change_tree_identity_but_predicate_literals_do() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(3)?;
    let entities = [version(1, 10, true), version(2, 20, false)];
    fixture.seed(&entities)?;
    let original = selection(true, None, false);
    let mut relabeled = original.clone();
    let PropertyId::EntityId(score_id) = score() else { unreachable!() };
    let PropertyId::EntityId(active_id) = active() else { unreachable!() };
    relabeled.order_by.as_mut().unwrap()[0].path = ankurah_core_types::PropertyPath::registered(score_id, "renamed_score", vec![]);
    let Predicate::And(_, predicate) = &mut relabeled.predicate else { unreachable!() };
    let Predicate::Comparison { left, .. } = predicate.as_mut() else { unreachable!() };
    *left = Box::new(Expr::Path(ankurah_core_types::PropertyPath::registered(active_id, "renamed_active", vec![])));
    assert_eq!(original, relabeled, "resolved property equality ignores display labels");

    let first = fixture.manager.open(original).await?;
    let second = fixture.manager.open(relabeled.clone()).await?;
    assert!(std::ptr::eq(first.store().inner(), second.store().inner()));
    assert_eq!(first.store().id(), second.store().id());
    assert_eq!(ids(&second.view()).await?, vec![entities[0].id]);

    let Predicate::And(_, predicate) = &mut relabeled.predicate else { unreachable!() };
    let Predicate::Comparison { right, .. } = predicate.as_mut() else { unreachable!() };
    *right = Box::new(Expr::Literal(Value::Bool(false)));
    let different = fixture.manager.open(relabeled).await?;
    assert_ne!(first.store().id(), different.store().id());
    assert_eq!(ids(&different.view()).await?, vec![entities[1].id]);
    assert_eq!(fixture.manager.trees().len(), 2);
    Ok(())
}

#[tokio::test]
async fn exact_limits_and_orders_have_distinct_trees_with_id_ties_ascending() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(4)?;
    fixture.seed(&[version(2, 10, true), version(1, 10, true), version(3, 20, true)])?;
    let mut backing_ids = Vec::new();
    for (limit, descending, expected) in [(Some(1), false, vec![1]), (None, false, vec![1, 2, 3]), (Some(1), true, vec![3])] {
        let tree = fixture.manager.open(selection(false, limit, descending)).await?;
        backing_ids.push(tree.store().id().clone());
        let expected: Vec<_> = expected.into_iter().map(|id| EntityId::from_bytes([id; 32])).collect();
        assert_eq!(ids(&tree.view()).await?, expected);
    }
    assert!(backing_ids.iter().enumerate().all(|(i, id)| backing_ids[..i].iter().all(|other| other != id)));
    assert_eq!(fixture.manager.trees().len(), 3);
    Ok(())
}

#[tokio::test]
async fn an_empty_tree_uses_declared_numeric_type_before_the_first_insert() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let tree = fixture.manager.open(selection(false, None, false)).await?;
    assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::I64);
    assert_eq!(tree.view().count(..).await?, 0);
    let entity = version(1, -12, true);
    fixture.commit(&[(None, Some(entity.clone()))]).await?;
    assert_eq!(ids(&tree.view()).await?, vec![entity.id]);
    assert_eq!(tree.audit().await?.members, 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_opens_of_one_selection_share_the_registered_tree() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    fixture.seed(&[version(1, 10, true)])?;
    let selection = selection(false, None, false);
    let (entered, scanning) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *fixture.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let mut first = pin!(fixture.manager.open(selection.clone()));
    assert!(poll_once(first.as_mut()).is_pending());
    scanning.await?;
    let mut second = pin!(fixture.manager.open(selection));
    assert!(poll_once(second.as_mut()).is_pending());
    release.send(()).unwrap();
    let first = first.await?;
    let second = second.await?;
    assert!(std::ptr::eq(first.store().inner(), second.store().inner()));
    assert_eq!(fixture.manager.trees().len(), 1);
    Ok(())
}

#[tokio::test]
async fn recalling_an_evicted_tree_reconciles_current_entities_under_the_same_id() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let all = selection(false, None, false);
    let active = selection(true, None, false);
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let tree = fixture.manager.open(all.clone()).await?;
    let id = tree.store().id().clone();
    let weak = tree.downgrade();
    drop(tree);
    drop(fixture.manager.open(active).await?);
    assert!(weak.upgrade().is_none(), "LRU eviction ended the old tree's maintenance");
    let mut after = before.clone();
    after.score = 40;
    after.head = Clock::genesis(EventId::from_bytes([45; 32]));
    let added = version(2, 20, false);
    fixture.commit(&[(Some(before), Some(after.clone())), (None, Some(added.clone()))]).await?;
    let recalled = fixture.manager.open(all).await?;
    assert_eq!(recalled.store().id(), &id);
    assert_eq!(ids(&recalled.view()).await?, vec![added.id, after.id]);
    assert_eq!(recalled.audit().await?.members, 2);
    Ok(())
}

#[tokio::test]
async fn a_new_manager_resumes_the_same_backing_tree_from_current_storage() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let selection = selection(false, None, false);
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let tree = fixture.manager.open(selection.clone()).await?;
    let id = tree.store().id().clone();
    drop(tree);
    drop(fixture.manager);
    let mut after = before.clone();
    after.score = 50;
    after.head = Clock::genesis(EventId::from_bytes([46; 32]));
    let mut trx = fixture.storage.transaction();
    write_versions(&mut trx, &[(Some(before), Some(after.clone()))]).await?;
    trx.commit().await?.committed()?;
    let manager = TreeManager::<_, Ristretto255>::new(fixture.storage, fixture.publication, CacheSettings { bytes: 1 << 20 }, 1);
    let reopened = manager.open(selection).await?;
    assert_eq!(reopened.store().id(), &id);
    let members: Vec<_> = reopened.view().range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, after.head);
    assert_eq!(reopened.view().count(50i64..=50).await?, 1);
    reopened.audit().await?;
    Ok(())
}

#[tokio::test]
async fn entity_publication_waits_until_a_scanned_tree_is_registered() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let (entered, scanning) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *fixture.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let mut opening = pin!(fixture.manager.open(selection(false, None, false)));
    assert!(poll_once(opening.as_mut()).is_pending());
    scanning.await?;

    let mut after = before.clone();
    after.score = 20;
    after.head = Clock::genesis(EventId::from_bytes([47; 32]));
    let changes = [(Some(before), Some(after.clone()))];
    let mut committing = pin!(fixture.commit(&changes));
    assert!(poll_once(committing.as_mut()).is_pending(), "publication cannot pass a paused initial scan");
    release.send(()).unwrap();
    let tree = opening.await?;
    committing.await?;
    let members: Vec<_> = tree.view().range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, after.head);
    assert_eq!(tree.view().count(20i64..=20).await?, 1);
    Ok(())
}

#[tokio::test]
async fn opening_waits_for_an_entity_commit_before_scanning() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let (entered, committing) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *fixture.storage.hold_commit.lock().unwrap() = Some((entered, resume));
    let entity = version(1, 10, true);
    let changes = [(None, Some(entity.clone()))];
    let mut commit = pin!(fixture.commit(&changes));
    assert!(poll_once(commit.as_mut()).is_pending());
    committing.await?;
    let mut opening = pin!(fixture.manager.open(selection(false, None, false)));
    assert!(poll_once(opening.as_mut()).is_pending(), "opening waits for canonical state publication");
    release.send(()).unwrap();
    commit.await?;
    let tree = opening.await?;
    assert_eq!(ids(&tree.view()).await?, vec![entity.id]);
    Ok(())
}

#[tokio::test]
async fn a_failed_open_commit_registers_nothing_and_can_be_retried() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let entity = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&entity))?;
    let selection = selection(false, None, false);
    fixture.storage.fail_next_commit();
    assert!(fixture.manager.open(selection.clone()).await.is_err());
    assert!(fixture.manager.trees().is_empty());
    let retried = fixture.manager.open(selection).await?;
    assert_eq!(ids(&retried.view()).await?, vec![entity.id]);
    retried.audit().await?;
    Ok(())
}

#[tokio::test]
async fn cancelling_an_open_before_commit_registers_nothing_and_releases_its_locks() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let entity = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&entity))?;
    let selection = selection(false, None, false);
    let (entered, committing) = oneshot::channel();
    let (_release, resume) = oneshot::channel();
    *fixture.storage.hold_commit.lock().unwrap() = Some((entered, resume));
    {
        let mut opening = pin!(fixture.manager.open(selection.clone()));
        assert!(poll_once(opening.as_mut()).is_pending());
        committing.await?;
    }
    assert!(fixture.manager.trees().is_empty());
    let retried = fixture.manager.open(selection).await?;
    assert_eq!(ids(&retried.view()).await?, vec![entity.id]);
    retried.audit().await?;
    Ok(())
}

#[tokio::test]
async fn id_and_system_ordering_use_their_declared_builtin_types() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    fixture.seed(&[version(2, 10, true), version(1, 20, true)])?;
    let mut by_id = selection(false, None, false);
    by_id.order_by.as_mut().unwrap()[0].path = PropertyId::Id.into();
    let tree = fixture.manager.open(by_id).await?;
    assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::EntityId);
    assert_eq!(ids(&tree.view()).await?, vec![version(1, 0, false).id, version(2, 0, false).id]);

    let by_name = Selection {
        predicate: Predicate::MemberOf(ModelId::System(SystemModel::Property)),
        order_by: Some(vec![OrderByItem { path: PropertyId::System(SystemProperty::Name).into(), direction: OrderDirection::Asc }]),
        limit: None,
    };
    let tree = fixture.manager.open(by_name).await?;
    assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::String);
    let PropertyId::EntityId(active) = active() else { unreachable!() };
    let PropertyId::EntityId(score) = score() else { unreachable!() };
    assert_eq!(ids(&tree.view()).await?, vec![active, score]);
    Ok(())
}

#[tokio::test]
async fn missing_or_invalid_property_metadata_fails_before_registration() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let mut missing = selection(false, None, false);
    missing.order_by.as_mut().unwrap()[0].path = PropertyId::EntityId(EntityId::from_bytes([99; 32])).into();
    assert!(matches!(fixture.manager.open(missing).await, Err(RetrievalError::EntityNotFound(_))));

    fixture.storage.set_state(property_state(score(), "score", "not-a-type")?);
    assert!(matches!(fixture.manager.open(selection(false, None, false)).await, Err(RetrievalError::InvalidState(_))));
    assert!(fixture.manager.trees().is_empty());
    Ok(())
}

#[tokio::test]
async fn opening_after_reset_rebuilds_the_backing_tree_and_preserves_live_views() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let mut selection = selection(false, None, false);
    selection.order_by.as_mut().unwrap()[0].path = PropertyId::Id.into();
    fixture.seed(&[version(1, 10, true)])?;
    let old = fixture.manager.open(selection.clone()).await?;

    {
        let _publication = fixture.publication.lock().await;
        fixture.storage.delete_all().await?;
    }
    let current = version(2, 20, true);
    fixture.seed(std::slice::from_ref(&current))?;
    assert!(old.view().count(..).await.is_err());
    let reused = fixture.manager.open(selection.clone()).await?;
    assert!(std::ptr::eq(old.store().inner(), reused.store().inner()), "recovery preserves the resident handle");

    let refreshed = fixture.manager.refresh(selection.clone()).await?;
    assert_eq!(refreshed.store().id(), old.store().id());
    assert!(refreshed.store().same_tree(old.store()), "the adapter follows the rebuilt backing incarnation");
    assert_eq!(ids(&refreshed.view()).await?, vec![current.id]);
    assert_eq!(ids(&old.view()).await?, vec![current.id], "existing views follow the completed rebuild");
    assert_eq!(fixture.manager.trees().len(), 1);
    assert!(std::ptr::eq(refreshed.store().inner(), fixture.manager.open(selection).await?.store().inner()));

    let inserted = version(3, 30, true);
    fixture.commit(&[(None, Some(inserted.clone()))]).await?;
    assert_eq!(ids(&refreshed.view()).await?, vec![current.id, inserted.id]);
    refreshed.audit().await?;
    Ok(())
}

#[tokio::test]
async fn opening_recovers_a_committed_adapter_refusal_while_rollback_keeps_the_resident_valid() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let selection = selection(false, None, false);
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let old = fixture.manager.open(selection.clone()).await?;
    let mut after = before.clone();
    after.score = 30;
    after.head = Clock::genesis(EventId::from_bytes([61; 32]));
    let changes = [(Some(before.clone()), Some(after.clone()))];

    for commit in [false, true] {
        {
            let _publication = fixture.publication.lock().await;
            let mut trx = fixture.storage.transaction();
            write_versions(&mut trx, &changes).await?;
            let applied = crate::sync::storage_adapter::apply(
                &fixture.storage,
                &mut trx,
                vec![crate::sync::TreeChange::Refused(old.clone(), ankurah_digest_tree::Error::Inconsistent("test refusal"))],
            )
            .await?;
            assert!(matches!(applied.refused(), [(_, ankurah_digest_tree::Error::Inconsistent(_))]));
            assert!(applied.retired().is_empty());
            if commit {
                applied.landed(&trx.commit().await?.committed()?);
            }
        }

        if commit {
            assert!(old.view().count(..).await.is_err(), "only the committed refusal retires the tree");
        } else {
            assert_eq!(old.view().count(10i64..=10).await?, 1, "rollback leaves the original contents readable");
            assert_eq!(fixture.storage.get_state(before.id).await?.payload.state.head, before.head);
        }
    }

    let refreshed = fixture.manager.open(selection).await?;
    assert!(std::ptr::eq(old.store().inner(), refreshed.store().inner()), "the rebuilt incarnation keeps its original shared handle");
    assert_eq!(old.view().count(30i64..=30).await?, 1);
    let members: Vec<_> = refreshed.view().range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, after.head);
    refreshed.audit().await?;
    Ok(())
}
