use super::*;

#[tokio::test]
async fn views_and_streams_keep_evicted_trees_maintained() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let first = selection(false, None, false);
    let second = selection(true, None, false);
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let tree = fixture.manager.open(first.clone()).await?;
    let weak = tree.downgrade();
    let view = fixture.manager.view(first.clone()).await?;
    let cloned = view.clone();
    drop((tree, view));
    drop(fixture.manager.open(second.clone()).await?);
    assert_eq!(fixture.manager.trees().len(), 2, "the cloned view keeps the evicted tree live");

    let mut after = before.clone();
    after.head = Clock::genesis(EventId::from_bytes([55; 32]));
    fixture.commit(&[(Some(before), Some(after.clone()))]).await?;
    let members: Vec<_> = cloned.range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, after.head);

    let stream = cloned.range(..)?;
    drop(cloned);
    let inserted = version(2, 20, true);
    fixture.commit(&[(None, Some(inserted.clone()))]).await?;
    assert_eq!(stream.map_ok(|leaf| leaf.id.0).try_collect::<Vec<_>>().await?, vec![after.id, inserted.id]);
    assert!(weak.upgrade().is_none(), "consuming the stream releases the last owner");
    assert_eq!(fixture.manager.trees().len(), 1);
    assert!(fixture.manager.tree(&selection_key(&first)?).is_none());
    assert!(fixture.manager.tree(&selection_key(&second)?).is_some(), "the most recent tree remains retained");
    Ok(())
}

#[tokio::test]
async fn clones_and_transactions_keep_evicted_trees_maintained() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let tree = fixture.manager.open(selection(false, None, false)).await?;
    let weak = tree.downgrade();
    let cloned = tree.clone();
    drop(tree);
    drop(fixture.manager.open(selection(true, None, false)).await?);
    let entity = version(1, 10, true);
    fixture.commit(&[(None, Some(entity.clone()))]).await?;
    assert_eq!(ids(&cloned.view()).await?, vec![entity.id]);

    let transaction = cloned.transaction();
    drop(cloned);
    let mut after = entity.clone();
    after.head = Clock::genesis(EventId::from_bytes([56; 32]));
    fixture.commit(&[(Some(entity), Some(after.clone()))]).await?;
    let members: Vec<_> = transaction.tree().view().range(..)?.try_collect().await?;
    assert_eq!(members[0].payload.0, after.head);

    let prepared = transaction.prepare().await;
    let inserted = version(2, 20, true);
    fixture.commit(&[(None, Some(inserted.clone()))]).await?;
    assert_eq!(ids(&prepared.tree().view()).await?, vec![after.id, inserted.id]);
    drop(prepared);
    assert!(weak.upgrade().is_none());
    assert_eq!(fixture.manager.trees().len(), 1);
    Ok(())
}

#[tokio::test]
async fn only_requests_refresh_lru_retention() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    let first = selection(false, None, false);
    let second = selection(true, None, false);
    let third = selection(false, Some(1), false);
    drop(fixture.manager.open(first.clone()).await?);
    drop(fixture.manager.open(second.clone()).await?);
    drop(fixture.manager.view(first.clone()).await?);
    assert_eq!(fixture.manager.trees().len(), 2);
    fixture.commit(&[(None, Some(version(1, 10, true)))]).await?;
    drop(fixture.manager.open(third).await?);
    assert!(fixture.manager.tree(&selection_key(&second)?).is_none(), "enumeration and maintenance leave request recency alone");
    assert!(fixture.manager.tree(&selection_key(&first)?).is_some(), "requesting the view refreshed retention");
    Ok(())
}

#[tokio::test]
async fn zero_idle_capacity_still_reuses_a_tree_while_a_caller_owns_it() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let selection = selection(false, None, false);
    let first = fixture.manager.open(selection.clone()).await?;
    let weak = first.downgrade();
    let second = fixture.manager.open(selection.clone()).await?;
    assert!(std::ptr::eq(first.store().inner(), second.store().inner()));
    drop((first, second));
    assert!(weak.upgrade().is_none());
    assert!(fixture.manager.tree(&selection_key(&selection)?).is_none());
    assert!(fixture.manager.trees().is_empty());
    Ok(())
}

#[tokio::test]
async fn opening_new_selections_prunes_expired_weak_entries_without_maintenance() -> anyhow::Result<()> {
    for (capacity, most_entries) in [(0, 1), (1, 2)] {
        let fixture = Fixture::<Ristretto255>::new(capacity)?;
        for limit in 1..=16 {
            drop(fixture.manager.open(selection(false, Some(limit), false)).await?);
            // Do not call trees(): its own pruning would conceal expired
            // entries accumulating during read-only query traffic.
            assert!(fixture.manager.trees.lock().unwrap().len() <= most_entries, "idle capacity {capacity}, request {limit}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn maintenance_keeps_trees_alive_until_commit_lands() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let selection = selection(false, None, false);
    let before = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&before))?;
    let tree = fixture.manager.open(selection.clone()).await?;
    let mut after = before.clone();
    after.score = 20;
    let changes = [(Some(before), Some(after))];
    let _publication = fixture.publication.lock().await;
    let mut trx = fixture.storage.transaction();
    write_versions(&mut trx, &changes).await?;
    let applied = fixture.manager.apply(&mut trx, &changes).await?;
    drop(tree);
    assert!(fixture.manager.tree(&selection_key(&selection)?).is_some());
    applied.landed(&trx.commit().await?.committed()?);
    assert!(fixture.manager.tree(&selection_key(&selection)?).is_none());
    Ok(())
}

#[tokio::test]
async fn a_pre_refresh_view_keeps_maintenance_alive_after_the_refreshed_handle_and_lru_release_it() -> anyhow::Result<()> {
    for capacity in [0, 1] {
        let fixture = Fixture::<Ristretto255>::new(capacity)?;
        let selected = selection(false, None, false);
        let before = version(1, 10, true);
        fixture.seed(std::slice::from_ref(&before))?;
        let old = fixture.manager.open(selected.clone()).await?;
        let weak = old.downgrade();
        let view = old.view();
        drop(old);

        fixture.storage.fail_next_commit();
        assert!(fixture.manager.refresh(selected.clone()).await.is_err());
        assert_eq!(ids(&view).await?, vec![before.id], "a failed refresh leaves the original tree readable");

        drop(fixture.manager.refresh(selected).await?);
        drop(fixture.manager.open(selection(true, None, false)).await?);
        // With no retained handle to the first tree, only this pre-refresh view
        // can keep its registry entry alive. A replacement Inner would lose it.
        let inserted = version(2, 20, false);
        fixture.commit(&[(None, Some(inserted.clone()))]).await?;
        assert_eq!(ids(&view).await?, vec![before.id, inserted.id]);
        assert!(weak.upgrade().is_some());

        drop(view);
        assert!(weak.upgrade().is_none());
        assert_eq!(fixture.manager.trees().len(), capacity);
    }
    Ok(())
}
