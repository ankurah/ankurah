use std::{pin::pin, sync::atomic::Ordering, time::Duration};

use ankurah_digest_tree::test_support::poll_once;
use tokio::sync::oneshot;

use super::*;

fn defer_writes(manager: &TreeManager<TestStorage, Ristretto255>) {
    manager.set_write_behind(crate::sync::WriteBehind { max_changes: usize::MAX, max_delay: Duration::from_secs(3600) });
}

fn moved(before: &Version, score: i64) -> Version {
    Version { score, head: Clock::genesis(EventId::from_bytes([score as u8; 32])), ..before.clone() }
}

async fn durable(fixture: &Fixture, tree: &DigestTree<TestStorage, Ristretto255>) -> anyhow::Result<DigestTree<TestStorage, Ristretto255>> {
    Ok(crate::sync::storage_adapter::open(fixture.storage.clone(), fixture.manager.cache.clone(), tree.store().id()).await?)
}

#[tokio::test]
async fn write_behind_flushes_at_its_change_or_delay_boundary() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    fixture.manager.set_write_behind(crate::sync::WriteBehind { max_changes: 2, max_delay: Duration::from_secs(3600) });
    let a = version(1, 10, true);
    fixture.seed(std::slice::from_ref(&a))?;
    let tree = fixture.manager.open(selection(false, None, false)).await?;
    let backing = durable(&fixture, &tree).await?;
    assert!(fixture.manager.flush_after().is_none());
    let b = moved(&a, 20);
    fixture.commit(&[(Some(a), Some(b.clone()))]).await?;
    assert_eq!(backing.view().count(10i64..=10).await?, 1);
    assert!(fixture.manager.flush_after().is_some());
    let c = moved(&b, 30);
    fixture.commit(&[(Some(b), Some(c.clone()))]).await?;
    assert_eq!(backing.view().count(30i64..=30).await?, 1);
    assert!(fixture.manager.flush_after().is_none());

    fixture.manager.set_write_behind(crate::sync::WriteBehind { max_changes: usize::MAX, max_delay: Duration::ZERO });
    let d = moved(&c, 40);
    fixture.commit(&[(Some(c), Some(d))]).await?;
    assert_eq!(backing.view().count(40i64..=40).await?, 1);
    assert!(fixture.manager.flush_after().is_none());
    Ok(())
}

#[tokio::test]
async fn lost_write_behind_recovers_the_durable_old_key_after_restart_or_eviction() -> anyhow::Result<()> {
    for restart in [false, true] {
        let fixture = Fixture::<Ristretto255>::new(0)?;
        defer_writes(&fixture.manager);
        let selection = selection(false, None, false);
        let a = version(1, 10, true);
        let b = moved(&a, 20);
        let c = moved(&b, 30);
        fixture.seed(std::slice::from_ref(&a))?;
        let tree = fixture.manager.open(selection.clone()).await?;
        let backing = durable(&fixture, &tree).await?;
        let id = tree.store().id().clone();
        fixture.commit(&[(Some(a), Some(b.clone()))]).await?;
        assert_eq!(tree.view().count(20i64..=20).await?, 1);
        assert_eq!(backing.view().count(10i64..=10).await?, 1, "write-behind has not persisted the new key");
        let weak = tree.downgrade();
        drop(tree);
        assert!(weak.upgrade().is_none(), "zero idle capacity drops the unflushed overlay");

        let mut trx = fixture.storage.transaction();
        write_versions(&mut trx, &[(Some(b), Some(c.clone()))]).await?;
        trx.commit().await?.committed()?;
        let manager = if restart {
            drop(fixture.manager);
            TreeManager::new(fixture.storage.clone(), fixture.publication, CacheSettings { bytes: 1 << 20 }, 0)
        } else {
            fixture.manager
        };
        let recalled = manager.open(selection).await?;
        assert_eq!(recalled.store().id(), &id);
        assert_eq!(recalled.view().count(10i64..=20).await?, 0, "both superseded keys are absent");
        let leaves: Vec<_> = recalled.view().range(..)?.try_collect().await?;
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].payload.0, c.head);
        assert_eq!(recalled.view().count(30i64..=30).await?, 1);
        assert_eq!(recalled.audit().await?.members, 1);
        manager.flush().await?;
        assert_eq!(backing.view().count(30i64..=30).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn a_second_manager_flush_invalidates_and_recovers_a_stale_overlay() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    defer_writes(&fixture.manager);
    let other = TreeManager::<_, Ristretto255>::new(
        fixture.storage.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
        CacheSettings { bytes: 1 << 20 },
        1,
    );
    defer_writes(&other);
    let selection = selection(false, None, false);
    let a = version(1, 10, true);
    let b = moved(&a, 20);
    let c = moved(&b, 30);
    fixture.seed(std::slice::from_ref(&a))?;
    let first = fixture.manager.open(selection.clone()).await?;
    let second = other.open(selection).await?;
    fixture.commit(&[(Some(a), Some(b.clone()))]).await?;
    assert_eq!(first.view().count(20i64..=20).await?, 1);

    let (entered, captured) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *fixture.storage.hold_state_snapshot.lock().unwrap() = Some((entered, resume));
    let mut flushing = pin!(fixture.manager.flush());
    assert!(poll_once(flushing.as_mut()).is_pending());
    captured.await?;

    let mut trx = fixture.storage.transaction();
    write_versions(&mut trx, &[(Some(b), Some(c.clone()))]).await?;
    trx.commit().await?.committed()?;
    other.flush().await?;
    assert_eq!(second.view().count(30i64..=30).await?, 1);
    assert!(first.view().count(..).await.is_err(), "a stale durable base cannot be combined with the old overlay");

    release.send(()).unwrap();
    flushing.await?;
    assert_eq!(first.view().count(10i64..=20).await?, 0);
    assert_eq!(first.view().count(30i64..=30).await?, 1);
    assert_eq!(first.view().span_digest(..).await?, second.view().span_digest(..).await?);
    let persisted = durable(&fixture, &first).await?;
    assert_eq!(persisted.view().span_digest(..).await?, first.view().span_digest(..).await?);
    assert_eq!(persisted.audit().await?.members, 1);
    Ok(())
}

#[tokio::test]
async fn limit_refill_includes_an_unchanged_candidate_when_the_boundary_moves() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let entities = [version(1, 10, true), version(2, 20, true), version(3, 30, true)];
    fixture.seed(&entities)?;
    let tree = fixture.manager.open(selection(true, Some(2), false)).await?;
    let moved = moved(&entities[0], 40);
    fixture.commit(&[(Some(entities[0].clone()), Some(moved.clone()))]).await?;
    assert_eq!(ids(&tree.view()).await?, vec![entities[1].id, entities[2].id]);
    let mut removed = entities[1].clone();
    removed.active = Some(false);
    removed.head = Clock::genesis(EventId::from_bytes([55; 32]));
    fixture.commit(&[(Some(entities[1].clone()), Some(removed))]).await?;
    assert_eq!(ids(&tree.view()).await?, vec![entities[2].id, moved.id]);
    let leaves: Vec<_> = tree.view().range(..)?.try_collect().await?;
    assert_eq!(leaves[0].payload.0, entities[2].head, "refill retained the unchanged entity's exact head");
    assert_eq!(tree.audit().await?.members, 2);
    Ok(())
}

#[tokio::test]
async fn limited_membership_uses_entity_ids_without_order_and_for_descending_ties() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    fixture.seed(&[version(3, 10, true), version(1, 10, true), version(2, 10, true)])?;
    let mut unordered = selection(false, Some(2), false);
    unordered.order_by = None;
    for selection in [unordered, selection(false, Some(2), true)] {
        let tree = fixture.manager.open(selection).await?;
        assert_eq!(ids(&tree.view()).await?, vec![version(1, 0, false).id, version(2, 0, false).id]);
    }
    Ok(())
}

#[tokio::test]
async fn nested_json_limit_tracks_query_membership_and_refills_with_unchanged_states() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(2)?;
    fixture.storage.set_state(property_state(score(), "score", "json")?);
    let json_state = |id: u8, value: serde_json::Value| state(EntityId::from_bytes([id; 32]), model(), [(score(), Value::Json(value))]);
    let entities = [
        json_state(1, serde_json::json!({"rank": 10}))?,
        json_state(2, serde_json::json!({"rank": 2}))?,
        json_state(3, serde_json::json!({"rank": 30}))?,
        json_state(4, serde_json::json!({}))?,
    ];
    for entity in &entities {
        fixture.storage.set_state(entity.clone());
    }
    let mut selected = selection(false, Some(2), false);
    selected.order_by.as_mut().unwrap()[0].path.subpath.push("rank".into());
    let tree = fixture.manager.open(selected.clone()).await?;
    assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::Json);
    let mut selected_ids = ids(&tree.view()).await?;
    selected_ids.sort();
    assert_eq!(selected_ids, vec![entities[0].payload.entity_id, entities[1].payload.entity_id]);

    let updated = json_state(2, serde_json::json!({"rank": 40}))?;
    let mut trx = fixture.storage.transaction();
    trx.set_state(&entities[1].payload.state.head, &updated).await?;
    fixture.manager.invalidate();
    trx.commit().await?.committed()?;
    fixture.manager.maintain().await?;
    let mut leaves: Vec<_> = tree.view().range(..)?.try_collect().await?;
    leaves.sort_by_key(|leaf| leaf.id.0);
    assert_eq!(leaves.iter().map(|leaf| leaf.id.0).collect::<Vec<_>>(), vec![entities[0].payload.entity_id, entities[2].payload.entity_id]);
    assert_eq!(leaves[0].payload.0, entities[0].payload.state.head);
    assert_eq!(tree.audit().await?.members, 2);

    selected.limit = None;
    let all = fixture.manager.open(selected).await?;
    assert_eq!(all.view().count(..).await?, 4, "a missing JSON subpath remains a member with a null ordering key");
    assert_eq!(all.audit().await?.members, 4);
    Ok(())
}

#[tokio::test]
async fn interrupted_or_rejected_scans_never_publish_partial_changes_or_a_cursor() -> anyhow::Result<()> {
    for reject_finish in [false, true] {
        let fixture = Fixture::<Ristretto255>::new(1)?;
        defer_writes(&fixture.manager);
        let before: Vec<_> = (0..300u16)
            .map(|index| {
                let mut bytes = [211; 32];
                bytes[..2].copy_from_slice(&index.to_be_bytes());
                Version { id: EntityId::from_bytes(bytes), ..version(1, i64::from(index), true) }
            })
            .collect();
        fixture.seed(&before)?;
        let tree = fixture.manager.open(selection(false, None, false)).await?;
        let original_cursor = tree.store().buffered().cursor.clone();
        let backing = durable(&fixture, &tree).await?;
        let changes: Vec<_> = before
            .into_iter()
            .map(|before| {
                let after = moved(&before, before.score + 1000);
                (Some(before), Some(after))
            })
            .collect();
        let mut trx = fixture.storage.transaction();
        write_versions(&mut trx, &changes).await?;
        fixture.manager.invalidate();
        trx.commit().await?.committed()?;

        if reject_finish {
            fixture.storage.reject_state_finish.store(true, Ordering::SeqCst);
            assert!(fixture.manager.maintain().await.is_err());
            fixture.storage.reject_state_finish.store(false, Ordering::SeqCst);
        } else {
            let (entered, paused) = oneshot::channel();
            let (_release, resume) = oneshot::channel();
            *fixture.storage.hold_state_page.lock().unwrap() = Some((1, (entered, resume)));
            {
                let mut maintaining = pin!(fixture.manager.maintain());
                assert!(poll_once(maintaining.as_mut()).is_pending());
                paused.await?;
            }
        }
        assert_eq!(tree.store().buffered().cursor, original_cursor);
        assert!(tree.view().count(..).await.is_err(), "an unfinished scan cannot clear the local invalidation");
        assert_eq!(backing.view().count(0i64..300).await?, 300);
        assert_eq!(backing.view().count(1000i64..1300).await?, 0);
        fixture.manager.maintain().await?;
        assert_ne!(tree.store().buffered().cursor, original_cursor);
        assert_eq!(tree.view().count(0i64..300).await?, 0);
        assert_eq!(tree.view().count(1000i64..1300).await?, 300);
        assert_eq!(tree.audit().await?.members, 300);
    }
    Ok(())
}

#[tokio::test]
async fn an_interrupted_initial_build_resumes_and_reconciles_changed_members() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(0)?;
    let before: Vec<_> = (0..600u16)
        .map(|index| {
            let mut bytes = [211; 32];
            bytes[..2].copy_from_slice(&index.to_be_bytes());
            Version { id: EntityId::from_bytes(bytes), ..version(1, i64::from(index), true) }
        })
        .collect();
    fixture.seed(&before)?;
    let selected = selection(true, None, false);
    let (entered, paused) = oneshot::channel();
    let (_release, resume) = oneshot::channel();
    *fixture.storage.hold_state_page.lock().unwrap() = Some((1, (entered, resume)));
    {
        let mut opening = pin!(fixture.manager.open(selected.clone()));
        assert!(poll_once(opening.as_mut()).is_pending());
        paused.await?; // The first page is durable, but no cursor is published.
    }
    assert!(fixture.manager.trees().is_empty());

    let removed = Version { active: Some(false), ..moved(&before[0], 10000) };
    let changed = moved(&before[1], 10001);
    fixture.seed(&[removed, changed.clone()])?;
    let resumed = TreeManager::<_, Ristretto255>::new(
        fixture.storage.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
        CacheSettings { bytes: 1 << 20 },
        0,
    );
    let tree = resumed.open(selected).await?;
    assert_eq!(tree.audit().await?.members, 599);
    let expected: Vec<_> = before[2..].iter().map(|version| version.id).chain([changed.id]).collect();
    assert_eq!(ids(&tree.view()).await?, expected, "the rescan removes obsolete membership and its previous ordered key");
    let leaves: Vec<_> = tree.view().range(10001i64..=10001)?.try_collect().await?;
    assert_eq!(leaves[0].payload.0, changed.head);
    Ok(())
}
