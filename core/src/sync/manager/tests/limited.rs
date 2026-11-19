use std::sync::atomic::Ordering;

use super::*;

fn queries(fixture: &Fixture) -> usize { fixture.storage.state_selection_reads.load(Ordering::Relaxed) }

#[tokio::test]
async fn unchanged_heads_and_outsiders_do_not_requery_the_selected_set() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let before = [version(1, 10, true), version(2, 20, true), version(3, 30, true)];
    fixture.seed(&before)?;
    let selected = selection(true, Some(2), false);
    let tree = fixture.manager.open(selected.clone()).await?;
    assert_eq!(queries(&fixture), 1);
    fixture.manager.open(selected).await?;
    let head_only = Version { head: Clock::genesis(EventId::from_bytes([44; 32])), ..before[0].clone() };
    let outside = Version { score: 40, head: Clock::genesis(EventId::from_bytes([45; 32])), ..before[2].clone() };
    fixture.commit(&[(Some(before[0].clone()), Some(head_only.clone())), (Some(before[2].clone()), Some(outside))]).await?;
    assert_eq!(queries(&fixture), 1, "no full query or refill for unchanged membership/order");
    let leaves: Vec<_> = tree.view().range(..)?.try_collect().await?;
    assert_eq!(leaves[0].payload.0, head_only.head);
    assert_eq!(ids(&tree.view()).await?, vec![before[0].id, before[1].id]);
    Ok(())
}

#[tokio::test]
async fn an_outward_move_refills_but_an_entering_prefix_can_close_the_gap() -> anyhow::Result<()> {
    for enter in [false, true] {
        let fixture = Fixture::<Ristretto255>::new(1)?;
        let before = [version(1, 10, true), version(2, 20, true), version(3, 30, true), version(4, 40, true)];
        fixture.seed(&before)?;
        let tree = fixture.manager.open(selection(true, Some(2), false)).await?;
        let moved = Version { score: 100, head: Clock::genesis(EventId::from_bytes([50; 32])), ..before[1].clone() };
        let mut changes = vec![(Some(before[1].clone()), Some(moved))];
        if enter {
            changes.push((Some(before[3].clone()), Some(Version { score: 0, ..before[3].clone() })));
        }
        fixture.commit(&changes).await?;
        let expected = if enter { vec![before[3].id, before[0].id] } else { vec![before[0].id, before[2].id] };
        assert_eq!(ids(&tree.view()).await?, expected);
        assert_eq!(queries(&fixture), if enter { 1 } else { 2 });
        assert_eq!(tree.audit().await?.members, 2);
    }
    Ok(())
}

#[tokio::test]
async fn pruning_an_early_change_page_does_not_lose_a_later_replacement() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let original = version(255, 0, true);
    fixture.seed(std::slice::from_ref(&original))?;
    let tree = fixture.manager.open(selection(true, Some(3), false)).await?;
    let added: Vec<_> = (1..=260u16)
        .map(|n| {
            let mut id = [7; 32];
            id[..2].copy_from_slice(&n.to_be_bytes());
            Version { id: EntityId::from_bytes(id), ..version(1, i64::from(n), true) }
        })
        .collect();
    let mut changes: Vec<_> = added.iter().cloned().map(|version| (None, Some(version))).collect();
    changes.push((Some(original), None));
    fixture.commit(&changes).await?;
    assert_eq!(ids(&tree.view()).await?, added[..3].iter().map(|version| version.id).collect::<Vec<_>>());
    assert_eq!(queries(&fixture), 2, "one final refill recovers the candidate pruned before the last change page");
    assert_eq!(tree.audit().await?.members, 3);
    Ok(())
}

#[tokio::test]
async fn failed_finish_keeps_the_old_cursor_until_the_whole_refill_can_publish() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    let before = [version(1, 10, true), version(2, 20, true), version(3, 30, true)];
    fixture.seed(&before)?;
    let tree = fixture.manager.open(selection(true, Some(2), false)).await?;
    let cursor = tree.store().buffered().cursor.clone();
    let mut transaction = fixture.storage.transaction();
    write_versions(&mut transaction, &[(Some(before[0].clone()), None)]).await?;
    fixture.manager.invalidate();
    transaction.commit().await?.committed()?;
    fixture.storage.reject_state_finish.store(true, Ordering::SeqCst);
    assert!(fixture.manager.maintain().await.is_err());
    assert_eq!(tree.store().buffered().cursor, cursor);
    assert!(tree.view().count(..).await.is_err());
    fixture.storage.reject_state_finish.store(false, Ordering::SeqCst);
    fixture.manager.maintain().await?;
    assert_eq!(ids(&tree.view()).await?, vec![before[1].id, before[2].id]);
    assert_ne!(tree.store().buffered().cursor, cursor);
    Ok(())
}

#[tokio::test]
async fn incremental_membership_matches_complete_queries_across_mutations_and_ties() -> anyhow::Result<()> {
    for order in [None, Some(false), Some(true)] {
        for limit in [0, 1, 7, 30] {
            let fixture = Fixture::<Ristretto255>::new(1)?;
            let mut entities: Vec<_> = (1..=24).map(|id| version(id, i64::from(id % 5), id % 3 != 0)).collect();
            fixture.seed(&entities)?;
            let mut selected = selection(true, Some(limit), order == Some(true));
            if order.is_none() {
                selected.order_by = None;
            }
            let tree = fixture.manager.open(selected).await?;
            let mut random = 482u64;
            for round in 0..24u8 {
                let mut changes = Vec::new();
                for index in [usize::from(round % 24), usize::from((round + 7) % 24), usize::from((round + 17) % 24)] {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let before = entities[index].clone();
                    let after = Version {
                        score: ((random >> 8) % 9) as i64 - 4,
                        active: Some(random & 1 != 0),
                        head: Clock::genesis(EventId::from_bytes([round.wrapping_add(80); 32])),
                        ..before.clone()
                    };
                    entities[index] = after.clone();
                    changes.push((Some(before), Some(after)));
                }
                fixture.commit(&changes).await?;
                let mut expected: Vec<_> = entities.iter().filter(|entity| entity.active == Some(true)).collect();
                expected.sort_by(|a, b| {
                    let compared = match order {
                        None => std::cmp::Ordering::Equal,
                        Some(false) => a.score.cmp(&b.score),
                        Some(true) => b.score.cmp(&a.score),
                    };
                    compared.then_with(|| a.id.cmp(&b.id))
                });
                let expected: Vec<_> = expected.into_iter().take(limit as usize).map(|entity| entity.id).collect();
                assert_eq!(ids(&tree.view()).await?, expected, "order={order:?} limit={limit} round={round}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn changed_ordering_metadata_cannot_publish_a_cursor_under_the_old_key_type() -> anyhow::Result<()> {
    let fixture = Fixture::<Ristretto255>::new(1)?;
    fixture.seed(&[version(1, 10, true)])?;
    let tree = fixture.manager.open(selection(true, Some(2), false)).await?;
    let cursor = tree.store().buffered().cursor.clone();
    fixture.storage.set_state(property_state(score(), "renamed", "i64")?);
    fixture.manager.invalidate();
    fixture.manager.maintain().await?;
    assert_ne!(tree.store().buffered().cursor, cursor, "a display-name change preserves the order");
    let cursor = tree.store().buffered().cursor.clone();
    fixture.storage.set_state(property_state(score(), "renamed", "string")?);
    fixture.manager.invalidate();
    assert!(fixture.manager.maintain().await.is_err());
    assert_eq!(tree.store().buffered().cursor, cursor);
    assert!(tree.view().count(..).await.is_err());
    Ok(())
}

#[tokio::test]
async fn a_json_subpath_encodes_the_extracted_value_not_the_base_property_type() -> anyhow::Result<()> {
    for (kind, value) in [
        ("string", Value::String(r#"{"rank":2}"#.into())),
        ("binary", Value::Binary(br#"{"rank":2}"#.to_vec())),
        ("object", Value::Object(br#"{"rank":2}"#.to_vec())),
    ] {
        let fixture = Fixture::<Ristretto255>::new(1)?;
        fixture.storage.set_state(property_state(score(), "score", kind)?);
        let entity = state(EntityId::from_bytes([1; 32]), model(), [(score(), value)])?;
        fixture.storage.set_state(entity.clone());
        let mut selected = selection(false, None, false);
        selected.order_by.as_mut().unwrap()[0].path.subpath.push("rank".into());
        let tree = fixture.manager.open(selected).await?;
        assert_eq!(ids(&tree.view()).await?, vec![entity.payload.entity_id]);
        assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::Json);
    }
    Ok(())
}
