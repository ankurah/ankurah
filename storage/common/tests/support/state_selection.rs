//! Selection snapshots must use the engine's ordinary query semantics.

use std::{collections::BTreeMap, num::NonZeroUsize};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core::{
    error::{MutationError, RetrievalError},
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StateScan, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, ModelId, PropertyId, State, StateBuffers};

fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

pub async fn selection_matches_fetch(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let model = ModelId::EntityId(id(101));
    let score = PropertyId::EntityId(id(201));
    let json = PropertyId::EntityId(id(202));
    // 208 sorts before 1 as base64 text, and after it as an EntityId.
    assert!(id(208).to_base64() < id(1).to_base64());
    for (byte, value) in [(208, Some(5)), (1, Some(5)), (209, Some(10)), (2, Some(2)), (210, None), (3, None)] {
        let backend = LWWBackend::new();
        if let Some(value) = value {
            backend.set(score, Some(Value::I64(value)));
            backend.set(json, Some(Value::Json(format!(r#"{{"rank":{value}}}"#).parse()?)));
        } else if byte == 210 {
            backend.set(json, Some(Value::Json(r#"{"rank":null}"#.parse()?)));
        }
        let event = ankurah_core::test_helpers::commit_backend(&backend, byte)?;
        let state = Attested::opt(
            EntityState {
                entity_id: id(byte),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: [model].into_iter().collect(),
                    head: Clock::singleton(&event),
                },
            },
            None,
        );
        let mut transaction = engine.transaction();
        transaction.set_state(&Clock::default(), &state).await?;
        transaction.commit().await?.committed()?;
    }
    let mut json_path = PropertyPath::from(json);
    json_path.subpath.push("rank".into());
    for predicate in [Predicate::True, Predicate::MemberOf(model)] {
        for path in [PropertyPath::from(score), json_path.clone()] {
            for direction in [OrderDirection::Asc, OrderDirection::Desc] {
                for limit in [0, 1, 4, 10] {
                    let selection = Selection {
                        predicate: predicate.clone(),
                        order_by: Some(vec![OrderByItem { path: path.clone(), direction: direction.clone() }]),
                        limit: Some(limit),
                    };
                    assert_snapshot(engine, &selection).await?;
                }
            }
        }
        let selection = Selection { predicate: predicate.clone(), order_by: None, limit: Some(2) };
        let states = assert_snapshot(engine, &selection).await?;
        assert_eq!(states, [id(1), id(2)], "LIMIT without ordering uses raw ascending entity ID");
        let selection = Selection {
            predicate,
            order_by: Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Desc }]),
            limit: Some(2),
        };
        assert_eq!(assert_snapshot(engine, &selection).await?, [id(210), id(209)], "explicit ID direction is preserved");
    }
    let tied = Predicate::And(
        Box::new(Predicate::MemberOf(model)),
        Box::new(Predicate::Comparison {
            left: Box::new(Expr::Path(score.into())),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::I64(5))),
        }),
    );
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        let selection =
            Selection { predicate: tied.clone(), order_by: Some(vec![OrderByItem { path: score.into(), direction }]), limit: Some(1) };
        assert_eq!(assert_snapshot(engine, &selection).await?, [id(1)], "ties use ascending raw entity ID");
    }
    assert!(
        engine.state_snapshot(StateScan::Selection(&Selection { predicate: Predicate::True, order_by: None, limit: None })).await.is_err(),
        "a selection scan requires LIMIT"
    );
    Ok(())
}

/// Optimistic engines must validate even pages served from selected memory.
pub async fn selection_rejects_changed_boundary(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(2) };
    let mut snapshot = engine.state_snapshot(StateScan::Selection(&selection)).await?;
    let state = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?.remove(0);
    let mut transaction = engine.transaction();
    transaction.set_state(&state.payload.state.head, &state).await?;
    transaction.commit().await?.committed()?;
    for result in [snapshot.next_page(NonZeroUsize::new(1).unwrap()).await.map(|_| ()), snapshot.finish().await.map(|_| ())] {
        assert!(matches!(result, Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict)));
    }
    Ok(())
}

pub async fn selection_tracks_updated_keys(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let model = ModelId::EntityId(id(111));
    let score = PropertyId::EntityId(id(211));
    let active = PropertyId::EntityId(id(212));
    let state = |byte, score_value, included, seed| -> anyhow::Result<_> {
        let backend = LWWBackend::new();
        backend.set(score, Some(Value::I64(score_value)));
        backend.set(active, Some(Value::Bool(included)));
        let event = ankurah_core::test_helpers::commit_backend(&backend, seed)?;
        Ok(Attested::opt(
            EntityState {
                entity_id: id(byte),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: [model].into_iter().collect(),
                    head: Clock::singleton(&event),
                },
            },
            None,
        ))
    };
    let before = [state(11, 30, true, 11)?, state(12, 10, true, 12)?, state(13, 20, false, 13)?];
    let after = [state(11, 5, true, 21)?, state(13, 20, true, 23)?];
    let mut transaction = engine.transaction();
    for state in &before {
        transaction.set_state(&Clock::default(), state).await?;
    }
    transaction.commit().await?.committed()?;
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(active.into())),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        order_by: Some(vec![OrderByItem { path: score.into(), direction: OrderDirection::Asc }]),
        limit: Some(1),
    }
    .and_member_of(model);
    for (direction, expected) in [(OrderDirection::Asc, id(12)), (OrderDirection::Desc, id(11))] {
        let mut ordered = selection.clone();
        ordered.order_by.as_mut().unwrap()[0].direction = direction;
        assert_eq!(assert_snapshot(engine, &ordered).await?, [expected]);
    }
    let mut transaction = engine.transaction();
    for (old, new) in [(&before[0], &after[0]), (&before[2], &after[1])] {
        transaction.set_state(&old.payload.state.head, new).await?;
    }
    transaction.commit().await?.committed()?;
    for (direction, expected) in [(OrderDirection::Asc, id(11)), (OrderDirection::Desc, id(13))] {
        let mut ordered = selection.clone();
        ordered.order_by.as_mut().unwrap()[0].direction = direction;
        let mut unlimited = ordered.clone();
        unlimited.limit = None;
        assert_eq!(
            engine.fetch_states(&unlimited).await?[0].payload.entity_id,
            expected,
            "native numeric ordering after key update, without implicit ID tie-breaker"
        );
        assert_eq!(engine.fetch_states(&ordered).await?[0].payload.entity_id, expected, "native numeric ordering after key update");
        assert_eq!(assert_snapshot(engine, &ordered).await?, [expected]);
    }
    Ok(())
}

async fn assert_snapshot(engine: &impl StorageEngine, selection: &Selection<Resolved>) -> anyhow::Result<Vec<EntityId>> {
    let expected = engine.fetch_states(selection).await?;
    let mut snapshot = engine.state_snapshot(StateScan::Selection(selection)).await?;
    let mut selected = Vec::new();
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(1).unwrap()).await?;
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        selected.extend(page);
    }
    assert_eq!(selected, expected, "selection snapshot differs from fetch: {selection:?}");
    assert!(selected.len() as u64 <= selection.limit.unwrap());
    let ids: Vec<_> = selected.iter().map(|state| state.payload.entity_id).collect();
    assert_eq!(snapshot.get_states(&ids).await?, selected.into_iter().map(Some).collect::<Vec<_>>());
    snapshot.finish().await?;
    Ok(ids)
}
