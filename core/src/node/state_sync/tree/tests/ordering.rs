use super::*;
use crate::property::backend::{LWWBackend, PropertyBackend};
use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate};

fn with_values(id: u8, values: &[(proto::PropertyId, Option<Value>)]) -> anyhow::Result<Attested<EntityState>> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(*property, value.clone());
    }
    crate::test_helpers::commit_backend(&backend, 1)?;
    let mut state = state(id, 1);
    state.payload.state.state_buffers = proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into());
    Ok(state)
}

#[tokio::test]
async fn unknown_and_nested_ordering_use_captured_values_with_stable_ties() -> anyhow::Result<()> {
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([42; 32]));
    let catalog = CatalogManager::default();
    for nested in [false, true] {
        let mut path = PropertyPath::from(property);
        if nested {
            path.subpath = vec!["rank".into()];
        }
        let rows = [(1, None), (2, Some(5)), (3, Some(5)), (4, Some(10))]
            .into_iter()
            .map(|(id, value)| {
                let value = if nested {
                    Some(Value::Json(value.map_or_else(|| serde_json::json!({}), |value| serde_json::json!({"rank": value}))))
                } else {
                    value.map(Value::I64)
                };
                with_values(id, &[(property, value)])
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let cache = QueryTreeCache::default();
        cache.refresh(spec(), rows).await?;
        for (direction, limit, expected) in
            [(OrderDirection::Asc, 3, vec![1, 2, 3]), (OrderDirection::Desc, 2, vec![1, 4]), (OrderDirection::Desc, 3, vec![1, 2, 4])]
        {
            let selection = Selection {
                predicate: Predicate::True,
                order_by: Some(vec![OrderByItem { path: path.clone(), direction }]),
                limit: Some(limit),
            };
            assert!(QueryTreeCache::request(&selection, &catalog).is_err(), "ordering cannot use the scalar planner");
            let mut view = cache.current_snapshot().await?.select(&selection, &catalog).await?;
            assert_eq!(
                view.states.keys().copied().collect::<Vec<_>>(),
                expected.into_iter().map(|id| state(id, 1).payload.entity_id).collect::<Vec<_>>()
            );
            assert_eq!(view.span_digest(super::super::super::whole()).await?.digest.count(), limit as i64);
        }
        assert_eq!(cache.current_snapshot().await?.states.len(), 4);
    }
    Ok(())
}

#[tokio::test]
async fn descending_scalar_limit_preserves_missing_first_and_id_ties() -> anyhow::Result<()> {
    let property = proto::PropertyId::System(proto::SystemProperty::Label);
    let catalog = CatalogManager::default();
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(property), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let spec = QueryTreeCache::spec(&selection, &catalog);
    assert!(!spec.key_spec().keyparts.is_empty());
    let rows = [(1, None), (2, Some("20")), (3, Some("20")), (4, Some("10"))]
        .into_iter()
        .map(|(id, value)| with_values(id, &[(property, value.map(|value| Value::String(value.into())))]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTreeCache::default();
    let view = cache.refresh(spec, rows).await?.select(&selection, &catalog).await?;
    assert_eq!(view.states.keys().copied().collect::<Vec<_>>(), [1, 2].map(|id| state(id, 1).payload.entity_id));
    Ok(())
}

#[tokio::test]
async fn narrower_descending_null_scope_filters_before_limiting_a_broader_tree() -> anyhow::Result<()> {
    let group = proto::PropertyId::System(proto::SystemProperty::Label);
    let rank = proto::PropertyId::System(proto::SystemProperty::Name);
    let predicate = |operator, value: &str| Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::from(group))),
        operator,
        right: Box::new(Expr::Literal(Value::String(value.into()))),
    };
    let spec = TreeSpec::new(
        KeySpec::new(vec![
            ankurah_core_types::IndexKeyPart::asc(group, ValueType::String),
            ankurah_core_types::IndexKeyPart::asc(rank, ValueType::String),
        ]),
        predicate(ComparisonOperator::GreaterThanOrEqual, "a"),
    );
    let selection = Selection {
        predicate: predicate(ComparisonOperator::Equal, "b"),
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(rank), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let catalog = CatalogManager::default();
    assert!(spec.serves(&QueryTreeCache::request(&selection, &catalog)?).is_some(), "the scalar planner supports this narrower scope");
    let rows = [(1, "a", None), (2, "b", None), (3, "b", Some("09")), (4, "b", Some("08")), (5, "c", None)]
        .into_iter()
        .map(|(id, group_value, rank_value)| {
            with_values(
                id,
                &[(group, Some(Value::String(group_value.into()))), (rank, rank_value.map(|value| Value::String(value.into())))],
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cache = QueryTreeCache::default();
    let mut view = cache.refresh(spec.clone(), rows).await?.select(&selection, &catalog).await?;
    assert_eq!(view.spec, spec, "query ordering never changes physical tree identity");
    assert_eq!(view.states.keys().copied().collect::<Vec<_>>(), [2, 3].map(|id| state(id, 1).payload.entity_id));
    assert_eq!(view.span_digest(super::super::super::whole()).await?.digest.count(), 2);
    assert_eq!(cache.current_snapshot().await?.states.len(), 5);
    Ok(())
}
