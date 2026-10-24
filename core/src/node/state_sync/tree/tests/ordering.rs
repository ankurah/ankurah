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
async fn unknown_and_nested_ordering_cannot_select_a_complete_tree() -> anyhow::Result<()> {
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
        let mut cache = QueryTree::new(spec(), Entries::new());
        cache.refresh_states(rows).await?;
        for (direction, limit) in [(OrderDirection::Asc, 3), (OrderDirection::Desc, 2), (OrderDirection::Desc, 3)] {
            let selection = Selection {
                predicate: Predicate::True,
                order_by: Some(vec![OrderByItem { path: path.clone(), direction }]),
                limit: Some(limit),
            };
            assert!(request_for(&selection, &catalog).is_err(), "ordering cannot use the scalar planner");
            let snapshot = cache.snapshot();
            assert!(snapshot.cannot_select(&selection, &catalog));
            let error = snapshot.select(&selection, &catalog).err().unwrap();
            assert!(error.downcast_ref::<UnsupportedTreeSelection>().is_some());
        }
        assert_eq!(cache.snapshot().entries.len(), 4);
    }
    Ok(())
}

#[tokio::test]
async fn descending_scalar_limit_with_missing_values_requires_storage_ordering() -> anyhow::Result<()> {
    let property = proto::PropertyId::System(proto::SystemProperty::Label);
    let catalog = CatalogManager::default();
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(property), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let spec = spec_for(&selection, &catalog);
    assert!(!spec.key_spec().keyparts.is_empty());
    let rows = [(1, None), (2, Some("20")), (3, Some("20")), (4, Some("10"))]
        .into_iter()
        .map(|(id, value)| with_values(id, &[(property, value.map(|value| Value::String(value.into())))]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut cache = QueryTree::new(spec, Entries::new());
    let view = cache.refresh_states(rows).await?;
    assert!(view.cannot_select(&selection, &catalog));
    assert!(view.select(&selection, &catalog).err().unwrap().downcast_ref::<UnsupportedTreeSelection>().is_some());
    Ok(())
}

#[tokio::test]
async fn narrower_descending_null_scope_cannot_select_a_complete_tree() -> anyhow::Result<()> {
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
    assert!(spec.serves(&request_for(&selection, &catalog)?).is_some(), "the scalar planner supports this narrower scope");
    let rows = [(1, "a", None), (2, "b", None), (3, "b", Some("09")), (4, "b", Some("08")), (5, "c", None)]
        .into_iter()
        .map(|(id, group_value, rank_value)| {
            with_values(
                id,
                &[(group, Some(Value::String(group_value.into()))), (rank, rank_value.map(|value| Value::String(value.into())))],
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut cache = QueryTree::new(spec.clone(), Entries::new());
    let snapshot = cache.refresh_states(rows).await?;
    assert!(snapshot.cannot_select(&selection, &catalog));
    assert!(snapshot.select(&selection, &catalog).err().unwrap().downcast_ref::<UnsupportedTreeSelection>().is_some());
    assert_eq!(cache.snapshot().entries.len(), 5);
    Ok(())
}

#[tokio::test]
async fn descending_scalar_limit_cannot_select_a_complete_tree_even_without_missing_values() -> anyhow::Result<()> {
    let property = proto::PropertyId::System(proto::SystemProperty::Label);
    let catalog = CatalogManager::default();
    let selection = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(property), direction: OrderDirection::Desc }]),
        limit: Some(2),
    };
    let spec = spec_for(&selection, &catalog);
    let rows = [(1, "30"), (2, "20"), (3, "20"), (4, "10")]
        .into_iter()
        .map(|(id, value)| with_values(id, &[(property, Some(Value::String(value.into())))]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut tree = QueryTree::new(spec, Entries::new());
    let snapshot = tree.refresh_states(rows).await?;
    // The rule reads the selection alone: whether today's rows happen to
    // carry the value cannot decide how a shared tree ranks them tomorrow.
    assert!(snapshot.cannot_select(&selection, &catalog));
    assert!(snapshot.select(&selection, &catalog).is_err());
    Ok(())
}

#[tokio::test]
async fn descending_range_reuses_a_broader_tree_with_nulls_outside_the_range() -> anyhow::Result<()> {
    let property = proto::PropertyId::System(proto::SystemProperty::Label);
    let catalog = CatalogManager::default();
    let spec = TreeSpec::new(KeySpec::new(vec![ankurah_core_types::IndexKeyPart::asc(property, ValueType::String)]), Predicate::True);
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(property))),
            operator: ComparisonOperator::GreaterThan,
            right: Box::new(Expr::Literal(Value::String("10".into()))),
        },
        order_by: Some(vec![OrderByItem { path: PropertyPath::from(property), direction: OrderDirection::Desc }]),
        limit: Some(1),
    };
    let mut tree = QueryTree::new(spec, Entries::new());
    let rows = [(1, None), (2, Some("20")), (3, Some("30"))]
        .into_iter()
        .map(|(id, value)| with_values(id, &[(property, value.map(|value| Value::String(value.into())))]))
        .collect::<anyhow::Result<_>>()?;
    let snapshot = tree.refresh_states(rows).await?;
    assert!(!snapshot.cannot_select(&selection, &catalog));
    let selected = snapshot.select(&selection, &catalog)?;
    assert_eq!(selected.entries.keys().copied().collect::<Vec<_>>(), [state(3, 1).payload.entity_id]);
    assert_eq!(tree.snapshot().entries.len(), 3);
    Ok(())
}
