use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, OrderByItem, OrderDirection, Predicate};
use ankurah_proto::EntityId;
use ankurah_storage_sled::{determine_iteration_strategy, order_by_matches_iteration, IterationStrategy};

#[test]
fn test_id_range_strategy_optimization() {
    // Test that "WHERE id > x ORDER BY id" generates IdRange strategy and skips sorting
    let test_id = EntityId::new();
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Literal::String(test_id.to_base64()))),
    };

    let order_by = Some(vec![OrderByItem { identifier: Identifier::Property("id".to_string()), direction: OrderDirection::Asc }]);

    // Test strategy determination
    let strategy = determine_iteration_strategy(&predicate, &order_by);

    // Should use IdRange strategy for id constraints
    assert!(matches!(strategy, IterationStrategy::IdRange { .. }));

    // Test that ORDER BY matches iteration (should skip sorting)
    let skip_sorting = order_by_matches_iteration(&order_by, &strategy);
    assert!(skip_sorting, "ORDER BY id ASC should match IdRange iteration order");
}

#[test]
fn test_id_order_by_only_optimization() {
    // Test that "ORDER BY id DESC" (no WHERE) generates FullScan reverse and skips sorting
    let predicate = Predicate::True;
    let order_by = Some(vec![OrderByItem { identifier: Identifier::Property("id".to_string()), direction: OrderDirection::Desc }]);

    // Test strategy determination
    let strategy = determine_iteration_strategy(&predicate, &order_by);

    // Should use FullScan with reverse=true
    assert!(matches!(strategy, IterationStrategy::FullScan { reverse: true }));

    // Test that ORDER BY matches iteration (should skip sorting)
    let skip_sorting = order_by_matches_iteration(&order_by, &strategy);
    assert!(skip_sorting, "ORDER BY id DESC should match FullScan reverse iteration");
}

#[test]
fn test_non_id_order_by_requires_sorting() {
    // Test that "ORDER BY name" requires in-memory sorting
    let predicate = Predicate::True;
    let order_by = Some(vec![OrderByItem { identifier: Identifier::Property("name".to_string()), direction: OrderDirection::Asc }]);

    // Test strategy determination
    let strategy = determine_iteration_strategy(&predicate, &order_by);

    // Should use default FullScan forward
    assert!(matches!(strategy, IterationStrategy::FullScan { reverse: false }));

    // Test that ORDER BY does NOT match iteration (requires sorting)
    let skip_sorting = order_by_matches_iteration(&order_by, &strategy);
    assert!(!skip_sorting, "ORDER BY name should require in-memory sorting");
}

#[test]
fn test_multi_field_order_by_requires_sorting() {
    // Test that "ORDER BY id, name" requires in-memory sorting (can't optimize multi-field)
    let predicate = Predicate::True;
    let order_by = Some(vec![
        OrderByItem { identifier: Identifier::Property("id".to_string()), direction: OrderDirection::Asc },
        OrderByItem { identifier: Identifier::Property("name".to_string()), direction: OrderDirection::Asc },
    ]);

    // Test strategy determination - should still optimize iteration for id
    let strategy = determine_iteration_strategy(&predicate, &order_by);
    assert!(matches!(strategy, IterationStrategy::FullScan { reverse: false }));

    // Test that multi-field ORDER BY does NOT match iteration (requires sorting)
    let skip_sorting = order_by_matches_iteration(&order_by, &strategy);
    assert!(!skip_sorting, "Multi-field ORDER BY should require in-memory sorting");
}

#[test]
fn test_direction_mismatch_requires_sorting() {
    // Test that "WHERE id > x ORDER BY id DESC" with IdRange (forward) requires sorting
    let test_id = EntityId::new();
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(Literal::String(test_id.to_base64()))),
    };

    let order_by = Some(vec![OrderByItem {
        identifier: Identifier::Property("id".to_string()),
        direction: OrderDirection::Desc, // Mismatch: IdRange is always forward
    }]);

    // Test strategy determination
    let strategy = determine_iteration_strategy(&predicate, &order_by);
    assert!(matches!(strategy, IterationStrategy::IdRange { reverse: false, .. }));

    // Test that direction mismatch requires sorting
    let skip_sorting = order_by_matches_iteration(&order_by, &strategy);
    assert!(!skip_sorting, "ORDER BY id DESC with forward IdRange should require sorting");
}
