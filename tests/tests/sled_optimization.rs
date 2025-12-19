use ankql::ast::{ComparisonOperator, Expr, Literal, OrderByItem, OrderDirection, PathExpr, Predicate, Selection};
use ankurah::{policy::DEFAULT_CONTEXT as c, proto::EntityId, Model, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize, Clone)]
pub struct TestEntity {
    pub name: String,
    pub value: i64,
}

#[tokio::test]
async fn test_id_range_optimization_integration() -> Result<()> {
    // Setup
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context_async(c).await;

    // Create multiple entities
    let mut entity_ids = Vec::new();
    for i in 0..10 {
        let trx = context.begin();
        let entity = trx.create(&TestEntity { name: format!("Entity {}", i), value: i as i64 }).await?;
        entity_ids.push(entity.id());
        trx.commit().await?;

        // Small delay to ensure different timestamps in EntityId
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
    }

    // Test 1: Simple ORDER BY id ASC with LIMIT (should use optimization)
    let selection_asc = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PathExpr::simple("id".to_string()), direction: OrderDirection::Asc }]),
        limit: Some(5),
    };

    // Fetch results directly from storage collection to test optimization
    let collection_id = TestEntity::collection();
    let storage_collection = node.collections.get(&collection_id).await?;
    let results_asc = storage_collection.fetch_states(&selection_asc).await?;

    // Should get exactly 5 results
    assert_eq!(results_asc.len(), 5, "Should return exactly 5 results due to LIMIT");

    // Results should be in ascending ID order
    let result_ids_asc: Vec<EntityId> = results_asc.iter().map(|r| r.payload.entity_id).collect();
    for i in 1..result_ids_asc.len() {
        assert!(result_ids_asc[i] > result_ids_asc[i - 1], "Results should be in ascending order");
    }

    // Test 2: ORDER BY id DESC (should use FullScan reverse + skip sorting)
    let selection_desc = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PathExpr::simple("id".to_string()), direction: OrderDirection::Desc }]),
        limit: Some(3),
    };

    let results_desc = storage_collection.fetch_states(&selection_desc).await?;

    // Validate descending results
    assert_eq!(results_desc.len(), 3, "Should return exactly 3 results due to LIMIT");

    let result_ids_desc: Vec<EntityId> = results_desc.iter().map(|r| r.payload.entity_id).collect();
    for i in 1..result_ids_desc.len() {
        assert!(result_ids_desc[i] < result_ids_desc[i - 1], "Results should be in descending order");
    }

    // Test 3: ORDER BY name (should require in-memory sorting, no optimization)
    let selection_name = Selection {
        predicate: Predicate::True,
        order_by: Some(vec![OrderByItem { path: PathExpr::simple("name".to_string()), direction: OrderDirection::Asc }]),
        limit: Some(5),
    };

    let results_name = storage_collection.fetch_states(&selection_name).await?;

    // Validate name-sorted results - we just check that we get the expected count
    // The actual sorting behavior is tested in unit tests
    assert_eq!(results_name.len(), 5, "Should return exactly 5 results due to LIMIT");

    Ok(())
}

#[tokio::test]
async fn test_id_range_with_where_clause() -> Result<()> {
    // This test validates that WHERE id >= X ORDER BY id works correctly

    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    let context = node.context_async(c).await;

    // Create several entities
    let mut entity_ids = Vec::new();
    for i in 0..8 {
        let trx = context.begin();
        let entity = trx.create(&TestEntity { name: format!("Entity {}", i), value: i as i64 }).await?;
        entity_ids.push(entity.id());
        trx.commit().await?;

        // Ensure different timestamps for EntityIDs
        tokio::time::sleep(tokio::time::Duration::from_millis(3)).await;
    }

    entity_ids.sort();
    let start_id = entity_ids[2].clone(); // Start from the 3rd entity

    // Query with WHERE id >= start_id ORDER BY id
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(PathExpr::simple("id".to_string()))),
            operator: ComparisonOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Literal(Literal::String(start_id.to_base64()))),
        },
        order_by: Some(vec![OrderByItem { path: PathExpr::simple("id".to_string()), direction: OrderDirection::Asc }]),
        limit: Some(3),
    };

    let collection_id = TestEntity::collection();
    let storage_collection = node.collections.get(&collection_id).await?;
    let results = storage_collection.fetch_states(&selection).await?;

    // Should get exactly 3 results
    assert_eq!(results.len(), 3, "Should return exactly 3 results due to LIMIT");

    // Results should be in ascending order and all >= start_id
    let result_ids: Vec<EntityId> = results.iter().map(|r| r.payload.entity_id).collect();

    // Verify ordering
    for i in 1..result_ids.len() {
        assert!(result_ids[i] > result_ids[i - 1], "Results should be in strictly ascending order");
    }

    // Verify all results are >= start_id
    for id in &result_ids {
        assert!(id >= &start_id, "All results should be >= start_id");
    }

    Ok(())
}
