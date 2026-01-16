//! Multi-column ORDER BY tests with secondary sort verification for IndexedDB.
//!
//! These tests verify that:
//! 1. Secondary columns are correctly sorted when primary column has duplicates
//! 2. LIMIT respects secondary column ordering
//! 3. Mixed-direction ORDER BY works via order_by_spill (in-memory sorting)
//!
//! Note: IndexedDB doesn't support DESC indexes natively. Same-direction
//! ORDER BY (all ASC or all DESC) works via index + ScanDirection. Mixed-direction
//! ORDER BY (e.g., ASC, DESC) uses order_by_spill for in-memory sorting of
//! columns that can't be satisfied by the index scan direction.

mod common;

use ankurah::Model;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use common::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

// ============================================================================
// Test Model with Multiple Sortable Fields
// ============================================================================

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Product {
    pub category: String,
    pub name: String,
    pub price: i64,
    pub stock: i64,
}

fn product_tuples(products: &[ProductView]) -> Vec<(String, String, i64, i64)> {
    products.iter().map(|p| (p.category().unwrap(), p.name().unwrap(), p.price().unwrap(), p.stock().unwrap())).collect()
}

fn names(products: &[ProductView]) -> Vec<String> { products.iter().map(|p| p.name().unwrap()).collect() }

fn prices(products: &[ProductView]) -> Vec<i64> { products.iter().map(|p| p.price().unwrap()).collect() }

async fn create_products(ctx: &ankurah::Context, products: Vec<(&str, &str, i64, i64)>) -> Result<(), anyhow::Error> {
    let trx = ctx.begin();
    for (category, name, price, stock) in products {
        trx.create(&Product { category: category.to_string(), name: name.to_string(), price, stock }).await?;
    }
    trx.commit().await?;
    Ok(())
}

// ============================================================================
// Same-Direction ORDER BY Tests (Should Work)
// ============================================================================

/// Verify secondary column is sorted when primary column has duplicates (both ASC)
#[wasm_bindgen_test]
pub async fn test_secondary_sort_asc_asc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create products with duplicate categories
    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
            ("Books", "Textbook", 50, 30),
            ("Books", "Magazine", 10, 200),
        ],
    )
    .await?;

    // ORDER BY category ASC, name ASC
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category ASC, name ASC").await?;

    // Books first (ASC), then Electronics
    // Within each category, names should be ASC
    assert_eq!(
        names(&results),
        vec!["Magazine", "Novel", "Textbook", "Laptop", "Phone", "Tablet"],
        "Secondary column (name) should be sorted ASC within each category"
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Verify both columns are sorted DESC using Reverse scan direction
#[wasm_bindgen_test]
pub async fn test_secondary_sort_desc_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
            ("Books", "Textbook", 50, 30),
            ("Books", "Magazine", 10, 200),
        ],
    )
    .await?;

    // ORDER BY category DESC, name DESC
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category DESC, name DESC").await?;

    // Electronics first (DESC), then Books
    // Within each category, names should be DESC
    assert_eq!(names(&results), vec!["Tablet", "Phone", "Laptop", "Textbook", "Novel", "Magazine"], "Both columns should be sorted DESC");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// Mixed-Direction ORDER BY Tests (Require order_by_spill - Currently Broken)
// ============================================================================

/// Mixed direction: primary ASC, secondary DESC
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_secondary_sort_asc_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
            ("Books", "Textbook", 50, 30),
            ("Books", "Magazine", 10, 200),
        ],
    )
    .await?;

    // ORDER BY category ASC, name DESC
    // This requires order_by_spill since IndexedDB can't mix directions
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category ASC, name DESC").await?;

    // Books first (ASC), then Electronics
    // Within each category, names should be DESC
    assert_eq!(
        names(&results),
        vec!["Textbook", "Novel", "Magazine", "Tablet", "Phone", "Laptop"],
        "Secondary column (name) should be sorted DESC within each category (via spill)"
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Mixed direction: primary DESC, secondary ASC
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_secondary_sort_desc_asc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
            ("Books", "Textbook", 50, 30),
            ("Books", "Magazine", 10, 200),
        ],
    )
    .await?;

    // ORDER BY category DESC, name ASC
    // This requires order_by_spill since IndexedDB scans in Reverse for DESC
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category DESC, name ASC").await?;

    // Electronics first (DESC), then Books
    // Within each category, names should be ASC
    assert_eq!(
        names(&results),
        vec!["Laptop", "Phone", "Tablet", "Magazine", "Novel", "Textbook"],
        "Secondary column (name) should be sorted ASC within each category (DESC primary)"
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Three-column ORDER BY with mixed directions
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_three_column_order_by() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create products with duplicates in first two columns
    create_products(
        &ctx,
        vec![
            ("A", "X", 100, 1),
            ("A", "X", 200, 2),
            ("A", "X", 50, 3),
            ("A", "Y", 100, 4),
            ("A", "Y", 200, 5),
            ("B", "X", 100, 6),
            ("B", "X", 200, 7),
        ],
    )
    .await?;

    // ORDER BY category ASC, name ASC, price DESC
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category ASC, name ASC, price DESC").await?;

    // A-X first (sorted by price DESC: 200, 100, 50)
    // Then A-Y (sorted by price DESC: 200, 100)
    // Then B-X (sorted by price DESC: 200, 100)
    assert_eq!(prices(&results), vec![200, 100, 50, 200, 100, 200, 100], "Three-column sort with mixed directions");

    // Verify the full ordering
    let tuples = product_tuples(&results);
    assert_eq!(
        tuples,
        vec![
            ("A".into(), "X".into(), 200, 2),
            ("A".into(), "X".into(), 100, 1),
            ("A".into(), "X".into(), 50, 3),
            ("A".into(), "Y".into(), 200, 5),
            ("A".into(), "Y".into(), 100, 4),
            ("B".into(), "X".into(), 200, 7),
            ("B".into(), "X".into(), 100, 6),
        ]
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Three-column ORDER BY with minimal spill: DESC, DESC, ASC (reverse scan, spill c)
/// This tests the reverse scan direction with only the last column needing spill
#[wasm_bindgen_test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
pub async fn test_three_column_desc_desc_asc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // Create products with duplicate (category, name) pairs to verify tertiary sort
    create_products(
        &ctx,
        vec![
            ("B", "Y", 100, 1), // B-Y group
            ("B", "Y", 200, 2),
            ("B", "Y", 50, 3),
            ("B", "X", 150, 4), // B-X group
            ("A", "Z", 300, 5), // A-Z group
            ("A", "Z", 250, 6),
        ],
    )
    .await?;

    // ORDER BY category DESC, name DESC, price ASC
    // Category DESC: B first, then A
    // Name DESC within category: Y before X (for B), Z (for A)
    // Price ASC within (category, name): smallest price first
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category DESC, name DESC, price ASC").await?;

    // B-Y (prices ASC: 50, 100, 200), B-X (150), A-Z (prices ASC: 250, 300)
    assert_eq!(prices(&results), vec![50, 100, 200, 150, 250, 300]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// LIMIT with Multi-Column ORDER BY Tests (TopK Path)
// ============================================================================

/// Two-column mixed direction with LIMIT: DESC, ASC (reverse scan, spill b)
/// Tests TopKStream with reverse scan direction
#[wasm_bindgen_test]
pub async fn test_topk_desc_asc_limit() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("C", "Apple", 100, 1),
            ("C", "Banana", 100, 2),
            ("C", "Cherry", 100, 3),
            ("B", "Date", 100, 4),
            ("B", "Elderberry", 100, 5),
            ("A", "Fig", 100, 6),
        ],
    )
    .await?;

    // ORDER BY category DESC, name ASC LIMIT 4
    // Category DESC: C, B, A
    // Name ASC within category
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category DESC, name ASC LIMIT 4").await?;

    // C (Apple, Banana, Cherry), B (Date) - stops at 4
    assert_eq!(names(&results), vec!["Apple", "Banana", "Cherry", "Date"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Three-column with LIMIT: ASC, ASC, DESC (forward scan, spill c)
/// Tests TopKStream with forward scan and tertiary spill
#[wasm_bindgen_test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
pub async fn test_topk_three_column_asc_asc_desc_limit() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(&ctx, vec![("A", "X", 300, 1), ("A", "X", 100, 2), ("A", "X", 200, 3), ("A", "Y", 150, 4), ("B", "X", 400, 5)]).await?;

    // ORDER BY category ASC, name ASC, price DESC LIMIT 3
    // A-X group (prices DESC: 300, 200, 100), then A-Y, then B-X
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category ASC, name ASC, price DESC LIMIT 3").await?;

    // First 3: A-X with prices 300, 200, 100
    assert_eq!(prices(&results), vec![300, 200, 100]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Three-column with LIMIT: DESC, DESC, ASC (reverse scan, spill c)
/// Tests TopKStream with reverse scan and tertiary spill
#[wasm_bindgen_test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
pub async fn test_topk_three_column_desc_desc_asc_limit() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(&ctx, vec![("B", "Y", 200, 1), ("B", "Y", 50, 2), ("B", "Y", 100, 3), ("B", "X", 150, 4), ("A", "Z", 300, 5)]).await?;

    // ORDER BY category DESC, name DESC, price ASC LIMIT 3
    // B first (DESC), Y before X (DESC), prices ASC within B-Y
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category DESC, name DESC, price ASC LIMIT 3").await?;

    // First 3: B-Y with prices ASC: 50, 100, 200
    assert_eq!(prices(&results), vec![50, 100, 200]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Verify LIMIT respects secondary column ASC ordering
#[wasm_bindgen_test]
pub async fn test_limit_respects_secondary_order_asc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "Zebra", 100, 1), ("A", "Apple", 100, 2), ("A", "Mango", 100, 3), ("B", "Zebra", 100, 4), ("B", "Apple", 100, 5)],
    )
    .await?;

    // ORDER BY category ASC, name ASC LIMIT 3
    // Should get: A-Apple, A-Mango, A-Zebra (not just any 3 from category A)
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category ASC, name ASC LIMIT 3").await?;

    assert_eq!(names(&results), vec!["Apple", "Mango", "Zebra"], "LIMIT should respect secondary column ASC ordering");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// LIMIT with mixed-direction ORDER BY
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_limit_respects_secondary_order_desc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "Zebra", 100, 1), ("A", "Apple", 100, 2), ("A", "Mango", 100, 3), ("B", "Zebra", 100, 4), ("B", "Apple", 100, 5)],
    )
    .await?;

    // ORDER BY category ASC, name DESC LIMIT 3
    // Should get: A-Zebra, A-Mango, A-Apple
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category ASC, name DESC LIMIT 3").await?;

    assert_eq!(names(&results), vec!["Zebra", "Mango", "Apple"], "LIMIT should respect secondary column DESC ordering");

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Verify LIMIT at category boundary respects secondary sort (same direction)
#[wasm_bindgen_test]
pub async fn test_limit_at_category_boundary() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "Item1", 100, 1), ("A", "Item2", 100, 2), ("B", "Item3", 100, 3), ("B", "Item4", 100, 4), ("C", "Item5", 100, 5)],
    )
    .await?;

    // LIMIT 3 should get both A items and first B item (by name ASC)
    let results = ctx.fetch::<ProductView>("price > 0 ORDER BY category ASC, name ASC LIMIT 3").await?;

    assert_eq!(
        product_tuples(&results),
        vec![("A".into(), "Item1".into(), 100, 1), ("A".into(), "Item2".into(), 100, 2), ("B".into(), "Item3".into(), 100, 3),]
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// Inequality + Multi-Column ORDER BY Tests
// ============================================================================

/// Verify inequality predicate with multi-column ORDER BY (same direction)
#[wasm_bindgen_test]
pub async fn test_inequality_with_secondary_sort() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
            ("Books", "Textbook", 50, 30),
        ],
    )
    .await?;

    // price >= 50 ORDER BY category ASC, name ASC
    let results = ctx.fetch::<ProductView>("price >= 50 ORDER BY category ASC, name ASC").await?;

    // Books: Textbook (50) only - Novel (20) excluded
    // Electronics: all three
    assert_eq!(names(&results), vec!["Textbook", "Laptop", "Phone", "Tablet"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Range query with mixed-direction ORDER BY
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_range_with_secondary_sort() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "P1", 100, 1), ("A", "P2", 200, 2), ("A", "P3", 300, 3), ("B", "P4", 150, 4), ("B", "P5", 250, 5), ("B", "P6", 350, 6)],
    )
    .await?;

    // price >= 150 AND price <= 300 ORDER BY category ASC, name DESC
    let results = ctx.fetch::<ProductView>("price >= 150 AND price <= 300 ORDER BY category ASC, name DESC").await?;

    // A: P3 (300), P2 (200) - P1 excluded
    // B: P5 (250), P4 (150) - P6 excluded
    assert_eq!(names(&results), vec!["P3", "P2", "P5", "P4"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// Equality Prefix + Multi-Column ORDER BY Tests
// ============================================================================

/// Equality prefix with same-direction ORDER BY
#[wasm_bindgen_test]
pub async fn test_equality_prefix_with_secondary_sort_asc() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
        ],
    )
    .await?;

    // category = 'Electronics' ORDER BY name ASC
    let results = ctx.fetch::<ProductView>("category = 'Electronics' ORDER BY name ASC").await?;

    // Within Electronics, sorted by name ASC
    assert_eq!(names(&results), vec!["Laptop", "Phone", "Tablet"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Equality prefix with mixed-direction ORDER BY
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_equality_prefix_with_secondary_sort_mixed() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Laptop", 1000, 5),
            ("Electronics", "Phone", 500, 10),
            ("Electronics", "Tablet", 300, 15),
            ("Books", "Novel", 20, 100),
        ],
    )
    .await?;

    // category = 'Electronics' ORDER BY name ASC, price DESC
    let results = ctx.fetch::<ProductView>("category = 'Electronics' ORDER BY name ASC, price DESC").await?;

    // Within Electronics, sorted by name ASC
    assert_eq!(names(&results), vec!["Laptop", "Phone", "Tablet"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Equality prefix with duplicate secondary values (mixed direction)
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_equality_prefix_with_duplicate_secondary() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Widget", 100, 1),
            ("Electronics", "Widget", 200, 2),
            ("Electronics", "Widget", 50, 3),
            ("Electronics", "Gadget", 150, 4),
        ],
    )
    .await?;

    // category = 'Electronics' ORDER BY name ASC, price DESC
    let results = ctx.fetch::<ProductView>("category = 'Electronics' ORDER BY name ASC, price DESC").await?;

    // Gadget first, then Widget sorted by price DESC
    assert_eq!(
        product_tuples(&results),
        vec![
            ("Electronics".into(), "Gadget".into(), 150, 4),
            ("Electronics".into(), "Widget".into(), 200, 2),
            ("Electronics".into(), "Widget".into(), 100, 1),
            ("Electronics".into(), "Widget".into(), 50, 3),
        ]
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Empty result set with multi-column ORDER BY should not error
#[wasm_bindgen_test]
pub async fn test_empty_result_multi_column_order() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(&ctx, vec![("A", "P1", 100, 1)]).await?;

    let results = ctx.fetch::<ProductView>("category = 'NonExistent' ORDER BY name ASC").await?;

    assert!(results.is_empty());

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// Single result with multi-column ORDER BY
#[wasm_bindgen_test]
pub async fn test_single_result_multi_column_order() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    create_products(&ctx, vec![("A", "P1", 100, 1)]).await?;

    let results = ctx.fetch::<ProductView>("category = 'A' ORDER BY name ASC").await?;

    assert_eq!(results.len(), 1);
    assert_eq!(names(&results), vec!["P1"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// All duplicates in primary column with same-direction sorting
#[wasm_bindgen_test]
pub async fn test_all_duplicates_primary_same_direction() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // All same category, unique names
    create_products(&ctx, vec![("Same", "C", 300, 1), ("Same", "A", 100, 2), ("Same", "B", 200, 3), ("Same", "D", 50, 4)]).await?;

    // ORDER BY category ASC, name ASC
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category ASC, name ASC").await?;

    // All Same category, sorted by name ASC: A, B, C, D
    assert_eq!(names(&results), vec!["A", "B", "C", "D"]);

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}

/// All duplicates in primary column with mixed directions (tertiary sort)
/// Tests mixed-direction ORDER BY using order_by_spill
#[wasm_bindgen_test]
pub async fn test_all_duplicates_primary_mixed_direction() -> Result<(), anyhow::Error> {
    let (ctx, db_name) = setup_context().await?;

    // All same category, different names and prices
    create_products(
        &ctx,
        vec![
            ("Same", "C", 300, 1),
            ("Same", "A", 100, 2),
            ("Same", "B", 200, 3),
            ("Same", "A", 50, 4), // Duplicate name, different price
        ],
    )
    .await?;

    // ORDER BY category ASC, name ASC, price DESC
    let results = ctx.fetch::<ProductView>("stock > 0 ORDER BY category ASC, name ASC, price DESC").await?;

    assert_eq!(
        product_tuples(&results),
        vec![
            ("Same".into(), "A".into(), 100, 2),
            ("Same".into(), "A".into(), 50, 4),
            ("Same".into(), "B".into(), 200, 3),
            ("Same".into(), "C".into(), 300, 1),
        ]
    );

    IndexedDBStorageEngine::cleanup(&db_name).await?;
    Ok(())
}
