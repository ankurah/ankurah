//! Multi-column ORDER BY tests with secondary sort verification.
//!
//! These tests verify that:
//! 1. Secondary columns are correctly sorted when primary column has duplicates
//! 2. Mixed-direction ORDER BY (ASC/DESC combinations) works correctly
//! 3. LIMIT respects secondary column ordering
//! 4. order_by_spill produces correct results when index doesn't fully satisfy ORDER BY

use super::common::*;
use ankurah::Model;
use serde::{Deserialize, Serialize};

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

#[allow(dead_code)]
fn categories(products: &[ProductView]) -> Vec<String> { products.iter().map(|p| p.category().unwrap()).collect() }

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

async fn fetch_products(ctx: &ankurah::Context, query: &str) -> Result<Vec<ProductView>, anyhow::Error> {
    ctx.fetch::<ProductView>(query).await.map_err(Into::into)
}

// ============================================================================
// Secondary Sort Verification Tests
// ============================================================================

/// Verify secondary column is sorted when primary column has duplicates (both ASC)
#[tokio::test]
async fn test_secondary_sort_asc_asc() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price > 0 ORDER BY category ASC, name ASC").await?;

    // Books first (ASC), then Electronics
    // Within each category, names should be ASC
    assert_eq!(
        names(&results),
        vec!["Magazine", "Novel", "Textbook", "Laptop", "Phone", "Tablet"],
        "Secondary column (name) should be sorted ASC within each category"
    );

    Ok(())
}

/// Verify secondary column is sorted DESC when primary is ASC
#[tokio::test]
async fn test_secondary_sort_asc_desc() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price > 0 ORDER BY category ASC, name DESC").await?;

    // Books first (ASC), then Electronics
    // Within each category, names should be DESC
    assert_eq!(
        names(&results),
        vec!["Textbook", "Novel", "Magazine", "Tablet", "Phone", "Laptop"],
        "Secondary column (name) should be sorted DESC within each category"
    );

    Ok(())
}

/// Verify secondary column is sorted ASC when primary is DESC
#[tokio::test]
async fn test_secondary_sort_desc_asc() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price > 0 ORDER BY category DESC, name ASC").await?;

    // Electronics first (DESC), then Books
    // Within each category, names should be ASC
    assert_eq!(
        names(&results),
        vec!["Laptop", "Phone", "Tablet", "Magazine", "Novel", "Textbook"],
        "Secondary column (name) should be sorted ASC within each category (DESC primary)"
    );

    Ok(())
}

/// Verify secondary column is sorted DESC when primary is DESC
#[tokio::test]
async fn test_secondary_sort_desc_desc() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price > 0 ORDER BY category DESC, name DESC").await?;

    // Electronics first (DESC), then Books
    // Within each category, names should be DESC
    assert_eq!(names(&results), vec!["Tablet", "Phone", "Laptop", "Textbook", "Novel", "Magazine"], "Both columns should be sorted DESC");

    Ok(())
}

// ============================================================================
// Three-Column ORDER BY Tests
// ============================================================================

/// Three-column ORDER BY with minimal spill: DESC, DESC, ASC (reverse scan, spill c)
/// This tests the reverse scan direction with only the last column needing spill
#[tokio::test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
async fn test_three_column_desc_desc_asc() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "stock > 0 ORDER BY category DESC, name DESC, price ASC").await?;

    // Debug: print actual results
    eprintln!("Results: {:?}", product_tuples(&results));
    eprintln!("Prices: {:?}", prices(&results));

    // B-Y (prices ASC: 50, 100, 200), B-X (150), A-Z (prices ASC: 250, 300)
    assert_eq!(prices(&results), vec![50, 100, 200, 150, 250, 300]);

    Ok(())
}

/// Verify three-column ORDER BY where each group has unique values
/// Tests that secondary sort determines order when tertiary doesn't matter
#[tokio::test]
async fn test_three_column_order_by_unique_groups() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    // Create products with unique (category, name) pairs - no duplicates requiring tertiary sort
    create_products(&ctx, vec![("A", "X", 100, 1), ("A", "Y", 200, 2), ("A", "Z", 300, 3), ("B", "X", 400, 4), ("B", "Y", 500, 5)]).await?;

    // ORDER BY category ASC, name ASC
    let results = fetch_products(&ctx, "stock > 0 ORDER BY category ASC, name ASC").await?;

    // A group first (name ASC: X, Y, Z), then B group (name ASC: X, Y)
    assert_eq!(names(&results), vec!["X", "Y", "Z", "X", "Y"]);
    assert_eq!(prices(&results), vec![100, 200, 300, 400, 500]);

    Ok(())
}

// ============================================================================
// LIMIT with Multi-Column ORDER BY Tests (TopK Path)
// ============================================================================

/// Two-column mixed direction with LIMIT: DESC, ASC (reverse scan, spill b)
/// Tests TopKStream with reverse scan direction
#[tokio::test]
async fn test_topk_desc_asc_limit() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price > 0 ORDER BY category DESC, name ASC LIMIT 4").await?;

    // C (Apple, Banana, Cherry), B (Date) - stops at 4
    assert_eq!(names(&results), vec!["Apple", "Banana", "Cherry", "Date"]);

    Ok(())
}

/// Three-column with LIMIT: ASC, ASC, DESC (forward scan, spill c)
/// Tests TopKStream with forward scan and tertiary spill
#[tokio::test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
async fn test_topk_three_column_asc_asc_desc_limit() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(&ctx, vec![("A", "X", 300, 1), ("A", "X", 100, 2), ("A", "X", 200, 3), ("A", "Y", 150, 4), ("B", "X", 400, 5)]).await?;

    // ORDER BY category ASC, name ASC, price DESC LIMIT 3
    // A-X group (prices DESC: 300, 200, 100), then A-Y, then B-X
    let results = fetch_products(&ctx, "stock > 0 ORDER BY category ASC, name ASC, price DESC LIMIT 3").await?;

    // First 3: A-X with prices 300, 200, 100
    assert_eq!(prices(&results), vec![300, 200, 100]);

    Ok(())
}

/// Three-column with LIMIT: DESC, DESC, ASC (reverse scan, spill c)
/// Tests TopKStream with reverse scan and tertiary spill
#[tokio::test]
#[ignore] // Blocked by #210: i64 values sorted lexicographically instead of numerically
async fn test_topk_three_column_desc_desc_asc_limit() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(&ctx, vec![("B", "Y", 200, 1), ("B", "Y", 50, 2), ("B", "Y", 100, 3), ("B", "X", 150, 4), ("A", "Z", 300, 5)]).await?;

    // ORDER BY category DESC, name DESC, price ASC LIMIT 3
    // B first (DESC), Y before X (DESC), prices ASC within B-Y
    let results = fetch_products(&ctx, "stock > 0 ORDER BY category DESC, name DESC, price ASC LIMIT 3").await?;

    // First 3: B-Y with prices ASC: 50, 100, 200
    assert_eq!(prices(&results), vec![50, 100, 200]);

    Ok(())
}

/// Verify LIMIT respects secondary column ordering
#[tokio::test]
async fn test_limit_respects_secondary_order() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "Zebra", 100, 1), ("A", "Apple", 100, 2), ("A", "Mango", 100, 3), ("B", "Zebra", 100, 4), ("B", "Apple", 100, 5)],
    )
    .await?;

    // ORDER BY category ASC, name ASC LIMIT 3
    // Should get: A-Apple, A-Mango, A-Zebra (not just any 3 from category A)
    let results = fetch_products(&ctx, "price > 0 ORDER BY category ASC, name ASC LIMIT 3").await?;

    assert_eq!(names(&results), vec!["Apple", "Mango", "Zebra"], "LIMIT should respect secondary column ASC ordering");

    // ORDER BY category ASC, name DESC LIMIT 3
    // Should get: A-Zebra, A-Mango, A-Apple
    let results = fetch_products(&ctx, "price > 0 ORDER BY category ASC, name DESC LIMIT 3").await?;

    assert_eq!(names(&results), vec!["Zebra", "Mango", "Apple"], "LIMIT should respect secondary column DESC ordering");

    Ok(())
}

/// Verify LIMIT at category boundary respects secondary sort
#[tokio::test]
async fn test_limit_at_category_boundary() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "Item1", 100, 1), ("A", "Item2", 100, 2), ("B", "Item3", 100, 3), ("B", "Item4", 100, 4), ("C", "Item5", 100, 5)],
    )
    .await?;

    // LIMIT 3 should get both A items and first B item (by name ASC)
    let results = fetch_products(&ctx, "price > 0 ORDER BY category ASC, name ASC LIMIT 3").await?;

    assert_eq!(
        product_tuples(&results),
        vec![("A".into(), "Item1".into(), 100, 1), ("A".into(), "Item2".into(), 100, 2), ("B".into(), "Item3".into(), 100, 3),]
    );

    Ok(())
}

// ============================================================================
// Inequality + Multi-Column ORDER BY Tests
// ============================================================================

/// Verify inequality predicate with multi-column ORDER BY
#[tokio::test]
async fn test_inequality_with_secondary_sort() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "price >= 50 ORDER BY category ASC, name ASC").await?;

    // Books: Textbook (50) only - Novel (20) excluded
    // Electronics: all three
    assert_eq!(names(&results), vec!["Textbook", "Laptop", "Phone", "Tablet"]);

    Ok(())
}

/// Verify range query with multi-column ORDER BY
#[tokio::test]
async fn test_range_with_secondary_sort() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(
        &ctx,
        vec![("A", "P1", 100, 1), ("A", "P2", 200, 2), ("A", "P3", 300, 3), ("B", "P4", 150, 4), ("B", "P5", 250, 5), ("B", "P6", 350, 6)],
    )
    .await?;

    // price >= 150 AND price <= 300 ORDER BY category ASC, name DESC
    let results = fetch_products(&ctx, "price >= 150 AND price <= 300 ORDER BY category ASC, name DESC").await?;

    // A: P3 (300), P2 (200) - P1 excluded
    // B: P5 (250), P4 (150) - P6 excluded
    assert_eq!(names(&results), vec!["P3", "P2", "P5", "P4"]);

    Ok(())
}

// ============================================================================
// Equality Prefix + Multi-Column ORDER BY Tests
// ============================================================================

/// Verify equality prefix with multi-column ORDER BY on remaining columns
#[tokio::test]
async fn test_equality_prefix_with_secondary_sort() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

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
    let results = fetch_products(&ctx, "category = 'Electronics' ORDER BY name ASC, price DESC").await?;

    // Within Electronics, sorted by name ASC
    assert_eq!(names(&results), vec!["Laptop", "Phone", "Tablet"]);

    Ok(())
}

/// Verify equality prefix with secondary sort (unique secondary values)
#[tokio::test]
async fn test_equality_prefix_with_unique_secondary() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(
        &ctx,
        vec![
            ("Electronics", "Widget", 100, 1),
            ("Electronics", "Gadget", 150, 2),
            ("Electronics", "Tablet", 200, 3),
            ("Electronics", "Phone", 50, 4),
        ],
    )
    .await?;

    // category = 'Electronics' ORDER BY name ASC
    let results = fetch_products(&ctx, "category = 'Electronics' ORDER BY name ASC").await?;

    // Sorted by name ASC: Gadget, Phone, Tablet, Widget
    assert_eq!(names(&results), vec!["Gadget", "Phone", "Tablet", "Widget"]);

    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Empty result set with multi-column ORDER BY should not error
#[tokio::test]
async fn test_empty_result_multi_column_order() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(&ctx, vec![("A", "P1", 100, 1)]).await?;

    let results = fetch_products(&ctx, "category = 'NonExistent' ORDER BY name ASC, price DESC").await?;

    assert!(results.is_empty());

    Ok(())
}

/// Single result with multi-column ORDER BY
#[tokio::test]
async fn test_single_result_multi_column_order() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    create_products(&ctx, vec![("A", "P1", 100, 1)]).await?;

    let results = fetch_products(&ctx, "category = 'A' ORDER BY name ASC, price DESC").await?;

    assert_eq!(results.len(), 1);
    assert_eq!(names(&results), vec!["P1"]);

    Ok(())
}

/// All duplicates in primary column, verify secondary sort (unique secondary)
#[tokio::test]
async fn test_all_duplicates_primary() -> Result<(), anyhow::Error> {
    let ctx = setup_context().await?;

    // All same category, unique names
    create_products(&ctx, vec![("Same", "C", 300, 1), ("Same", "A", 100, 2), ("Same", "B", 200, 3), ("Same", "D", 50, 4)]).await?;

    // ORDER BY category ASC, name ASC
    let results = fetch_products(&ctx, "stock > 0 ORDER BY category ASC, name ASC").await?;

    // All Same category, sorted by name ASC: A, B, C, D
    assert_eq!(names(&results), vec!["A", "B", "C", "D"]);

    Ok(())
}
