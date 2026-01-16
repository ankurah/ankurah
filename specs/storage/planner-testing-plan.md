# Planner Unit Testing Plan

This document describes the exhaustive testing strategy for the query planner's plan generation logic.

**Location**: `storage/common/src/planner.rs`

## What the Planner Does

The planner takes a `Selection` (predicate + ORDER BY + LIMIT) and generates candidate `Plan`s. Each plan specifies:

| Field | Description |
|-------|-------------|
| `index_spec` | Which index columns to use and their directions |
| `bounds` | Key range bounds (equality prefix + inequality range) |
| `scan_direction` | Forward or Reverse cursor movement |
| `order_by_spill` | ORDER BY columns requiring in-memory sort |
| `remaining_predicate` | Predicates not pushed to index (evaluated per-row) |

The planner generates **multiple candidate plans** (Index plan + TableScan fallback). The storage engine chooses which to execute based on index availability.

---

## Test Dimensions

### 1. Predicate Types

| Type | Example | Pushdown Behavior |
|------|---------|-------------------|
| Equality | `name = 'Alice'` | Becomes equality prefix in bounds |
| Greater than | `age > 30` | Exclusive lower bound |
| Greater or equal | `age >= 30` | Inclusive lower bound |
| Less than | `age < 30` | Exclusive upper bound |
| Less or equal | `age <= 30` | Inclusive upper bound |
| Range (same col) | `age > 20 AND age < 40` | Both bounds on same column |
| Multiple equalities | `a = 1 AND b = 2` | Multi-column equality prefix |
| Mixed eq + ineq | `a = 1 AND b > 10` | Equality prefix + inequality |
| OR predicates | `a = 1 OR b = 2` | Cannot push down (remaining_predicate) |
| NOT predicates | `NOT (a = 1)` | Cannot push down |
| IN predicates | `a IN (1, 2, 3)` | Special handling |

### 2. ORDER BY Patterns

| Pattern | Scan Direction | order_by_spill |
|---------|----------------|----------------|
| No ORDER BY | Forward | Empty |
| Single ASC | Forward | Empty |
| Single DESC | Reverse | Empty |
| Multi ASC | Forward | Empty |
| Multi DESC | Reverse | Empty |
| Mixed ASC first | Forward | DESC columns spilled |
| Mixed DESC first | Reverse | ASC columns spilled |

### 3. Predicate + ORDER BY Interactions

| Scenario | index_spec includes | Notes |
|----------|---------------------|-------|
| Equality only | Equality columns | No ORDER BY columns needed |
| ORDER BY only | ORDER BY columns | No predicate columns |
| Eq + ORDER BY same col | Column once | Equality bounds, ordered scan |
| Eq + ORDER BY diff cols | Eq cols + ORDER BY cols | Eq prefix, then ORDER BY |
| Ineq + ORDER BY same col | Column once | Inequality bounds, ordered scan |
| Ineq + ORDER BY diff cols | Ineq col or ORDER BY cols | May need remaining_predicate |

### 4. Special Cases

| Case | Expected Behavior |
|------|-------------------|
| Empty predicate | TableScan with full scan |
| Impossible range (`x > 10 AND x < 5`) | EmptyScan |
| Primary key equality | Optimized single-key lookup |
| JSON path predicates | Path becomes column name |
| Very large numbers | i64::MIN/MAX handled correctly |
| Empty string equality | Valid equality predicate |
| Unicode strings | Proper string handling |

---

## Existing Test Coverage

### ORDER BY Direction Tests ✓

| Test | Pattern | Verified |
|------|---------|----------|
| `test_order_by_desc_single_field` | Single DESC | scan_direction = Reverse |
| `test_order_by_all_desc` | DESC, DESC | scan_direction = Reverse |
| `test_order_by_mixed_directions_asc_first` | ASC, DESC | Forward + spill [DESC] |
| `test_order_by_mixed_directions_desc_first` | DESC, ASC | Reverse + spill [ASC] |
| `test_order_by_mixed_three_fields` | ASC, DESC, DESC | Forward + spill [DESC, DESC] |

### ORDER BY + Predicate Tests ✓

| Test | Scenario | Verified |
|------|----------|----------|
| `basic_order_by` | Collection eq + ORDER BY | Eq bounds + ORDER BY cols in index |
| `order_by_with_covered_inequality` | Eq + ineq + ORDER BY | Eq + ineq bounds, ORDER BY in index |
| `test_order_by_with_equality` | Multi eq + ORDER BY | Multi eq prefix + ORDER BY |
| `test_order_by_with_equality_and_desc` | Eq + DESC ORDER BY | Eq bounds + Reverse scan |
| `test_order_by_with_inequality_and_desc` | Ineq + DESC ORDER BY | Ineq bounds + Reverse scan |

### Full Index Support Tests ✓

These test plans where the index fully satisfies ORDER BY:

| Test | Pattern | Notes |
|------|---------|-------|
| `test_full_support_single_desc` | DESC | No spill needed |
| `test_full_support_mixed_directions` | ASC, DESC | **Sled only** - byte-inversion |
| `test_full_support_all_desc` | DESC, DESC | No spill needed |
| `test_full_support_with_equality_and_mixed_order` | Eq + ASC, DESC | Eq prefix + mixed |

### Bounds Generation Tests ✓

| Test | Predicate | Verified |
|------|-----------|----------|
| `test_single_inequality_plan_structure` | `score > 50` | Open lower bound |
| `test_multiple_inequalities_same_field_plan_structure` | `score > 50 AND score < 100` | Range bounds |
| `test_multiple_inequalities_different_fields_plan_structures` | `score > 50 AND age < 30` | Multiple plans |
| `test_single_equality_plan_structure` | `name = 'Alice'` | Equality bounds |
| `test_multiple_equalities_plan_structure` | `name = 'Alice' AND age = 30` | Multi-eq prefix |
| `test_equality_with_inequality_plan_structure` | `name = 'Alice' AND score > 50` | Eq prefix + ineq |

### Edge Case Tests ✓

| Test | Case | Verified |
|------|------|----------|
| `test_impossible_range` | `age > 50 AND age < 30` | Returns EmptyScan |
| `test_or_only_predicate` | `a = 1 OR b = 2` | Cannot push down |
| `test_complex_nested_predicate` | Nested AND/OR | Partial pushdown |
| `test_large_numbers` | i64 boundaries | Correct bounds |
| `test_empty_string_equality` | `name = ''` | Valid equality |
| `test_empty_string_with_other_fields` | `'' + other eq` | Multi-eq with empty |

### Primary Key Tests ✓

| Test | Case | Verified |
|------|------|----------|
| `test_primary_key_only_equality` | `id = X` | Single-key lookup |
| `test_primary_key_only_with_order_by` | `id = X ORDER BY` | Key lookup + ORDER BY |
| `test_primary_key_with_non_primary_order_by` | `id = X ORDER BY name` | Mixed handling |
| `test_primary_key_not_equal` | `id != X` | Cannot optimize |
| `test_primary_key_range_intersection` | `id > X AND id < Y` | Range bounds |
| `test_mixed_primary_and_secondary_predicates` | `id + other fields` | Combined handling |

### JSON Path Tests ✓

| Test | Case | Verified |
|------|------|----------|
| `test_json_path_equality` | `data.field = X` | Path becomes column |
| `test_json_path_with_order_by` | `data.field + ORDER BY` | Path in index_spec |
| `test_deep_json_path` | `data.a.b.c` | Deep path handling |
| `test_json_path_full_pushdown` | Full pushdown case | No remaining_predicate |
| `test_json_path_inequality` | `data.field > X` | Path with inequality |
| `test_json_path_mixed_predicates` | Multiple JSON paths | Combined handling |

---

## Coverage Status (Updated 2026-01-15)

### All 8 Three-Column Direction Patterns ✅ COMPLETE

| Pattern | Scan | Spilled | Status |
|---------|------|---------|--------|
| ASC, ASC, ASC | Forward | None | ✅ `test_order_by_three_asc_asc_asc` |
| ASC, ASC, DESC | Forward | c | ✅ `test_order_by_three_asc_asc_desc` |
| ASC, DESC, ASC | Forward | b, c | ✅ `test_order_by_three_asc_desc_asc` |
| ASC, DESC, DESC | Forward | b, c | ✅ `test_order_by_mixed_three_fields` |
| DESC, ASC, ASC | Reverse | b, c | ✅ `test_order_by_three_desc_asc_asc` |
| DESC, ASC, DESC | Reverse | b, c | ✅ `test_order_by_three_desc_asc_desc` |
| DESC, DESC, ASC | Reverse | c | ✅ `test_order_by_mixed_three_fields_desc_first` |
| DESC, DESC, DESC | Reverse | None | ✅ `test_order_by_three_desc_desc_desc` |

### Inequality Operator Variants ✅ COMPLETE

| Operator | With ASC | With DESC | Status |
|----------|----------|-----------|--------|
| `>` | ✅ | ✅ | `test_single_inequality_plan_structure`, etc. |
| `>=` | ✅ | ✅ | `test_greater_or_equal_inclusive_lower_bound`, `test_gte_with_desc_order_by` |
| `<` | ✅ | - | `test_less_than_exclusive_upper_bound` |
| `<=` | ✅ | ✅ | `test_less_or_equal_inclusive_upper_bound`, `test_lte_with_desc_order_by` |
| `> AND <` | ✅ | - | `test_multiple_inequalities_same_field_plan_structure` |
| `>= AND <=` | ✅ | - | `test_range_inclusive_both` |
| `> AND <=` | ✅ | - | `test_range_mixed_gt_lte` |
| `>= AND <` | ✅ | - | `test_range_mixed_gte_lt` |

### Equality Prefix Length Variations ✅ COMPLETE

| Eq Prefix | With Ineq | With ORDER BY | Status |
|-----------|-----------|---------------|--------|
| 0 | ✅ | ✅ | Multiple tests |
| 1 | ✅ | ✅ | `test_single_equality_plan_structure` |
| 2 | ✅ | ✅ | `test_multiple_equalities_plan_structure` |
| 3 | ✅ | ✅ | `test_three_equality_with_inequality`, `test_three_equality_with_order_by` |
| 4+ | ✅ | - | `test_four_column_equality_prefix` |

### order_by_spill Verification ✅ COMPLETE

| Test | Verifies |
|------|----------|
| `test_spill_preserves_column_order` | Column order in spill list |
| `test_spill_preserves_directions` | Correct direction per spilled column |
| `test_spill_with_limit` | LIMIT doesn't affect spill needs |
| `test_table_scan_spill_matches_full_order_by` | TableScan spills full ORDER BY |
| `test_no_spill_when_fully_satisfied` | Empty spill when index satisfies |
| `test_equality_prefix_affects_spill` | Equality prefix + ORDER BY interaction |

---

## Comprehensive Test Matrix

### ORDER BY Direction Combinations

All direction patterns should be tested:

```rust
#[test]
fn test_order_by_asc_asc_asc() {
    // ORDER BY a ASC, b ASC, c ASC
    // Expected: Forward scan, no spill
}

#[test]
fn test_order_by_asc_asc_desc() {
    // ORDER BY a ASC, b ASC, c DESC
    // Expected: Forward scan, spill [c DESC]
}

#[test]
fn test_order_by_asc_desc_asc() {
    // ORDER BY a ASC, b DESC, c ASC
    // Expected: Forward scan, spill [b DESC, c ASC]
}

#[test]
fn test_order_by_asc_desc_desc() {
    // ORDER BY a ASC, b DESC, c DESC
    // Expected: Forward scan, spill [b DESC, c DESC]
    // ✓ ALREADY COVERED
}

#[test]
fn test_order_by_desc_asc_asc() {
    // ORDER BY a DESC, b ASC, c ASC
    // Expected: Reverse scan, spill [b ASC, c ASC]
}

#[test]
fn test_order_by_desc_asc_desc() {
    // ORDER BY a DESC, b ASC, c DESC
    // Expected: Reverse scan, spill [b ASC, c DESC]
}

#[test]
fn test_order_by_desc_desc_asc() {
    // ORDER BY a DESC, b DESC, c ASC
    // Expected: Reverse scan, spill [c ASC]
}

#[test]
fn test_order_by_desc_desc_desc() {
    // ORDER BY a DESC, b DESC, c DESC
    // Expected: Reverse scan, no spill
}
```

### Inequality Bound Inclusivity

Each operator with each direction:

```rust
#[test]
fn test_greater_than_asc() {
    // x > 10 ORDER BY x ASC
    // Expected: exclusive lower bound, Forward
}

#[test]
fn test_greater_than_desc() {
    // x > 10 ORDER BY x DESC
    // Expected: exclusive lower bound, Reverse
}

#[test]
fn test_greater_or_equal_asc() {
    // x >= 10 ORDER BY x ASC
    // Expected: inclusive lower bound, Forward
}

#[test]
fn test_greater_or_equal_desc() {
    // x >= 10 ORDER BY x DESC
    // Expected: inclusive lower bound, Reverse
}

// ... similarly for <, <=, and all range combinations
```

### Equality Prefix with ORDER BY Directions

```rust
#[test]
fn test_two_eq_prefix_order_by_desc() {
    // a = 1 AND b = 2 ORDER BY c DESC
    // Expected: eq prefix [a, b], Reverse, no spill
}

#[test]
fn test_three_eq_prefix_order_by_mixed() {
    // a = 1 AND b = 2 AND c = 3 ORDER BY d ASC, e DESC
    // Expected: eq prefix [a, b, c], Forward, spill [e DESC]
}
```

---

## Test Organization

### Current Structure

All planner tests are in `storage/common/src/planner.rs` under `#[cfg(test)] mod tests`.

### Recommended Structure

Keep tests in `planner.rs` but organize into submodules:

```rust
#[cfg(test)]
mod tests {
    mod order_by_directions {
        // All 8 three-column direction patterns
        // Two-column patterns
        // Single-column patterns
    }

    mod bounds_generation {
        // All inequality operators
        // Range combinations
        // Inclusivity verification
    }

    mod equality_prefix {
        // Various prefix lengths
        // Combined with inequality
        // Combined with ORDER BY
    }

    mod order_by_spill {
        // Verify spill contents
        // Verify spill order
        // Verify spill directions
    }

    mod special_cases {
        // Impossible ranges
        // OR predicates
        // JSON paths
        // Primary keys
        // Empty strings
        // Large numbers
    }
}
```

---

## Implementation Phases (Completed 2026-01-15)

### Phase 1: Audit Existing Tests ✅
- [x] Verify all inequality operators have explicit tests
- [x] Verify bounds inclusivity is tested for each operator
- [x] Document which tests cover which operators

### Phase 2: Add Missing Direction Tests ✅
- [x] Add all 8 three-column direction patterns
- [x] Verify order_by_spill contents match expected

### Phase 3: Add Comprehensive Bounds Tests ✅
- [x] Test each operator: `>`, `>=`, `<`, `<=`
- [x] Test each range combination: `> AND <`, `>= AND <=`, `> AND <=`, `>= AND <`
- [x] Test with ASC and DESC ORDER BY for each

### Phase 4: Add Equality Prefix Tests ✅
- [x] Test prefix lengths 0, 1, 2, 3, 4
- [x] Combined with each inequality operator
- [x] Combined with ORDER BY directions

### Phase 5: Verify order_by_spill Correctness ✅
- [x] Add tests that explicitly verify spill column order
- [x] Add tests that verify spill direction per column
- [x] Test spill with LIMIT

---

## Relationship to Execution Tests

The planner tests verify **plan generation is correct**. The execution tests (see `execution-testing-plan.md`) verify **plan execution produces correct results**.

| Aspect | Planner Tests | Execution Tests |
|--------|---------------|-----------------|
| Location | `storage/common/src/planner.rs` | `storage/{sled,indexeddb}/tests/` |
| Verifies | Plan structure | Query results |
| Scope | Pure logic | Storage integration |
| Speed | Fast (no I/O) | Slower (database ops) |
| Failures indicate | Planner bug | Planner or storage bug |

Both are needed:
- Planner tests catch logic errors early
- Execution tests catch integration issues

A bug could be:
1. **Planner generates wrong plan** → Planner test fails
2. **Plan is correct but storage executes wrong** → Execution test fails
3. **Both** → Both fail

---

## Appendix: Planner Test Reorganization Plan

### Guiding Principles

1. **Use `git mv`, not copy-from-memory** - When moving code between files, use editor refactoring tools or careful cut/paste to preserve exact code. Don't rewrite from memory.

2. **Submodules over separate files** - Planner tests benefit from staying in `planner.rs` (close to the code they test). Use `mod` blocks for organization rather than separate files.

3. **Keep related tests together** - Tests for a specific behavior should be in the same submodule even if they cover multiple dimensions.

### Current Planner Test Files

| File | LOC | Tests | Action | Rationale |
|------|-----|-------|--------|-----------|
| `storage/common/src/planner.rs` | ~800 (tests) | 40+ | **REORGANIZE** | Add submodules, keep in same file |
| `storage/sled/tests/normalization_tests.rs` | 381 | ~15 | **KEEP** | Tests Sled-specific `normalize()` |
| `storage/sled/src/planner_integration.rs` | (inline) | ~5 | **KEEP** | Unit tests for Sled bounds conversion |

### Current Test Organization in `planner.rs`

```rust
#[cfg(test)]
mod tests {
    // ~40 tests in flat structure
    // No submodules
    // Tests interleaved by topic

    fn basic_order_by() { ... }
    fn order_by_with_covered_inequality() { ... }
    fn no_collection_field() { ... }
    fn test_order_by_with_equality() { ... }
    fn test_order_by_desc_single_field() { ... }
    // ... etc
}
```

### Recommended Reorganization

```rust
#[cfg(test)]
mod tests {
    // Shared test utilities
    mod helpers {
        // plan!() macro, assertion helpers
    }

    mod order_by_direction {
        // GROUP 1: Direction patterns
        fn test_single_asc() { ... }
        fn test_single_desc() { ... }
        fn test_two_col_asc_asc() { ... }
        fn test_two_col_desc_desc() { ... }
        fn test_two_col_asc_desc() { ... }  // spill
        fn test_two_col_desc_asc() { ... }  // spill
        fn test_three_col_asc_asc_asc() { ... }
        fn test_three_col_asc_asc_desc() { ... }
        fn test_three_col_asc_desc_asc() { ... }
        fn test_three_col_asc_desc_desc() { ... }  // existing
        fn test_three_col_desc_asc_asc() { ... }
        fn test_three_col_desc_asc_desc() { ... }
        fn test_three_col_desc_desc_asc() { ... }  // exists as test_order_by_three_desc_desc_asc
        fn test_three_col_desc_desc_desc() { ... }
    }

    mod bounds_generation {
        // GROUP 2: Inequality operators and bounds
        fn test_gt_exclusive_lower() { ... }
        fn test_gte_inclusive_lower() { ... }
        fn test_lt_exclusive_upper() { ... }
        fn test_lte_inclusive_upper() { ... }
        fn test_range_exclusive_both() { ... }
        fn test_range_inclusive_both() { ... }
        fn test_range_mixed_gt_lte() { ... }
        fn test_range_mixed_gte_lt() { ... }
    }

    mod equality_prefix {
        // GROUP 3: Equality prefix handling
        fn test_single_equality() { ... }
        fn test_two_equalities() { ... }
        fn test_three_equalities() { ... }
        fn test_equality_with_inequality() { ... }
        fn test_equality_with_order_by() { ... }
        fn test_equality_with_both() { ... }
    }

    mod order_by_spill {
        // GROUP 4: Spill verification
        fn test_spill_single_column() { ... }
        fn test_spill_multiple_columns() { ... }
        fn test_spill_preserves_direction() { ... }
        fn test_spill_order_matches_order_by() { ... }
    }

    mod special_cases {
        // GROUP 5: Edge cases and special handling
        fn test_impossible_range_empty_scan() { ... }
        fn test_or_predicate_not_pushed() { ... }
        fn test_primary_key_optimization() { ... }
        fn test_json_path_handling() { ... }
        fn test_empty_string_equality() { ... }
        fn test_large_numbers() { ... }
        fn test_unicode_strings() { ... }
    }
}
```

### Migration Steps

#### Step 1: Add Submodule Structure (No Test Changes)
```rust
// In planner.rs, wrap existing tests in submodules
// Move tests WITHOUT modifying their content

#[cfg(test)]
mod tests {
    mod order_by_direction {
        use super::*;

        // Move existing direction tests here verbatim
        // git diff should show only line moves, not content changes
    }

    // ... other submodules
}
```

#### Step 2: Add Missing Tests to Appropriate Submodules
```rust
mod order_by_direction {
    // ... existing tests ...

    // ADD: Missing three-column patterns
    #[test]
    fn test_three_col_desc_desc_asc() {
        assert_eq!(
            plan!("__collection = 'album' ORDER BY a DESC, b DESC, c ASC"),
            vec![
                Plan::Index {
                    index_spec: KeySpec::new(vec![
                        asc!("__collection", ValueType::String),
                        asc!("a", ValueType::String),
                        asc!("b", ValueType::String)
                    ]),
                    scan_direction: ScanDirection::Reverse,
                    bounds: bounds!("__collection" => ("album"..="album")),
                    remaining_predicate: Predicate::True,
                    order_by_spill: vec![oby_asc!("c")]  // Only c spilled
                },
                // ... TableScan fallback
            ]
        );
    }
}
```

#### Step 3: Verify No Behavior Change
```bash
# Run tests before and after, compare results
cargo test -p ankurah-storage-common planner::tests -- --nocapture > before.txt
# ... make changes ...
cargo test -p ankurah-storage-common planner::tests -- --nocapture > after.txt
diff before.txt after.txt  # Should be empty (same tests, same results)
```

### Related Files - No Changes Needed

#### `storage/sled/tests/normalization_tests.rs`
- Tests the `normalize()` function specific to Sled's bounds handling
- **Action**: KEEP as-is
- These are unit tests for Sled's planner integration layer, not the shared planner

#### `storage/sled/src/planner_integration.rs`
- Contains inline `#[test]` functions for bounds conversion
- **Action**: KEEP as-is
- Engine-specific integration tests belong with that engine

### Verification Checklist

After reorganization:
- [ ] All existing tests still pass
- [ ] Test count unchanged (reorganization, not addition/deletion)
- [ ] `git diff` shows only line movements within the file
- [ ] No test logic was modified during the move
- [ ] New tests added only after reorganization is complete

### Phases

1. Add submodule structure, move existing tests
2. Add missing direction tests (7 patterns)
3. Add bounds operator coverage tests
4. Add order_by_spill verification tests
