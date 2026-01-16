# Index Pushdown Execution Testing Plan

This document describes the **exhaustive** testing strategy for index pushdown **execution** across storage engines (Sled, IndexedDB).

**Related**: See `planner-testing-plan.md` for planner unit test coverage.

## What These Tests Cover

Execution tests verify that queries return **correct results in correct order**:
- Bounds are correctly translated to storage-native ranges
- Cursor direction produces expected ordering
- `order_by_spill` correctly sorts spilled columns
- LIMIT terminates scan early
- Edge cases handled correctly

**Location**: `storage/{sled,indexeddb-wasm}/tests/`

---

## Test Dimensions

| Dimension | Values | Combinations |
|-----------|--------|--------------|
| ORDER BY column count | 1, 2, 3 | 3 |
| Direction pattern | See below | Varies |
| Inequality operator | `>`, `>=`, `<`, `<=`, ranges | 8 |
| Equality prefix length | 0, 1, 2, 3 | 4 |
| Predicate/ORDER BY alignment | Same col, different col | 2 |
| LIMIT | None, partial, exact, over, 0, 1 | 6 |
| Result characteristics | Empty, single, duplicates | 3 |

### Direction Patterns by Column Count

| Columns | Patterns | For IndexedDB |
|---------|----------|---------------|
| 1 | ASC, DESC | 2 - both native |
| 2 | ASC/ASC, DESC/DESC, ASC/DESC, DESC/ASC | 2 native, 2 need spill |
| 3 | All 8 combinations (2³) | 2 native, 6 need spill |

**Full theoretical matrix**: Would be 10,000+ combinations. We use strategic coverage instead.

---

## Storage Engine Behavior

### Sled
- **Index keys**: Byte-inversion encodes direction per-column
- **Native support**: All direction combinations work via index key design
- **order_by_spill**: Implemented but rarely used in practice (indexes created on-demand with exact directions)
- **Future-proofing**: order_by_spill implementation retained for when we support pre-defined indexes with fixed directions

### IndexedDB
- **Index keys**: All components stored ASC only
- **Native support**: Only uniform directions (all ASC or all DESC via reverse scan)
- **order_by_spill**: ✅ **IMPLEMENTED** - uses `SortedStream`/`TopKStream` from storage-common (partition-aware sorting)
- **Spill rules**: For `ORDER BY a <DIR1>, b <DIR2>, c <DIR3>`:
  - Forward scan: First column where DIR ≠ ASC must be spilled (and all after)
  - Reverse scan: First column where DIR ≠ DESC must be spilled (and all after)

### IndexedDB Spill Examples

| ORDER BY Pattern | Scan Direction | Columns Spilled | Notes |
|------------------|----------------|-----------------|-------|
| `a ASC` | Forward | None | Native |
| `a DESC` | Reverse | None | Native |
| `a ASC, b ASC` | Forward | None | Native |
| `a DESC, b DESC` | Reverse | None | Native |
| `a ASC, b DESC` | Forward | b | b needs DESC, comes out ASC |
| `a DESC, b ASC` | Reverse | b | b needs ASC, comes out DESC |
| `a ASC, b ASC, c DESC` | Forward | c | Minimal spill case |
| `a DESC, b DESC, c ASC` | Reverse | c | Minimal spill case - added but `#[ignore]` (#210) |
| `a ASC, b DESC, c DESC` | Forward | b, c | b breaks, rest spilled |
| `a DESC, b ASC, c ASC` | Reverse | b, c | b breaks, rest spilled |
| `a ASC, b DESC, c ASC` | Forward | b, c | b breaks, rest spilled |
| `a DESC, b ASC, c DESC` | Reverse | b, c | b breaks, rest spilled |

---

## Exhaustive Coverage Matrix

### Single Column ORDER BY

**Planner tests**: Verify correct `scan_direction` and `bounds`
**Execution tests**: Verify correct result order and count

#### Without Equality Prefix (eq_prefix = 0)

| Operator | ASC | DESC | Planner | Sled | IDB |
|----------|-----|------|---------|------|-----|
| `>` | Forward, exclusive lower | Reverse, exclusive lower | ✓ | ✓ | ✓ |
| `>=` | Forward, inclusive lower | Reverse, inclusive lower | ✓ | ✓ | ✓ |
| `<` | Forward, exclusive upper | Reverse, exclusive upper | ✓ | ✓ | ✓ |
| `<=` | Forward, inclusive upper | Reverse, inclusive upper | ✓ | ✓ | ✓ |
| `> AND <` | Forward, exclusive range | Reverse, exclusive range | ✓ | ✓ | ✓ |
| `>= AND <=` | Forward, inclusive range | Reverse, inclusive range | ✓ | ✓ | ✓ |
| `> AND <=` | Forward, mixed range | Reverse, mixed range | ✓ | ✓ | ✓ |
| `>= AND <` | Forward, mixed range | Reverse, mixed range | ✓ | ✓ | ✓ |

**Subtotal: 16 test cases**

#### With 1 Equality Prefix (eq_prefix = 1)

Pattern: `category = 'X' AND timestamp <OP> Y ORDER BY timestamp <DIR>`

| Operator | ASC | DESC | Notes |
|----------|-----|------|-------|
| `>` | ✓ | ✓ | Bounds within eq prefix |
| `>=` | ✓ | ✓ | Bounds within eq prefix |
| `<` | ✓ | ✓ | Bounds within eq prefix |
| `<=` | ✓ | ✓ | Bounds within eq prefix |
| `> AND <` | ✓ | ✓ | Range within eq prefix |
| `>= AND <=` | ✓ | ✓ | Range within eq prefix |
| `> AND <=` | ✓ | ✓ | Range within eq prefix |
| `>= AND <` | ✓ | ✓ | Range within eq prefix |

**Subtotal: 16 test cases**

#### With 2 Equality Prefix (eq_prefix = 2)

Pattern: `room = 'X' AND deleted = false AND timestamp <OP> Y ORDER BY timestamp <DIR>`

This is the "chat message" pattern that triggered the original bug.

| Operator | ASC | DESC | Notes |
|----------|-----|------|-------|
| `>` | ✓ | ✓ | |
| `>=` | ✓ | **FIXED** | Was bug: DESC + >= + 2eq returned 0 |
| `<` | ✓ | ✓ | |
| `<=` | ✓ | ✓ | |
| `> AND <` | ✓ | ✓ | |
| `>= AND <=` | ✓ | ✓ | |
| `> AND <=` | ✓ | ✓ | |
| `>= AND <` | ✓ | ✓ | |

**Subtotal: 16 test cases**

#### With 3 Equality Prefix (eq_prefix = 3)

Pattern: `org = 'X' AND room = 'Y' AND deleted = false AND timestamp <OP> Z ORDER BY timestamp <DIR>`

| Operator | ASC | DESC |
|----------|-----|------|
| `>` | ✓ | ✓ |
| `>=` | ✓ | ✓ |
| `<` | ✓ | ✓ |
| `<=` | ✓ | ✓ |
| (ranges) | ✓ | ✓ |

**Subtotal: 16 test cases**

**Single column total: 64 test cases**

---

### Two Column ORDER BY - Same Direction

Native for both Sled and IndexedDB.

#### ASC, ASC (Forward scan)

| Eq Prefix | Operators | Sled | IDB | Notes |
|-----------|-----------|------|-----|-------|
| 0 | All 8 | ✓ | ✓ | Secondary sort verified with duplicates |
| 1 | All 8 | ✓ | ✓ | |
| 2 | All 8 | ✓ | ✓ | |

**Subtotal: 24 test cases**

#### DESC, DESC (Reverse scan)

| Eq Prefix | Operators | Sled | IDB | Notes |
|-----------|-----------|------|-----|-------|
| 0 | All 8 | ✓ | ✓ | Secondary sort verified with duplicates |
| 1 | All 8 | ✓ | ✓ | |
| 2 | All 8 | ✓ | ✓ | |

**Subtotal: 24 test cases**

**Two column same-dir total: 48 test cases**

---

### Two Column ORDER BY - Mixed Direction

**Sled**: Native (byte-inversion)
**IndexedDB**: Requires order_by_spill for second column

#### ASC, DESC (Forward scan, spill col2 for DESC)

| Eq Prefix | Operators | Sled | IDB | Notes |
|-----------|-----------|------|-----|-------|
| 0 | All 8 | ✓ | ✅ | Needs order_by_spill |
| 1 | All 8 | ✓ | ✅ | |
| 2 | All 8 | ✓ | ✅ | |

#### DESC, ASC (Reverse scan, spill col2 for ASC)

| Eq Prefix | Operators | Sled | IDB | Notes |
|-----------|-----------|------|-----|-------|
| 0 | All 8 | ✓ | ✅ | Needs order_by_spill |
| 1 | All 8 | ✓ | ✅ | |
| 2 | All 8 | ✓ | ✅ | |

**Two column mixed-dir total: 48 test cases** ✅

---

### Three Column ORDER BY

Test with data having duplicates in first two columns to verify tertiary sort.

#### All Same Direction (Native)

| Pattern | Scan | Spilled | Sled | IDB |
|---------|------|---------|------|-----|
| ASC, ASC, ASC | Forward | None | ✓ | ✓ |
| DESC, DESC, DESC | Reverse | None | ✓ | ✓ |

**Subtotal: 2 test cases (native on both)**

#### Minimal Spill (Only col3 spilled)

| Pattern | Scan | Spilled | Sled | IDB | Notes |
|---------|------|---------|------|-----|-------|
| ASC, ASC, DESC | Forward | c | ✓ | ✅ | |
| DESC, DESC, ASC | Reverse | c | ⏸️ | ⏸️ | Added but `#[ignore]` - blocked by #210 |

**Subtotal: 2 test cases (1 passing, 1 ignored pending #210)**

#### Col2 Breaks (col2 and col3 spilled)

| Pattern | Scan | Spilled | Sled | IDB |
|---------|------|---------|------|-----|
| ASC, DESC, ASC | Forward | b, c | ✓ | ✅ |
| ASC, DESC, DESC | Forward | b, c | ✓ | ✅ |
| DESC, ASC, ASC | Reverse | b, c | ✓ | ✅ |
| DESC, ASC, DESC | Reverse | b, c | ✓ | ✅ |

**Subtotal: 4 test cases** ✅

**Three column total: 8 test cases** ✅

---

### Mixed Direction + LIMIT (TopK Path)

Tests that exercise `TopKStream` with partition-aware sorting. These are critical because TopK has different code paths than simple sort.

#### Two Column Mixed + LIMIT

| Pattern | Scan | Spilled | Sled | IDB | Notes |
|---------|------|---------|------|-----|-------|
| ASC, DESC LIMIT N | Forward | b | ✓ | ✓ | Has test |
| DESC, ASC LIMIT N | Reverse | b | ✓ | ✓ | Added (string cols only) |

#### Three Column + LIMIT

| Pattern | Scan | Spilled | Sled | IDB | Notes |
|---------|------|---------|------|-----|-------|
| ASC, ASC, DESC LIMIT N | Forward | c | ⏸️ | ⏸️ | Added but `#[ignore]` - blocked by #210 |
| DESC, DESC, ASC LIMIT N | Reverse | c | ⏸️ | ⏸️ | Added but `#[ignore]` - blocked by #210 |

**Subtotal: 4 test cases (2 passing, 2 ignored pending #210)**

---

### Predicate Not On ORDER BY Column

Filter by column A, sort by column B. Tests remaining_predicate handling.

| Scenario | Planner | Sled | IDB |
|----------|---------|------|-----|
| `price > 100 ORDER BY name ASC` | ✓ | ✓ | ✓ |
| `price > 100 ORDER BY name DESC` | ✓ | ✓ | ✓ |
| `price > 100 ORDER BY category ASC, name ASC` | ✓ | ✓ | ✓ |
| `price > 100 ORDER BY category DESC, name DESC` | ✓ | ✓ | ✓ |
| `price > 100 ORDER BY category ASC, name DESC` | ✓ | ✓ | ✅ |
| `price > 100 AND stock < 50 ORDER BY name ASC` | ✓ | ✓ | ✓ |
| `price > 100 AND stock < 50 ORDER BY name DESC` | ✓ | ✓ | ✓ |

**Subtotal: 7 test cases** ✅

---

### Edge Cases

| Scenario | Sled | IDB | Notes |
|----------|------|-----|-------|
| Empty result set | ✓ | ✓ | Query matches nothing |
| Single result | ✓ | ✓ | Exactly one match |
| All duplicates in primary col | ✓ | ✓ | Verify secondary sort |
| LIMIT = 0 | ✓ | ✓ | Should return empty |
| LIMIT = 1 | ✓ | ✓ | Minimum non-empty |
| LIMIT < result count | ✓ | ✓ | Early termination |
| LIMIT = result count | ✓ | ✓ | Exact match |
| LIMIT > result count | ✓ | ✓ | No truncation |
| MIN_INT boundary | ✓ | ✓ | i64::MIN in bounds |
| MAX_INT boundary | ✓ | ✓ | i64::MAX in bounds |
| Empty string in equality | ✓ | ✓ | `name = ''` |
| Unicode in equality | ✓ | ✓ | `name = '日本語'` |

**Subtotal: 12 test cases**

---

## Coverage Summary

| Category | Test Cases | Sled | IDB | Notes |
|----------|------------|------|-----|-------|
| Single column | 64 | 64 | 64 | |
| Two column same-dir | 48 | 48 | 48 | |
| Two column mixed-dir | 48 | 48 | 48 | ✅ |
| Three column | 8 | 7+1⏸️ | 7+1⏸️ | 1 ignored (#210) |
| Mixed-dir + LIMIT (TopK) | 4 | 2+2⏸️ | 2+2⏸️ | 2 ignored (#210) |
| Predicate not on ORDER BY | 7 | 7 | 7 | ✅ |
| Edge cases | 12 | 12 | 12 | |
| **TOTAL** | **191** | **188+3⏸️** | **188+3⏸️** | |

**order_by_spill: Implemented 2026-01-15** ✅
**Mixed-dir + LIMIT tests: All added (2 passing, 2 ignored pending #210)**
**Three-column tests: 1 added but ignored pending #210**

---

## Required Implementations

### ~~P0: IndexedDB order_by_spill~~ ✅ COMPLETED (2026-01-15)

**File**: `storage/indexeddb-wasm/src/collection.rs`

**Implementation**: Uses `SortedStream` and `TopKStream` from `storage/common/src/sorting.rs` via the `ValueSetStream` trait. This provides partition-aware sorting (sorts within presort partitions, not globally).

Key changes:
- `IdbRecord` struct implements `Filterable` trait
- Uses `.sort_by(order_by_spill)` for partition-aware sorting
- Uses `.top_k(order_by_spill, limit)` for partition-aware TopK with LIMIT
- Extracts both presort and spill column values for partition detection

**Commit**: `0b680ee` (fix: IndexedDB uses partition-aware sorting from storage-common)

### ~~P1: Add Missing Tests (Both Sled and IndexedDB)~~ ✅ COMPLETED (2026-01-16)

All tests have been added to both storage engines:

| # | Pattern | Status | Notes |
|---|---------|--------|-------|
| 1 | `ORDER BY a DESC, b DESC, c ASC` | ⏸️ `#[ignore]` | Blocked by #210 (i64 lexicographic sort) |
| 2 | `ORDER BY a DESC, b ASC LIMIT N` | ✅ Passing | Uses string columns only |
| 3 | `ORDER BY a ASC, b ASC, c DESC LIMIT N` | ⏸️ `#[ignore]` | Blocked by #210 (i64 lexicographic sort) |
| 4 | `ORDER BY a DESC, b DESC, c ASC LIMIT N` | ⏸️ `#[ignore]` | Blocked by #210 (i64 lexicographic sort) |

**Locations**:
- Sled: `tests/tests/sled/multi_column_order_by.rs`
- IndexedDB: `storage/indexeddb-wasm/tests/multi_column_order_by.rs`

**Note**: Tests 1, 3, 4 ORDER BY `price` (i64) and fail because the planner hardcodes `ValueType::String` for ORDER BY columns. Fix requires schema-aware type resolution (tracked in #210).

---

## Known Bugs - Fixed

### 1. DESC + >= + 2 Equality Prefix (IndexedDB)

**Query**: `room = X AND deleted = false AND timestamp >= Y ORDER BY timestamp DESC`

**Symptom**: Returned 0 results instead of expected 5.

**Root cause**: `lowerBound([room, deleted, ts])` with `Prev` cursor started at max key in index (outside equality prefix). Prefix guard saw wrong prefix and terminated immediately.

**Fix**: In `plan_bounds_to_idb_range()`, cap upper bound at equality prefix boundary for Reverse scans with open-ended upper bound.

**PR**: #XXX

---

## Test File Organization

### Current Structure (Scattered)

```
storage/sled/tests/
  multi_column_order_by.rs      # 455 LOC - 95% overlap with IDB
  normalization_tests.rs        # 381 LOC - unique planner internals
  collection_boundary.rs        # 59 LOC - specialized
  inclusion_and_ordering.rs     # 54 LOC - 100% overlap with IDB
  pagination.rs                 # 48 LOC - redundant

storage/indexeddb-wasm/tests/
  desc_ordering.rs              # 498 LOC - 90% overlap with desc_inequality
  multi_column_order_by.rs      # 590 LOC - 95% overlap with Sled
  inclusion_and_ordering.rs     # 239 LOC - 100% overlap with Sled
  edge_cases.rs                 # 182 LOC - IDB-specific

tests/tests/
  desc_inequality.rs            # 691 LOC - comprehensive reference
  pagination_cursor.rs          # 356 LOC - E2E pagination
```

**Total: 3,553 LOC with ~40% redundancy**

### Recommended Structure

```
tests/tests/index_pushdown/
  mod.rs                        # Shared test harness, engine abstraction
  single_column.rs              # 64 cases × 2 engines
  two_column_same_dir.rs        # 48 cases × 2 engines
  two_column_mixed_dir.rs       # 48 cases (Sled native, IDB needs spill)
  three_column.rs               # 8 cases
  predicate_not_on_orderby.rs   # 7 cases
  edge_cases.rs                 # 12 cases

storage/common/src/planner.rs   # Planner unit tests (see planner-testing-plan.md)
storage/sled/tests/
  normalization_tests.rs        # Keep - unique planner internals
  collection_boundary.rs        # Keep - specialized
storage/indexeddb-wasm/tests/
  edge_cases.rs                 # Keep - IDB-specific encoding
```

**Estimated: ~2,100 LOC with 0% redundancy**

---

## Implementation Phases

### Phase 1: Implement order_by_spill for IndexedDB ✅ COMPLETED
- [x] Add partition-aware sorting using `SortedStream`/`TopKStream` from storage-common
- [x] Mixed-direction tests now pass
- [x] All previously blocked tests now work

### Phase 2: Add Missing Test Cases (Both Sled and IndexedDB) ✅ COMPLETED
- [x] `ORDER BY a DESC, b DESC, c ASC` (3-col minimal spill) - added, `#[ignore]` pending #210
- [x] `ORDER BY a DESC, b ASC LIMIT N` (2-col TopK) - added, passing
- [x] `ORDER BY a ASC, b ASC, c DESC LIMIT N` (3-col TopK) - added, `#[ignore]` pending #210
- [x] `ORDER BY a DESC, b DESC, c ASC LIMIT N` (3-col TopK) - added, `#[ignore]` pending #210
- [x] Verify equality prefix 3 tests exist - yes, in `desc_inequality.rs` line 278
- [ ] Add LIMIT = 0 edge case - not yet added (low priority)

### Phase 3: Consolidate Test Files
- [ ] Create unified test harness with engine abstraction
- [ ] Migrate scenarios from scattered files
- [ ] Remove redundant files
- [ ] Verify coverage maintained

### Phase 4: Planner Unit Test Audit
See `planner-testing-plan.md` for detailed planner test coverage and gaps.

---

## Appendix: Sled order_by_spill Note

Sled's `order_by_spill` implementation is retained for **future-proofing**. Currently:
- Indexes are created on-demand with exact direction patterns
- The planner requests indexes matching the ORDER BY exactly
- Therefore order_by_spill is rarely/never populated for Sled

However, this may change if we support:
- Pre-defined indexes with fixed direction patterns
- Index reuse across different query patterns
- Cost-based optimization choosing suboptimal-direction indexes

The implementation cost is minimal and the test coverage ensures correctness when needed.

---

## Appendix: Test File Reorganization Plan

### Guiding Principles

1. **Use `git mv`, not copy-from-memory** - Preserve git history by moving/renaming files rather than recreating content from scratch. This maintains blame history and makes reviews easier.

2. **Extract scenarios, don't rewrite** - When consolidating, extract the test *scenarios* (setup data, query, expected results) into shared structures. Don't rewrite test logic from memory.

3. **Keep engine-specific tests in place** - Tests for storage-specific behavior (encoding edge cases, collection boundaries) stay with that storage engine.

4. **Unified cross-engine tests in `tests/tests/`** - Tests that should run identically on both engines go in the main integration test directory.

5. **One migration per PR** - Move one file at a time to keep PRs reviewable.

### Current Execution Test Files

#### Sled Tests (`storage/sled/tests/`)

| File | LOC | Overlap | Action | Rationale |
|------|-----|---------|--------|-----------|
| `multi_column_order_by.rs` | 455 | 95% w/ IDB | **MIGRATE** | Extract scenarios to unified tests |
| `normalization_tests.rs` | 381 | None | **KEEP** | Unique - tests Sled's `normalize()` function |
| `collection_boundary.rs` | 59 | None | **KEEP** | Unique - tests collection prefix guard |
| `inclusion_and_ordering.rs` | 54 | 100% w/ IDB | **DELETE** | Exact duplicate, keep IDB version during migration |
| `pagination.rs` | 48 | Subset | **DELETE** | Redundant with `pagination_cursor.rs` |

#### IndexedDB Tests (`storage/indexeddb-wasm/tests/`)

| File | LOC | Overlap | Action | Rationale |
|------|-----|---------|--------|-----------|
| `desc_ordering.rs` | 498 | 90% w/ desc_ineq | **MIGRATE** | Extract scenarios to unified tests |
| `multi_column_order_by.rs` | 590 | 95% w/ Sled | **MIGRATE** | Extract scenarios to unified tests |
| `inclusion_and_ordering.rs` | 239 | 100% w/ Sled | **MIGRATE** | Use as base for unified version |
| `edge_cases.rs` | 182 | None | **KEEP** | Unique - IndexedDB encoding edge cases |
| `predicate_checks.rs` | 141 | N/A | **KEEP** | Not ORDER BY focused |

#### Integration Tests (`tests/tests/`)

| File | LOC | Overlap | Action | Rationale |
|------|-----|---------|--------|-----------|
| `desc_inequality.rs` | 691 | Medium | **MIGRATE** | Good template for unified tests |
| `pagination_cursor.rs` | 356 | Medium | **KEEP** | E2E LiveQuery pagination, higher-level |
| `where_clause.rs` | 134 | N/A | **KEEP** | WHERE syntax parsing, not ORDER BY |

### Target Structure

```
tests/tests/index_pushdown/
├── mod.rs                      # Test harness with engine abstraction
├── single_column.rs            # git mv from desc_ordering.rs, refactor
├── two_column_same_dir.rs      # git mv from multi_column_order_by.rs, refactor
├── two_column_mixed_dir.rs     # git mv from multi_column_order_by.rs, refactor
├── three_column.rs             # NEW - add missing cases
├── predicate_not_on_orderby.rs # Extract from desc_inequality.rs
└── edge_cases.rs               # Extract from various files

storage/sled/tests/
├── normalization_tests.rs      # KEEP - Sled-specific
└── collection_boundary.rs      # KEEP - Sled-specific

storage/indexeddb-wasm/tests/
├── edge_cases.rs               # KEEP - IDB-specific encoding
└── predicate_checks.rs         # KEEP - not ORDER BY focused
```

### Migration Steps

#### Step 0: Preparation
```bash
# Create the new directory structure
mkdir -p tests/tests/index_pushdown
```

#### Step 1: Create Test Harness
Create `tests/tests/index_pushdown/mod.rs` with:
- `TestCase` struct (setup data, query, expected order)
- `run_on_sled()` and `run_on_indexeddb()` functions
- Macros for declaring test cases that run on both engines

#### Step 2: Migrate `desc_inequality.rs` (Best Starting Point)
```bash
# Move the file, preserving history
git mv tests/tests/desc_inequality.rs tests/tests/index_pushdown/single_column.rs

# Then refactor in place to use the harness
# The scenarios are already well-structured
```

#### Step 3: Migrate `inclusion_and_ordering.rs`
```bash
# Use the IndexedDB version as base (slightly more complete)
git mv storage/indexeddb-wasm/tests/inclusion_and_ordering.rs tests/tests/index_pushdown/inclusion_ordering.rs

# Delete the Sled duplicate
git rm storage/sled/tests/inclusion_and_ordering.rs

# Refactor to use harness, run on both engines
```

#### Step 4: Migrate Multi-Column Tests
```bash
# Use IndexedDB version as base (more LOC = more scenarios)
git mv storage/indexeddb-wasm/tests/multi_column_order_by.rs tests/tests/index_pushdown/two_column_same_dir.rs

# Delete Sled duplicate
git rm storage/sled/tests/multi_column_order_by.rs

# Split into two_column_same_dir.rs and two_column_mixed_dir.rs
# Add three_column.rs with missing direction patterns
```

#### Step 5: Migrate DESC Ordering
```bash
git mv storage/indexeddb-wasm/tests/desc_ordering.rs tests/tests/index_pushdown/desc_scenarios.rs

# Merge unique scenarios into single_column.rs
# Delete desc_scenarios.rs after extraction
```

#### Step 6: Cleanup
```bash
# Delete redundant files
git rm storage/sled/tests/pagination.rs

# Verify all 187 test cases are covered
# Run full test suite to confirm no regressions
```

### File-by-File Migration Notes

#### `desc_inequality.rs` → `single_column.rs`
- Already well-organized by equality prefix length
- Has comprehensive operator coverage
- Good starting point - use as template for test case structure

#### `multi_column_order_by.rs` (both versions) → `two_column_*.rs` + `three_column.rs`
- Split by direction pattern (same vs mixed)
- Add missing three-column patterns
- The `#[ignore]` tests become un-ignored after order_by_spill is implemented

#### `desc_ordering.rs` → merge into `single_column.rs`
- 90% overlap with desc_inequality.rs
- Extract any unique scenarios before merging
- Delete after extraction

#### `inclusion_and_ordering.rs` → `inclusion_ordering.rs` or merge into others
- Tests set inclusion with ordering
- Could be standalone or merged into relevant files
- Small file, easy to migrate

### Verification Checklist

After each migration step:
- [ ] `cargo test` passes for Sled
- [ ] `wasm-pack test` passes for IndexedDB
- [ ] Coverage matrix shows no regression
- [ ] Git history preserved (`git log --follow <file>`)

### Phases

1. Implement order_by_spill for IndexedDB
2. Create harness + migrate desc_inequality.rs
3. Migrate multi_column_order_by.rs (both)
4. Migrate remaining, add missing cases
5. Cleanup redundant files, verify 100% coverage
