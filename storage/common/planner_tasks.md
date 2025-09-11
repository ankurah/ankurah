# Planner Implementation Tasks

## Overview

The Planner generates all possible index scan plans for a query. Each plan represents a single index scan with a range that guarantees all matching records are included. The executor will apply the residual predicate to filter results.

## Core Data Structures

### 1. Plan (formerly IndexScan)

```rust
pub struct Plan {
    index_fields: Vec<IndexField>,  // The fields and their directions in the index
    scan_direction: ScanDirection,  // Direction to scan the index (forward/backward)
    range: Range,
    amended_predicate: ankql::ast::Predicate,  // Original predicate minus consumed conjuncts
    requires_sort: bool,  // Future-proofing: true if ORDER BY doesn't match index
}

pub struct IndexField {
    name: String,
    direction: IndexDirection,  // Direction of this field in the index structure
}

pub enum IndexDirection {
    Asc,
    Desc,
}
```

### 2. Range

```rust
pub struct Range {
    from: Bound,
    to: Bound,
}

pub enum Bound {
    Unbounded,
    Inclusive(Vec<Property>),  // Uses core/src/property types
    Exclusive(Vec<Property>),
}
```

### 3. ScanDirection

```rust
pub enum ScanDirection {
    Asc,
    Desc,
}
```

## Planner Configuration

```rust
pub struct PlannerConfig {
    supports_desc_indexes: bool,  // false for IndexedDB, true for engines with real DESC indexes
}
```

## Implementation Tasks

### Phase 1: Predicate Analysis (`predicate.rs`)

1. **Implement ConjunctFinder**
   - Extract conjuncts that can be flattened (what "top-level" really means)
   - Example: `(foo = 1 AND (? AND bar = 2)) AND (? OR (baz = 3 AND zed = 4))`
     - `foo = 1` and `bar = 2` are conjuncts (can be flattened)
     - `baz = 3` and `zed = 4` are NOT conjuncts (blocked by OR)
   - Return owned predicates, not references
   - Handle nested ANDs recursively
   - Stop at OR boundaries (OR breaks conjunct chains)
   - Ordering: preserve order of appearance in original predicate
   - Add comment about potential future optimization for `foo > 10 OR foo > 20` patterns

### Phase 2: Plan Generation (`planner.rs`)

2. **Define Plan Structure**

   - Create Plan, Range, Bound, and ScanDirection types
   - Use Property types from core/src/property for values

3. **Implement Planner::plan()**

   - Input: `ankql::ast::Selection`, `PlannerConfig`
   - Output: `Vec<Plan>`
   - Note: Predicate already contains `__collection = collection_id` (added by executor)
   - Algorithm:
     ```
     a. Extract all conjuncts using ConjunctFinder
     b. Separate conjuncts into equalities and inequalities
     c. Group inequalities by field (e.g., age > 25 AND age < 50 → single range)
     d. If ORDER BY present:
        - Create index: [__collection, ...equalities, inequality_if_matches_first_orderby, ...order_by_fields]
        - Use inequality for range if it matches first ORDER BY field
     e. For each inequality field (or group):
        - Create index: [__collection, ...equalities, inequality_field]
        - Use inequality/inequalities for range bounds
     f. For equality-only queries:
        - Create index: [__collection, ...equalities] (ordered by predicate appearance)
     g. Generate default [__collection, id] plan
     h. Deduplicate plans based on index_fields
     i. For each plan:
        - Calculate amended_predicate (remove consumed conjuncts)
        - Set requires_sort based on ORDER BY alignment
     ```

4. **ORDER BY Plan Generation**

   - If ORDER BY exists, ALL components must be in the index in the same order
   - Index structure: [__collection, ...equalities, inequality_if_on_first_orderby_field, ...order_by_fields]
   - Examples:

     ```
     Query: __collection = "album" ORDER BY foo, bar
     Plan: index_fields: [__collection, foo, bar]
           range: { from: Inclusive(["album"]), to: Inclusive(["album"]) }

     Query: __collection = "album" AND foo > 10 ORDER BY foo, bar
     Plan: index_fields: [__collection, foo, bar]
           range: { from: Exclusive(["album", 10]), to: Unbounded }

     Query: __collection = "album" AND age = 30 ORDER BY foo, bar
     Plan: index_fields: [__collection, age, foo, bar]
           range: { from: Inclusive(["album", 30]), to: Inclusive(["album", 30]) }

     Query: __collection = "album" AND age > 25 ORDER BY foo, bar
     Plan: index_fields: [__collection, age, foo, bar]
           range: { from: Exclusive(["album", 25]), to: Unbounded }
           amended_predicate: (empty - all consumed)
     ```

   - All ORDER BY fields must have same direction
   - Set requires_sort = false when index aligns with ORDER BY

5. **Conjunct-based Plan Generation**

   - For each conjunct that's a comparison:
     - Extract field name and operator
     - Determine optimal scan direction
     - Create appropriate Range based on operator
     - Track which conjuncts were "consumed"
   - Generate amended_predicate (original minus consumed conjuncts)

6. **Range Construction Algorithm**

   - Handle operator types:
     - `=` → `Range { from: Inclusive([...values]), to: Inclusive([...values]) }`
     - `>` → `Range { from: Exclusive([...values]), to: Unbounded }`
     - `>=` → `Range { from: Inclusive([...values]), to: Unbounded }`
     - `<` → `Range { from: Unbounded, to: Exclusive([...values]) }`
     - `<=` → `Range { from: Unbounded, to: Inclusive([...values]) }`
   - Collection ID always first component of range
   - **Important constraint**: Maximum of ONE inequality FIELD per range scan
   - Multiple inequalities on SAME field can be combined into one range
   - Equality constraints can be used for all prefix fields
   - Examples:

     ```
     Query: __collection = "album" AND age > 25
     Index: [__collection, age]
     Range: { from: Exclusive(["album", 25]), to: Unbounded }

     Query: __collection = "album" AND name = "Alice" AND age > 25
     Index: [__collection, name, age]  // equalities first
     Range: { from: Exclusive(["album", "Alice", 25]), to: Unbounded }

     Query: __collection = "album" AND age > 25 AND age < 50
     Single plan: index [__collection, age]
     Range: { from: Exclusive(["album", 25]), to: Exclusive(["album", 50]) }

     Query: __collection = "album" AND age > 25 AND score < 100
     Must generate TWO plans:
     Plan 1: index [__collection, age], range uses age > 25
     Plan 2: index [__collection, score], range uses score < 100

     Query: __collection = "album" AND age = 30 AND score > 50 ORDER BY score
     Plan: index_fields: [__collection, age, score]
           range: { from: Exclusive(["album", 30, 50]), to: Unbounded }
           amended_predicate: (empty - all consumed)
     ```

7. **Index Field Direction Handling**
   - For IndexedDB (supports_desc_indexes = false):
     - All IndexField directions set to Asc
     - Use scan_direction to control cursor direction
   - For other engines (supports_desc_indexes = true):
     - IndexField directions can be Asc or Desc
     - Match ORDER BY directions when possible
8. **Conjunct Consumption Rules**

   - A conjunct is "consumed" by an index if:
     - It's an equality on any prefix field of the index
     - OR it's an inequality on ONE field used for range bounds
   - Field ordering rules:
     - Equalities ALWAYS come first (after \_\_collection)
     - Ordered by appearance in predicate for determinism
     - Maximum ONE inequality FIELD per plan (but multiple inequalities on same field OK)
   - Example:

     ```
     Predicate: __collection = "album" AND age > 25 AND name = "Alice" AND score < 100
     Generates these plans:
     Plan 1: Index [__collection, name, age], consumes all except score < 100
     Plan 2: Index [__collection, name, score], consumes all except age > 25

     Predicate: __collection = "album" AND age > 25 AND age < 50 AND score = 100
     Plan: Index [__collection, score, age], consumes ALL conjuncts
           Range: { from: Exclusive(["album", 100, 25]), to: Exclusive(["album", 100, 50]) }
     ```

9. **Plan Deduplication**
   - Plans are considered duplicate if they have identical:
     - index_fields (same fields in same order with same directions)
     - scan_direction
   - Keep the plan with the most restrictive range (smallest scan)

### Phase 3: Testing

10. **Unit Tests**

- Test ConjunctFinder with various predicate structures
- Test Range construction for different operators
- Test Plan generation for simple queries
- Test Plan generation with ORDER BY
- Test amended predicate generation

11. **Integration Tests**

- Port relevant tests from current indexes.rs
- Ensure all plans are valid (would return correct results)
- Verify amended predicates are correct

### Phase 4: Migration (Future)

12. **IndexedDB Integration** (after tests pass)
    - Update IndexedDB implementation to use Planner
    - Convert Plan to IndexedDB-specific execution
    - Remove old Optimizer code

## Algorithm Edge Cases

1. **Empty Conjuncts**

   - If no conjuncts found (e.g., all ORs), only generate default plan
   - Amended predicate = original predicate

2. **Incompatible ORDER BY**

   - If ORDER BY has mixed directions and supports_desc_indexes = false
   - Generate plan anyway but set requires_sort = true (executor will reject)

3. **Range Bound Conflicts**

   - Example: `age > 30 AND age < 20` (impossible range)
   - Still generate the plan with the range, let executor handle empty result

4. **Multi-field Index Prefix Matching**

   - Can only use constraints that form a prefix of the index
   - Example: Index [__collection, age, name], constraint on name only → can't use

5. **IN and BETWEEN Operators**
   - Currently not supported in range construction
   - Remain in amended_predicate for post-scan filtering

## Key Design Decisions

1. **All indexes are composite** - Even single-field indexes include \_\_collection
2. **Single index per plan** - No index intersection or multi-index plans
3. **Auto-index creation** - Planner identifies needed indexes; executor creates them
4. **No sorting in plans** - ORDER BY must align with index or query fails
5. **Plan selection deferred** - Planner generates all plans; selection happens later
6. **Conjuncts only** - OR branches break conjunct extraction (except special cases)
7. **Index direction flexibility** - Config flag controls whether backend supports DESC indexes
8. **One inequality per scan** - Maximum of one inequality comparison can be used in range bounds
9. **Equality fields first** - All equality comparisons precede inequality/ORDER BY fields in index
10. **Deterministic field ordering** - Fields ordered by appearance in predicate for consistency

## Success Criteria

- All current indexes.rs tests pass with new implementation
- Clean separation between plan generation and execution
- Storage-agnostic planning logic
- Clear path for future optimizer implementation
