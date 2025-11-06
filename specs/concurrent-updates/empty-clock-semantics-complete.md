# Empty Clock Semantics Implementation - Complete

## Summary

Successfully implemented NULL semantics for empty clocks (`[]`), treating them as incomparable/Disjoint with everything (including other empty clocks), similar to SQL NULL values.

## Changes Made

### 1. Core Type Updates

**`core/src/causal_dag/relation.rs`:**

- Updated `CausalRelation::Disjoint` to have `Option<Id>` for `subject_root` and `other_root`
- `None` indicates an empty clock (no root)
- Added documentation explaining empty clock semantics

**`proto/src/request.rs`:**

- Updated wire protocol `CausalRelation::Disjoint` to match
- `subject_root: Option<EventId>` and `other_root: Option<EventId>`

### 2. Comparison Logic

**`core/src/causal_dag/compare.rs`:**

- Added early-return in `compare()` to detect empty clocks and return `Disjoint`
- Empty vs Empty → `Disjoint { subject_root: None, other_root: None }`
- Empty vs Non-empty → `Disjoint { subject_root: None, other_root: Some(id) }`
- Updated all existing `Disjoint` construction sites to wrap roots in `Some()`

### 3. Test Updates

**`core/src/causal_dag/tests.rs`:**

- Updated `test_empty_clocks` to expect `Disjoint` instead of `Equal` or `StrictDescends/Ascends`
- Updated `test_compare_event_unstored` to expect `Disjoint` for empty clock comparisons
- All 20 causal DAG tests pass ✅

## Semantics

### Empty Clock Comparison Matrix

| Subject | Other | Result     | Explanation                  |
| ------- | ----- | ---------- | ---------------------------- |
| `[]`    | `[]`  | `Disjoint` | NULL != NULL                 |
| `[]`    | `[x]` | `Disjoint` | NULL incomparable with value |
| `[x]`   | `[]`  | `Disjoint` | Value incomparable with NULL |
| `[x]`   | `[x]` | `Equal`    | Normal comparison            |

### Entity Creation Flow

Empty clocks in entity creation are handled specially by `Entity::apply_event()`:

```rust
if event.is_entity_create() {  // parent.is_empty()
    let mut state = self.state.write().unwrap();
    if state.head.is_empty() {
        // Accept genesis event, bypass lineage check
        // This is the ONLY place where [] vs [] interaction is allowed
    }
}
// Otherwise fall through to lineage comparison → Disjoint → rejected
```

**Protection against duplicate genesis:**

1. Genuine remote creation: `head = []`, accepts via early-return
2. Locally created uncommitted: `get_retrieve_or_create` awaits commit/rollback
3. Duplicate genesis attempt: `head = [existing]`, falls through to lineage → `Disjoint` → rejected

### Call Site Analysis

Documented all `Entity::apply_event()` call sites in [apply_event_call_sites.md](./apply_event_call_sites.md):

- **4 call sites** identified
- All protected from incorrectly accepting duplicate genesis events
- Server echo race resolved via await in `get_retrieve_or_create`

## Critical Issue Discovered

**Task #11:** EventBridge events are not persisted before `EntityTransaction.commit()`

- `apply_deltas()` commits entities but doesn't persist staged events
- `context.rs` call site never calls `store_used_events()` - **events are lost!**
- See [apply_event_call_sites.md](./apply_event_call_sites.md) for details

## Remaining Tasks

From TODO list:

- [x] Task #2: Fix empty clock semantics ✅
- [x] Task #3: Update test_empty_clocks ✅
- [ ] Task #4: Tighten `is_entity_create()` guard (optional hardening)
- [ ] Task #5: Server echo race (resolved via await mechanism)
- [ ] Task #6: WeakEntitySet abstraction and tests
- [ ] Task #7: Clean up MARK debug statements
- [ ] Task #11: Fix EventBridge event persistence ordering ⚠️ **CRITICAL**

## Verification

- ✅ All 20 causal DAG tests pass
- ✅ Empty clock semantics correctly implemented
- ✅ Entity creation protection verified
- ✅ No regressions in comparison logic

