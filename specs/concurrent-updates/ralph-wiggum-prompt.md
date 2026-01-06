# Ralph Wiggum Implementation Prompt

## Context

You are implementing the concurrent-updates feature on branch `trusted_causal_assertions` (PR #140). The goal is to build robust support for concurrent event handling in a distributed event-sourced system.

**Read these specs first:**
- `specs/concurrent-updates/implementation-status.md` - Complete architectural overview
- `specs/concurrent-updates/spec.md` - Functional requirements
- `specs/concurrent-updates/entity-apply-todos.md` - Specific implementation notes

## Current State

- Branch is rebased onto main
- ~25 build errors remain
- Core `event_dag` module exists but needs alignment with `proto::CausalRelation`

## Implementation Phases

### Phase 1: Fix Build Errors

Fix the ~25 compile errors. Main issues:
- `Retrieve` trait no longer has associated types - update trait bounds
- `stage_events`/`mark_event_used` moved from trait to inherent methods
- Field rename: `deltas` → `initial` in proto
- Remove `crate::lineage` imports (use `event_dag` instead)

Run `cargo build` and fix errors iteratively.

### Phase 2: Align AbstractCausalRelation with proto::CausalRelation

**File:** `core/src/event_dag/relation.rs`

Transform:
```rust
// FROM (current)
pub enum AbstractCausalRelation<Id> {
    Equal,
    StrictDescends,
    NotDescends { meet: Vec<Id> },      // ← conflates two cases
    Incomparable,                        // ← wrong name
    PartiallyDescends { meet: Vec<Id> }, // ← obsolete
    BudgetExceeded { subject_frontier, other_frontier },
}

// TO (aligned with proto)
pub enum AbstractCausalRelation<Id> {
    Equal,
    StrictDescends,
    StrictAscends,                       // ← NEW
    DivergedSince {                      // ← renamed from NotDescends
        meet: Vec<Id>,
        subject: Vec<Id>,                // immediate children of meet toward subject
        other: Vec<Id>,                  // immediate children of meet toward other
    },
    Disjoint {                           // ← renamed from Incomparable
        gca: Option<Vec<Id>>,
        subject_root: Id,
        other_root: Id,
    },
    BudgetExceeded { subject: Vec<Id>, other: Vec<Id> },
}
```

### Phase 3: Fix Comparison Algorithm

**File:** `core/src/event_dag/comparison.rs`

Add symmetric tracking to detect `StrictAscends`:
- Currently tracks `unseen_comparison_heads` for `StrictDescends`
- Add `unseen_subject_heads` - when comparison's traversal sees all subject heads → `StrictAscends`

Update `check_result()` to return proper variants:
- Subject sees all of comparison → `StrictDescends`
- Comparison sees all of subject → `StrictAscends`
- Neither sees all, common ancestors exist → `DivergedSince`
- No common ancestors → `Disjoint`

Collect forward chains during traversal (accumulate in memory for now).

### Phase 4: Update Entity.apply_event

**File:** `core/src/entity.rs`

Update the match arms:
```rust
match compare_unstored_event(getter, event, &head, budget).await? {
    AbstractCausalRelation::Equal => return Ok(false),  // no-op
    AbstractCausalRelation::StrictDescends => { /* apply, set head */ },
    AbstractCausalRelation::StrictAscends => return Ok(false),  // NEW: no-op
    AbstractCausalRelation::DivergedSince { meet, subject, other } => {
        // Merge with proper per-backend semantics
        // For now: apply operations, augment head
        // TODO: implement per-property LWW resolution
    },
    AbstractCausalRelation::Disjoint { .. } => return Err(LineageError::Disjoint.into()),
    AbstractCausalRelation::BudgetExceeded { .. } => return Err(...),
}
```

### Phase 5: Run Tests

```bash
cargo test
```

Fix any failing tests. Key test scenarios:
1. Linear history (StrictDescends, StrictAscends)
2. Simple concurrency (DivergedSince with single-step branches)
3. Deep concurrency (late arrival of long branch)

## Key Principles

1. **proto::CausalRelation is source of truth** - AbstractCausalRelation must align
2. **StrictAscends = no-op** - older events arriving late should be ignored
3. **DivergedSince = true concurrency** - requires merge
4. **Per-property LWW** - causal depth wins, lexicographic only for identical depth
5. **Yrs just works** - apply operations, CRDT handles merge
6. **Don't over-engineer** - get it working first, optimize later

## Out of Scope (for this PR)

- `StateAndRelation` handling (follow-up #199)
- Forward chain cache optimization (follow-up #200)
- Full LWWCausalRegister per-property tracking (can be a follow-up)

## Success Criteria

1. Build passes (`cargo build`)
2. Tests pass (`cargo test`)
3. No spurious multi-heads for linear histories
4. StrictAscends correctly detected and ignored
5. DivergedSince properly identified for true concurrency
