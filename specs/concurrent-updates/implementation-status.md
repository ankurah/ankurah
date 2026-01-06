# Concurrent Updates: Implementation Status

**Branch:** `trusted_causal_assertions`
**Date:** 2026-01-05

---

## Executive Summary

This document captures the complete architectural understanding for implementing robust concurrent update handling. The `event_dag` module provides the foundation, but significant work remains to align with the `proto::CausalRelation` specification and implement proper per-backend merge semantics.

---

## 1. The Fundamental Model

### 1.1 Event DAG Structure

Every entity is a directed acyclic graph (DAG) of events:
- **Genesis event**: Entity creation, no parents
- **Subsequent events**: Have a parent clock (the entity's head when the event was created)
- **Clock**: An antichain representing the minimal frontier of known history

```
Linear history:          Concurrent history:
      A                        A
      |                       / \
      B                      B   C
      |                      |   |
      C                      D   E
      |
   head: [C]              head: [D, E]
```

### 1.2 The Core Question

When entity has head `[H]` and event `E` arrives:
1. How does `E` relate causally to `H`?
2. What should we do with `E`'s operations?
3. What should the new head be?

---

## 2. CausalRelation (Source of Truth: proto)

```rust
// proto/src/request.rs
pub enum CausalRelation {
    /// Identical lattice points.
    Equal,

    /// Subject strictly after other: Past(subject) ⊃ Past(other).
    /// Action: apply subject's state/events.
    StrictDescends,

    /// Subject strictly before other: Past(subject) ⊂ Past(other).
    /// Action: no-op (keep current state).
    StrictAscends,

    /// Both sides have advanced since the meet (GCA).
    /// Action: merge according to backend semantics.
    DivergedSince {
        meet: Clock,      // Greatest common ancestor frontier
        subject: Clock,   // Immediate children of meet toward subject
        other: Clock,     // Immediate children of meet toward other
    },

    /// Proven different genesis events (single-root invariant violated).
    /// Action: reject per policy.
    Disjoint {
        gca: Option<Clock>,
        subject_root: EventId,
        other_root: EventId,
    },

    /// Traversal could not complete under budget.
    /// Action: return frontiers to resume later.
    BudgetExceeded { subject: Clock, other: Clock },
}
```

### 2.1 Current AbstractCausalRelation (Needs Alignment)

```rust
// core/src/event_dag/relation.rs - CURRENT (incorrect)
pub enum AbstractCausalRelation<Id> {
    Equal,                        // ✓ correct
    StrictDescends,               // ✓ correct
    NotDescends { meet },         // ✗ conflates StrictAscends + DivergedSince
    Incomparable,                 // ✗ should be Disjoint with roots
    PartiallyDescends { meet },   // ✗ OBSOLETE - remove
    BudgetExceeded { ... },       // ✓ correct (but uses BTreeSet, should align)
}
```

**Note on generics:** `AbstractCausalRelation<Id>` uses `Vec<Id>`/`BTreeSet<Id>` instead of `Clock` so unit tests don't need to construct content-hashed `EventId`s. Consider an `AbstractClock<Id>` trait to formalize this abstraction.

---

## 3. The Comparison Algorithm

### 3.1 Current Implementation

The backward BFS in `comparison.rs` walks from both clocks toward common ancestors.

**Currently tracks:**
- `unseen_comparison_heads`: comparison head members not yet seen from subject's traversal
- When this reaches 0 → `StrictDescends`

**Missing:**
- `unseen_subject_heads`: subject head members not yet seen from comparison's traversal
- When this reaches 0 → `StrictAscends`

### 3.2 Detection Logic

```
Subject traversal sees all of comparison → StrictDescends
Comparison traversal sees all of subject → StrictAscends
Neither sees all of other, but common ancestors exist → DivergedSince
No common ancestors found → Disjoint
Budget exhausted before determination → BudgetExceeded
```

### 3.3 The StrictAscends Bug

Current code returns `NotDescends` for both:
- True concurrency (should be `DivergedSince`)
- Subject is older (should be `StrictAscends`)

This causes spurious multi-heads when older events arrive late (issue #198 likely).

---

## 4. Backend-Specific Merge Semantics

### 4.1 YrsBackend (CRDT)

Yrs handles concurrency internally:
- Maintains its own vector clock
- `apply_update()` is idempotent
- Operations commute and converge automatically

**Action for DivergedSince:** Apply operations in causal order (oldest→newest). Yrs handles the rest.

### 4.2 LWWBackend → LWWCausalRegister

Current implementation is broken for concurrency:

```rust
// Current: just overwrites, no causality awareness
for (property_name, new_value) in changes {
    values.insert(property_name, ValueEntry { value: new_value, committed: true });
}
```

**Required behavior for DivergedSince:**

For each property modified in the incoming event:
1. Determine which event "wins" for that property
2. **Causal depth wins**: The event further from the meet along its branch wins
3. **Lexicographic tiebreak**: Only when causal depths are identical
4. Apply only winning values

**Critical:** Do NOT use lexicographic ordering except when causality is absolutely identical. A deep concurrency arriving late should still win if it has greater causal depth.

### 4.3 Per-Property Resolution

The LWW resolution is **per-property**, not per-event:
- Event A might win for property X (deeper along its branch)
- Event B might win for property Y (deeper along its branch)
- The merged state combines winning values from both branches

---

## 5. Forward Chain Handling

### 5.1 The Problem

`DivergedSince` returns the meet and immediate children, but applying events requires the full forward chains from meet to each tip.

### 5.2 Decision: Accumulate in Memory

For simplicity, accumulate full forward chains in memory during backward traversal:
- Reverse the chain as we walk backward
- Store as `Vec<Event>` ready to apply

**TODO (future optimization):** Implement memory-cached forward event index to avoid storing full chains. Create GitHub issue after landing this branch.

### 5.3 Retriever Role

The `Retrieve` trait + `CausalNavigator`:
- Provides events for comparison (`expand_frontier`)
- Provides events for replay (by ID)
- Sources: staged events (zero cost) → local storage (low cost) → remote peers (high cost)
- Supports budget-limited traversal

---

## 6. Deep Concurrency Scenario (Key Test Case)

```
          A (genesis)
         / \
        B   C
        |   |
        D   E
        |   |
        F   G
        |   |
        H   I
```

Entity processes `A → B → D → F → H`. Head is `[H]`.
Then entire branch `C → E → G → I` arrives late (as EventBridge).

### 6.1 Comparison Result

Comparing `[G]` (I's parent) vs `[H]`:
- Meet: `[A]` (where they diverged)
- Subject's first step after A: `[C]`
- Other's first step after A: `[B]`
- Result: `DivergedSince { meet: [A], subject: [C], other: [B] }`

### 6.2 LWW Resolution

For property P modified in both branches:
1. Find which event last set P in subject's chain (e.g., event G at depth 4 from A)
2. Find which event last set P in other's chain (e.g., event D at depth 2 from A)
3. G wins (depth 4 > depth 2)
4. If both at same depth → lexicographic event ID tiebreak

### 6.3 Yrs Resolution

Just apply `C → E → G → I` in order. Yrs merges internally.

---

## 7. Multi-Head Semantics

### 7.1 Invariant

From the spec:
> "The system MAY preserve multi-heads when unsubsumed concurrency remains. It MUST NOT create multi-heads for linear histories."

### 7.2 After DivergedSince Merge

- If subject's tip causally subsumes all of other's tips → head = `[subject_tip]`
- If truly concurrent with unsubsumed tips → head = `[subject_tip, other_tips...]`

The current bug creates multi-heads for `StrictAscends` (linear history), violating this invariant.

---

## 8. Entity-Level vs Backend-Level Resolution

### 8.1 Design Decision

**Option chosen:** Resolve conflicts at entity level, backends stay simple.

The entity:
1. Receives `DivergedSince` with forward chains
2. Walks through subject's chain, oldest to newest
3. For each event, for each property it modifies:
   - Checks if that property was also modified in other's chain
   - Determines winner using causal depth comparison
   - Only applies if this event wins
4. Passes filtered operations to backend

This centralizes conflict resolution logic where causal context is available.

### 8.2 LWWCausalRegister Tracking

The backend still needs to track which event set each property (for future concurrent arrivals):

```rust
struct CausalValueEntry {
    value: Option<Value>,
    event_id: EventId,  // Which event set this value
    committed: bool,
}
```

When a new concurrent event arrives, the entity can query the backend for current event_id and compare causality.

---

## 9. State Application

When receiving a state snapshot instead of events:

| Relation | Action |
|----------|--------|
| Equal | No-op |
| StrictDescends | Accept state, set head = state.head |
| StrictAscends | No-op (we have newer) |
| DivergedSince | Need events for proper merge |
| Disjoint | Reject per policy |

For DivergedSince with state snapshot:
- Request supplemental events via retriever
- If events unavailable, may accept attested state per PolicyAgent

---

## 10. Required Changes

### Phase 1: Align AbstractCausalRelation

- [ ] Add `StrictAscends` variant
- [ ] Rename `NotDescends { meet }` → `DivergedSince { meet, subject, other }`
- [ ] Rename `Incomparable` → `Disjoint { gca, subject_root, other_root }`
- [ ] Remove `PartiallyDescends` (obsolete)
- [ ] Add `AbstractClock<Id>` trait for test ergonomics

### Phase 2: Fix Comparison Algorithm

- [ ] Add `unseen_subject_heads` tracking (symmetric with `unseen_comparison_heads`)
- [ ] Detect and return `StrictAscends`
- [ ] Collect forward chains during backward traversal
- [ ] Capture genesis roots for `Disjoint`

### Phase 3: Update Entity.apply_event

- [ ] Handle `StrictAscends` as no-op
- [ ] Handle `DivergedSince`:
  - Walk forward chains
  - Compute per-property winners (causal depth, then lexicographic)
  - Apply only winning operations
- [ ] Handle `StrictDescends` with multi-step replay

### Phase 4: LWWCausalRegister

- [ ] Soft-rename `LWWBackend` → `LWWCausalRegister`
- [ ] Track `event_id` per property
- [ ] Expose current event_id for conflict resolution queries

### Phase 5: Comprehensive Tests

- [ ] Linear history: StrictDescends with gaps, StrictAscends
- [ ] Simple concurrency: DivergedSince with single-step branches
- [ ] Deep concurrency: Late arrival of long branch (the key test)
- [ ] Multi-head: Concurrent event arriving at already multi-head entity
- [ ] Yrs vs LWW: Same scenario, different backends

---

## 11. Build Status

~25 compile errors remain after rebase. Main issues:
- Trait bound mismatches (`Retrieve` no longer has associated types)
- Field renames (`deltas` → `initial`)
- Removed `crate::lineage` references
- Staging method signatures

---

## 12. Open Questions / Decisions Needed

### 12.1 Forward Chain Storage (DECIDED)

**Decision:** Store full chains in memory for now.
**TODO:** Create GitHub issue for memory-cached forward event index optimization after landing branch.

### 12.2 AbstractClock<Id> (DECIDED)

**Decision:** Implement trait for test ergonomics. Add good comments.
Tests use simple integer IDs; production uses real `Clock`.

### 12.3 Per-Property vs Per-Event (CLARIFIED)

**Answer:** Per-property. The LWW resolution is per-property—different properties may have different winners from different branches.

### 12.4 Yrs and LWW Resolution (CLARIFIED)

- **Yrs:** Just apply operations; Yrs handles merge internally
- **LWW:** Causal depth from meet determines winner; lexicographic only for identical depth

### 12.5 Remaining Questions

1. **StrictDescends with multi-step gaps:** When incoming event is multiple hops ahead, we need all intermediate events. Currently relies on EventBridge providing them. What if events are missing?
   - Likely answer: Fall back to state application if events unavailable

2. **Causal depth calculation:** Is depth counted from genesis or from meet?
   - Likely answer: From meet (the divergence point)

3. **Multi-head merge complexity:** When head is `[H, G]` and event `I` arrives, different head members may have different meets with `I`. How to handle?
   - Likely answer: Find meet with entire clock (union of pasts), then process

---

## 13. Test Scenarios

| Scenario | Initial Head | Incoming | Expected Relation | Expected Action |
|----------|-------------|----------|-------------------|-----------------|
| Linear advance | [B] | D (B→C→D) | StrictDescends | Apply C, D; head→[D] |
| Ancestor arrival | [D] | B | StrictAscends | No-op; head stays [D] |
| Simple concurrent | [B] | C (A→C) | DivergedSince{A,[C],[B]} | Merge; head→[B,C] or merged |
| Deep concurrent | [H] | I (long branch) | DivergedSince{A,...} | Merge with depth comparison |
| Multi-head + new | [H,G] | I | DivergedSince | Merge into [H,G,I] or collapsed |

---

## 14. Files to Modify

| File | Changes |
|------|---------|
| `core/src/event_dag/relation.rs` | Align enum variants |
| `core/src/event_dag/comparison.rs` | Add symmetric tracking, forward chains |
| `core/src/entity.rs` | Update apply_event handlers |
| `core/src/property/backend/lww.rs` | Add event_id tracking |
| `proto/src/request.rs` | Already correct (source of truth) |
| Tests | Comprehensive scenarios |
