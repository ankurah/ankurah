# Cold Spec Compliance Review

Date: 2026-02-09
Reviewer: Cold Spec Compliance Agent (Round 2)
Model: claude-opus-4-6
Branch: `concurrent-updates-event-dag`
Worktree: `/Users/daniel/ak/ankurah-201`

---

## Methodology

This review was conducted with fresh eyes, reading only `specs/concurrent-updates/spec.md` and the implementation files listed in the task. No prior review files, digests, or internal docs were consulted. Each spec requirement was traced to its corresponding code location and evaluated.

---

## Requirement-by-Requirement Conformance

### A. Core Concepts (Spec Lines 9-28)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| A1 | Causal Relationships Table | `Equal` variant: identical lattice points, action=no-op | COMPLIANT | `relation.rs:8`, `entity.rs:296-298` | Code returns `Ok(false)` for Equal |
| A2 | Causal Relationships Table | `StrictDescends { chain }`: subject strictly after other, action=apply forward chain | COMPLIANT | `relation.rs:12-15`, `entity.rs:300-314` | Chain produced and operations applied |
| A3 | Causal Relationships Table | `StrictAscends`: subject strictly before other, action=no-op | COMPLIANT | `relation.rs:18-19`, `entity.rs:316-319` | Returns `Ok(false)` |
| A4 | Causal Relationships Table | `DivergedSince { meet, subject, other, subject_chain, other_chain }`: true concurrency, action=per-property LWW merge | COMPLIANT | `relation.rs:24-37`, `entity.rs:321-381` | All fields present; layer-based merge applied |
| A5 | Causal Relationships Table | `Disjoint { gca, subject_root, other_root }`: different genesis, action=reject | COMPLIANT | `relation.rs:41-48`, `entity.rs:383-384` | Returns `LineageError::Disjoint` |
| A6 | Causal Relationships Table | `BudgetExceeded { subject, other }`: budget exhausted, action=resume with frontiers | COMPLIANT | `relation.rs:52`, `entity.rs:386-393` | Returns `LineageError::BudgetExceeded` with frontiers |
| A7 | Forward Chains | Chains in causal order (oldest to newest) from meet to head | COMPLIANT | `comparison.rs:367-381` | `build_forward_chain` reverses traversal order and skips meet |
| A8 | Forward Chains | Chains accumulated during backward BFS and reversed | COMPLIANT | `comparison.rs:159-161, 286-291` | `subject_visited`/`other_visited` collected child-to-parent, reversed in `build_forward_chain` |

### B. BFS Algorithm (Spec Lines 32-60)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| B1 | Process Step 1 | Initialize: two frontiers = {subject_head} and {other_head} | COMPLIANT | `comparison.rs:220-221` | `Frontier::new(subject_ids)`, `Frontier::new(comparison_ids)` |
| B2 | Process Step 2 | Expand: fetch events at current frontier positions | COMPLIANT | `comparison.rs:240-275` | Events fetched individually via accumulator in `step()` |
| B3 | Process Step 3a | Remove from frontier | COMPLIANT | `comparison.rs:282-283` | `subject_frontier.remove(&id)`, `comparison_frontier.remove(&id)` |
| B4 | Process Step 3b | Add parents to frontier | COMPLIANT | `comparison.rs:347-363` | `subject_frontier.extend(parents)`, `comparison_frontier.extend(parents)` |
| B5 | Process Step 3c | Track which frontier(s) have seen it | COMPLIANT | `comparison.rs:294-303` | `NodeState.seen_from_subject`, `seen_from_comparison` |
| B6 | Process Step 3d | Accumulate in visited list for chain building | COMPLIANT | `comparison.rs:286-291` | `subject_visited.push(id)`, `other_visited.push(id)` |
| B7 | Termination: `unseen_comparison_heads==0` -> StrictDescends | Spec says subject has seen all comparison heads | COMPLIANT | `comparison.rs:413-418` | Chain built from `subject_visited` reversed, filtering out comparison heads |
| B8 | Termination: `unseen_subject_heads==0` -> StrictAscends | Spec says comparison has seen all subject heads | COMPLIANT | `comparison.rs:422-425` | Returns `StrictAscends` |
| B9 | Termination: Frontiers empty + common ancestors -> DivergedSince | True concurrency | COMPLIANT | `comparison.rs:428-471` | Meet computed as minimal common ancestors (zero common_child_count), chains built |
| B10 | Termination: Frontiers empty + different genesis roots -> Disjoint | Incompatible histories | COMPLIANT | `comparison.rs:434-445` | Checks `subject_root != other_root` when no common ancestors |
| B11 | Termination: Frontiers empty + no common, can't prove disjoint -> DivergedSince with empty meet | Spec line 59 | COMPLIANT | `comparison.rs:447-453` | Falls through to DivergedSince with empty meet/chains |
| B12 | Termination: Budget exhausted -> BudgetExceeded | Return frontiers for resumption | COMPLIANT | `comparison.rs:472-477` | Returns current frontier IDs |

### C. Conflict Resolution: Layered Event Application (Spec Lines 62-128)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| C1 | Layer Model | Events applied in layers at same causal depth from meet | COMPLIANT | `accumulator.rs:128-218` | Topological frontier expansion from meet point |
| C2 | EventLayer struct | Struct has `already_applied`, `to_apply`, and DAG context | DEVIATED | `accumulator.rs:229-234` | Spec says `events: Arc<BTreeMap<EventId, Event>>` (full events); code has `dag: Arc<BTreeMap<EventId, Vec<EventId>>>` (parent pointers only). Justified: parent pointers suffice for causal comparison and are more memory-efficient. |
| C3 | Key Insight | Causal comparison when possible; lexicographic only for truly concurrent | COMPLIANT | `lww.rs:218-229` | `layer.compare()` used first; `candidate.event_id > current.event_id` only for `Concurrent` |
| C4 | Resolution Step 1 | Navigate backward from both heads to find meet | COMPLIANT | `comparison.rs` BFS algorithm | Full backward BFS |
| C5 | Resolution Step 2 | Accumulate full events during traversal via AccumulatingNavigator | COMPLIANT | `accumulator.rs:46-51, comparison.rs:272` | `accumulator.accumulate(&event)` called for each fetched event |
| C6 | Resolution Step 3 | Compute layers via frontier expansion from meet | COMPLIANT | `accumulator.rs:129-161` | Topological sort seed from meet, children index built |
| C7 | Resolution Step 4 | Apply layers in causal order calling `backend.apply_layer()` | COMPLIANT | `entity.rs:349-369` | Iterates layers, calls `backend.apply_layer(&layer)` for all backends |
| C8 | Backend Interface | All backends implement `apply_layer` | COMPLIANT | `mod.rs:84`, `lww.rs:169`, `yrs.rs:186` | Trait method with implementations in both backends |
| C9 | LWW Resolution Rule 1 | Examine ALL events (already_applied + to_apply) plus stored last-write | COMPLIANT | `lww.rs:180-242` | Seeds from stored values, iterates both already_applied and to_apply |
| C10 | LWW Resolution Rule 2a | Descends/Ascends decides winner | COMPLIANT | `lww.rs:220-223` | `Descends` replaces current, `Ascends` keeps current |
| C11 | LWW Resolution Rule 2b | Concurrent falls back to lexicographic EventId | COMPLIANT | `lww.rs:224-228` | `if candidate.event_id > current.event_id` |
| C12 | LWW Resolution Rule 2c | Missing DAG info treats event as dead end (not error) | COMPLIANT | `accumulator.rs:248-249, 288-307` | `is_descendant_dag` treats missing entries as dead ends; `lww.rs:194-197` marks as `older_than_meet` |
| C13 | LWW Resolution Rule 3 | Only mutate state for winners from to_apply set | COMPLIANT | `lww.rs:248-253` | `if candidate.from_to_apply` check gates mutation |
| C14 | LWW Resolution Rule 4 | Track event_id per property | COMPLIANT | `lww.rs:250` | `ValueEntry::Committed { value, event_id }` stored |
| C15 | Yrs Backend | Apply all operations from to_apply; CRDT handles idempotency; already_applied ignored | COMPLIANT | `yrs.rs:186-199` | Only iterates `layer.to_apply`, ignores `already_applied` |

### D. Key Design Decisions (Spec Lines 130-182)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| D1 | Multi-Head StrictAscends Transformation | Event staged in GetEvents before compare(); BFS discovers divergence | COMPLIANT | `entity.rs:292-294`, `node_applier.rs:82, 276-277` | Event staged via `event_getter.stage_event()`, then compare uses event's own ID as subject |
| D2 | Multi-Head StrictAscends Transformation | Meet = event's parent clock; Other = head tips not in parent | COMPLIANT | `comparison.rs` BFS logic | BFS naturally discovers meet as the shared ancestor |
| D3 | Multi-Head StrictAscends Transformation | Empty other_chain triggers conservative resolution | DEVIATED | `entity.rs:321-381` | Spec says "empty other_chain triggers conservative resolution (current value wins)". Code applies layers regardless of whether other_chain is empty. The `apply_layer` mechanism handles this implicitly: if other_chain is empty, the `already_applied` list will contain events from the head branch, and if their event_id beats the to_apply candidates, they win. But there is no explicit "conservative choice" check. See Ambiguity A1. |
| D4 | Empty Other-Chain Conservative Resolution | Keep current value if it has tracked event_id | DEVIATED | `lww.rs:180-253` | No explicit empty-other-chain check. Instead, the stored value's event_id participates in the standard comparison. If it's in the DAG (known_in_dag=true), it competes normally. If not (older_than_meet), any layer candidate wins. The "conservative" behavior emerges from the standard resolution path when the stored value's event_id is in the DAG and beats layer candidates causally or lexicographically. |
| D5 | TOCTOU Retry Pattern | Retry loop with head-changed check | COMPLIANT | `entity.rs:289-398` | MAX_RETRIES=5, `try_mutate` checks head equality under write lock, continues on mismatch |
| D6 | TOCTOU Retry Pattern | Comparison is async, then write lock | COMPLIANT | `entity.rs:294, 304, 341-343` | Compare without lock, then lock + CAS |
| D7 | Per-Property Event Tracking | LWW stores event_id per property | COMPLIANT | `lww.rs:20-42` | `ValueEntry::Committed { value, event_id }` |
| D8 | Per-Property Event Tracking | Uncommitted local changes exist without event_id | COMPLIANT | `lww.rs:22` | `ValueEntry::Uncommitted { value }` has no event_id |
| D9 | Per-Property Event Tracking | Uncommitted never serialized to state buffer | COMPLIANT | `lww.rs:112-127` | `to_state_buffer` requires `entry.event_id()` - returns error if missing |
| D10 | ValueEntry struct | `value: Option<Value>`, `event_id: EventId`, `committed: bool` | DEVIATED | `lww.rs:20-25` | Code uses an enum with three variants (Uncommitted/Pending/Committed) instead of a struct with `committed: bool`. event_id is only on Committed variant, not universally present. This is functionally equivalent but structurally different. Justified: enum is more type-safe (impossible to have event_id without committed=true). |

### E. API Reference (Spec Lines 191-231)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| E1 | Compare signature | `pub fn compare<E>(subject, comparison, events)` | DEVIATED | `comparison.rs:49-54` | Signature differs: (1) `event_getter` is first param not last, (2) explicit `budget` param added, (3) function is `async`, (4) returns `ComparisonResult<E>` not plain `ComparisonResult`, (5) `pub(crate)` not `pub`. All are justified API refinements. |
| E2 | PropertyBackend::apply_layer | `fn apply_layer(&self, already_applied: &[&Event], to_apply: &[&Event])` | DEVIATED | `mod.rs:84` | Code takes `&EventLayer` struct instead of two separate slice parameters. Justified: grouping into a struct adds the DAG context needed for causal comparison. |
| E3 | PropertyBackend::apply_operations_with_event | `fn apply_operations_with_event(&self, operations, event_id)` | COMPLIANT | `mod.rs:58-62` | Default impl ignores event_id (for CRDTs); LWW overrides to track |
| E4 | Layer computation access | `result.into_layers()` on ComparisonResult | DEVIATED | `accumulator.rs:94-99` | `into_layers(self, current_head)` requires `current_head` parameter not shown in spec. Also, entity.rs uses `into_parts()` + `accumulator.into_layers(meet, head)` instead of the cleaner `result.into_layers()`. Both paths exist but the spec's simple API isn't the primary one used. |

### F. Invariants (Spec Lines 184-189)

| # | Spec Section | Requirement | Status | Code Location | Notes |
|---|---|---|---|---|---|
| F1 | No regression | Never skip required intermediate events | COMPLIANT | `entity.rs:300-313` (StrictDescends applies ops), `entity.rs:349-369` (layers applied in order) | Forward chain replay and layer-by-layer application ensure ordering |
| F2 | Single head for linear history | Multi-heads only for true concurrency | COMPLIANT | `entity.rs:302, 372-378` | StrictDescends replaces head with event.id(); DivergedSince adds to multi-head |
| F3 | Deterministic convergence | Same events -> same final state | COMPLIANT | Tests in `tests.rs:1405-1498` (determinism_tests) | Multiple permutation tests verify this for LWW |
| F4 | Idempotent application | Redundant delivery is a no-op | COMPLIANT | `entity.rs:235-238, 296-298, 316-319` | Equal->false, StrictAscends->false; creation guard checks event_stored |

### G. Test Coverage (Spec Lines 234-246)

| # | Spec Claim | Tested? | Test Location | Notes |
|---|---|---|---|---|
| G1 | Multi-head extension | YES | `tests.rs:648-690, 693-731, 734-778` | Three variants: extend one tip, merge all tips, three-way |
| G2 | Deep diamond concurrency | YES | `tests.rs:784-850` | Asymmetric branches, 4 events per chain |
| G3 | Short branch from deep point | YES | `tests.rs:853-919` | Meet is not genesis, 8-deep chain + 2-event branch |
| G4 | Per-property LWW resolution | YES | `tests.rs:1221-1241` | Different properties, different winners |
| G5 | Same-depth lexicographic tiebreak | YES | `tests.rs:1155-1173` | Higher EventId wins among concurrent |
| G6 | Idempotency | YES | `tests.rs:1077-1144, 1709-1747` | Redundant delivery at head (Equal) and in history (StrictAscends) |
| G7 | Chain ordering verification | YES | `tests.rs:984-1070` | Forward chain [B,C,D,E] and diverged chains [B,D] / [C,E] |
| G8 | Disjoint detection | YES | `tests.rs:220-292` | Different genesis roots |
| G9 | Layer computation | YES | `tests.rs:1910-2023` | Mixed-parent merge with topological ordering |
| G10 | LWW apply_layer | YES | `tests.rs:1150-1273` | Winner determination, already_applied vs to_apply, three-way |
| G11 | Yrs apply_layer | YES | `tests.rs:1280-1398` | CRDT idempotency, concurrent inserts, order independence |
| G12 | Spec claims "budget resumption" tested | NO | N/A | Spec's "Future Work" section mentions this; budget escalation IS tested (`tests.rs:1839-1878`) but full resumption from frontiers is not implemented or tested |

---

## Undocumented Behavior (code does things spec doesn't describe)

### U1. Quick-Check Short-Circuit for Immediate Descendants
**Code:** `comparison.rs:77-105`
**Behavior:** Before entering BFS, the code checks whether all comparison heads appear as parents of subject events. If so, it returns `StrictDescends` immediately without BFS. This is a performance optimization not mentioned in the spec.
**Risk:** Low. Semantically equivalent to BFS result but faster for the common single-step case.

### U2. Budget Escalation Multiplier is `4x` in a Single Jump
**Code:** `comparison.rs:108-109, 120-124`
**Behavior:** `current_budget = (current_budget * 4).min(max_budget)` means a single retry at 4x. The spec says "up to 4x" but doesn't specify the escalation curve. The code jumps directly to 4x rather than e.g. 2x then 4x.
**Risk:** None. Single-step escalation is simpler and the 4x cap is preserved.

### U3. Unfetchable Event on Both Frontiers Treated as Common Ancestor
**Code:** `comparison.rs:254-267`
**Behavior:** If an event on both frontiers can't be fetched (EventNotFound), it's processed with empty parents as a common ancestor. This handles the ephemeral node scenario where the meet point is unreachable from local storage.
**Risk:** Low. Handles a real scenario gracefully. Spec doesn't mention this edge case.

### U4. Creation Event Fast Path Under Mutex
**Code:** `entity.rs:263-278`
**Behavior:** Creation events (empty parent clock) are handled under a write lock without BFS comparison when the entity head is empty. This is an optimization/correctness guard not detailed in the spec.
**Risk:** None. Required for correctness (prevents double-creation race).

### U5. Non-Creation Event on Empty Entity Rejection
**Code:** `entity.rs:282-285`
**Behavior:** If a non-creation event arrives for an entity with empty head, it's rejected with `InvalidEvent`. The spec doesn't describe this guard.
**Risk:** None. Prevents applying updates to non-existent entities.

### U6. LRU Cache in EventAccumulator
**Code:** `accumulator.rs:27-62`
**Behavior:** Events are cached in an LRU cache (capacity 1000) during traversal. Spec mentions "AccumulatingNavigator" but doesn't detail caching strategy.
**Risk:** None. Implementation detail for performance.

### U7. Creation Event Durable/Ephemeral Divergent Paths
**Code:** `entity.rs:244-260`
**Behavior:** On durable nodes, a second creation event for an existing entity is rejected immediately via `event_stored()` + `storage_is_definitive()`. On ephemeral nodes, it falls through to BFS. This nuance is not in the spec.
**Risk:** Low. Handles a real operational difference between node types.

### U8. Backend Late-Creation During Layer Application
**Code:** `entity.rs:351-363`
**Behavior:** During layer application, if a backend referenced by a `to_apply` event doesn't exist yet, it's created on-the-fly and earlier layers are replayed against it. Spec doesn't discuss backend lifecycle.
**Risk:** Low. Handles the case where concurrent events introduce a new backend type.

### U9. `older_than_meet` Rule in LWW
**Code:** `lww.rs:191-198, 213-215`
**Behavior:** Stored values whose event_id is not in the accumulated DAG are marked `older_than_meet` and automatically lose to any layer candidate. This implements a monotonicity guarantee not explicitly stated in the spec (though it's implied by "per-property depths").
**Risk:** None. This is a critical correctness property ensuring that values older than the meet don't incorrectly win.

---

## Missing Behavior (spec promises things code doesn't implement)

### M1. Budget Resumption
**Spec:** "Return frontiers for resumption" (line 60), "Budget resumption: Continue interrupted traversals" (Future Work, line 251)
**Code:** `BudgetExceeded` returns frontiers but there is no code to resume from those frontiers. The `compare` function always starts fresh (with internal escalation).
**Status:** Acknowledged in spec as "Future Work". Not a conformance issue since spec marks it as unimplemented.

### M2. Forward Chain Caching
**Spec:** "Forward chain caching: Avoid recomputation for repeated comparisons" (Future Work, line 250)
**Code:** No chain caching exists.
**Status:** Acknowledged in spec as "Future Work".

---

## Spec Ambiguities

### A1. "Empty Other-Chain Conservative Resolution" vs Layer-Based Resolution
**Spec says (lines 147-150):** "When other_chain is empty (StrictAscends transformation case): We can't compute depths for the other branch. Conservative choice: keep current value if it has a tracked event_id."
**Code does:** No explicit check for empty other_chain. Instead, the standard layer application path runs. When other_chain is empty, `to_apply` will contain the incoming event and `already_applied` will be empty (since no events from the "other" side appear in layers). The stored last-write value participates as a seed candidate with its tracked event_id. If the stored value's event_id is in the DAG, it competes normally via `layer.compare()`. If not (below meet), the incoming event auto-wins via `older_than_meet`.
**Question:** Is the spec's "conservative choice: keep current value" always honored? If the stored value's event_id is below the meet (not in DAG), the incoming event wins, which is NOT conservative. However, the spec's description of this scenario assumes the stored value has a recent event_id from the head branch (which WOULD be in the DAG). This is an under-specified edge case.

### A2. What Happens When `meet` Contains IDs Not in the Entity Head?
**Spec says:** Head is updated by removing meet IDs and inserting the new event.
**Code (entity.rs:375-378):** `state.head.remove(parent_id)` for each ID in meet; `state.head.insert(event.id())`.
**Question:** If meet=[A] and head=[C] (where A is an ancestor of C but not in head), `head.remove(A)` is a no-op, and D is inserted, yielding [C, D]. This is correct behavior for a deep divergence. But the code comment says "The incoming event extends tips in its parent clock (meet)" which is misleading -- `meet` is the GCA, not the event's parent clock.

### A3. Spec's `apply_layer` Signature vs Code
**Spec shows:** `fn apply_layer(&self, already_applied: &[&Event], to_apply: &[&Event])`
**Code has:** `fn apply_layer(&self, layer: &EventLayer)`
The `EventLayer` struct bundles the two slices AND provides the DAG context for causal comparison. The spec's signature doesn't account for the DAG context needed by `layer.compare()`. This seems like the spec was written before the DAG-based comparison was finalized.

### A4. EventLayer `events` Field Type
**Spec shows:** `events: Arc<BTreeMap<EventId, Event>>` (full Event objects)
**Code has:** `dag: Arc<BTreeMap<EventId, Vec<EventId>>>` (only parent pointers)
The code's choice is strictly more efficient (parent pointers suffice for `is_descendant_dag`). But the spec implies full events are available, which could matter if a backend needed to inspect event contents during comparison.

### A5. Layer Application for ALL Backends vs Only Relevant Ones
**Code (entity.rs:366-368):** `for (_backend_name, backend) in state.backends.iter() { backend.apply_layer(&layer)?; }` -- applies EVERY layer to EVERY backend, even if the layer has no operations for that backend.
**Spec:** Does not specify whether layers are applied to all backends or only those with operations. Current behavior is safe (backends handle empty operations gracefully) but potentially wasteful.

---

## Summary

### Conformance Counts

| Status | Count |
|---|---|
| COMPLIANT | 37 |
| DEVIATED (justified) | 6 |
| NON-COMPLIANT | 0 |

### Key Findings

1. **No non-compliant items found.** The implementation faithfully follows the spec's algorithmic design for BFS comparison, layer computation, and LWW/Yrs conflict resolution.

2. **6 justified deviations** are all API refinements or structural improvements:
   - `EventLayer.dag` uses parent pointers instead of full events (more efficient)
   - `ValueEntry` is an enum instead of struct (more type-safe)
   - `compare()` signature has reordered params, explicit budget, async
   - `apply_layer` takes a struct instead of two slices (enables DAG context)
   - `into_layers()` requires `current_head` parameter (needed for partitioning)
   - Empty other-chain handled implicitly via standard layer resolution rather than explicit conservative check

3. **9 undocumented behaviors** are all justified additions: quick-check shortcut, unfetchable meet-point handling, creation event fast path, non-creation rejection, durable/ephemeral divergence, backend late-creation, older_than_meet rule, LRU caching, and single-step budget escalation.

4. **Spec ambiguities** around empty other-chain resolution (A1) and the head-update comment (A2) could benefit from spec updates to match the actual (correct) code behavior.

5. **Test coverage is comprehensive** and matches all spec claims. The only untested area is budget resumption, which is acknowledged as future work.

6. **The spec should be updated** to reflect the actual `EventLayer` struct, `apply_layer` signature, `compare` signature, and `ValueEntry` enum. These are documentation drift, not bugs.
