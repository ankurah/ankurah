# Holistic Code Review: PR #201 -- Concurrent Updates Event DAG

**Reviewer:** Independent systems engineer (code-only review, no PR comments consulted)
**Branch:** `concurrent-updates-event-dag` based off `main`
**Scope:** ~11,500 lines added, ~2,800 removed across 90 files

---

## 1. Executive Summary

This PR replaces the old monolithic `lineage.rs` (~1,000 lines) with a well-structured `event_dag/` module (~1,400 lines of core logic, ~1,800 lines of unit tests) and introduces a layered concurrent event application model. The old code performed causal comparison but lacked proper concurrent event resolution -- it could detect divergence but had no mechanism for per-property LWW merge or CRDT-aware layer application. This PR closes that gap with a clean, multi-phase design: backward BFS comparison to find the meet point, forward chain accumulation, layer computation from the meet, and per-backend conflict resolution within each layer.

The overall architecture is sound and well-decomposed. The separation between comparison (pure DAG logic), navigation (storage abstraction), layer computation, and backend-specific resolution is clean and testable. The abstraction over event ID types via `EventId`, `TClock`, and `TEvent` traits enables unit testing with simple integer IDs while the production code uses `ankurah_proto::EventId`. The test suite is thorough at the unit level and includes meaningful integration tests for DAG structure verification, LWW resolution, Yrs concurrency, and multi-node scenarios.

However, there are several areas of concern. The `apply_event` method in `entity.rs` has a subtle inconsistency in the `DivergedSince` head update logic compared to the `StrictDescends` path. The `children_of` helper in `layers.rs` performs a linear scan over all events for every call, creating O(n^2) behavior. The TOCTOU retry loop for `DivergedSince` uses a different locking pattern than `StrictDescends`, which could mask bugs or cause divergent behavior if `try_mutate` is later modified. The `build_forward_chain` method uses a simple position-based trim that may not handle all DAG topologies correctly. These are individually minor but collectively warrant attention before merging to main.

---

## 2. Architecture Assessment

**Module decomposition is excellent.** The event_dag module is split into seven focused files:

| File | Responsibility | Lines |
|------|---------------|-------|
| `traits.rs` | Abstract types (`EventId`, `TClock`, `TEvent`) | 53 |
| `relation.rs` | `AbstractCausalRelation` enum | 53 |
| `frontier.rs` | Frontier management with tainting | 53 |
| `navigator.rs` | `CausalNavigator` trait + `AccumulatingNavigator` | 148 |
| `comparison.rs` | BFS comparison state machine | 613 |
| `layers.rs` | Layer computation + intra-layer causal compare | 465 |
| `tests.rs` | Unit tests | 1801 |

**Strengths:**
- The `AbstractCausalRelation<Id>` enum is generic over ID type, enabling easy unit testing with `u32` IDs while production uses `proto::EventId`.
- The `CausalNavigator` trait cleanly abstracts storage access, allowing the comparison algorithm to be tested without any storage backend.
- The `AccumulatingNavigator` wrapper elegantly captures events during traversal for later use in layer computation, avoiding a second pass.
- The assertion system (`AssertionRelation`, `TaintReason`) is forward-looking -- it provides hooks for server-attested causal shortcuts without complicating the core algorithm.

**Concerns:**
- `comparison.rs` at 613 lines is the largest file and contains both the public API (`compare`, `compare_unstored_event`) and the internal `Comparison` state machine. The state machine alone has substantial complexity. Consider whether `Comparison` could be extracted to its own file.
- The `layers.rs` file mixes two distinct responsibilities: layer computation (`compute_layers`, `compute_ancestry`) and intra-layer causal comparison (`EventLayer::compare`, `is_descendant`). These serve different call sites and could be separated.

---

## 3. Algorithm Correctness

### BFS Comparison

The backward BFS in `comparison.rs` is fundamentally sound. The dual-frontier approach with simultaneous expansion correctly identifies all six relationship types. Key observations:

**Correct behaviors:**
- The `unseen_comparison_heads` / `unseen_subject_heads` counters provide early termination for `StrictDescends` / `StrictAscends` without needing to traverse the entire DAG.
- Meet computation via `common_child_count == 0` correctly identifies minimal common ancestors (nodes that are common but have no common children).
- Origin tracking correctly propagates which comparison heads reach each node, enabling proper `outstanding_heads` accounting.

**Potential issues:**

1. **`build_forward_chain` trimming logic (comparison.rs line 447-460):** The method reverses the visited list and finds the first position of a meet node, then skips everything up to and including it. For a simple linear chain this works, but in a DAG with multiple meet nodes at different positions in the visited list, the `position` call finds only the *first* occurrence. If the meet has multiple members, only the first one found triggers the skip. Events before other meet members would not be trimmed. Looking more carefully: `meet` is a set, and `chain.iter().position(|id| meet.contains(id))` finds the position of the first event (in forward order) that is *any* meet member. Everything after that position is kept. This is correct for the common case of a single meet node. For multi-node meet sets (e.g., meet = {M1, M2} where M1 < M2 in causal order), if M1 appears first in the forward chain, everything after M1 is kept -- but M2 and everything between M1 and M2 would incorrectly be included in the chain. This is acceptable because meet events should not appear in the "visited" list under normal operation (they are the boundary, not part of either side's exclusive history), but the comment does not explain this assumption.

2. **Empty-clock handling (comparison.rs line 53-62):** Both empty clocks return `DivergedSince` with all-empty fields. This is semantically questionable -- two empty clocks arguably should be `Equal`, not diverged. The current behavior works because the callers handle it, but it could confuse future API consumers.

### compare_unstored_event

The transformation of `StrictAscends` into `DivergedSince` (comparison.rs lines 115-148) is the most subtle part of the algorithm and is well-documented with clear examples. The key insight -- that an unstored event whose parent is older than the head represents concurrency, not ancestry -- is correct and necessary for multi-head support.

**Concern:** The `other_chain` is left empty in the `StrictAscends` transformation case, and the LWW resolution relies on this to "conservatively" keep the current value. This creates a coupling between the comparison algorithm and the LWW backend that is not immediately obvious. If a new backend is added that does not understand the "empty other_chain means keep current value" convention, it could silently produce incorrect results. This coupling should be documented in the `PropertyBackend::apply_layer` contract.

---

## 4. Concurrency Model

### TOCTOU Retry Loop

The TOCTOU retry pattern in `entity.rs` has two variants:

**StrictDescends path (entity.rs line 260-269):** Uses `try_mutate` -- grabs write lock, checks head has not changed, applies under lock. This is clean and correct.

**DivergedSince path (entity.rs line 291-327):** Manually grabs write lock and checks head. This is functionally equivalent but uses a different code path:

```rust
let mut state = self.state.write().unwrap();
if state.head != head {
    warn!("Head changed during lineage comparison, retrying...");
    head = state.head.clone();
    continue;
}
```

This is correct but does not use `try_mutate`, creating an inconsistency. If `try_mutate` is later modified (e.g., to add metrics or logging), the DivergedSince path would not get those changes. Consider unifying.

**Head update in DivergedSince (entity.rs lines 321-326):**

```rust
for parent_id in &meet {
    state.head.remove(parent_id);
}
state.head.insert(event.id());
```

This removes meet-point event IDs from the head and adds the new event. The `meet` comes from the BFS comparison of the event's parent against the entity's head. If the event's parent is `[C]` and the head is `[B, C]`, the meet is `[C]`. Removing C from head `[B, C]` and adding the new event ID gives `[B, new_event]`. This is correct -- the new event supersedes C while B remains as a concurrent tip.

However, what if the meet includes event IDs that are not in the current head? The meet represents the greatest common ancestors of the two clocks, which could be deep in history (e.g., the genesis event). Removing a deep ancestor from the head is a no-op (it is not there), so this is technically safe. But it means the head update logic silently does nothing in cases where it perhaps should be doing something more nuanced (e.g., the event extends one branch deep in history, not at the tip).

**Recommendation:** Add an assertion or log when `state.head.remove(parent_id)` returns false (the ID was not in the head), as this may indicate an unexpected DAG topology.

### Retry count

`MAX_RETRIES = 5` is hardcoded. Under high contention (20+ concurrent transactions, as tested in `test_rapid_concurrent_transactions`), this could be exhausted. The test does handle `TOCTOUAttemptsExhausted` gracefully, but in production this would cause event application to fail. Consider making this configurable or implementing exponential backoff.

---

## 5. LWW Resolution

The LWW resolution in `lww.rs::apply_layer` (lines 169-254) is well-structured:

1. **Seed from stored values:** Current committed values with their event_ids become initial candidates.
2. **Compete all events:** Both `already_applied` and `to_apply` events contribute candidates.
3. **Causal comparison:** `layer.compare()` uses DAG context to determine descends/ascends/concurrent.
4. **Tiebreak:** Concurrent events use lexicographic EventId comparison.
5. **Selective mutation:** Only winners from `to_apply` are written to state.

**This is fundamentally correct.** The per-property tracking via `ValueEntry::Committed { event_id }` enables correct resolution even when events arrive out of order.

**Concerns:**

1. **`InsufficientCausalInfo` error (lww.rs line 203-207):** If `layer.compare()` fails (missing event in the DAG context), the entire `apply_layer` fails with `MutationError`. This is a hard failure for what might be a transient data availability issue. The spec mentions "bail out" but does not specify recovery.

2. **Stored value seeding (lww.rs lines 180-189):** The code requires `event_id` on all stored values, returning a hard error if any property lacks one. This is enforced at serialization time (`to_state_buffer`), but during the transition from old data formats (before per-property event tracking), existing entities may have properties without event_ids. There is no migration path visible in this PR.

3. **The `ValueEntry` enum has three variants: `Uncommitted`, `Pending`, `Committed`.** Only `Committed` carries an `event_id`. The transition to `Committed` only happens via `apply_operations_with_event`. This means locally-set values never become `Committed` until they round-trip through an event. `to_state_buffer` will fail if any `Pending` values exist (no event_id). This should only happen if `to_state_buffer` is called between `to_operations` and event application, which seems like a narrow window but could be triggered by concurrent operations.

---

## 6. API Design

**Public interfaces are clean.** The core API surface is well-designed:

- `AbstractCausalRelation` is generic over ID type -- clean for testing.
- `EventLayer` bundles `already_applied` / `to_apply` with DAG context (`Arc<BTreeMap>`) for self-contained resolution.
- The `CausalNavigator` trait is async and budget-aware, supporting future optimizations like attestation shortcuts.

**Concerns:**
- `compute_layers` takes `events: &BTreeMap<Id, E>` and internally clones it into an `Arc` (layers.rs line 107). For large event sets, this is an unnecessary allocation. Consider accepting `Arc<BTreeMap>` directly.
- The `EventLayer::compare` method returns `Ok(CausalRelation::Descends)` for `a == b` (layers.rs line 58-59). While defensively correct, `Descends` for identity is semantically odd.

---

## 7. Error Handling

**Generally good.** Errors are typed and propagated via `Result`.

**Concerns:**

1. **Panics on lock poisoning:** Throughout `entity.rs` and `lww.rs`, `.unwrap()` is used on `RwLock::read()` and `RwLock::write()`. In a distributed system, panic propagation can cause cascading failures.

2. **`AccumulatingNavigator::into_events` calls `self.events.into_inner().unwrap()`** (navigator.rs line 101). If the `RwLock` is poisoned, this panics.

3. **The `to_state_buffer` error for missing event_id** (lww.rs lines 117-122) returns `StateError::SerializationError` wrapping an `io::Error`. This is semantically wrong -- it is a state invariant violation, not a serialization error.

---

## 8. Test Coverage

**Unit tests (1,800 lines)** are comprehensive and well-organized, covering: linear history, concurrent history, disjoint detection, empty clocks, budget exceeded, self comparison, multiple roots, unstored events, redundant delivery, assertion shortcuts, multi-head bugs, deep concurrency, chain ordering, LWW layers, Yrs layers, determinism, and edge cases.

**Integration tests** cover: DAG structure (dag_auditing.rs), LWW resolution (lww_resolution.rs), determinism (determinism.rs), edge cases (edge_cases.rs), Yrs concurrency (yrs_concurrency.rs), StateAndEvent divergence (stateandvent_divergence.rs), multi-node (durable_ephemeral.rs, multi_ephemeral.rs), and notifications (notifications.rs).

**Gaps:**

1. **No test for `BudgetExceeded` during `apply_event`.** Budget is hardcoded to 100 in entity.rs.
2. **No test for TOCTOU retry exhaustion under contention.**
3. **No test for the `Disjoint` case in `apply_event`.**
4. **No test for layer computation with multi-parent events followed by further concurrency.**
5. **No negative test for `InsufficientCausalInfo`.**

---

## 9. Code Quality

**Naming:** Excellent throughout. `meet`, `subject`/`comparison`, `already_applied`/`to_apply` are all clear and well-chosen.

**Idiomaticness:** Appropriate use of `BTreeMap`/`BTreeSet` for deterministic ordering. Good use of `Arc<BTreeMap>` in `EventLayer`.

**Documentation:** Module-level doc comments are thorough. The spec document is excellent. Inline comments explain subtle transformations well.

**Code duplication:** `layer_from_refs`, `layer_from_refs_with_context`, and `make_lww_event` are copy-pasted across four test modules. Consider extracting to a shared test utility.

---

## 10. Potential Issues

1. **`children_of` performance (layers.rs lines 159-166):** Linear scan over all events for each parent lookup creates O(N * K * W) behavior. A pre-computed parent-to-children index would be O(1) per lookup.

2. **`is_descendant` traversal (layers.rs lines 201-230):** Full DFS for each property comparison. Worst case O(M^2 * P * N) for large concurrent branches.

3. **Unbounded head growth.** No head compaction mechanism. Concurrent events from deep branch points will grow the head indefinitely.

4. **`to_state_buffer` fails for Pending values.** Narrow timing window but could be triggered by concurrent operations.

5. **Missing event handling during `expand_frontier`.** Silently omitted events leave unfetchable IDs in the frontier, consuming budget without progress.

6. **`apply_layer` called on all backends for every layer.** Each backend ignores irrelevant operations, but this is unnecessary work.

---

## 11. Strengths

1. **Principled design.** The spec, layer model, and implementation are the product of careful thought.

2. **Testability.** Trait-based abstraction enables pure unit testing without storage or networking.

3. **Assertion system.** Forward-looking hooks for server-attested causal shortcuts.

4. **Determinism guarantees.** `BTreeMap`/`BTreeSet` + lexicographic tiebreaking ensures convergence.

5. **The `AccumulatingNavigator` pattern.** Elegant capture of events during traversal.

6. **TestDag and assert_dag! macro.** Excellent integration test infrastructure.

7. **Conservative conflict resolution.** Safe default for the StrictAscends transformation case.

8. **Clean backend separation.** LWW gets causal comparison; Yrs gets CRDT semantics. `apply_layer` abstracts the difference cleanly.
