# Independent Review (Codex)

**Scope:** Direct code review of `/Users/daniel/ak/ankurah-201` core logic (comparison, layers, LWW backend, entity apply paths, retrieval). This is independent of the prior review digests/matrices.

---

## Findings (Ordered by Severity)

### Critical
1. **LWW `apply_layer` can hard‑fail with `InsufficientCausalInfo` when a stored value’s `event_id` is not in the accumulated event map.**
   - `apply_layer` seeds candidates from stored values, then calls `layer.compare()` to resolve against new candidates. `layer.compare()` calls `is_descendant()`, which hard‑errors if an event is missing from `EventLayer.events`. This can happen when the stored value was last written prior to the meet point and that event was not accumulated during BFS.
   - Impact: valid concurrent updates can abort with a hard error (not merely pick the “wrong” winner).
   - Evidence: `core/src/property/backend/lww.rs:169-208`, `core/src/event_dag/layers.rs:201-222`.

### High
2. **LocalRetriever ignores staged events during comparison.**
   - `LocalRetriever::expand_frontier` only queries storage. Staged events exist but are not consulted here, so BFS can miss parents that are in the same batch but not yet stored.
   - Impact: missing ancestry during compare/layering → incorrect results or `InsufficientCausalInfo` failures.
   - Evidence: `core/src/retrieval.rs:65-80` (no staged lookup), contrasted with staged event infrastructure elsewhere in the file.

3. **BudgetExceeded is a hard error in `apply_event` with a fixed budget of 100 and no resumption mechanism.**
   - Any deep asymmetry (>100 traversal rounds) rejects valid events. There is no ability to resume or escalate the budget.
   - Impact: correctness failure under deep histories or adversarial DAGs.
   - Evidence: `core/src/entity.rs:242-341`, `core/src/event_dag/comparison.rs:265-305` (budget consumption is per round).

### Medium
4. **Re‑delivery of a historical (non‑head) event can insert an ancestor into the head.**
   - `compare_unstored_event` only treats an event as redundant if it is currently in the head, not if it is already in history. `apply_event` does not guard against history re‑delivery.
   - Impact: invalid multi‑head states with ancestor + descendant as tips.
   - Evidence: `core/src/event_dag/comparison.rs:93-156`, `core/src/entity.rs:248-327`.

5. **`compute_layers` is O(n^2) due to `children_of` scanning the entire event map per lookup.**
   - This becomes a practical DoS/perf issue with moderately large event histories.
   - Evidence: `core/src/event_dag/layers.rs:100-166`.

6. **StrictDescends chain is computed but unused; correctness depends on causal delivery assumption.**
   - `apply_event` ignores the forward chain for StrictDescends. If events ever arrive out‑of‑order, missing chain replay would be incorrect.
   - Evidence: `core/src/entity.rs:256-270`, `core/src/event_dag/comparison.rs:103-114`.

### Low
7. **`build_forward_chain` is not a topological sort for multi‑meet DAGs.**
   - Harmless today because chains are not used for layer application, but it is part of public relation results and could mislead future consumers.
   - Evidence: `core/src/event_dag/comparison.rs:445-459`.

---

## Overall Assessment

The core DAG comparison and layering architecture is strong, but the LWW `InsufficientCausalInfo` hard‑failure is a correctness blocker for merging. The staged‑events gap and BudgetExceeded hard error are also significant operational risks.

---

## Suggested Next Steps (Priority Order)

1. **Fix LWW missing‑ancestor failure**: treat missing stored event IDs as older than meet (auto‑lose) or expand the accumulated event map to include stored event IDs that may compete. Add a regression test.
2. **Consult staged events in LocalRetriever** or gate merge on the EventAccumulator refactor that removes staged‑event side channels.
3. **Make comparison budget configurable or resumable**, or document it explicitly as a hard limit and raise it significantly.
4. **Prevent historical event re‑delivery from modifying head** (explicit history check in `apply_event` or reject when event already in storage).
5. **Optimize `compute_layers`** with a parent→children index to avoid O(n^2) scanning.
