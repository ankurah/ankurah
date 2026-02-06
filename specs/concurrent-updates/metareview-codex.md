# Meta-Review (Codex): Concurrent Updates Review Pack

**Scope:** Synthesis of the review-digest, holistic review, and all review-matrix files under `specs/concurrent-updates/` (PR #201). I did not inspect the code directly for this meta-review; I cross-checked reviewer claims against each other and assessed convergence on high‑risk issues.

---

## 1. High-Confidence Consensus (Across Multiple Reviews)

1. **LWW causal resolution can hard-fail with `InsufficientCausalInfo`** when a stored value’s `event_id` is older than the meet point and therefore not present in the accumulated events map. This is flagged as **Critical** by A4, B2, B4 and called out as likely in A1/A3/B1. The failure mode is worse than “wrong winner”: it can abort `apply_event()` for valid concurrent updates.
2. **Budget=100 is too small for deep asymmetry and returns hard errors**, with no resumption path despite spec language implying resumption. This appears in A1, A4, B1, B3, digest, and holistic. This is both correctness (valid events rejected) and operational risk.
3. **`children_of` is O(n) per lookup leading to O(n^2) layer computation**. Flagged in A1/A2/A4/B1/B2/B4/holistic. This is a practical DoS vector and a perf regression at moderate N.
4. **LocalRetriever ignores staged events** (retrieval.rs): recognized as correctness risk in digest, A1, A4, and others. The EventAccumulator refactor is the planned fix, but it is still a correctness hole in this branch.
5. **Spec drift is substantial** (apply_layer signature, ValueEntry states, BudgetExceeded action, idempotency claims). A3/B3 are consistent and detailed about this.

---

## 2. Critical / High Issues That Likely Block Merge

### 2.1 LWW: Missing Ancestry Causes Hard Error (Critical)
- Multiple reviewers independently identify that `apply_layer` can hard-fail when the stored value’s `event_id` is not in the `EventLayer.events` map.
- This appears in routine scenarios (property set long before divergence, then updated on another branch). It’s not an exotic edge.
- Recommendation: implement an explicit “unknown ancestor” handling rule. Two viable patterns:
  - Treat missing stored event as **older than meet** and therefore always losing to any candidate in the layer.
  - Or expand the accumulated events set to include all stored event_ids that may compete (requires retrieval and increases cost).
- The proposed fix should be tested with: “property set at genesis, diverge, update on branch, merge” (currently missing).

### 2.2 BudgetExceeded Without Resumption (High)
- With depth >100 between meet and head, comparisons fail. This is a hard error today.
- Spec claims resumption using frontiers, but no API actually resumes (and current frontier-only data is insufficient).
- Recommendation: either (a) increase budget significantly and make it configurable, (b) implement a real resumption mechanism that preserves Comparison state, or (c) explicitly document this as a hard limit.

### 2.3 New Backend Creation During DivergedSince (High)
- B2 flags that if a backend is created in layer N, earlier layers’ ops for that backend are not replayed. This is a real correctness gap for entities where a backend type is first introduced on a concurrent branch.
- Recommendation: pre-scan all layers for backends before applying, or replay earlier layers for newly created backends.

---

## 3. Medium-Risk Issues (Correctness or Future Bugs)

- **Historical event re-delivery** creates invalid multi-head state (B1). If an event already in history but not at head is re‑applied, it can insert an ancestor into the head. The code assumes caller pre-checks for already-stored events, but `apply_event` does not enforce it.
- **`build_forward_chain` is not topological for multi-meet DAGs** (A1/A3/B1/B4). Currently “harmless” only because chains are not used for layer computation; if any consumer relies on these chains, it will be wrong.
- **Unbounded head growth** for repeated deep-branch updates is functionally correct but may degrade performance over time; no compaction strategy exists.
- **TOCTOU retry count and no backoff** are likely fragile under contention (A4/B4/holistic). Not fatal, but may drop events under load.

---

## 4. Disagreements / Uncertainties

- **StrictAscends transformation and “depth wins” semantics:**
  - Several reviews disagree on whether “deeper branch wins” is a real property. The code effectively uses causal dominance + lexicographic tiebreak; depth is not an explicit rule except via causal ancestry. The spec and tests may overstate depth semantics.
  - This is primarily a **documentation/spec clarity issue**, not necessarily a bug.

- **Head pruning uses `meet` instead of `parent`**:
  - Multiple reviews call this “fragile but correct by accident.” I agree: it works for the StrictAscends transformation and is a no-op for deep meets. It should be clarified or made more explicit (e.g., remove only current head tips that are ancestors of the incoming event).

---

## 5. Test Gaps (Most Agreed-On)

- No test for `InsufficientCausalInfo` in `apply_event` (despite being reachable).
- No test for BudgetExceeded during `apply_event`.
- No test for TOCTOU exhaustion under contention.
- No test for backend-creation-in-layer N missing earlier layers.
- No test validating chain ordering for multi-meet DAGs (if chains are intended for future use).

---

## 6. Suggested Merge Readiness Checklist (Priority Order)

1. **Fix LWW missing-ancestor failure** (Critical). Add tests for stored event below meet.
2. **Resolve BudgetExceeded behavior**: configurable budget and/or real resumption; update spec accordingly.
3. **Fix backend creation in mid-layer** so earlier layers are replayed or pre-applied.
4. **Decide and document idempotency guarantees**: enforce event-exists check in `apply_event` or explicitly document caller contract.
5. **Address staged-events gap** or explicitly gate merge on EventAccumulator refactor.
6. **Performance**: precompute parent→children index for `compute_layers`.

---

## 7. Overall Assessment

The architectural direction is strong and the test suite is unusually thorough. However, the LWW `InsufficientCausalInfo` failure mode is a **hard correctness blocker** for merging. Budget handling and backend-creation-in-layer gaps are also likely to surface in real usage. My recommendation is **do not merge** until those are resolved and corresponding tests are added.

If you want, I can translate this into a concrete fix plan or draft tests for the critical LWW scenario and BudgetExceeded behavior.
