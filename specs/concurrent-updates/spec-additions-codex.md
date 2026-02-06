# Spec Additions (Codex): Recommended Updates

**Context:** These are recommended additions/clarifications to the concurrent‑updates specification to align it with observed behavior and desired semantics.

---

## 1. LWW Resolution When Stored event_id Is Older Than Meet

**Problem:** LWW resolution currently requires causal comparison between the stored value’s `event_id` and incoming candidates. If the stored `event_id` predates the meet, it may not be present in the accumulated event set, causing `InsufficientCausalInfo` despite a valid update.

**Add to spec (Behavioral Rule):**
- If a stored value’s `event_id` is **not present** in the accumulated event set for a layer, it must be treated as **strictly older than the meet** and **must lose** to any candidate in the layer for the same property.
- If a candidate’s `event_id` is missing from the accumulated event set, this is an error (`InsufficientCausalInfo`) because the system cannot validate the candidate’s lineage.

**Rationale:** This preserves safety (don’t accept unknown candidates) while allowing legitimate updates to overwrite stale stored values whose lineage is known to be older than the divergence point.

---

## 2. Idempotency Contract for apply_event

**Problem:** `compare_unstored_event` only treats an event as redundant if it is currently in the head, not if it is already in history. This means re‑delivery of historical events can re‑insert ancestors into the head.

**Add to spec (Caller Contract + Implementation Guidance):**
- **Caller contract:** Before calling `apply_event`, the caller must check if the event is already stored for the entity.
- **Or implementation guarantee (preferred):** `apply_event` must consult storage to reject events already present in history.

**Rationale:** The spec currently claims idempotent application; this is only true if the history check is enforced.

---

## 3. BudgetExceeded Semantics

**Problem:** The spec implies resumption from frontiers, but the current implementation returns a hard error without resumption.

**Add to spec (Clarification):**
- `BudgetExceeded` means comparison terminated early. The returned frontiers are **not sufficient** to resume without additional internal state. Until resumption is implemented, callers must treat this as a **hard error** or retry with a larger budget.

**Optional improvement (if resumption intended):**
- Define a resumable comparison token that captures all required state (frontiers + visited sets + meet candidates + counters).

---

## 4. Staged/Seeded Events Visibility During Comparison

**Problem:** Comparisons must consider in‑flight events to avoid missing parents during batch application.

**Add to spec:**
- Comparison must be able to access **seeded/unstored** events supplied by the caller (e.g., an event batch) and treat them as first‑class in traversal.
- If a required parent is present only in the seed set, comparison must still succeed and include that event in the accumulated set.

**Rationale:** Prevents spurious `InsufficientCausalInfo` or wrong layer computation when applying batches.

---

## 5. StrictDescends Application Semantics

**Problem:** Spec implies replay of forward chain, but current behavior applies only the incoming event (assumes causal delivery).

**Add to spec (Clarification):**
- `StrictDescends` handling assumes **causal delivery** (all parents already applied). Implementations may optionally replay the forward chain, but are not required to if causal delivery is guaranteed by the protocol.

---

## 6. Layer Computation Complexity

**Problem:** Current `compute_layers` uses O(n²) child lookups.

**Add to spec (Performance Note):**
- Implementations should build a parent→children index (or equivalent) to avoid O(n²) scans when computing layers.

---

## 7. State Apply Result Contract

**Problem:** `apply_state` returns `Ok(false)` on divergence, which is semantically ambiguous.

**Add to spec (API change recommendation):**
- Replace `bool` return with a result enum, e.g.:
  - `Applied`
  - `AlreadyApplied`
  - `Older`
  - `DivergedRequiresEvents`

---

## 8. Documentation Alignment (API Signatures)

**Problem:** Spec describes older signatures for `apply_layer` and `ValueEntry`.

**Add to spec:**
- Update `apply_layer` signature to take an `EventLayer` that provides DAG context for causal comparisons.
- Update `ValueEntry` to reflect the `Uncommitted | Pending | Committed` states.

---

## Suggested Placement

These can be inserted into:
- **Spec: “LWW Resolution Within Layer”** (Section for Rule #1)
- **Spec: “Idempotency / Apply Event”** (Rule #2)
- **Spec: “BudgetExceeded”** (Rule #3)
- **Spec: “Event Seeding / Staging”** (Rule #4)
- **Spec: “StrictDescends Action Table”** (Rule #5)
- **Spec: “Performance Notes”** (Rule #6)
- **Spec: “apply_state Semantics”** (Rule #7)
- **Spec: “API Reference”** (Rule #8)
