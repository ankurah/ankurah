# Entity Apply Event/State TODOs

Notes preserved from `entity.rs` during event_dag migration and refined during architectural review (2026-01-05).

---

## 1. StrictAscends Detection

**Current bug:** `NotDescends` conflates `StrictAscends` (older event) with `DivergedSince` (true concurrency).

**Fix:** Add symmetric `unseen_subject_heads` tracking in comparison algorithm. When comparison's traversal sees all subject heads → `StrictAscends`.

**Action for StrictAscends:** No-op. The incoming event is older than what we have.

---

## 2. Event Replay for Multi-Hop StrictDescends

When applying an event that descends from current head by more than one step, we need to replay all intermediate events.

**Current behavior:** Apply only the final event's operations (incorrect for LWW)

**Correct behavior:** Replay all events in the forward chain from meet to incoming tip

**Solution:**
- EventBridge provides all events in oldest→newest order
- Apply them sequentially
- If events are missing, fall back to state application (with attestation per policy)

---

## 3. DivergedSince Per-Property Resolution

For LWW backends, conflict resolution is per-property:

1. Walk through subject's forward chain (oldest→newest)
2. For each property modified in each event:
   - Check if that property was also modified in other's chain
   - If so, compare causal depth from meet
   - **Deeper event wins** (further from meet along its branch)
   - Lexicographic tiebreak ONLY for identical depths
3. Apply only winning values

**Key insight:** Different properties may have different winners. The merged state combines values from both branches.

---

## 4. Forward Chain Accumulation

**Decision:** Accumulate full forward chains in memory during backward traversal.

**Rationale:** We need forward chains for:
- StrictDescends replay
- DivergedSince merge (to determine causal depths)

**Future optimization:** Memory-cached forward event index. Create GitHub issue after landing branch.

---

## 5. LWWCausalRegister Event Tracking

LWW backend needs to track which event set each property:

```rust
struct CausalValueEntry {
    value: Option<Value>,
    event_id: EventId,  // Which event set this value
    committed: bool,
}
```

This enables conflict resolution when concurrent events arrive:
- Entity queries backend for current event_id
- Compares causality with incoming event
- Applies only if incoming wins

---

## 6. Multi-Head Handling

When entity has multi-head `[H, G]` and event `I` arrives:

- Compute meet with entire clock (union of pasts)
- Different head members may have different relationships with `I`
- Apply merge logic considering all concurrent tips
- Result may collapse to single head or preserve multi-head

---

## 7. Security Note (State Application)

When applying state directly (vs events), ensure:
- State is attested per PolicyAgent
- If DivergedSince, prefer requesting events for proper merge
- State-only merge may lose conflict resolution precision

---

## 8. Yrs Backend (No Changes Needed)

Yrs handles concurrency internally:
- Maintains its own vector clock
- Operations are idempotent and commute
- Just apply in causal order; Yrs merges correctly

No per-property conflict resolution needed at entity level.
