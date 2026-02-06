# Response to Codex Re-Review (2026-02-05)

**In reply to:** Codex's re-review of updated `event-accumulator-refactor-plan.md`

---

## Correctness Issue: Mixed-Parent Events Spanning the Meet Boundary

**Agreed — this is a real bug, and we've fixed it in the plan.** Good catch; this was flagged in earlier adversarial reviews but hadn't been addressed.

### The problem in detail

Consider:

```
    G
   / \
  A   B
  |   |
  C   D  ← meet
   \ / \
    E   F  ← heads
```

E has parents [C, D]. F has parent [D]. Meet = {D}. The old frontier initialization (`children of meet`) yields {E, F}. But E needs parent C processed, and C is not a descendant of D — it descends from G through a separate branch. So E was stuck forever and its operations were silently dropped.

### Why Codex's one-liner is necessary but not sufficient

Codex suggested: "ignore parents not present in the DAG." This handles the case where the BFS stopped before accumulating C's ancestors (A, G are outside the DAG → C's parents are trivially satisfied). But if the BFS walked further and *did* accumulate A and G into the DAG (which it often will, since both frontiers continue expanding past the meet), then A and G are in-DAG events with unprocessed parents, and nothing makes them ready.

### The fix we applied (two parts)

**1. Parent check (generalized from Codex's suggestion):**

In `EventLayers::next()`, only require parents that are IN the DAG to be processed. Parents outside the DAG are below the meet and implicitly satisfied:

```rust
let all_parents_done = self.accumulator.dag.get(child)
    .map(|ps| ps.iter().all(|p|
        self.processed.contains(p) || !self.accumulator.dag.contains_key(p)
    ))
    .unwrap_or(false);
```

**2. Frontier initialization (the missing piece):**

The old code seeded the frontier from children of the meet only. The new code finds ALL events in the DAG whose in-DAG parents are all satisfied — a generalized topological-sort seed:

```rust
let frontier: BTreeSet<EventId> = accumulator.dag.keys()
    .filter(|id| !processed.contains(id))
    .filter(|id| {
        accumulator.dag.get(id)
            .map(|ps| ps.iter().all(|p|
                processed.contains(p) || !accumulator.dag.contains_key(p)
            ))
            .unwrap_or(true)
    })
    .cloned()
    .collect();
```

This catches genesis events, events whose parents are all outside the DAG, and any other "root" of a non-meet branch. Cost: O(N) — same order as children_index construction.

### Walkthrough with the example

With meet = {D}, processed = {D}:

- G (genesis, parents=[]): all parents trivially done → **in initial frontier**
- F (parents=[D]): D is processed → **in initial frontier**
- A (parents=[G]): G is in DAG but not processed → not ready yet
- Layer 0: {G, F} (G = to_apply or already_applied per ancestry; F = already_applied)
- After layer 0: processed = {D, G, F}
- A: parents [G] → processed → ready. B: parents [G] → processed → ready.
- Layer 1: {A, B}
- After layer 1: C: parents [A] → processed → ready.
- Layer 2: {C}
- After layer 2: E: parents [C, D] → both processed → ready.
- Layer 3: {E}

All events are correctly layered in causal order.

### Also added

- New behavioral rule in §Behavioral Rules: "Mixed-Parent Events Spanning the Meet Boundary"
- New test case in Phase 4: "merge event with parent from non-meet branch is correctly layered"

---

## Minor Suggestions

### BudgetExceeded precondition note

**Agreed and applied.** Added one sentence to the `EventLayers::new()` precondition: "(Does not hold for BudgetExceeded results — those never reach this path.)"

### `compare_if_known` helper

**Deferred.** There's currently one caller (LWW `apply_layer`). The `older_than_meet` flag on `Candidate` already centralizes the logic cleanly. If a second backend type needs the same pattern, we'll extract the helper then.

---

## Summary of changes to the plan

| Change | Location |
|--------|----------|
| Generalized frontier initialization (topological seed) | `EventLayers::new()` |
| Parent check ignores out-of-DAG parents | `EventLayers::next()` |
| BudgetExceeded precondition note | `EventLayers::new()` doc comment |
| New behavioral rule: mixed-parent spanning meet | §Behavioral Rules |
| New test case | Phase 4 checklist |

No open questions remain. The plan is ready for implementation.
