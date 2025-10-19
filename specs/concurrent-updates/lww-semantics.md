# Causal Last-Write-Wins (LWW) Register Semantics

## Overview

The LWW backend implements deterministic conflict resolution for concurrent updates to entity properties. Unlike traditional CRDT LWW registers that use timestamps or pure lexicographic ordering, our implementation uses **topological precedence** combined with lexicographic tiebreaking.

## Core Algorithm

When applying concurrent updates via a `ForwardView`, the LWW transaction processes events in **ReadySet order** (topological layers from the meet):

```
meet(A)
  ├─ b → c → d → m (subject/other branch)
  └─ q            (concurrent branch)
```

### Three-Tier Precedence Rules

The LWW backend resolves conflicts using these rules in order:

1. **Lineage Precedence (Primary Rule)**: Events in later ReadySets supersede events in earlier ReadySets
2. **Lexicographic Tiebreaking (Secondary Rule)**: Among events in the same ReadySet, the event with the lexicographically greatest EventId wins
3. **Primary Path Optimization**: Primary-only ReadySets before any concurrency are skipped (these are already applied)

### Topological Precedence Semantics

**Key Insight**: Events that are topologically "deeper" (more ReadySets from the meet) take precedence over shallower concurrent branches, even when both are causally concurrent from the meet's perspective.

#### Example: Late-Arriving Grandparent Concurrency

Consider this scenario:

```
a → b → c → d → ... → m  (current head: [m])
    └─ q                 (late-arriving, parent: [a])
```

**What happens when q arrives?**

1. **Meet identification**: [a] (common ancestor)
2. **ForwardView construction**:
   - Subject path (incoming): a → q
   - Other path (current): a → b → c → d → ... → m
3. **ReadySet organization** (by topological distance from meet):

   ```
   ReadySet 1: { q (Primary), b (Concurrency) }
   ReadySet 2: { c (Concurrency) }
   ReadySet 3: { d (Concurrency) }
   ...
   ReadySet N: { m (Concurrency) }
   ```

4. **Resolution**:
   - ReadySet 1: Between q and b, use lexicographic tiebreak (whichever EventId is greater)
   - ReadySet 2, 3, ..., N: Each supersedes the previous via **lineage precedence**
   - **Result**: m wins, even though q and m are both concurrent from the meet

**Why this makes sense**: The branch a→b→c→...→m represents a longer causal history. Even though q branched from a, the system has accumulated more causal information along the other branch. Topological precedence respects this accumulated history.

### Comparison with Traditional LWW

| Approach                             | Behavior                                                                | Trade-offs                                           |
| ------------------------------------ | ----------------------------------------------------------------------- | ---------------------------------------------------- |
| **Traditional (timestamp-based)**    | Uses wall-clock timestamps                                              | Clock skew issues, not deterministic                 |
| **Traditional (pure lexicographic)** | All concurrent events use EventId comparison only                       | Ignores causal depth, treats all concurrency equally |
| **Ankurah (topological precedence)** | Later ReadySets supersede earlier ones, lexicographic for same ReadySet | Respects causal chain depth, deterministic           |

## Implementation Details

### Transaction Lifecycle

```rust
let mut tx = backend.begin();  // Snapshot current state

for ready_set in forward_view.iter_ready_sets() {
    tx.apply_ready_set(&ready_set);  // Accumulate winners
}

tx.commit();  // Apply winners to backend
```

### Per-Property Resolution

Each property maintains a "current winner" as ReadySets are processed:

1. **First ReadySet with concurrency**: Initialize winners from candidates using lexicographic comparison
2. **Subsequent ReadySets**: Any event touching a property in this ReadySet supersedes the previous winner (lineage precedence)
3. **Commit**: Apply final winners to the backend's ValueRegisters

### Primary-Only Optimization

```rust
if !has_concurrency && !self.has_seen_concurrency {
    return Ok(());  // Skip - these are already applied to entity
}
```

This optimization skips Primary-only ReadySets that occur before any concurrency is detected, since these events are already reflected in the entity's state.

## Edge Cases

### Case 1: Multiple Properties with Different Winners

```
meet(A): {x: 10, y: 20}
   ├─ B: {x: 30}
   └─ C: {y: 40}
```

**Result**: x=30 (from B), y=40 (from C). Each property is resolved independently.

### Case 2: Same Property, Multiple Concurrent Updates

```
meet(A): {x: 10}
   ├─ B: {x: 20}
   └─ C: {x: 30}
```

Both B and C are in the same ReadySet (both children of A). **Result**: Lexicographic tiebreak determines the winner (EventId of B vs C).

### Case 3: Diamond with Convergence

```
    A {x: 10}
   / \
  B   C {x: 20}
   \ /
    D {x: 30}
```

**ReadySets**:

- ReadySet 1: {B, C} → x=20 wins (C's EventId > B's, assuming)
- ReadySet 2: {D} → x=30 wins (lineage precedence)

**Result**: x=30

### Case 4: No Concurrent Changes to Same Property

```
meet(A): {x: 10, y: 20}
   ├─ B: {x: 30}        (subject)
   └─ C: {y: 40, z: 50}  (concurrency)
```

No conflict! B updates x, C updates y and z. **Result**: x=30, y=40, z=50.

## Testing Strategy

The LWW implementation includes comprehensive unit tests covering:

1. **test_primary_only_produces_no_changes**: Verifies Primary-only optimization
2. **test_single_concurrency**: Basic concurrent update resolution
3. **test_lexicographic_tiebreak**: Multiple events in same ReadySet
4. **test_multiple_readysets_revise_winners**: Lineage precedence across ReadySets
5. **test_lineage_precedence_beats_lexicographic**: Later ReadySet supersedes earlier
6. **test_same_property_conflict**: Lexicographic tiebreak for same-property conflicts
7. **Integration tests**: Real-world concurrent transaction scenarios

## Future Considerations

### Alternative Semantics

If pure lexicographic (no topological bias) is needed in the future, the implementation would need to:

1. Collect ALL concurrent events at once (flatten ReadySets)
2. Group by property
3. Apply single lexicographic comparison per property
4. Lose the benefits of streaming ReadySet processing

This would change the semantics significantly and is not currently planned.

### Performance Characteristics

- **Time complexity**: O(E) where E is the number of events in the ForwardView
- **Space complexity**: O(P) where P is the number of unique properties touched
- **Streaming**: ReadySets are processed incrementally without full materialization

## References

- `core/src/property/backend/lww.rs` - Implementation
- `core/src/causal_dag/forward_view.rs` - ForwardView and ReadySet iterators
- `specs/concurrent-updates/spec.md` - Overall concurrent updates specification
