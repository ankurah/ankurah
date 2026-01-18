# LWWCausalRegister Fix: Per-Property Last-Write Competitors

## Summary
The current LWWCausalRegister implementation filters competing values by `current_head` membership. This is insufficient because a property’s last write may belong to an event that is **not** a head tip (e.g., a later event on the same branch did not touch that property). As a result, concurrent updates can incorrectly overwrite the true last write for a property.

This document specifies a fix: **always include per-property last-write candidates in competition**, and resolve dominance by causal relationship (lexicographic tie-break only when concurrent).

## Problem Statement
Current behavior in `apply_layer`:
- Candidates include events in the layer and values from concurrent head tips.
- Stored values whose `event_id` is not in the head are ignored.

This drops valid competitors. Head supersession does **not** imply per-property supersession.

## Reproducible Scenario
1. `A` (genesis)
2. `B1` (parent A) writes `title="X"` -> head = `[B1]`
3. `B2` (parent B1) does not touch `title` -> head = `[B2]` and stored `title` has `event_id=B1`
4. `C1` (parent A) arrives -> head = `[B2, C1]`
5. `D1` (parent C1) writes `title="Y"`

Current logic excludes `B1` because it is not in `current_head`. `D1` wins by default, even though `B1` and `D1` are concurrent and should compete.

## Intended Semantics
For each property:
1. Identify all **last-write candidates** (per-property last write from state, plus layer events touching that property).
2. Determine causal dominance:
   - If candidate A descends from candidate B, A wins.
   - If candidate B descends from candidate A, B wins.
3. If concurrent or incomparable, choose lexicographic max `EventId`.

## Fix Strategy
### Core rule
Always include the **stored per-property last-write value** as a candidate, regardless of head membership.

### Causal checks
Use event DAG context (via accumulator/refactor) to determine causal relation between candidates. Lexicographic tie-break only when concurrent.

### Required data
At minimum, each stored property value must retain:
- `event_id` (already present)

To resolve causal relations without extra fetches, integrate with the **EventAccumulator** refactor so the layer application can query causal relationships for candidate pairs.

## Integration with EventAccumulator Refactor
Proposed flow:
1. `ComparisonResult` retains an `EventAccumulator` seeded with incoming events.
2. `apply_layer` receives a `CausalAccessor` (from the accumulator) that can answer `relation(a, b)`.
3. LWWCausalRegister evaluates candidates using causal dominance; only falls back to lexicographic if concurrent or unknown.

This avoids side-channel re-fetching and ensures deterministic application.

## Open Questions
- Do we want to allow “unknown” relations (missing data) to default to lexicographic, or require fetches to confirm causality?
- Do we need to include historical last-write candidates from earlier layers, or is “current stored last-write + layer events” sufficient?
- Should causal checks be batched to reduce overhead in wide layers?

## Test Plan
Add tests that fail under current behavior:
- `test_lww_last_write_not_head_competes`: last-write event not in head still competes and can win.
- `test_lww_concurrent_lexicographic_fallback`: concurrent candidates resolve by EventId.
- `test_lww_causal_descends_wins`: causal descendant wins even if lexicographic is lower.

