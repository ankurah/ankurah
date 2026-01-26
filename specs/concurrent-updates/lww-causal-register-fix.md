# LWWCausalRegister Fix: Per-Property Last-Write Competitors

## Summary
The current LWWCausalRegister implementation filters competing values by `current_head` membership. This is insufficient because a property’s last write may belong to an event that is **not** a head tip (e.g., a later event on the same branch did not touch that property). As a result, concurrent updates can incorrectly overwrite the true last write for a property.

This document specifies a fix: **always include per-property last-write candidates in competition**, and resolve dominance by causal relationship (lexicographic tie-break only when concurrent). If causal information is incomplete, **bail out** (do not apply).

## Non-Legacy Requirement
There is no representable legacy state for this backend.
- The stored `event_id` for each property value is **required** (not `Option<_>`).
- If a state buffer lacks event IDs, it must be rejected or upgraded before applying.

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
3. If concurrent, choose lexicographic max `EventId`.
4. If causal relation cannot be determined with available DAG info, **bail out** (do not apply).

## Fix Strategy
### Core rule
Always include the **stored per-property last-write value** as a candidate, regardless of head membership.

### Causal checks
Use event DAG context (via accumulator/refactor) to determine causal relation between candidates. Lexicographic tie-break only when concurrent. If relation is not provable with available data, **bail out**.

## Integration with EventAccumulator Refactor
The EventLayer should be tied to the accumulator embedded in `Comparison`, so the backend can compare candidates without external lookup.

### Proposed API shape (draft)
```rust
/// Comparison result that retains accumulated DAG context.
pub struct ComparisonResult<Id, Ev> {
    pub relation: AbstractCausalRelation<Id>,
    accumulator: EventAccumulator<Id, Ev>,
}

impl<Id, Ev> ComparisonResult<Id, Ev> {
    /// Build an iterator over causal layers (only valid for DivergedSince).
    pub fn into_layers(self, current_head: &[Id]) -> Result<EventLayers<Id, Ev>, RetrievalError>;
}

/// Iterates over concurrency layers with access to causal comparison.
pub struct EventLayers<Id, Ev> {
    // owns accumulator + layer plan
}

impl<Id, Ev> EventLayers<Id, Ev> {
    /// Fetch next layer; returns error if DAG info is insufficient.
    pub async fn next_layer(&mut self) -> Result<Option<EventLayer<Id, Ev>>, RetrievalError>;
}

/// A concurrency layer, bound to the accumulator for causal comparisons.
pub struct EventLayer<Id, Ev> {
    pub already_applied: Vec<Ev>,
    pub to_apply: Vec<Ev>,
    // internal reference to accumulator context
}

impl<Id, Ev> EventLayer<Id, Ev> {
    /// Compare two event IDs using accumulated DAG context.
    /// Returns Err if relation cannot be proven with available data.
    pub fn compare(&self, a: &Id, b: &Id) -> Result<CausalRelation, RetrievalError>;
}

pub enum CausalRelation {
    Descends,
    Ascends,
    Concurrent,
}
```

### LWWCausalRegister usage (sketch)
For each property:
1. Build candidate set: stored last-write event_id + any layer events touching that property.
2. Reduce by repeated `EventLayer::compare` calls.
3. If any comparison yields Err, bubble up and abort apply.

## Open Questions
- Should “insufficient DAG info” be a specific error type to trigger refetch/resume?
- Should layer iteration guarantee all candidate event_ids are in the accumulator, or should LWW request backfill?
- What is the expected behavior when `apply_state` receives a state buffer missing event IDs (reject vs migrate)?

## Test Plan
Add tests that fail under current behavior:
- `test_lww_last_write_not_head_competes`: last-write event not in head still competes and can win.
- `test_lww_concurrent_lexicographic_fallback`: concurrent candidates resolve by EventId.
- `test_lww_causal_descends_wins`: causal descendant wins even if lexicographic is lower.
- `test_lww_insufficient_dag_bails`: missing DAG info aborts layer application.
