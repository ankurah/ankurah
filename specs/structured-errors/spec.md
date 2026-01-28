# Structured Errors Specification

## Problem
Current errors (e.g. "Mutation error: retrieval error: Storage error: Events not found") are nested wrappers with no actionable context. We need to know:
- **Intent** (subscribe/fetch/get/commit)
- **Operation** (which entity, which method)
- **Root cause** (what failed and why)

## Goals
- **Three-tier context**: intent → operation → root cause
- **Log-friendly**: readable in logs
- **FFI-safe**: public error types stable for wasm_bindgen + uniffi
- **Minimal change**: start with “events not found” path
- **Preserve read/write split**: Retrieval vs Mutation

## Semantics
- **Err ≠ Failure** by default. Denials/not-found/invalid input are expected outcomes.
- **Failure** is a value judgement: invariant broken or action cannot be performed as required.
- **Intent boundary decides Failure** (public API or explicit contract boundary).

### Policy
Policy hooks return **PolicyError**, not AccessDenied:
- `PolicyError::AccessDenied(AccessDenied)` — soft/decline
- `PolicyError::Failure(Report<...>)` — evaluation failure

## Design Decisions
1. **Public error enums stay stable.** Retrieval/Mutation/etc. remain the external contract.
2. **Failure variant added to public enums.** Used only when caller judges intent failure.
3. **FFI diagnostics.** `Failure` is skipped in uniffi; `.diagnostic()` exposes report string.
4. **Internal error taxonomy.** Storage/Lineage/Peer/Apply are internal types, mapped at boundaries.
5. **error-stack for composed traces.** Attach context as failures propagate.

## Preferred Pattern (incremental stack)
Internal functions return `Result<T, Report<_>>` so context is attached **where it exists**, without late assembly.

```text
// Internal functions return Report directly
fn compare(...) -> Result<Ordering, Report<StorageError>> {
  if missing_events {
    let mut report = Report::new(StorageError::EventsNotFound(missing));
    report.attach(LineageContext { subject_frontier, other_frontier, budget_remaining });
    return Err(report);
  }
}

// Entity: attach entity context
fn apply_state(...) -> Result<bool, Report<ApplyError>> {
  compare(...).map_err(|mut report| {
    report.attach(EntityContext { entity_id, collection, method: "apply_state" });
    report.change_context(ApplyError::Lineage)
  })?;
}

// Intent boundary: attach intent + wrap into Failure
fn subscribe(...) -> Result<(), RetrievalError> {
  apply_deltas(...).map_err(|mut report| {
    report.attach(IntentContext { operation: "subscribe", predicate, query_id });
    RetrievalError::Failure(Box::new(report))
  })?;
}
```

## Open Design Space
We may allow internal errors to **expose** a report (accessor) instead of returning `Report<_>` directly, but the preferred pattern is `Result<T, Report<_>>` with incremental attachment.
