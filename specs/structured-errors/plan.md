# Structured Errors Plan

## Scope
Start with the “events not found” path only.

## Steps (minimal)
1. Add `error-stack` dependency.
2. Define context structs: `LineageContext`, `EntityContext`, `IntentContext`.
3. Add `Failure` variants + `.diagnostic()` to public enums.
4. Implement incremental report attachment in lineage → entity → intent.
5. Ensure wasm/uniffi surface stays `Result<_, RetrievalError/MutationError>`.

## Verification
- Run `rt_missing_events_from_state_snapshot`.
- Confirm diagnostic includes intent/entity/lineage context.
- Build with uniffi + wasm features.
