# Audit Summary (core)

## Public surface (shared wasm + uniffi)
- `Context` methods (get/fetch/query)
- `Transaction` methods (commit/create/delete)
- Public error enums in `core/src/error.rs`

## Conflations to fix
- Internal storage/lineage/peer paths returning public enums.
- `anyhow::Error` in internal paths hiding semantics.
- `AccessDenied` used for evaluation failures (should be PolicyError).

## Soft vs Failure guidance
- **Soft/decline**: AccessDenied, Validation, Parse/Property/Cast/Index errors.
- **Failure**: invariants, corruption, missing required events, policy evaluation failure.

## Immediate target
- “events not found” path: lineage → entity → intent boundary using Report attachments.
