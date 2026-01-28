# Structured Errors Specification

## Problem
Current errors are nested wrappers with no actionable context. We need:
- **Soft errors** (AccessDenied, NotFound) - clean, matchable, no stack traces
- **Hard failures** (internal errors) - full diagnostic context via error-stack

## Goals
1. **FFI-safe public errors** - `RetrievalError`, `MutationError`, `PropertyError` stable for wasm/uniffi
2. **Internal errors stay internal** - storage, lineage, state errors never cross API boundary
3. **Rich diagnostics for failures** - `.diagnostic()` returns full error-stack trace
4. **No cross-wrapping** - error types are cleanly separated by concern

## Public Error Types

| Type | Used By | Soft Variants | Hard Variant |
|------|---------|---------------|--------------|
| `RetrievalError` | get, fetch, query | NotFound, AccessDenied, InvalidQuery, Timeout | Failure |
| `MutationError` | create, commit | AccessDenied, AlreadyExists, InvalidUpdate, Rejected, Timeout | Failure |
| `PropertyError` | property accessors | Missing, InvalidVariant, InvalidValue, TransactionClosed, Serialize/Deserialize | Failure |

## Internal Error Types

Used inside the crate, converted to `Failure` at boundaries:
- `StorageError` - database/storage failures
- `StateError` - state serialization errors
- `LineageError` - causal ordering failures
- `RequestError` - peer communication errors
- `ApplyError` - errors applying remote updates

## Implementation

See [plan.md](./plan.md) for detailed design decisions and implementation patterns.

See [tasks.md](./tasks.md) for remaining work items.
