# Structured Errors Specification

> **Status: Core migration complete.** All storage backends compile with `StorageError`. Remaining work is enriching diagnostic output. See [tasks.md](./tasks.md).

## Objective

Errors should be **actionable for users** and **diagnostic for developers**:
- **Soft errors** (NotFound, AccessDenied) are matchable enum variants with no internal details
- **Hard failures** (internal errors) provide full diagnostic context via `.diagnostic()` method

## Constraints

1. **FFI stability** - Public error types (`RetrievalError`, `MutationError`, `PropertyError`) are stable for wasm/uniffi
2. **No leaking internals** - Storage, lineage, and state errors never appear in public API
3. **Separation of concerns** - Each error type covers one domain, no cross-wrapping

## Public API

| Type | Domain | Example Soft Variants |
|------|--------|----------------------|
| `RetrievalError` | get, fetch, query | NotFound, AccessDenied, InvalidQuery |
| `MutationError` | create, commit | AccessDenied, AlreadyExists, Rejected |
| `PropertyError` | property access | Missing, InvalidVariant |

All have a `Failure` variant for internal errors, with `.diagnostic()` returning the full error chain.

## Implementation Details

See [plan.md](./plan.md) for design decisions and patterns.
See [tasks.md](./tasks.md) for remaining work.
