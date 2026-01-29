# Public API Error Types (Client Reference)

## RetrievalError

Returned from: `Context::get`, `fetch`, `query`; `Transaction::get`

| Variant | Type | Client Action |
|---------|------|---------------|
| `NotFound(Entity/Collection)` | Soft | Handle missing data |
| `AccessDenied` | Soft | Check permissions, inform user |
| `InvalidQuery` | Soft | Fix query syntax/semantics |
| `Timeout` | Soft | Retry or inform user |
| `Failure` | Hard | Log `.diagnostic()`, report bug |

## MutationError

Returned from: `Transaction::create`, `commit`; `Context::commit_local_trx`

| Variant | Type | Client Action |
|---------|------|---------------|
| `AccessDenied` | Soft | Check permissions |
| `AlreadyExists` | Soft | Handle conflict |
| `InvalidUpdate(reason)` | Soft | Fix mutation logic |
| `Rejected` | Soft | Handle rejection, maybe retry |
| `Timeout` | Soft | Retry or inform user |
| `Failure` | Hard | Log `.diagnostic()`, report bug |

## PropertyError

Returned from: property accessors on `View` and `Mutable` types

| Variant | Type | Client Action |
|---------|------|---------------|
| `Missing` | Soft | Handle absent value |
| `InvalidVariant` | Soft | Check data/schema |
| `InvalidValue` | Soft | Check data format |
| `TransactionClosed` | Soft | Fix transaction lifecycle |
| `SerializeError` | Soft | Check value being set |
| `DeserializeError` | Soft | Check stored data |
| `Failure` | Hard | Log `.diagnostic()`, report bug |

## Handling Failures

For any `Failure` variant:

```rust
match error {
    SomeError::Failure(report) => {
        // Get full diagnostic trace
        let diagnostic = error.diagnostic().unwrap();
        log::error!("Internal error: {}", diagnostic);
        // This is a bug - report it
    }
    // Handle soft errors normally
    _ => { ... }
}
```

The `.diagnostic()` method returns `Option<String>` - it's `Some` only for `Failure` variants.
