# UniFFI Support in Ankurah

Ankurah supports generating native bindings via [UniFFI](https://mozilla.github.io/uniffi-rs/), enabling use from Swift, Kotlin, Python, and React Native (via [uniffi-bindgen-react-native](https://github.com/jhugman/uniffi-bindgen-react-native)).

## When to Use UniFFI

| Target | Use |
|--------|-----|
| Web browser | WASM (`ankurah/wasm` feature) |
| React Native | UniFFI (`ankurah/react-native` feature) |
| Native iOS/Android | UniFFI (`ankurah/uniffi` feature) |
| Rust server | No bindings needed |

## Features

```toml
[features]
# For React Native apps
react-native = ["ankurah/react-native"]

# For native Swift/Kotlin apps (no React)
uniffi = ["ankurah/uniffi", "dep:uniffi"]

[dependencies]
uniffi = { version = "0.29", optional = true, features = ["tokio"] }
```

**Note**: Users must add `uniffi` as a direct dependency - it cannot be re-exported through Ankurah. See [cross-crate.md](cross-crate.md) for why.

## Generated Types

For a model like `Message`, the `uniffi` feature generates:

| Type | Purpose |
|------|---------|
| `MessageView` | Read-only view (UniFFI Object) |
| `MessageMutable` | Editable handle (UniFFI Object) |
| `MessageOps` | Singleton with CRUD methods |
| `MessageInput` | Creation data (UniFFI Record) |
| `MessageRef` | Typed entity reference wrapper |
| `MessageLiveQuery` | Reactive query subscription |
| `MessageResultSet` | Query results collection |
| `MessageChangeSet` | Subscription change notifications |

## Documentation

| Doc | Contents |
|-----|----------|
| [basics.md](basics.md) | Records vs Objects, Arc wrapping, argument patterns |
| [cross-crate.md](cross-crate.md) | Using UniFFI types across crate boundaries |
| [derive-macros.md](derive-macros.md) | How the derive macro generates UniFFI code |

## Quick Reference

### Records vs Objects

| Type | Has Methods? | In `Vec<T>` | In `Option<T>` |
|------|--------------|-------------|----------------|
| Record | ❌ | ✅ `Vec<T>` | ✅ `Option<T>` |
| Object | ✅ | ❌ Use `Vec<Arc<T>>` | ❌ Use `Option<Arc<T>>` |

### Cross-Crate Function Signatures

| Pattern | Works? |
|---------|--------|
| `fn foo(arg: &T)` | ✅ Borrowed args work |
| `fn foo(arg: T)` | ❌ Owned args fail cross-crate |
| `fn foo() -> T` | ✅ Owned returns work |
| `fn foo() -> Vec<Arc<T>>` | ✅ Collections need Arc |

### Async Functions

UniFFI async functions need tokio. Use:

```rust
#[uniffi::export(async_runtime = "tokio")]
impl MessageOps {
    pub async fn query(&self, ...) -> Result<...> { ... }
}
```

And in Cargo.toml:
```toml
uniffi = { version = "0.29", features = ["tokio"] }
```

## Reference Implementation

The [ankurah-react-native-template](https://github.com/ankurah/ankurah-react-native-template) demonstrates UniFFI integration with React Native. It ports the browser chat app from `ankurah-react-sled-template` and serves as the validation testbed for the UniFFI feature.

## References

- [UniFFI Documentation](https://mozilla.github.io/uniffi-rs/)
- [uniffi-bindgen-react-native](https://github.com/jhugman/uniffi-bindgen-react-native)
- [UniFFI Proc Macros](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html)


