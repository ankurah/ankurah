# Cross-Crate UniFFI

> **Pain Level**: High 🔥  
> **Resolution**: Success 🎉

This document captures hard-won learnings about using UniFFI types across crate boundaries. Much of this was discovered empirically after ChatGPT confidently provided incorrect guidance.

## TL;DR

**External types (types from other UniFFI crates) work automatically with proc-macros.** You don't need `use_remote_type!` for them. Just use the types directly.

## Key Concepts

### UniFfiTag

Each crate that calls `uniffi::setup_scaffolding!()` gets its own `UniFfiTag` type. UniFFI traits like `FfiConverter<UT>`, `Lift<UT>`, `Lower<UT>`, `LiftRef<UT>` are parameterized by this tag.

### External vs Remote Types

Per [UniFFI documentation](https://mozilla.github.io/uniffi-rs/latest/types/remote_ext_types.html):

- **External types**: Types from other crates that **use UniFFI**. These work automatically with proc-macros.
- **Remote types**: Types from crates that **don't use UniFFI** (e.g., `log::Level`, `std::net::IpAddr`). These require `#[uniffi::remote]` or `use_remote_type!`.

### use_remote_type! is NOT for External Types

`use_remote_type!` is specifically for **remote + external** scenarios - when one UniFFI crate wraps a non-UniFFI type, and another UniFFI crate wants to use that wrapped type. Using it for plain external types causes "conflicting implementations" errors.

## What Works Cross-Crate

| Operation | Works? | Notes |
|-----------|--------|-------|
| `&T` (borrow arg) | ✅ | Uses `LiftRef` trait |
| `T` (owned arg) | ❌ | `Lift` trait doesn't work cross-crate |
| `T` (owned return) | ✅ | Uses `Lower` trait |
| `Arc<T>` (return) | ✅ | Special handling for Objects |

### Cross-Crate Examples

```rust
// ✅ WORKS: borrowed args, owned return
#[uniffi::export]
pub fn get_message(ctx: &Context, id: &EntityId) -> MessageView { ... }

// ✅ WORKS: borrowed external type arg
#[uniffi::export]  
pub fn process_message(msg: &MessageView) -> String { ... }

// ✅ WORKS: Arc of external type
#[uniffi::export]
pub fn wrap_message(msg: &MessageView) -> Arc<MessageView> { ... }

// ❌ FAILS: owned external type arg
#[uniffi::export]
pub fn take_message(msg: MessageView) -> String { ... }  // Lift<UniFfiTag> not satisfied
```

## Critical: Use `::uniffi::` Not `::crate::derive_deps::uniffi::`

When generating UniFFI derives in proc-macros, **always use `::uniffi::Object`** directly, not a re-exported path like `::ankurah::derive_deps::uniffi::Object`.

**Why?** The `#[derive(uniffi::Object)]` macro generates code that references `crate::UniFfiTag`. If you use `::other_crate::uniffi::Object`, the derive macro runs in `other_crate`'s context and generates `other_crate::UniFfiTag` references, which then fail to resolve in the user's crate.

```rust
// ❌ WRONG - UniFfiTag resolves in ankurah crate context
quote! { #[derive(::ankurah::derive_deps::uniffi::Object)] }

// ✅ CORRECT - UniFfiTag resolves in user's crate context  
quote! { #[derive(::uniffi::Object)] }
```

## Users Must Have `uniffi` as a Direct Dependency

Unlike other derive macro dependencies (like `serde`), **`uniffi` cannot be re-exported through a facade crate**. Users must add it directly:

```toml
[features]
uniffi = ["ankurah/uniffi", "dep:uniffi"]

[dependencies]
uniffi = { version = "0.29", optional = true }
```

### Why Re-exporting Doesn't Work for UniFFI

The common pattern of re-exporting macro dependencies through a facade crate:

```rust
// In ankurah/src/lib.rs
pub mod derive_deps {
    pub use ::uniffi;  // Re-export for derive macros
}

// In generated code
#[derive(::ankurah::derive_deps::uniffi::Object)]
```

This pattern works for most derive macros because they generate code that doesn't care about `crate::` resolution. But UniFFI's `#[derive(Object)]` generates:

```rust
impl FfiConverter<crate::UniFfiTag> for MyType { ... }
```

The `crate::` here resolves relative to **where the derive path starts**. With `::ankurah::derive_deps::uniffi::Object`, `crate` = `ankurah`. With `::uniffi::Object`, `crate` = the user's crate.

### Opinion: Re-exporting Macro Dependencies is Fragile

The practice of re-exporting dependencies for derive macros to use (e.g., `::mycrate::derive_deps::serde`) is convenient but introduces hidden coupling:

**Pros:**
- Users don't need to manually add transitive dependencies
- Version alignment is automatic

**Cons:**
- Breaks when the derive macro uses `crate::` internally (like UniFFI)
- Obscures actual dependencies from users and tooling
- Can cause subtle breakage when the re-exported crate changes behavior
- Makes it harder to understand what's actually being used

**Recommendation:** For derive macros that generate non-trivial code (especially those using `crate::` references), require users to add direct dependencies. It's more explicit, more robust, and avoids macro hygiene surprises.

## Architecture Pattern

```
┌─────────────────────────────────────────────────────────────┐
│  model crate                                                │
│  - uniffi::setup_scaffolding!()                            │
│  - #[derive(uniffi::Object)] on View/Mutable structs       │
│  - Functions returning Vec<T>, Option<T> defined HERE      │
│  - Callback traits defined HERE                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ uniffi_reexport_scaffolding!()
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  bindings crate (cdylib)                                   │
│  - uniffi::setup_scaffolding!()                            │
│  - model_crate::uniffi_reexport_scaffolding!()             │
│  - App-specific glue (init_node, logging, etc.)            │
│  - Can use &T args, T returns for external types           │
│  - CANNOT use owned args or generic container returns      │
└─────────────────────────────────────────────────────────────┘
```

## Common Errors and Solutions

### "cannot find type `UniFfiTag` in the crate root"

**Cause**: Using `::other_crate::uniffi::Object` instead of `::uniffi::Object`  
**Solution**: Change to `::uniffi::Object` in generated code

**Also**: Missing `uniffi::setup_scaffolding!()` in a crate that uses UniFFI derives  
**Solution**: Add `uniffi::setup_scaffolding!();` to the crate's `lib.rs`

### "conflicting implementations of trait `FfiConverter<UniFfiTag>`"

**Cause**: Using `use_remote_type!` for an external type that already has UniFFI derives  
**Solution**: Don't use `use_remote_type!` for external types - they work automatically

### "the trait bound `T: Lift<UniFfiTag>` is not satisfied" (for owned args)

**Cause**: Owned arguments of external types don't work cross-crate  
**Solution**: Use `&T` (borrowed reference) instead of `T`

### "the trait bound `Vec<T>: Lower<UniFfiTag>` is not satisfied"

**Cause**: Generic containers with external types don't work cross-crate  
**Solution**: Define the function in the same crate as type `T`

### "there is no reactor running, must be called from the context of a Tokio 1.x runtime"

**Cause**: UniFFI async functions default to using `async_compat` which doesn't provide a full tokio runtime  
**Solution**: Add `#[uniffi::export(async_runtime = "tokio")]` to async method impls, and enable the `tokio` feature on the `uniffi` dependency:
```toml
uniffi = { version = "0.29", features = ["tokio"] }
```

### "could not find `async_compat` in `deps`"

**Cause**: Missing `tokio` feature on `uniffi` dependency when using `async_runtime = "tokio"`  
**Solution**: Add `features = ["tokio"]` to the uniffi dependency in Cargo.toml

## What ChatGPT Got Wrong

1. **"Each crate's UniFfiTag is isolated, types can't be shared"** - False for external types with proc-macros
2. **"You need use_remote_type! to use types from other UniFFI crates"** - Wrong, that's for remote types
3. **"The only viable path is wrapper types or avoiding UniFFI in library crates"** - Unnecessary complexity
4. **"uniffi_reexport_scaffolding! unifies UniFfiTag types"** - It only re-exports symbols for linking

## References

- [UniFFI: Remote and External Types](https://mozilla.github.io/uniffi-rs/latest/types/remote_ext_types.html)
- [UniFFI: Proc Macros](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html)


