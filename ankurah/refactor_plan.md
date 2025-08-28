# LiveQuery Refactor Plan

## Overview

Refactor from `context.subscribe()` + `ResultSetSignal` to `context.query()` + `LiveQuery<R>` pattern.

## Core Changes

### 1. ResultSet<R> Refactor (core/src/resultset.rs)

```rust
pub struct ResultSet<R> {
    entities: Arc<SafeMap<EntityId, Entity>>,
    loaded: AtomicBool,
    _phantom: PhantomData<R>,
}
```

**Implement on ResultSet:**

- `Signal` - delegates to entities broadcast
- `Subscribe<ChangeSet<R>>` - converts updates to ChangeSet with ALL entities
- `Get<Vec<R>>` - converts entities to Vec<R>
- `Peek<Vec<R>>` - same without tracking

### 2. LiveQuery<R> Structure (core/src/context.rs)

```rust
pub struct LiveQuery<R: View> {
    local_subscription: LocalSubscription,
    resultset: ResultSet<R>,
    error: Arc<Mutex<Option<RetrievalError>>>,
}
```

**Implement as pure delegation:**

- `Signal` - delegate to resultset
- `Subscribe<ChangeSet<R>>` - delegate to resultset
- `Get<Vec<R>>` - delegate to resultset
- `Peek<Vec<R>>` - delegate to resultset

### 3. PredicateState in Reactor

- Replace `matching_entities: Arc<SafeMap<EntityId, Entity>>`
- With `resultset: ResultSet<Entity>`
- Set `loaded` flag in `reactor.initialize()` (not on MembershipChange::Initial)

### 4. Context::query() - Make Synchronous

```rust
pub fn query<R: View>(&self, args) -> Result<LiveQuery<R>, RetrievalError> {
    // 1. Parse args, create reactor subscription
    // 2. Call add_predicate, get ResultSet<Entity>
    // 3. Create LiveQuery with typed ResultSet<R>
    // 4. Spawn async initialization task
    // 5. Return LiveQuery immediately
}
```

### 5. Async Initialization

New function `initialize_query_async`:

- Fetch initial states from storage/peers
- Convert to entities
- Call `reactor.initialize()` - sets loaded flag
- Set error state if fails

### 6. ReactorSubscription::add_predicate

- Return `ResultSet<Entity>` instead of raw Arc<SafeMap>

## WASM Macro Changes (derive/src/model.rs)

### 1. Remove WasmSignal

- Remove `#[derive(WasmSignal)]` from `#resultset_name`
- Keep `#[wasm_bindgen]` on both `{Model}ResultSet` and `{Model}LiveQuery`

### 2. Generate {Model}LiveQuery

```rust
#[wasm_bindgen]
pub struct {Model}LiveQuery {
    inner: LiveQuery<{Model}View>
}

impl {Model}LiveQuery {
    pub fn value(&self) -> {Model}ResultSet {
        // Since ResultSet<R> implements Get<Vec<R>>, convert to wrapped ResultSet
        let items = self.inner.get();
        {Model}ResultSet { items, loaded: self.inner.resultset.loaded() }
    }
    pub fn peek(&self) -> {Model}ResultSet
    pub fn subscribe(&self, callback: JsFunction) -> SubscriptionGuard
    pub fn error(&self) -> Option<String>  // Check error state
}
```

### 3. Update query() Function

```rust
pub fn query(context: &Context, predicate: String) -> Result<{Model}LiveQuery, JsValue> {
    let livequery = context.query::<{Model}View>(predicate)?;
    Ok({Model}LiveQuery { inner: livequery })
}
```

## Key Implementation Details

1. **ResultSet as Core Signal**: ResultSet becomes the signal/reactive container, not just a data holder
2. **Loaded State**: Track via AtomicBool in ResultSet, set in `reactor.initialize()`
3. **LiveQuery as Wrapper**: LiveQuery<R> purely delegates to ResultSet<R> for signal operations
4. **Type Erasure**: PredicateState uses ResultSet<Entity>, LiveQuery uses ResultSet<R>
5. **Error Handling**: Store errors in LiveQuery, expose via getter
6. **ChangeSet**: Include ALL matching entities in resultset field
7. **RAII**: LiveQuery handles cleanup via Drop, no separate handle needed

## Testing Strategy

1. Test synchronous return with empty initial state
2. Verify async initialization completes
3. Test error propagation from background task
4. Verify WASM bindings work correctly
5. Test ResultSet signal traits implementation
