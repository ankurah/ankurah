# Background, Notes, and Tasks for the ankurah-signals crate

(keep this up to date as we go)

The ankurah-signals crate is a library which implements a reactive programming model in the style of "signals" - we are loosely inspired by the preactjs signals library and also by some aspects of the `reactive-graph` rust crate - but ultimately we are not trying to create the best fit for ankurah, not merely to implement the same thing as has been done elsewhere.

## What is a signal to us?

- It can be observed (though we generally like to separate writers and readers)
- Chains/DAGs of signals may be constructed including mapping, filtering, and other operations
- Notifications are performed efficiently via "dirty" flagging, and transformations/clones are generally performed lazily when the value is needed (and dirty flag is set), not eagerly by writers
- Some signals may own their own data, in which case they outlive their upstream signals, and others keep their upstream signals alive. This should be transparent to the user from the documentation, and preferably implicitly in the interface of each signal type
- We do not guarantee that each upstream change will trigger one notification downstream. Any additional changes between the notification and the the subsequent .get() of the value may be lost. This is expressly desired for performance reasons.

Our incremental approach to developing this crate:

we have started with some basic signal types and a few basic accessors.
We had initially used a different crate `reactive-graph` and its sister crate `any_spawner` to implement the signals used throughout ankurah. While this library is powerful and well designed, we have determined that there are specific needs for our use case that are not met by this library.

So the initial goal here is to replace the existing uses of `reactive-graph` and `any_spawner` with this crate and develop the necessary functionality incrementally as we replace each use case. There is a subcrate in this workspace called `ankurah-react-signals` which wraps `reactive-graph` types for use in react.

One key thing to understand is that this library is not just for the internal use of the ankurah crates and the rust application code using them, but also via wasm-bindgen to javascript/typescript code which is using ankurah. At present, we are generating wrapper structs+impls in derive/src/wasm_signal.rs which work in concert with the `ankurah-react-signals` crate mentioned above. We will NOT be replacing this macro, but rather updating it to use the new `ankurah-signals` crate (abandoning `ankurah-react-signals` in place). Unlike native rust code, wasm-bindgen does not support generics - so we need to "monomorphize" the code for each signal type via macros (first by the Ankurah model macro, and then by the signals macro - both of which live in ankurah-derive crate).

## Current Status: Migration Complete, Debugging React Updates

**COMPLETED TASKS:**
[x] basic signal types Mut and Read
[x] First, basic use case is using ankurah-signals at least minimally (ankurah-websocket-client)
[x] create tests to validate Observer functionality in ankurah-signals, especially for nested dispatch scenarios (React components render as a tree, so observers need to be properly chained/restored) - Comprehensive tests in `signals/tests/observer_context.rs` covering basic subscription, nested observers, multiple signals, and cleanup
[x] design and implement observer chain storage to handle nested observers (CURRENT_CONTEXT should restore previous observer when .finish() is called, not just unset) - Implemented OBSERVER_STACK (RefCell<Vec<ObserverContext>>) with proper push/pop behavior
[x] implement useObserve hook for react which (unlike `ankurah-react-signals`) does not take a closure, and returns an observer object that you call .finish() on (this is critical, and replaces the current useSignals hook which takes a closure) - Implemented in `signals/src/react.rs` with proper React integration via useSyncExternalStore and useRef
[x] Renamed `react-signals/` to `deprecated-react-signals/` and removed all usage of `ankurah-react-signals` from Cargo.toml files
[x] Removed `reactive_graph`, `any_spawner`, and `ankurah-react-signals` dependencies from all crates
[x] Updated derive macros (`derive/src/wasm_signal.rs` and `derive/src/model.rs`) to use `ankurah-signals` types instead of `reactive_graph`
[x] Updated `connectors/websocket-client-wasm` to use new signal types (`Mut<ConnectionState>`, `Read<ConnectionState>`)
[x] Set up proper feature architecture: `wasm` feature for basic WASM functionality, `react` feature for React-specific hooks
[x] Removed `Set` trait, made `Mut::set()` a direct method on the struct
[x] Added `Mut::value()` method for direct reads without observer tracking (vs `Read::get()` which does tracking)
[x] update examples/react-app to use the new hook and remove use of `ankurah-react-signals`

**CURRENT ISSUE - IN PROGRESS:**
[ ] Fix subscription handle lifecycle management in CurrentContext::track() so React component updates work - PROBLEM: SubscriptionHandle objects are dropped immediately after creation, preventing notifications - LOCATION: `signals/src/core.rs` in `CurrentContext::track()` method

- SYMPTOM: React components don't re-render when signals change, even though data is updated in the backend - DEBUGGING: Added extensive console logging to `examples/react-app/src/App.tsx` to trace signal values and component renders - ROOT CAUSE: Need to store subscription handles in Observer/Renderer structs to keep them alive - ATTEMPTED FIX: Tried unsafe transmute hack but it caused test hangs, reverted - NEXT STEP: Implement proper storage of handles within Observer/Renderer via store_handle methods

**EXAMPLE USAGE (working pattern):**

```typescript
function MyReactComponent(current_time: StringRead<number>) {
  const observer = useObserve();
  try {
    return (
      <div>
        <p>The time is: {current_time.get()}</p>
      </div>
    );
  } finally {
    observer.finish();
  }
}
```

```typescript
function MyReactComponent(current_time: StringRead<number>) {
  // StringRead type generated by ankurah-derive macro to wrap a Read<String> signal because wasm-bindgen does not support generics

  const observer = useObserve();
  // useObserve does the following key things that we must implement:
  // 1. create (or reuse) exactly one Observer which corresponds to this react component (current withSignals implementation uses a thread local variable)
  // 2. attach the Observer to the react component such that a re-render is triggered when the Observer is notified
  // 3. set the CURRENT_CONTEXT (thread-local variable) to this observer, storing the previous observer for restoration after .finish() is called
  // 4. return the observer object
  // 5. restore the previous CURRENT_CONTEXT when .finish() is called so that other signal accesses are attached to the correct observer

  try {
    return (
      <div>
        {/* calling .get() on a signal will register a subscription to the signal with the current observer */}
        <p>The time is: {current_time.get()}</p>
      </div>
    );
  } finally {
    // just to ensure the observer is closed out, even in the unlikely event of an exception
    // (which gets rethrown after finish is called)
    observer.finish();
  }
}
```

## Technical Implementation Notes

### Observer Context Management

- **OBSERVER_STACK**: Thread-local `RefCell<Vec<ObserverContext>>` manages nested observer contexts
- **Push/Pop Behavior**: `CurrentContext::set()` pushes, `CurrentContext::unset()` pops (restores previous)
- **React Integration**: `useObserve()` hook properly sets/unsets context around component renders

### React Hook Implementation

- **Location**: `signals/src/react.rs`
- **Dependencies**: Uses React's `useRef` and `useSyncExternalStore` for state management and re-render triggering
- **Error Handling**: Matches original `withSignals` pattern (catch and rethrow via early return)
- **Observer Reuse**: Single Observer per React component, stored in useRef to persist across renders

### Feature Architecture

- **`wasm` feature**: Enables basic WASM bindings (`wasm-bindgen`, `js-sys`)
- **`react` feature**: Depends on `wasm`, adds React-specific hooks and types
- **Conditional Compilation**: React module only compiled when `react` feature enabled

### Signal Types

- **`Mut<T>`**: Mutable signal with `.set()` method and `.value()` for direct reads
- **`Read<T>`**: Read-only signal with `.get()` method that performs observer tracking
- **Observer Tracking**: `.get()` automatically subscribes current observer, `.value()` does not

### Subscription Lifecycle Issue

- **Problem**: Handles dropped immediately in `CurrentContext::track()`
- **Impact**: No notifications sent to observers, React components don't update
- **Solution Needed**: Store handles in Observer/Renderer structs to keep them alive

## Verification Status

- ✅ **WASM Builds**: `wasm-pack build --target web --debug` succeeds
- ✅ **React App Builds**: `bun run build` succeeds
- ✅ **Console Logging**: Extensive debug logging added to track signal values and component renders
- ✅ **Feature Flags**: Proper conditional compilation working
- ✅ **Observer Tests**: All tests pass in `signals/tests/observer_context.rs`
- ❌ **React Updates**: Components don't re-render when signals change (subscription handle issue)

## Later tasks (ONLY after subscription handle lifecycle is fixed):

[ ] finish implementing Memo signal type (which is currently broken) whose purpose is to own a memoized output of a transformation closure. (open to renaming this if we can come up with a better name)
[ ] determine if we need a `Signal` trait or not. Initially we should not use one, favoring concrete types and impls for each signal type - but this may become useful later.
[ ] review https://github.com/preactjs/signals/tree/main/packages/react-transform to implement preprocessor that wraps each react component in a try block with the the useSignals hook + finish() before and after the component render.
