# Ankurah Signals

A signals library for the Ankurah distributed database. Inspired by Preact signals and designed specifically for Ankurah's needs, supporting both native Rust and WebAssembly/React usage.

## Basic Usage in Rust

```rust
use ankurah_signals::{Mut, Subscribe, CallbackObserver};
use std::sync::Arc;

let day = Mut::new("Friday");
let vibe = Mut::new("grindcore");

// Sometimes directly subscribing to the signal can be useful
// your listener will not be called immediately, only when the signal changes
// the subscription will be removed when the SubscriptionGuard is dropped
let _guard1 = day.subscribe(|v| println!("The day is: {}", v));
let _guard2 = vibe.subscribe(|v| println!("Vibe: {}", v));

// For things that need to track signal dependencies, we can use an Observer
let renderer = {
    let day = day.read(); // Read<T> signals can be constructed from a Mut<T> signal
    let vibe = vibe.read();
    CallbackObserver::new(Arc::new(move ||{
        // Your "render" function that uses signals
        // The Observer will automatically subscribe to the signals used
        // during this dispatch using the thread-local CurrentObserver
        println!("It's {day} and I'm {vibe}")
    }))
};

renderer.trigger(); // trigger the initial "render". Signals used will be tracked by the observer.
// Should print:
// It's Friday and I'm grindcore

vibe.set("chillmaxing"); // triggers the direct signal subscription AND the observer
// Should print:
// Vibe: chillmaxing
// It's Friday and I'm chillmaxing

day.set("Saturday");
// Should print:
// The day is: Saturday
// It's Saturday and I'm chillmaxing
```

## Basic Usage in React

You should have a rust crate that exports the wasm bindings to your react app.
You will need to include the `ankurah-signals` crate with the `react` feature enabled.

```toml
[dependencies]
ankurah-signals = { version = "0.1", features = ["react"] }
```

### React Component Pattern

```typescript
import { useObserve } from "my_wasm_bindgen_crate";

function MyComponent({ currentTime }: { currentTime: StringRead }) {
  const observer = useObserve();

  try {
    return (
      <div>
        <p>The time is: {currentTime.get()}</p>
      </div>
    );
  } finally {
    observer.finish();
  }
}

// this will be made more ergonomic in the future using a vite preprocessor to add this automatically
// in the meahwhile, you can make a helper function if desired:
import { useObserve } from "my_wasm_bindgen_crate";

export function signalObserver<T>(f: React.FC<T>): React.FC<T> {
  return (props: T) => {
    const observer = useObserve();
    try {
      return f(props);
    } finally {
      observer.finish();
    }
  };
}

export const MyComponent = signalObserver(({ fooSignal }: MyProps) => {
  return (
    <div>
      <p>The foo is: {fooSignal.value}</p>
      {/* fooSignal is tracked by the ReactObserver, and MyComponent will
      be re-rendered when fooSignal changes */}
    </div>
  );
});
```
