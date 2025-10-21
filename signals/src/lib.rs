/*!
A signals library designed to support Ankurah's needs - but can be used independently as well.
Supports both native Rust and WebAssembly/React use cases.

# Basic Usage in Rust

```rust
# use ankurah_signals::{Mut, Subscribe, CallbackObserver};
# use std::sync::Arc;

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
        println!("It's {day} and I'm {vibe}") // the Observer will automatically subscribe to the signals
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

*/

pub mod broadcast;
mod context;
pub mod observer;
pub mod porcelain;
pub mod signal;
mod value;

#[cfg(feature = "react")]
pub mod react;

pub use broadcast::BroadcastId;
pub use context::*;
pub use observer::*;
pub use porcelain::*;
pub use signal::*;

#[cfg(feature = "react")]
pub use react::*;
