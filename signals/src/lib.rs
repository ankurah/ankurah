/*!
A reactive signals library for ankurah

# Design requirements:
- Must be dyn object safe - Not sure if we want traits, but if we do, they must be dyn object safe.
- Writers and readers must be different types
- writers should not implement subscription methods
- should be able to derive directly from either writer or reader via .map()
- reader should keep a reference to the present value
- Should offer closure subscriptions for T ?Clone (not sure what other bounds are needed)
- Should offer stream subscriptions for T: Clone (not sure what other bounds are needed)

# Nomenclature (not sure if applicable to different types or traits - we will have to see):
- fn subscribe_now - immediately calls the given closure with the current value, and also with future values. Only applicable to stateful types
- fn subscribe - does not immediately call the given closure - only when the value changes. could be stateless or stateful

# Basic usage

```rust
use ankurah_signals::*;

let signal = Mut::new(42);
// cant subscribe to a mutable signal
signal.read().subscribe(|value: &i32| println!("Read value: {}", value));
// signal.map(|value| *value * 2).subscribe(|value| println!("Mapped value: {value}"));
signal.set(43);
// Should print:
// Read value: 42
// Mapped value: 84
// Read value: 43
// Mapped value: 86
```
# Observer usage

```rust
use ankurah_signals::*;

let name = Mut::new("Buffy".to_string());
let age = Mut::new(29);
// let retired = age.map(|age| *age > 65);
let renderer = {
    let name = name.read();
    let age = age.read();
    Renderer::new(move || println!("name: {name}, age: {age}"))
};

renderer.render();
// name: Buffy, age: 29, retired: false

// observer is still listening to all three signals
age.set(70); // So render gets called (but only ONCE, not twice)
// name: Buffy, age: 70, retired: true
```

*/

mod core;
mod observer;
mod signal;
mod subscription;
mod traits;

#[cfg(feature = "wasm")]
mod react;

pub use core::*;
pub use observer::*;
pub use signal::*;
pub use subscription::*;
pub use traits::*;

#[cfg(feature = "wasm")]
pub use react::*;
