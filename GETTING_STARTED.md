# Getting Started

This project is in early development. To explore the current features, follow these steps:

## Prerequisites

- **Install Rust:**

  - [rustup.rs](https://rustup.rs/)

- **Install Cargo Watch** (useful for development workflow):

  ```bash
  cargo install cargo-watch
  ```

- **Install wasm-pack:**
  - [wasm-pack installer](https://rustwasm.github.io/wasm-pack/installer/)

## Server Setup

- **Start the Server** (keep this running):
  ```bash
  cargo run -p ankurah-example-server
  ```

## React Example App

1. **Compile the Wasm Bindings** (keep this running):

   - Navigate to the `wasm-bindings` example directory:

   ```bash
   cd examples/wasm-bindings
   'wasm-pack build --target web --debug'
   ```

2. **Install Bun** (npm/node might work, but Bun is faster):

   - [Bun installation guide](https://bun.sh/docs/installation)

3. **Run the React Example App** (keep this running):
   ```bash
   cd examples/react-app
   bun install
   bun dev
   ```

## Leptos Example App

1. **Install Trunk** (build tool used by Leptos):

   ```bash
   cargo install trunk
   ```

2. **Run the Leptos Example App** (keep this running):
   ```bash
   trunk serve --open
   ```

> **Note:** For the Leptos app, there is no need to build the Wasm bindings crate separately.
