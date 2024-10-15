# Ankurah: The root of all cosmic manifestations (of state)

## Warning: It is a very early work in progress, with a few major milestones envisioned:

- Major Milestone 1 - Getting the foot in the door

  - Production-usable event-sourced ORM which uses an off the shelf database for storage
  - Rust structs for data modeling
  - Signals pattern for notifications
  - WASM Bindings for client-side use
  - React Bindings
  - Basic included data-types: including CRDT text and primative types
  - Very basic embedded KV backing store (Sled DB)
  - Basic, single field queries (auto indexed?)

- Major Milestone 2 - Stuff we need, but can live without for a bit

  - Postgres, and TiKV Backends
  - Graph Functionality
  - Multi-field queries
  - A robust recursive query AST which can define queries declaratively
  - User definable data types

- Major Milestone 3 - Maybe someday...

  - P2P functionality
  - Portable cryptographic identities
  - E2EE
  - Hypergraph functionality

# Getting started (It worked at one point, but I broke it)

1. Install rust:
   https://rustup.rs/

2. Install cargo watch (useful for dev workflow)

   ```
   cargo install cargo-watch
   ```

3. Start the server:

   ```
   # from the root directory of the repo
   cargo watch -x 'run --bin ankurah-server'
   ```

4. Install wasm-pack
   https://rustwasm.github.io/wasm-pack/installer/

5. Compile the web client:

   ```
   cd web
   cargo watch -s 'wasm-pack build --target web --debug'
   ```

6. install `bun` (npm/node might work, but I haven't tested it, and bun is way faster)

   https://bun.sh/docs/installation

   ```
   cd examples/webapp
   bun dev
   ```
