# Ankurah: The root of all cosmic manifestations (of state)

## Warning: It is a very early work in progress, with a few major milestones envisioned:

- Major Milestone 1 - Getting the foot in the door

  - Production-usable event-sourced ORM which uses an off the shelf database for storage
  - Rust structs for data modeling
  - Signals pattern for notifications
  - WASM Bindings for client-side use
  - Websockets server and client
  - React Bindings
  - Basic included data-types: including CRDT text(yrs crate) and primitive types
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
