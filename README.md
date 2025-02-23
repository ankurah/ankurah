<div style="width:100%; display:flex; background:linear-gradient(0deg, rgba(112,182,255,1) 0%, rgba(200,200,255,1) 100%); padding: 8px; margin: -15px 0 10px 0">
<img src="./logo-128.png" alt="Ankurah Logo" width=64 height=64 style=""/>
<div style="margin: auto 0 auto 5px">The root of all cosmic projections of state</div>
</div>

Ankurah is a state-management framework that enables real-time data synchronization across multiple nodes with built-in observability.

It supports multiple storage and data type backends to enable no-compromise representation of your data.

This project is in the early stages of development, and is not yet ready for production use.

## Key Features

- **Schema-First Design**: Define data models using Rust structs with an ActiveRecord-style interface - View/Mutable
- **Content-filtered pub/sub**: Subscribe to changes on a collection using a SQL-like query
- **Real-Time Observability**: Signal-based pattern for tracking entity changes
- **Distributed Architecture**: Multi-node synchronization with event sourcing
- **Flexible Storage**: Support for multiple storage backends (Sled, Postgres, TiKV)
- **Isomorphic code**: Server applications and Web applications use the same code, including first-class support for React and Leptos out of the box

## Core Concepts

- **Model**: A struct describing fields and types for entities in a collection (data binding)
- **Collection**: A group of entities of the same type (similar to a database table, and backed by a table in the postgres backend)
- **Entity**: A discrete identity in a collection - Dynamic schema (similar to a schema-less database row)
- **View**: A read-only representation of an entity - Typed by the model
- **Mutable**: A mutable state representation of an entity - Typed by the model
- **Event**: An atomic change that can be applied to an entity - used for synchronization and audit trail

## Quick Start

1. Start the server:

```bash
cargo run -p ankurah-example-server

# or dev mode
cargo watch -x 'run -p ankurah-example-server'
```

2. Build WASM bindings:

```bash
cd examples/wasm-bindings
wasm-pack build --target web --debug

# or dev mode
cargo watch -s 'wasm-pack build --target web --debug'
```

3. Run the React example app:

```bash
cd examples/react-app
bun install
bun dev
```

Then load http://localhost:5173/ in one regular browser tab, and one incognito browser tab and play with the example app.
You can also use two regular browser tabs, but they share one IndexedDB local storage backend, so it's not as good of a test.
In this example, the "server" process is a native Rust process whose node is flagged as "durable", meaning that it attests it
will not lose data. The "client" process is a WASM process that is also durable in some sense, but not to be relied upon to have
all data. The demo server currently uses the sled backend, but postgres is also supported, and TiKV support is planned.

## Example: Inter-Node Subscription

```rust
// Create server and client nodes
let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).context(c);
let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new()).context(c);

// Connect nodes using local process connection
let _conn = LocalProcessConnection::new(&server, &client).await?;

// Subscribe to changes on the client
let _subscription = client.subscribe::<_,_,AlbumView>("name = 'Origin of Symmetry'", |changes| {
    println!("Received changes: {}", changes);
}).await?;

// Create a new album on the server
let trx = server.begin();
let album = trx.create(&Album {
    name: "Origin of Symmetry".into(),
    year: "2001".into(),
}).await?;
trx.commit().await?;
```

## Design Philosophy

Ankurah follows an event-sourced architecture where:

- All operations have unique IDs and precursor operations
- Entity state is maintained per node with operation tree tracking
- Operations use ULID for distributed ID generation
- Entity IDs are derived from their creation operation

## Development Milestones

### Major Milestone 1 - Getting the foot in the door

- Production-usable event-sourced ORM which uses an off the shelf database for storage
- Rust structs for data modeling
- Signals pattern for notifications
- WASM Bindings for client-side use
- Websockets server and client
- React Bindings
- Basic included data-types: including CRDT text(yrs crate) and primitive types
- Very basic embedded KV backing store (Sled DB)
- Basic, single field queries (auto indexed?)
- Postgres
- Multi-field queries
- A robust recursive query AST which can define queries declaratively

### Major Milestone 2 - Stuff we need, but can live without for a bit

- and TiKV Backends
- Graph Functionality
- User definable data types

### Major Milestone 3 - Maybe someday...

- P2P functionality
- Portable cryptographic identities
- E2EE
- Hypergraph functionality

For more details, see the [repository documentation](https://github.com/ankurah/ankurah).
And join the [Discord server](https://discord.gg/XMUUxsbT5S) to be part of the discussion!
