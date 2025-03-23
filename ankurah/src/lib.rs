//! # Ankurah
//!
//! <div style="display:flex; background:linear-gradient(0deg, rgba(112,182,255,1) 0%, rgba(200,200,255,1) 100%); padding: 8px; margin: -15px 0 10px 0">
//! <img
//!  src="data:image/png;base64,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"
//!  alt="Ankurah Logo" width=64 height=64
//!  style=""
//! />
//! <div style="margin: auto 0 auto 5px">The root of all cosmic projections of state</div>
//! </div>
//!
//!
//! Ankurah is a state-management framework that enables real-time
//! data synchronization across multiple nodes with built-in observability.
//!
//! It supports multiple storage and data type backends to enable no-compromise representation of your data.
//!
//! This project is in the early stages of development, and is not yet ready for production use.
//!
//! ## Key Features
//!
//! - **Schema-First Design**: Define data models using Rust structs with an ActiveRecord-style interface - [View](Model::View)/[Mutable](Model::Mutable)
//! - **Content-filtered pub/sub**: Subscribe to changes on a collection using a SQL-like query - [subscribe](Node::subscribe)
//! - **Real-Time Observability**: Signal-based pattern for tracking entity changes
//! - **Distributed Architecture**: Multi-node synchronization with event sourcing
//! - **Flexible Storage**: Support for multiple storage backends (Sled, Postgres, TiKV)
//! - **Isomorphic code**: Server applications and Web applications use the same code, including first-class support for React and Leptos out of the box
//!
//! ## Core Concepts
//!
//! - **Model**: A struct describing fields and types for entities in a collection (data binding)
//! - **Collection**: A group of entities of the same type (similar to a database table, and backed by a table in the postgres backend)
//! - **Entity**: A discrete identity in a collection - Dynamic schema (similar to a schema-less database row)
//! - **View**: A read-only representation of an entity - Typed by the model
//! - **Mutable**: A mutable state representation of an entity - Typed by the model
//! - **Event**: An atomic change that can be applied to an entity - used for syncrhonization and audit trail
//!
//! ## Quick Start
//!
//! 1. Start the server:
//! ```bash
//! cargo run -p ankurah-example-server
//!
//! # or dev mode
//! cargo watch -x 'run -p ankurah-example-server'
//! ```
//!
//! 2. Build WASM bindings:
//! ```bash
//! cd examples/wasm-bindings
//! wasm-pack build --target web --debug
//!
//! # or dev mode
//! cargo watch -s 'wasm-pack build --target web --debug'
//! ```
//!
//! 3. Run the React example app:
//! ```bash
//! cd examples/react-app
//! bun install
//! bun dev
//! ```
//!
//! Then load http://localhost:5173/ in one regular browser tab, and one incognito browser tab and play with the example app.
//! You can also use two regular browser tabs, but they share one IndexedDB local storage backend, so it's not as good of a test.
//! In this example, the "server" process is a native Rust process whose node is flagged as "durable", meaning that it attests it
//! will not lose data. The "client" process is a WASM process that is also durable in some sense, but not to be relied upon to have
//! all data. The demo server currently uses the sled backend, but postgres is also supported, and TiKV support is planned.
//!
//! ## Example: Inter-Node Subscription
//!
//! ```rust
//! # use ankurah::{Node,Model};
//! # use ankurah_storage_sled::SledStorageEngine;
//! # use ankurah_connector_local_process::LocalProcessConnection;
//! # use std::sync::Arc;
//! # #[derive(Model, Debug)]
//! # pub struct Album {
//! #     name: String,
//! #     year: String,
//! # }
//! #
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create server and client nodes
//!     let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), ankurah::policy::PermissiveAgent::new());
//!     let client = Node::new(Arc::new(SledStorageEngine::new_test()?), ankurah::policy::PermissiveAgent::new());
//!
//!     // Connect nodes using local process connection
//!     let _conn = LocalProcessConnection::new(&server, &client).await?;
//!
//!     // Get contexts for the server and client
//!     let server = server.context(ankurah::policy::DEFAULT_CONTEXT);
//!     let client = client.context(ankurah::policy::DEFAULT_CONTEXT);
//!
//!     // Subscribe to changes on the client
//!     let _subscription = client.subscribe::<_,AlbumView>("name = 'Origin of Symmetry'", |changes| {
//!         println!("Received changes: {}", changes);
//!     }).await?;
//!
//!     // Create a new album on the server
//!     let trx = server.begin();
//!     trx.create(&Album {
//!         name: "Origin of Symmetry".into(),
//!         year: "2001".into(),
//!     }).await;
//!     trx.commit().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Design Philosophy
//!
//! Ankurah follows an event-sourced architecture where:
//! - All operations have unique IDs and precursor operations
//! - Entity state is maintained per node with operation tree tracking
//! - Operations use ULID for distributed ID generation
//! - Entity IDs are derived from their creation operation
//!
//! For more details, see the [repository documentation](https://github.com/ankurah/ankurah).
//! And join the [Discord server](https://discord.gg/XMUUxsbT5S) to be part of the discussion!

pub use ankql;
pub use ankurah_core as core;
#[cfg(feature = "derive")]
pub use ankurah_derive as derive;
pub use ankurah_proto as proto;

pub use proto::ID;
// Re-export commonly used types
pub use ankurah_core::{
    changes,
    context::Context,
    error,
    event::Event,
    model,
    model::Mutable,
    model::View,
    node::{MatchArgs, Node},
    policy::{self, PermissiveAgent},
    property::{self, Property},
    resultset::ResultSet,
    storage,
    subscription::SubscriptionHandle,
    transaction, Model,
};

// TODO move this somewhere else - it's a dependency of the signal derive macro
#[doc(hidden)]
#[cfg(feature = "react")]
pub trait GetSignalValue: reactive_graph::traits::Get {
    fn cloned(&self) -> Box<dyn GetSignalValue<Value = Self::Value>>;
}

// Add a blanket implementation for any type that implements Get + Clone
#[doc(hidden)]
#[cfg(feature = "react")]
impl<T> GetSignalValue for T
where
    T: reactive_graph::traits::Get + Clone + 'static,
    T::Value: 'static,
{
    fn cloned(&self) -> Box<dyn GetSignalValue<Value = T::Value>> { Box::new(self.clone()) }
}

// Re-export the derive macro
#[cfg(feature = "derive")]
pub use ankurah_derive::*;

// Re-export dependencies needed by derive macros
// #[cfg(feature = "derive")]
#[doc(hidden)]
pub mod derive_deps {
    #[cfg(feature = "react")]
    pub use crate::GetSignalValue;
    #[cfg(feature = "react")]
    pub use ::ankurah_react_signals;
    #[cfg(feature = "react")]
    pub use ::js_sys;
    #[cfg(feature = "react")]
    pub use ::reactive_graph;
    pub use ::tracing; // Why does this fail with a Sized error: `the trait `GetSignalValue` cannot be made into an object the trait cannot be made into an object because it requires `Self: Sized``
                       // pub use reactive_graph::traits::Get as GetSignalValue; // and this one works fine?
    pub use ::ankurah_proto;
    #[cfg(feature = "react")]
    pub use ::wasm_bindgen;
    #[cfg(feature = "react")]
    pub use ::wasm_bindgen_futures;

    pub use ::serde_json;
}
