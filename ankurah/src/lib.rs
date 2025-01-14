//! # Ankurah
//!
//! Ankurah is a distributed, observable data synchronization framework that enables real-time
//! data synchronization across multiple nodes with built-in observability.
//!
//! ## Key Features
//!
//! - **Schema-First Design**: Define data models using Rust structs with an ActiveRecord-style interface
//! - **Real-Time Observability**: Signal-based pattern for tracking entity changes
//! - **Distributed Architecture**: Multi-node synchronization with event sourcing
//! - **Flexible Storage**: Support for multiple storage backends (Sled, Postgres, TiKV)
//! - **Web Framework Integration**: First-class support for React and Leptos
//!
//! ## Core Concepts
//!
//! - **Model**: A struct describing fields and types for entities in a collection
//! - **Collection**: A group of entities sharing the same Model type
//! - **Entity**: A discrete identity in a collection (similar to a database row)
//! - **View**: A read-only representation of an entity
//! - **Mutable**: A mutable state representation of an entity
//! - **Event**: An atomic change that can be applied to an entity
//!
//! ## Quick Start
//!
//! 1. Start the server:
//! ```bash
//! cargo run -p ankurah-example-server
//! ```
//!
//! 2. Build WASM bindings:
//! ```bash
//! cd examples/wasm-bindings
//! wasm-pack build --target web --debug
//! ```
//!
//! 3. Run the React example app:
//! ```bash
//! cd examples/react-app
//! bun install
//! bun dev
//! ```
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
//!     let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?));
//!     let client = Node::new(Arc::new(SledStorageEngine::new_test()?));
//!
//!     // Connect nodes using local process connection
//!     let _conn = LocalProcessConnection::new(&server, &client).await?;
//!
//!
//!     // Subscribe to changes on the client
//!     let _subscription = client.subscribe::<_,_,AlbumView>("name = 'Origin of Symmetry'", |changes| {
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

pub use ankql;
pub use ankurah_core as core;
#[cfg(feature = "derive")]
pub use ankurah_derive as derive;
pub use ankurah_proto as proto;

pub use proto::ID;
// Re-export commonly used types
pub use ankurah_core::{
    Model, changes, error,
    event::Event,
    model,
    model::Mutable,
    model::View,
    node::{FetchArgs, Node},
    property,
    resultset::ResultSet,
    storage, transaction,
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
    pub use ::reactive_graph; // Why does this fail with a Sized error: `the trait `GetSignalValue` cannot be made into an object the trait cannot be made into an object because it requires `Self: Sized``
    // pub use reactive_graph::traits::Get as GetSignalValue; // and this one works fine?
    pub use ::ankurah_proto;
    #[cfg(feature = "react")]
    pub use ::wasm_bindgen;
}
