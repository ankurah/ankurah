//! # Native Ankurah transport over iroh
//!
//! This crate connects native Ankurah nodes over [iroh](https://docs.rs/iroh)
//! QUIC connections. Its main use today is reaching a single self-hosted durable
//! node from native ephemeral clients when exposing a conventional WebSocket
//! service is inconvenient:
//!
//! ```text
//! native ephemeral client --\
//! native ephemeral client ---- self-hosted durable node
//! native ephemeral client --/
//! ```
//!
//! Iroh may establish each edge directly, through NAT traversal, or through a
//! configured relay. The relay is only a network path; it is not an Ankurah node.
//! This crate does not add peer discovery, direct client-to-client connections
//! or synchronization, multi-durable routing, or a mesh topology. It is a
//! transport building block that preserves the existing Ankurah protocol and
//! replication behavior.
//!
//! `IrohClient` and `IrohServer` describe who dials and who accepts, not whether
//! a Node is ephemeral or durable. Both sides exchange `Presence` and then carry
//! length-delimited bincode [`ankurah_proto::Message`] frames over one
//! bidirectional QUIC stream.
//!
//! The embedder constructs each [`iroh::Endpoint`] and configures its key, direct
//! addresses, relay, and address lookup. `IrohServer` then takes ownership of its
//! dedicated Endpoint and closes it on shutdown; a client Endpoint remains
//! externally managed and may be shared. At least the known server `EndpointId`,
//! or a complete `EndpointAddr`, must be delivered to clients out of band.
//! Persist the server endpoint secret key when its ID must remain stable across
//! restarts.
//!
//! Ankurah Node identity is not yet cryptographically bound to iroh Endpoint
//! identity, and browser/wasm targets are not supported. `IrohServer` owns its
//! Router and should currently be given a dedicated Endpoint.
//!
//! The first inbound frame must be Presence, is limited to 64 KiB, and must
//! arrive within ten seconds. Established messages have a 64 MiB frame cap.
//! Outbound buffering is limited to 32 frames and 64 MiB of queued payload;
//! exceeding either limit closes the connection rather than growing without
//! bound.
//!
//! ## Example
//!
//! ```rust,no_run
//! # use ankurah::{Node, PermissiveAgent};
//! # use ankurah_storage_sled::SledStorageEngine;
//! # use ankurah_connector_iroh::{IrohClient, IrohServer};
//! # use iroh::endpoint::presets;
//! # use std::sync::Arc;
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> anyhow::Result<()> {
//!     // Accepting side: a durable node behind an iroh endpoint.
//!     let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
//!     server_node.system.create().await?;
//!     // N0 uses Number 0's external relay and address-lookup services.
//!     let server_endpoint = iroh::Endpoint::bind(presets::N0).await?;
//!     server_endpoint.online().await;
//!     let server_addr = server_endpoint.addr();
//!     let server = IrohServer::new(server_node.clone(), server_endpoint);
//!
//!     // Dialing side: retain the endpoint because client shutdown does not close it.
//!     let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
//!     let client_endpoint = iroh::Endpoint::bind(presets::N0).await?;
//!     let client = IrohClient::new(client_node.clone(), client_endpoint.clone(), server_addr).await?;
//!     // Fail-fast startup: this returns the first connection error even though
//!     // the client continues reconnecting in the background.
//!     client.wait_connected().await?;
//!     client_node.system.wait_system_ready().await;
//!
//!     let _context = client_node.context(ankurah::policy::DEFAULT_CONTEXT)?;
//!     // Use the node normally here.
//!
//!     client.shutdown().await?;
//!     client_endpoint.close().await;
//!     server.shutdown().await?;
//!     Ok(())
//! }
//! ```

pub mod client;
pub(crate) mod connection;
pub mod sender;
pub mod server;

pub use client::{ConnectionError, ConnectionState, IrohClient};
pub use sender::IrohPeerSender;
pub use server::IrohServer;

/// ALPN protocol identifier for Ankurah-over-iroh connections.
pub const ALPN: &[u8] = b"ankurah/0";
