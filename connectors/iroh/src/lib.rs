//! # Ankurah iroh connector
//!
//! Connects Ankurah nodes over [iroh](https://docs.rs/iroh) peer-to-peer QUIC
//! connections. This is a pure transport: the topology, protocol messages, and
//! node identity scheme are identical to the websocket connectors. Ankurah node
//! identity is not yet bound to iroh endpoint identity (that binding is being
//! designed separately), and browser/wasm targets are not yet supported.
//!
//! Both sides speak length-delimited bincode [`ankurah_proto::Message`] frames
//! over a single bidirectional QUIC stream. Each side sends its own `Presence`
//! first and registers the remote peer with its `Node` upon receiving the other
//! side's `Presence`, exactly as the websocket client and server pair does.
//!
//! The embedder builds and owns the [`iroh::Endpoint`], including relay and
//! address lookup configuration:
//!
//! ```rust,no_run
//! # use ankurah::{Node, PermissiveAgent};
//! # use ankurah_storage_sled::SledStorageEngine;
//! # use ankurah_connector_iroh::{IrohClient, IrohServer};
//! # use std::sync::Arc;
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Accepting side: a durable node behind an iroh endpoint
//!     let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
//!     server_node.system.create().await?;
//!     let server_endpoint = iroh::Endpoint::bind(iroh::endpoint::presets::N0).await?;
//!     let server_addr = server_endpoint.addr();
//!     let server = IrohServer::new(server_node, server_endpoint);
//!
//!     // Dialing side: an ephemeral node connecting to the server's EndpointAddr
//!     let client_node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
//!     let client_endpoint = iroh::Endpoint::bind(iroh::endpoint::presets::N0).await?;
//!     let client = IrohClient::new(client_node, client_endpoint, server_addr).await?;
//!     client.wait_connected().await?;
//!
//!     // ... use the nodes ...
//!
//!     client.shutdown().await?;
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
