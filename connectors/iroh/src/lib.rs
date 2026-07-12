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
//! a Node is ephemeral or durable. Both sides exchange fresh challenges and
//! signed `Presence` frames, then carry signed, sequenced
//! [`ankurah_proto::Message`] frames over one bidirectional QUIC stream.
//!
//! The embedder constructs each [`iroh::Endpoint`] with the same Ed25519 seed
//! used by the Ankurah node and configures its direct addresses, relay, and
//! address lookup. [`endpoint_secret_key`] performs the seed-only conversion
//! between the two Ed25519 implementations. `IrohServer` then takes ownership of its
//! dedicated Endpoint and closes it on shutdown; a client Endpoint remains
//! externally managed and may be shared. At least the known server `EndpointId`,
//! or a complete `EndpointAddr`, must be delivered to clients out of band.
//! Persist the server endpoint secret key when its ID must remain stable across
//! restarts.
//!
//! Constructors reject an Endpoint whose identity differs from its Ankurah
//! Node. Every accepted Presence is also required to match QUIC's authenticated
//! remote endpoint identity. Browser/wasm targets are not supported.
//! `IrohServer` owns its Router and should be given a dedicated Endpoint.
//!
//! Handshake frames are limited to 64 KiB and must arrive within ten seconds.
//! Established messages have a 64 MiB frame cap.
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
//!     let server_key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
//!     let server_node = Node::new_durable_with_signing_key(
//!         Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), server_key.clone());
//!     server_node.system.create().await?;
//!     // N0 uses Number 0's external relay and address-lookup services.
//!     let server_endpoint = iroh::Endpoint::builder(presets::N0)
//!         .secret_key(ankurah_connector_iroh::endpoint_secret_key(&server_key))
//!         .bind().await?;
//!     server_endpoint.online().await;
//!     let server_addr = server_endpoint.addr();
//!     let server = IrohServer::new(server_node.clone(), server_endpoint)?;
//!
//!     // Dialing side: retain the endpoint because client shutdown does not close it.
//!     let client_key = ed25519_dalek::SigningKey::from_bytes(&[8; 32]);
//!     let client_node = Node::new_with_signing_key(
//!         Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), client_key.clone());
//!     let client_endpoint = iroh::Endpoint::builder(presets::N0)
//!         .secret_key(ankurah_connector_iroh::endpoint_secret_key(&client_key))
//!         .bind().await?;
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

/// Convert Ankurah's Ed25519 signing key into iroh's key type through the
/// standardized 32-byte seed. The crates intentionally use different major
/// versions of `ed25519-dalek`; sharing an implementation-specific key object
/// would couple their internals and can silently change key interpretation.
pub fn endpoint_secret_key(signing_key: &ed25519_dalek::SigningKey) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&signing_key.to_bytes())
}

pub(crate) fn endpoint_node_id(endpoint_id: iroh::EndpointId) -> ankurah_proto::NodeId {
    ankurah_proto::NodeId::from_bytes(*endpoint_id.as_bytes())
}

pub(crate) fn validate_local_identity(node_id: ankurah_proto::NodeId, endpoint_id: iroh::EndpointId) -> anyhow::Result<()> {
    let endpoint_node_id = endpoint_node_id(endpoint_id);
    anyhow::ensure!(
        endpoint_node_id == node_id,
        "iroh endpoint identity {endpoint_node_id} does not match Ankurah node identity {node_id}"
    );
    Ok(())
}

/// ALPN protocol identifier for Ankurah-over-iroh connections.
pub const ALPN: &[u8] = b"ankurah/1";
