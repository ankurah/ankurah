//! # Ankurah WebSocket Client
//!
//! A native (non-browser) WebSocket client for connecting an Ankurah node to another
//! Ankurah node which hosts a WebSocket server
//!
//! ## Automatic reconnection
//!
//!  Reconnects to the server if the connection is lost using exponential backoff
//!
//! ## Graceful shutdown
//!
//!   To shutdown the client, call the `shutdown` method. This will wait for the connection to be closed and then return.
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! # use ankurah::{Node, PermissiveAgent};
//! # use ankurah_storage_sled::SledStorageEngine;
//! # use ankurah_websocket_client::WebsocketClient;
//! # use ankurah::policy::DEFAULT_CONTEXT as c;
//! # use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create a client node
//!     let storage = Arc::new(SledStorageEngine::new_test()?);
//!     let my_node = Node::new(storage, PermissiveAgent::new());
//!
//!     // Create WebSocket client to connect to remote server (automatically starts connecting)
//!     let client = WebsocketClient::new(my_node.clone(), "ws://localhost:8080").await?;
//!
//!     println!("State: {}", client.state().value()); // State: Connected
//!
//!     // See [ankurah] for usage details
//!
//!     // When you're done, shutdown the client
//!     client.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! // See ankurah for basic details

pub mod client;
pub mod sender;

// Re-export the main types for easy use
pub use client::{ConnectionState, WebsocketClient};
pub use sender::WebsocketPeerSender;

// Re-export common types for convenience
pub use tokio_tungstenite::tungstenite::Error as TungsteniteError;
