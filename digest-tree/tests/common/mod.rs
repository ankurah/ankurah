//! What the top-level tests share, a module per concern: the members they
//! hold, trees of them, stores with quirks a real store may have, the
//! introduction's albums, and a connection between two nodes, faked.

// Each top-level test file compiles this module into its own binary and uses
// only part of it.
#![allow(dead_code, unused_imports, unused_macros, reason = "each test binary uses a different part of the shared fixtures")]

mod albums;
mod connection;
mod members;
mod stores;
mod trees;

pub use albums::*;
pub use connection::*;
pub use members::*;
pub use stores::*;
pub use trees::*;

pub use ankurah_digest_tree::test_support::poll_once;
