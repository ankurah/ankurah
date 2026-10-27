//! What the top-level tests share, a module per concern: the members they
//! hold and trees of them, stores with quirks a real store may have, the
//! primer's albums, and a peer's side of reconciling.

// Each top-level test file compiles this module into its own binary and uses
// only part of it.
#![allow(dead_code, unused_imports, reason = "each test binary uses a different part of the shared fixtures")]

mod albums;
mod members;
mod reconciling;
mod stores;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

pub use albums::*;
pub use members::*;
pub use reconciling::*;
pub use stores::*;

/// Poll `future` once, with a waker that does nothing: whether it is ready
/// yet, without waiting for it.
pub fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> { future.poll(&mut Context::from_waker(Waker::noop())) }
