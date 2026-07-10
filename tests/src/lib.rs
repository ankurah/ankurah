//! Tests for the Ankurah library.
//!
//! In addition to the integration tests under `tests/`, this crate exposes the
//! deterministic multi-node simulation harness (`sim`) so both the smoke-tier
//! scenario tests and the future C2 nightly scale-out can drive it.

pub mod common;
pub mod forge;
pub mod sim;
