//! Event DAG traversal and causal relationship comparison.
//!
//! This module provides pure DAG logic without any attestation concerns.
//! Attestations are handled at a higher layer - this code works only with
//! causal assertions and event relationships.

// Submodules are `pub` so the feature-gated `bench_support` module can reach
// them, but the `event_dag` module itself is `pub(crate)` in normal builds
// (see lib.rs), which caps everything here at `pub(crate)`. Only under the
// `bench-internals` feature does `event_dag` become `pub` and this surface
// become reachable from the separate benches compilation unit.
pub mod accumulator;
pub mod comparison;
pub(crate) mod frontier;
pub(crate) mod layers;
pub mod ordering;
pub mod relation;
pub mod stats;
#[cfg(test)]
mod tests;

pub(crate) use comparison::compare;
pub(crate) use layers::{CausalRelation, EventLayer};
pub(crate) use relation::AbstractCausalRelation;

/// Default budget for DAG traversal: large enough for typical histories but
/// bounded to prevent runaway traversal on malicious/corrupted data. Budget
/// escalation is handled internally by `compare` (up to 4x).
pub const DEFAULT_BUDGET: usize = 1000;
