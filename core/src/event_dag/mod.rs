//! Event DAG traversal and causal relationship comparison.
//!
//! This module provides pure DAG logic without any attestation concerns.
//! Attestations are handled at a higher layer - this code works only with
//! causal assertions and event relationships.

pub mod accumulator;
pub mod comparison;
pub mod frontier;
pub mod layers;
pub mod relation;
#[cfg(test)]
pub mod tests;

// Core types
pub use frontier::Frontier;
pub use relation::AbstractCausalRelation;

// Comparison functions
pub use comparison::compare;

// Layer computation
pub use layers::CausalRelation;

// Accumulator types
pub use accumulator::{ComparisonResult, EventAccumulator, EventLayers};
