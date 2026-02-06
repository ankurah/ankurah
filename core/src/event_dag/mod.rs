//! Event DAG traversal and causal relationship comparison.
//!
//! This module provides pure DAG logic without any attestation concerns.
//! Attestations are handled at a higher layer - this code works only with
//! causal assertions and event relationships.

pub mod accumulator;
pub mod comparison;
pub mod frontier;
pub mod layers;
pub mod navigator;
pub mod relation;
#[cfg(test)]
pub mod tests;
pub mod traits;

// Core types
pub use frontier::{Frontier, FrontierState, TaintReason};
pub use relation::AbstractCausalRelation;
pub use traits::{EventId, TClock, TEvent};

// Navigation
pub use navigator::{AccumulatingNavigator, AssertionRelation, AssertionResult, CausalNavigator, NavigationStep};

// Comparison functions
pub use comparison::{compare, compare_unstored_event};

// Layer computation
pub use layers::{compute_ancestry, compute_layers, CausalRelation, EventLayer};

// Accumulator types
pub use accumulator::{ComparisonResult, EventAccumulator, EventLayers};
// Note: accumulator::EventLayer intentionally NOT re-exported here to avoid
// conflict with layers::EventLayer during transition. Use fully qualified path.
