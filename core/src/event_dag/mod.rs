//! Event DAG traversal and causal relationship comparison.
//!
//! This module provides pure DAG logic without any attestation concerns.
//! Attestations are handled at a higher layer - this code works only with
//! causal assertions and event relationships.

pub(crate) mod accumulator;
pub(crate) mod comparison;
pub(crate) mod frontier;
pub(crate) mod layers;
pub(crate) mod ordering;
pub(crate) mod relation;
#[cfg(test)]
mod tests;

pub(crate) use comparison::compare;
pub(crate) use layers::{CausalRelation, EventLayer};
pub(crate) use relation::AbstractCausalRelation;

/// Default budget for DAG traversal — large enough for typical histories
/// but bounded to prevent runaway traversal on malicious/corrupted data.
/// Budget escalation is handled internally by `compare` (up to 4x).
pub(crate) const DEFAULT_BUDGET: usize = 1000;
