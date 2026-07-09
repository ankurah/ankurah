//! Ingest pipeline (RFC #268): planned application of events with uniform
//! execution semantics across every ingest lane.
//!
//! Sits between `entity` and the wire-shape adapters (`node_applier`, the
//! `node` commit lanes): adapters translate payloads into pipeline feeds, the
//! pipeline drives `entity`. The reactor and peer communication stay outside;
//! the pipeline returns outcomes and changes, feeders decide notification and
//! recovery. Uniformity has two deliberate exceptions, both documented at
//! their sites: the local commit lane executes its own phase two
//! (context.rs, relay-ordering), and join_system persists the peer-attested
//! root verbatim instead of routing through the shared state-apply
//! (system.rs, attestation provenance).

pub(crate) mod executor;
pub(crate) mod outcome;
pub(crate) mod plan;
pub(crate) mod staging;
pub(crate) mod state_apply;
pub(crate) mod unverified;
pub(crate) mod verify;

pub(crate) use executor::{execute_plan, PersistState};
pub(crate) use outcome::IngestOutcome;
pub(crate) use plan::{plan_entity, IngestPlan};
pub(crate) use staging::StagingArea;
pub(crate) use state_apply::apply_state_feed;
pub(crate) use unverified::UnverifiedEvents;
pub(crate) use verify::{check_generation, GenerationCheck};

use crate::error::{IngestError, LineageError, LineageRejection, MutationError, RetrievalError};

/// Translate comparison-layer failures into the typed taxonomy at the
/// pipeline boundary (M5). LineageError conflates the permanent Disjoint
/// verdict with the resumable BudgetExceeded liveness anomaly; the taxonomy
/// separates them (C4-08). Mutual MutationError/RetrievalError boxing from
/// the with_state path is flattened so the classification sees through it.
/// Everything else passes through unchanged: admission errors stay on their
/// existing surface (#274's jurisdiction), and storage-class failures keep
/// their engine detail.
pub(crate) fn type_comparison_error(e: MutationError) -> MutationError {
    match e {
        MutationError::LineageError(LineageError::Disjoint) => IngestError::Lineage(LineageRejection::Disjoint).into(),
        MutationError::LineageError(LineageError::BudgetExceeded { original_budget, subject_frontier, other_frontier }) => {
            IngestError::Budget { original_budget, subject_frontier, other_frontier }.into()
        }
        MutationError::RetrievalError(RetrievalError::MutationError(inner)) => type_comparison_error(*inner),
        other => other,
    }
}
