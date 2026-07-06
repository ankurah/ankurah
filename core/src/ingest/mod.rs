//! Ingest pipeline (RFC #268): planned application of events with uniform
//! execution semantics across every ingest lane.
//!
//! Sits between `entity` and the wire-shape adapters (`node_applier`, the
//! `node` commit lanes): adapters translate payloads into pipeline feeds, the
//! pipeline drives `entity`. The reactor and peer communication stay outside;
//! the pipeline returns outcomes and changes, feeders decide notification and
//! recovery.

// The substrate lands before its consumers; the allows come off when the
// executor arrives and the API goes live.
#[allow(dead_code)]
pub(crate) mod outcome;
#[allow(dead_code)]
pub(crate) mod plan;
#[allow(dead_code)]
pub(crate) mod staging;

#[allow(unused_imports)]
pub(crate) use outcome::{IngestOutcome, SkipReason};
#[allow(unused_imports)]
pub(crate) use plan::{plan_entity, IngestPlan};
#[allow(unused_imports)]
pub(crate) use staging::StagingArea;
