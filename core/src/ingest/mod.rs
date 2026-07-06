//! Ingest pipeline (RFC #268): planned application of events with uniform
//! execution semantics across every ingest lane.
//!
//! Sits between `entity` and the wire-shape adapters (`node_applier`, the
//! `node` commit lanes): adapters translate payloads into pipeline feeds, the
//! pipeline drives `entity`. The reactor and peer communication stay outside;
//! the pipeline returns outcomes and changes, feeders decide notification and
//! recovery.

pub(crate) mod executor;
pub(crate) mod outcome;
pub(crate) mod plan;
pub(crate) mod staging;
pub(crate) mod state_apply;

pub(crate) use executor::{execute_plan, PersistState};
pub(crate) use outcome::IngestOutcome;
pub(crate) use plan::plan_entity;
pub(crate) use staging::StagingArea;
pub(crate) use state_apply::apply_state_feed;
