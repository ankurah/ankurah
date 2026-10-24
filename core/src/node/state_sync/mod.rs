//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod resident;
mod responder;
mod tree;

#[cfg(test)]
mod fixtures;

pub(crate) use resident::{ResidentPin, ResidentTrees};
pub(crate) use responder::{answer_offer, compare_spans, sync_entities};
pub(crate) use tree::{QueryTree, Snapshot};
