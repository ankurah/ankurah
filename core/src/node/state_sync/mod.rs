//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod requester;
mod resident;
mod responder;
mod tree;

#[cfg(test)]
mod fixtures;

pub(crate) use requester::{check, fetch, subscribe, CheckOutcome, SyncedQuery};
pub(crate) use resident::{ResidentPin, ResidentSnapshot, ResidentTrees};
pub(crate) use responder::{answer_offer, compare_spans, sync_entities};
pub(crate) use tree::{QueryTree, Snapshot};
