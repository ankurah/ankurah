//! Synchronizing a node's copy of a query result with a peer's. The requester
//! offers its account of its copy; the responder compares it with a snapshot of
//! its own result, answers with its own account, and pushes what the requester
//! lacks on the update channel; the requester waits until its copy matches or
//! exceeds that account, and repairs what does not arrive. Both sides describe
//! their results from memory-only trees of entity heads that reactor
//! maintenance keeps current. Storage still owns entities and events; the
//! trees and their snapshots are derived data.

mod requester;
mod resident_trees;
mod responder;
mod tree;

#[cfg(test)]
mod fixtures;

pub(crate) use requester::{check, fetch, subscribe, CheckOutcome, SyncedQuery};
pub(crate) use resident_trees::ResidentTrees;
pub(crate) use responder::{answer_offer, compare_spans, sync_entities};
