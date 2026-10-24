//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod comparison;
mod repair;
mod resident;
mod tree;

#[cfg(test)]
mod fixtures;

pub(crate) use resident::{ResidentPin, ResidentTrees};
pub(crate) use tree::{QueryTree, Snapshot};
