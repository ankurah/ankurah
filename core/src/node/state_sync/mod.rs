//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod resident;
mod tree;

pub(crate) use resident::{ResidentPin, ResidentTrees};
pub(crate) use tree::{QueryTree, Snapshot};
