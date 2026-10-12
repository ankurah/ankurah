//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod client;
mod server;
mod tree;

pub(crate) use server::Comparisons;
pub(crate) use tree::{QueryTree, Snapshot};

// Local responder limits, deliberately absent from the wire request.
const PAGE_BYTES: usize = 64 * 1024;
const PAGE_WORK: usize = 32;
const LEAF_PAGE: usize = 32;
const MAX_REPAIR_BYTES: usize = 64 * 1024 * 1024;

fn whole() -> ankurah_proto::AddressRange { ankurah_proto::AddressRange::under(&ankurah_digest_tree::NodePrefix::root()) }
