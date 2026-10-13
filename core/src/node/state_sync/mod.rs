//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod client;
mod feedback;
mod incoming;
mod server;
mod tree;

pub(crate) use incoming::{Inbox, IncomingStates, IncomingStream};
pub(crate) use server::Comparisons;
pub(crate) use tree::{QueryTree, Snapshot};

// Local responder limits, deliberately absent from the wire request.
const PAGE_BYTES: usize = 64 * 1024;
// Cached snapshots/items are cheap enough to fill a reply. Tree reads and
// repairs that may load storage or walk history have a separate, lower cap.
const PAGE_STEPS: usize = 128;
const PAGE_WORK: usize = 32;
const LEAF_PAGE: usize = 32;
const MAX_REPAIR_BYTES: usize = 64 * 1024 * 1024;
const FRAME_BYTES: usize = PAGE_BYTES + 4096;
const WINDOW_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_BYTES: usize = 2 * FRAME_BYTES;
const MAX_INFLIGHT_FRAMES: usize = 128;

fn whole() -> ankurah_proto::AddressRange { ankurah_proto::AddressRange::under(&ankurah_digest_tree::NodePrefix::root()) }
