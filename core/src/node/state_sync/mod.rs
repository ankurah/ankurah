//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod budget;
mod client;
mod feedback;
mod incoming;
mod resident;
mod server;
pub(crate) mod source;
mod tree;

pub(crate) use client::{abandon_current_state, finish_state, receive_state, request_state, RefreshHandle};
pub(crate) use incoming::{Inbox, IncomingStates, IncomingStream};
pub(crate) use resident::{ResidentSubscription, ResidentTrees};
pub(crate) use server::expire_state_stream;
pub(crate) use server::{
    abandon_state_stream, continue_state, finish_state_round, kick_state_stream, prepare_state_stream, queue_live, queue_round,
    source_unavailable, start_state_stream, state_stream_bypasses_policy, PendingStateStream, StateStreams,
};
pub(crate) use tree::{QueryTreeCache, Snapshot};

// Local responder limits, deliberately absent from the wire request.
const PAGE_BYTES: usize = 64 * 1024;
// Cached snapshots/items are cheap enough to fill a reply. Tree reads and
// repairs that may load storage or walk history have a separate, lower cap.
const PAGE_STEPS: usize = 128;
const PAGE_WORK: usize = 32;
const HEAD_PAGE: usize = 128;
const INITIAL_INVENTORY_BYTES: usize = 256 * 1024;
const INITIAL_INVENTORY_HEADS: usize = 2048;
const MAX_SNAPSHOT_ROWS: usize = 128;
const MAX_REPAIR_BYTES: usize = 64 * 1024 * 1024;
const FRAME_BYTES: usize = PAGE_BYTES + 4096;
const WINDOW_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_BYTES: usize = 2 * FRAME_BYTES;
const MAX_INFLIGHT_FRAMES: usize = 128;

fn whole() -> ankurah_proto::TreeSpan { ankurah_proto::TreeSpan::under(&ankurah_digest_tree::NodePrefix::root()) }
