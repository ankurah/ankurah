//! Memory-only comparison of materialized query results. Storage still owns
//! entities and events; these trees and comparison snapshots are derived data.

mod client;
mod feedback;
mod incoming;
mod resident;
mod server;
mod tree;

pub(crate) use client::{abandon_current_state, finish_state, receive_state, request_state, RefreshHandle};
pub(crate) use incoming::{Inbox, IncomingStates, IncomingStream};
pub(crate) use resident::{ResidentPin, ResidentSubscription, ResidentTrees};
pub(crate) use server::expire_state_stream;
pub(crate) use server::{
    abandon_state_stream, apply_state_feedback, finish_state_round, kick_state_stream, prepare_state_stream, queue_live_changes,
    queue_round, source_unavailable, start_state_stream, state_stream_bypasses_policy, PendingStateStream, StateStreams,
};
pub(crate) use tree::{accept_peer_spec, QueryTree, Snapshot};
