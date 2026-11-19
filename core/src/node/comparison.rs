//! Bounded query comparison, with entity repairs on the peer update channel.

mod client;
mod repair;
pub(super) mod serve;
mod transport;
mod wire;
pub(super) use client::ComparisonBudget;
pub(super) use transport::IncomingComparisons;

pub(super) struct RepairReceipt {
    pub unavailable: Vec<ankurah_proto::EntityId>,
    pub bytes: usize,
    pub events: usize,
}

pub(super) fn response_error(error: anyhow::Error) -> ankurah_proto::NodeResponseBody {
    use ankurah_proto::{ComparisonFailure, NodeResponseBody};
    if let Some(refusal) = error.downcast_ref::<ComparisonFailure>() {
        return NodeResponseBody::Compared(Err(refusal.clone()));
    }
    if let Some(error) = error.downcast_ref::<crate::error::RetrievalError>() {
        if let crate::error::RetrievalError::StorageError(cause) = serve::restart(error.clone()) {
            if let Some(refusal) = cause.downcast_ref::<ComparisonFailure>() {
                return NodeResponseBody::Compared(Err(refusal.clone()));
            }
        }
    }
    NodeResponseBody::Error(error.to_string())
}
