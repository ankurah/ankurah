use crate::{error::NodeHaltReason, livequery::LiveQueryRegistry, reactor::Reactor};
use ankurah_proto::QueryId;

/// Node operations independent of a context's credentials, with storage and policy types erased.
pub(crate) trait NodeErased: Send + Sync {
    /// Reject operations after halt; startup is allowed.
    fn check_not_halted(&self) -> Result<(), NodeHaltReason>;

    fn reactor(&self) -> &Reactor;
    fn live_queries(&self) -> &LiveQueryRegistry;

    /// Remove the remote subscription when the livequery is dropped.
    fn unsubscribe_remote_query(&self, query_id: QueryId);
}
