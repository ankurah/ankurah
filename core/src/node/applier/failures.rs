//! Items of peers' updates that failed to apply, published for whatever sync
//! is waiting on their entities: a pushed item is applied by the connection's
//! task, not by the sync that asked for it, so the failure reaches the sync
//! only this way.

use crate::error::ApplyError;
use ankurah_proto::{state_sync::limits::MAX_PAGE_HEADS, EntityId};
use std::sync::Arc;
use tokio::sync::broadcast;

/// One item of a peer's update that failed to apply.
#[derive(Clone)]
pub(crate) struct ApplyFailure {
    /// The peer whose update carried the item.
    pub(crate) peer: EntityId,
    pub(crate) entity_id: EntityId,
    /// The failure, as the error of an application that failed for this item.
    pub(crate) error: Arc<ApplyError>,
}

/// The node's signal of pushed items that failed to apply. It keeps, for each
/// listener, as many failures as one push carries items; a listener that falls
/// further behind loses the oldest, and a sync that lost a failure waits out
/// its idle deadline, as it does for a push that never arrives.
pub(crate) struct ApplyFailures(broadcast::Sender<ApplyFailure>);

impl Default for ApplyFailures {
    fn default() -> Self { Self(broadcast::channel(MAX_PAGE_HEADS).0) }
}

impl ApplyFailures {
    /// Publish each item of `peer`'s update that `error` says failed to apply.
    /// An update refused whole names no item.
    pub(crate) fn publish(&self, peer: EntityId, error: ApplyError) {
        let ApplyError::Items(failed) = error else { return };
        for item in failed {
            let entity_id = item.entity_id;
            // Sending fails only while nothing listens, when there is no one to tell.
            let _ = self.0.send(ApplyFailure { peer, entity_id, error: Arc::new(ApplyError::Items(vec![item])) });
        }
    }

    /// Listen for the failures published from now on.
    pub(crate) fn listen(&self) -> broadcast::Receiver<ApplyFailure> { self.0.subscribe() }
}
