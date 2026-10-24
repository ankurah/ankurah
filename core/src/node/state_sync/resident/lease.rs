//! The handles that pin a resident and the snapshot requests they send its actor.

use super::{identity::QueryView, Lru};
use crate::{node::state_sync::Snapshot, reactor::maintenance::SourceError, session::ContextData};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::{mpsc, oneshot};

/// The handle every lease of one resident shares: the channel to its actor,
/// the stop signal its drop sends, whether its actor still runs, and how many
/// leases pin it.
pub(super) struct Resident<CD: ContextData> {
    pub(super) requests: mpsc::Sender<SnapshotRequest<CD>>,
    /// Dropped with the last handle, which stops the actor.
    pub(super) _stop: oneshot::Sender<()>,
    pub(super) alive: AtomicBool,
    pub(super) pins: AtomicUsize,
    pub(super) lru: Weak<Mutex<Lru<CD>>>,
}

/// Type-erased ownership lets a snapshot keep its resident without depending on
/// the node's credential type.
#[derive(Clone)]
pub(crate) struct ResidentPin {
    _lease: Arc<dyn Send + Sync>,
}

/// A pin on one resident, with the view its holder reads: while any lease
/// lives, the LRU keeps the resident.
pub(crate) struct ResidentLease<CD: ContextData> {
    pub(super) resident: Arc<Resident<CD>>,
    pub(super) view: Arc<QueryView<CD>>,
}
impl<CD: ContextData> ResidentLease<CD> {
    pub(super) fn new(resident: Arc<Resident<CD>>, view: Arc<QueryView<CD>>) -> Self {
        resident.pins.fetch_add(1, Ordering::AcqRel);
        Self { resident, view }
    }
    /// A snapshot of this lease's view after every commit published so far. The
    /// snapshot pins the resident while it lives.
    pub(crate) async fn snapshot(&self) -> anyhow::Result<ResidentSnapshot> {
        let (response, result) = oneshot::channel();
        self.resident
            .requests
            .send(SnapshotRequest { view: self.view.clone(), response })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        let mut taken = result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))??;
        taken.snapshot.resident = Some(ResidentPin { _lease: Arc::new(self.clone()) });
        Ok(taken)
    }
}
impl<CD: ContextData> Clone for ResidentLease<CD> {
    fn clone(&self) -> Self { Self::new(self.resident.clone(), self.view.clone()) }
}
impl<CD: ContextData> Drop for ResidentLease<CD> {
    fn drop(&mut self) {
        if self.resident.pins.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(lru) = self.resident.lru.upgrade() {
                lru.lock().unwrap().trim_idle();
            }
        }
    }
}

/// A snapshot of a resident's view, and the source revision it reflects.
pub(crate) struct ResidentSnapshot {
    pub(crate) revision: u64,
    pub(crate) snapshot: Snapshot,
}

/// A lease's request for a snapshot of its view, answered on its own channel.
pub(super) struct SnapshotRequest<CD: ContextData> {
    pub(super) view: Arc<QueryView<CD>>,
    pub(super) response: oneshot::Sender<anyhow::Result<ResidentSnapshot>>,
}

impl<CD: ContextData> SnapshotRequest<CD> {
    pub(super) fn reject(self, error: &anyhow::Error) {
        // A source error keeps its kind for the retry checks; any other keeps its causes.
        let error =
            error.downcast_ref::<SourceError>().map_or_else(|| anyhow::anyhow!("{error:#}"), |error| anyhow::anyhow!(error.clone()));
        let _ = self.response.send(Err(error));
    }
}
