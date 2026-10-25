//! The handles that pin a resident tree, and the snapshot requests they send its actor.

use super::{identity::QueryView, Lru};
use crate::{node::state_sync::tree::Snapshot, reactor::maintenance::SourceError, session::ContextData};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::{mpsc, oneshot, watch};

/// The handle every lease of one resident tree shares: the channel to its actor,
/// the stop signal its drop sends, whether its actor still runs, and how many
/// leases pin it.
pub(super) struct ResidentTree<CD: ContextData> {
    pub(super) requests: mpsc::Sender<SnapshotRequest<CD>>,
    /// Dropped with the last handle, which stops the actor.
    pub(super) _stop: oneshot::Sender<()>,
    pub(super) alive: AtomicBool,
    pub(super) pins: AtomicUsize,
    pub(super) lru: Weak<Mutex<Lru<CD>>>,
}

/// Type-erased ownership lets a snapshot keep its tree without depending on
/// the node's credential type.
#[derive(Clone)]
pub(crate) struct TreePin {
    _lease: Arc<dyn Send + Sync>,
}

/// A pin on one resident tree, with the view its holder reads: while any
/// lease lives, the LRU keeps the tree.
pub(crate) struct TreeLease<CD: ContextData> {
    pub(super) tree: Arc<ResidentTree<CD>>,
    pub(super) view: Arc<QueryView<CD>>,
}
impl<CD: ContextData> TreeLease<CD> {
    pub(super) fn new(tree: Arc<ResidentTree<CD>>, view: Arc<QueryView<CD>>) -> Self {
        tree.pins.fetch_add(1, Ordering::AcqRel);
        Self { tree, view }
    }
    /// A snapshot of this lease's view after every commit published so far.
    /// The snapshot and its change signal each pin the tree while they live.
    pub(crate) async fn snapshot(&self) -> anyhow::Result<LeasedSnapshot> {
        let (response, result) = oneshot::channel();
        self.tree
            .requests
            .send(SnapshotRequest { view: self.view.clone(), response })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        let (mut snapshot, receiver) = result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))??;
        let pin = TreePin { _lease: Arc::new(self.clone()) };
        snapshot.pin = Some(pin.clone());
        Ok(LeasedSnapshot { snapshot, changes: TreeChanges { receiver, _pin: pin } })
    }
}
impl<CD: ContextData> Clone for TreeLease<CD> {
    fn clone(&self) -> Self { Self::new(self.tree.clone(), self.view.clone()) }
}
impl<CD: ContextData> Drop for TreeLease<CD> {
    fn drop(&mut self) {
        if self.tree.pins.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(lru) = self.tree.lru.upgrade() {
                lru.lock().unwrap().trim_idle();
            }
        }
    }
}

/// A snapshot of a resident tree's view, and the signal of the tree's next change.
pub(crate) struct LeasedSnapshot {
    pub(crate) snapshot: Snapshot,
    pub(crate) changes: TreeChanges,
}

/// The signal of a resident tree's next change since a snapshot. It pins the
/// tree while it lives, so that whatever waits on it keeps its tree, however
/// many others the LRU holds and however large the tree.
pub(crate) struct TreeChanges {
    receiver: watch::Receiver<u64>,
    _pin: TreePin,
}

impl TreeChanges {
    /// Wait for the tree's next change; an error once the tree has stopped.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> { self.receiver.changed().await }
}

/// A lease's request for a snapshot of its view, answered on its own channel
/// with the snapshot and the tree's version to watch.
pub(super) struct SnapshotRequest<CD: ContextData> {
    pub(super) view: Arc<QueryView<CD>>,
    pub(super) response: oneshot::Sender<anyhow::Result<(Snapshot, watch::Receiver<u64>)>>,
}

impl<CD: ContextData> SnapshotRequest<CD> {
    pub(super) fn reject(self, error: &anyhow::Error) {
        // A source error keeps its kind for the retry checks; any other keeps its causes.
        let error =
            error.downcast_ref::<SourceError>().map_or_else(|| anyhow::anyhow!("{error:#}"), |error| anyhow::anyhow!(error.clone()));
        let _ = self.response.send(Err(error));
    }
}
