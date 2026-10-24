//! The handles that pin a resident and the commands they send its actor.

use super::{identity::QueryView, Lru};
use crate::{
    node::{state_sync::Snapshot, Node},
    policy::PolicyAgent,
    reactor::maintenance::SourceError,
    session::ContextData,
    storage::StorageEngine,
};
use ankurah_proto as proto;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::{mpsc, oneshot, Notify};

/// The handle every lease of one resident shares: the channel to its actor,
/// the stop signal its drop sends, the wake for its routes' cleanup, whether
/// its actor still runs, and how many leases pin it.
pub(super) struct Resident<CD: ContextData> {
    pub(super) commands: mpsc::Sender<Command<CD>>,
    /// Dropped with the last handle, which stops the actor.
    pub(super) _stop: oneshot::Sender<()>,
    pub(super) cleanup: Arc<Notify>,
    pub(super) alive: AtomicBool,
    pub(super) pins: AtomicUsize,
    pub(super) lru: Weak<Mutex<Lru<CD>>>,
}

/// Type-erased ownership lets live queries and protocol drivers hold the same RAII lease
/// without depending on the node's credential type.
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
    pub(crate) async fn snapshot(&self) -> anyhow::Result<ResidentSnapshot> {
        let (response, result) = oneshot::channel();
        self.resident
            .commands
            .send(Command::Snapshot { view: self.view.clone(), response })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        let mut taken = result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))??;
        taken.snapshot.resident = Some(ResidentPin { _lease: Arc::new(self.clone()) });
        Ok(taken)
    }
    pub(crate) async fn subscribe<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query: proto::QueryId,
        version: u32,
        catalog: bool,
        known: proto::KnownState,
    ) -> anyhow::Result<(proto::StateReply, ResidentSubscription<CD>)>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let owner = Arc::new(());
        let (response, result) = oneshot::channel();
        self.resident
            .commands
            .send(Command::Subscribe {
                view: self.view.clone(),
                peer,
                request,
                fingerprint,
                query,
                version,
                catalog,
                known,
                owner: Arc::downgrade(&owner),
                response,
            })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        let pending = result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))??;
        let reply = pending.initialize(node).await?;
        let subscription = ResidentSubscription { lease: self.clone(), owner: Some(owner), peer, stream: reply.frame.stream };
        Ok((reply, subscription))
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

/// A peer's live route on a resident, and the lease that keeps the resident.
/// Dropping it stops the route's delivery; the resident stays maintained in
/// the LRU.
pub(crate) struct ResidentSubscription<CD: ContextData> {
    pub(super) lease: ResidentLease<CD>,
    owner: Option<Arc<()>>,
    peer: proto::EntityId,
    stream: proto::StateStreamId,
}
impl<CD: ContextData> ResidentSubscription<CD> {
    pub(crate) async fn refresh(&self, number: u64, known: proto::KnownState) -> anyhow::Result<()> {
        let (response, result) = oneshot::channel();
        self.lease
            .resident
            .commands
            .send(Command::Refresh { view: self.lease.view.clone(), peer: self.peer, stream: self.stream, number, known, response })
            .await
            .map_err(|_| anyhow::anyhow!(SourceError::Changed))?;
        result.await.map_err(|_| anyhow::anyhow!(SourceError::Changed))?
    }
}
impl<CD: ContextData> Drop for ResidentSubscription<CD> {
    fn drop(&mut self) {
        // Wake cleanup after releasing the route's liveness token.
        self.owner.take();
        self.lease.resident.cleanup.notify_one();
    }
}

/// A snapshot of a resident's view, and the source revision it reflects.
pub(crate) struct ResidentSnapshot {
    pub(crate) revision: u64,
    pub(crate) snapshot: Snapshot,
}
/// What a lease asks its resident's actor to do, each answered on its own channel.
pub(super) enum Command<CD: ContextData> {
    Snapshot {
        view: Arc<QueryView<CD>>,
        response: oneshot::Sender<anyhow::Result<ResidentSnapshot>>,
    },
    Subscribe {
        view: Arc<QueryView<CD>>,
        peer: proto::EntityId,
        request: proto::RequestId,
        fingerprint: Vec<u8>,
        query: proto::QueryId,
        version: u32,
        catalog: bool,
        known: proto::KnownState,
        owner: Weak<()>,
        response: oneshot::Sender<anyhow::Result<crate::node::state_sync::PendingStateStream<CD>>>,
    },
    Refresh {
        view: Arc<QueryView<CD>>,
        peer: proto::EntityId,
        stream: proto::StateStreamId,
        number: u64,
        known: proto::KnownState,
        response: oneshot::Sender<anyhow::Result<()>>,
    },
}

impl<CD: ContextData> Command<CD> {
    pub(super) fn view(&self) -> &QueryView<CD> {
        match self {
            Self::Snapshot { view, .. } | Self::Subscribe { view, .. } | Self::Refresh { view, .. } => view,
        }
    }

    pub(super) fn reject(self, error: &anyhow::Error) {
        // A source error keeps its kind for the retry checks; any other keeps its causes.
        let error =
            error.downcast_ref::<SourceError>().map_or_else(|| anyhow::anyhow!("{error:#}"), |error| anyhow::anyhow!(error.clone()));
        match self {
            Self::Snapshot { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Subscribe { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Refresh { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}
