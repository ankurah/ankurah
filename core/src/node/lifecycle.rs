pub(super) mod state;

use crate::error::{MutationError, NodeHaltReason};
use crate::storage::{StorageCommitOutcome, StorageEngine, StorageTransaction};
use ankurah_signals::{Mut, Read, Wait};
use state::NodeState;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{watch, Notify};

/// A node's run state from startup to terminal halt, the storage writes it admits while it runs,
/// and the reset that halts the node and wipes its storage.
pub(crate) struct NodeLifecycle<SE>(Arc<Inner<SE>>);
impl<SE> Clone for NodeLifecycle<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE> {
    storage: Arc<SE>,
    node_state: Mut<NodeState>,
    writes: WritesInFlight,
    wipe: Mutex<Wipe>,
}

/// How far the reset's wipe of storage has got.
enum Wipe {
    /// No wipe has succeeded: none was asked for, or the last one failed.
    Unwiped,
    /// A wipe is running and reports its outcome on this channel.
    Running(watch::Receiver<Option<WipeOutcome>>),
    Wiped,
}

type WipeOutcome = Result<(), Arc<MutationError>>;

impl<SE> NodeLifecycle<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(storage: Arc<SE>) -> Self {
        Self(Arc::new(Inner {
            storage,
            node_state: Mut::new(NodeState::Startup),
            writes: WritesInFlight { count: Mutex::new(0), ended: Notify::new() },
            wipe: Mutex::new(Wipe::Unwiped),
        }))
    }

    pub(crate) fn node_state(&self) -> Read<NodeState> { self.0.node_state.read() }

    /// Publish successful initialization without reviving a halted node.
    pub(crate) fn mark_running(&self) {
        self.0.node_state.update(|state| {
            if matches!(state, NodeState::Startup) {
                *state = NodeState::Running;
            }
        });
    }

    /// Reject terminal halt without blocking the work that initializes this node.
    pub(crate) fn check_not_halted(&self) -> Result<(), NodeHaltReason> {
        self.0.node_state.with(|state| state.halt_reason().cloned().map_or(Ok(()), Err))
    }

    /// Wait for terminal halt and return its reason.
    pub(crate) async fn wait_halted(&self) -> NodeHaltReason { self.node_state().wait_for(|state| state.halt_reason().cloned()).await }

    /// Publish terminal halt; services observe the state and stop their own work.
    /// In-flight storage work may still finish.
    pub(crate) fn halt(&self, halt_reason: NodeHaltReason) -> NodeHaltReason {
        self.0.node_state.update(|state| match state {
            NodeState::Halted(reason) => reason.clone(),
            NodeState::Startup | NodeState::Running => {
                *state = NodeState::Halted(halt_reason.clone());
                halt_reason
            }
        })
    }

    /// Begin writing to storage, unless this node has halted. A reset's wipe waits until the
    /// write ends, so none lands in a wiped store.
    pub(crate) fn begin_storage_write(&self) -> Result<StorageWrite<'_>, NodeHaltReason> {
        // Counted before the halt is checked: a wipe that finds no write in flight has halted the
        // node first, so a write counted after that finds the halt here.
        let write = self.0.writes.begin();
        self.check_not_halted()?;
        Ok(write)
    }

    /// Halt with [`NodeHaltReason::Reset`] and wipe storage, as [`crate::Node::reset`] describes.
    pub(crate) async fn reset(&self) -> Result<(), MutationError> {
        self.halt(NodeHaltReason::Reset);
        let mut outcome = {
            let mut wipe = self.0.wipe.lock().unwrap_or_else(PoisonError::into_inner);
            match &*wipe {
                Wipe::Wiped => return Ok(()),
                // A wipe's sender lives until it reports, so a closed channel is a wipe that ended without reporting.
                Wipe::Running(outcome) if outcome.has_changed().is_ok() => outcome.clone(),
                Wipe::Unwiped | Wipe::Running(_) => {
                    let (report, outcome) = watch::channel(None);
                    crate::task::spawn(Self::wipe(self.0.clone(), report));
                    *wipe = Wipe::Running(outcome.clone());
                    outcome
                }
            }
        };
        let reported = outcome.wait_for(Option::is_some).await.map(|outcome| outcome.clone());
        match reported {
            Ok(Some(Ok(()))) => Ok(()),
            Ok(Some(Err(error))) => Err(MutationError::WipeFailed(error)),
            // `wait_for` yields a reported outcome, or fails once a wipe that panicked drops its sender.
            Ok(None) | Err(_) => Err(MutationError::WipeAbandoned),
        }
    }

    /// Wipe storage once no storage write is in flight, record whether it succeeded, and report it.
    async fn wipe(inner: Arc<Inner<SE>>, report: watch::Sender<Option<WipeOutcome>>) {
        inner.writes.until_none().await;
        let outcome = inner.storage.delete_all().await.map(|_| ()).map_err(Arc::new);
        *inner.wipe.lock().unwrap_or_else(PoisonError::into_inner) = if outcome.is_ok() { Wipe::Wiped } else { Wipe::Unwiped };
        if let Err(unheard) = report.send(Some(outcome)) {
            if let Some(Err(error)) = unheard.0 {
                tracing::error!(%error, "A reset's storage wipe failed with no caller waiting; the next reset retries it");
            }
        }
    }
}

/// The storage writes in flight on one node.
struct WritesInFlight {
    count: Mutex<usize>,
    /// Woken when the count returns to zero.
    ended: Notify,
}

impl WritesInFlight {
    fn begin(&self) -> StorageWrite<'_> {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        StorageWrite(self)
    }

    async fn until_none(&self) {
        loop {
            // Registered before the count is read, so the last write's end cannot be missed.
            let ended = self.ended.notified();
            if *self.count.lock().unwrap_or_else(PoisonError::into_inner) == 0 {
                return;
            }
            ended.await;
        }
    }
}

/// A storage write counted in flight until it drops; a reset's wipe waits until none is.
pub(crate) struct StorageWrite<'a>(&'a WritesInFlight);

impl StorageWrite<'_> {
    /// Commit `transaction`, ending this write once the commit returns.
    pub(crate) async fn commit<T: StorageTransaction>(self, transaction: T) -> Result<StorageCommitOutcome, MutationError> {
        transaction.commit().await
    }
}

impl Drop for StorageWrite<'_> {
    fn drop(&mut self) {
        let mut count = self.0.count.lock().unwrap_or_else(PoisonError::into_inner);
        *count -= 1;
        if *count == 0 {
            self.0.ended.notify_waiters();
        }
    }
}
