//! Receiver-owned, directional replication through bounded digest comparison.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use ankql::ast::{Resolved, Selection};
use ankurah_proto::{ComparisonFailure, KnownState, NodeId};
use ankurah_signals::{Mut, Read, Subscribe, Wait};

use super::Node;
use crate::{
    error::{MutationError, NodeDropped, RequestError, RetrievalError},
    policy::{ContextPolicy, PolicyAgent},
    session::SessionSet,
    storage::StorageEngine,
    util::cancel_flag::CancelFlag,
};

/// Progress of one directional replication interest.
#[derive(Clone, Debug)]
pub enum ReplicationState {
    /// Waiting for the configured source to become reachable.
    Connecting,
    /// Comparing a captured source Selection and applying missing history.
    Comparing,
    /// The receiver covers this source capture. The source may have changed
    /// since; local descendants and retained departed entities are allowed.
    /// Coverage follows storage-transaction completion; power-loss durability
    /// depends on the storage engine and its configuration.
    CaughtUp(KnownState),
    /// A refusal requiring changed credentials, limits, or an explicit retry.
    Failed(RetrievalError),
}

/// Maintains one Selection from a durable source into a durable receiver.
///
/// Clones share the interest; dropping the last handle cancels its current
/// transfer. Already accepted states and history remain stored. The application
/// recreates interests and supplies live credentials after restarting a Node.
#[derive(Clone)]
pub struct Replication(Arc<Inner>);

struct Inner {
    progress: Arc<Progress>,
    cancel: CancelFlag,
}

impl Drop for Inner {
    fn drop(&mut self) { self.cancel.cancel(); }
}

impl Replication {
    pub fn state(&self) -> Read<ReplicationState> { self.0.progress.state.read() }

    /// Wait for a completed source capture, or return the current refusal.
    /// An existing CaughtUp value describes the last completed capture; call
    /// `retry` first to request a new sweep explicitly.
    pub async fn wait_caught_up(&self) -> Result<KnownState, RetrievalError> {
        self.0
            .progress
            .state
            .wait_for(|state| match state {
                ReplicationState::CaughtUp(source) => Some(Ok(source.clone())),
                ReplicationState::Failed(error) => Some(Err(error.clone())),
                _ => None,
            })
            .await
    }

    /// Cancel an in-flight sweep and compare again using current credentials.
    /// A dropped or halted Node cannot be retried through an existing handle.
    pub fn retry(&self) { self.0.progress.restart(); }
}

struct Progress {
    state: Mut<ReplicationState>,
    revision: AtomicU64,
    wake: tokio::sync::Notify,
}

impl Progress {
    fn restart(&self) {
        let retry = self.state.update(|state| match state {
            ReplicationState::Failed(RetrievalError::NodeDropped(_) | RetrievalError::NodeHalted(_)) => false,
            _ => {
                self.revision.fetch_add(1, Ordering::Relaxed);
                *state = ReplicationState::Connecting;
                true
            }
        });
        if retry {
            self.wake.notify_one();
        }
    }

    fn publish(&self, revision: u64, next: ReplicationState) -> bool {
        // Retry and completion serialize through the same state lock. Checking
        // the revision before acquiring it would allow an obsolete result through.
        self.state.update(|state| {
            if self.revision.load(Ordering::Relaxed) != revision {
                return false;
            }
            *state = next;
            true
        })
    }
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    /// Replicate `selection` from `peer` into this durable Node.
    ///
    /// Each call creates one directional interest; the reverse direction is
    /// configured independently. An unlimited Selection retains its digest tree,
    /// not an entity result set. Sweeps compare captured versions and transfer
    /// event history within PolicyAgent's comparison limits. A source change is
    /// discovered by the next sweep, at most one second after the preceding one.
    /// The application reestablishes interests after restart; session secrets
    /// are never persisted by this handle.
    pub fn replicate_from(
        &self,
        peer: NodeId,
        selection: Selection<Resolved>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Replication, RetrievalError> {
        if !self.durable {
            return Err(MutationError::NotDurable.into());
        }
        self.system.check_not_halted()?;
        let sessions = sessions.into();
        self.sessions.attach(&sessions);
        let progress = Arc::new(Progress {
            state: Mut::new(ReplicationState::Connecting),
            revision: AtomicU64::new(0),
            wake: tokio::sync::Notify::new(),
        });
        let cancel = CancelFlag::default();
        let handle = Replication(Arc::new(Inner { progress: progress.clone(), cancel: cancel.clone() }));
        let changed = progress.clone();
        let session_watch = sessions.subscribe(move |_| changed.restart());
        let weak = self.weak();
        let run = self.run.clone();
        let ready = self.wait_ready();
        crate::task::spawn(async move {
            let _session_watch = session_watch;
            let stopped = || {
                weak.upgrade()
                    .and_then(|node| node.system.check_not_halted().err())
                    .map(RetrievalError::from)
                    .unwrap_or_else(|| NodeDropped.into())
            };
            // Readiness may wait indefinitely for provisioning. Its detached
            // signal must not retain the Node while the application drops it.
            let initialized = tokio::select! {
                biased;
                _ = cancel.wait_for_cancel() => return,
                _ = run.wait_value(false) => Err(stopped()),
                result = ready => result.map_err(RetrievalError::from),
            };
            if let Err(error) = initialized {
                progress.state.set(ReplicationState::Failed(error));
                return;
            }
            // A tree owner keeps continuation active between comparisons. Neither
            // it nor the waiting worker keeps the Node or public handle alive.
            let mut _tree = None;
            loop {
                let revision = progress.revision.load(Ordering::Relaxed);
                let attempt = async {
                    let node = weak.upgrade().ok_or(NodeDropped)?;
                    node.check_ready()?;
                    let connections = node.connections_of(peer);
                    let connection_id = connections
                        .iter()
                        .rev()
                        .copied()
                        .find(|id| node.peer_connections.get(id).is_some_and(|connection| connection.durable))
                        .ok_or_else(|| {
                            if connections.is_empty() {
                                RetrievalError::from(RequestError::PeerNotConnected)
                            } else {
                                RetrievalError::Other("replication source must be durable".into())
                            }
                        })?;
                    let credentials = sessions.current();
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
                    let mut scoped = selection.clone();
                    scoped.predicate = policy.filter_predicate(scoped.predicate)?;
                    progress.publish(revision, ReplicationState::Comparing);
                    node.reconcile_selection(connection_id, scoped, &credentials, None).await
                };
                let result = tokio::select! {
                    biased;
                    _ = cancel.wait_for_cancel() => return,
                    _ = run.wait_value(false) => {
                        progress.state.set(ReplicationState::Failed(stopped()));
                        return;
                    },
                    _ = progress.wake.notified() => continue,
                    result = attempt => result,
                };
                let (next, retry, tree) = match result {
                    Ok((source, tree)) => (ReplicationState::CaughtUp(source), true, Some(tree)),
                    Err(error) if retryable(&error) => (ReplicationState::Connecting, true, None),
                    Err(error) => (ReplicationState::Failed(error), false, None),
                };
                if !progress.publish(revision, next) {
                    continue;
                }
                if let Some(tree) = tree {
                    _tree = Some(tree);
                }
                let next_sweep = async {
                    if retry {
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                    } else {
                        futures::future::pending::<()>().await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = cancel.wait_for_cancel() => return,
                    _ = run.wait_value(false) => {
                        progress.state.set(ReplicationState::Failed(stopped()));
                        return;
                    },
                    _ = progress.wake.notified() => {},
                    _ = next_sweep => {},
                }
            }
        });
        Ok(handle)
    }
}

fn retryable(error: &RetrievalError) -> bool {
    match error {
        RetrievalError::RequestError(
            RequestError::PeerNotConnected
            | RequestError::ConnectionLost
            | RequestError::SendError(_)
            | RequestError::InternalChannelClosed,
        ) => true,
        RetrievalError::MutationError(error) => matches!(**error, MutationError::WriteConflict),
        RetrievalError::StorageError(error) => {
            matches!(error.downcast_ref::<ComparisonFailure>(), Some(ComparisonFailure::Expired | ComparisonFailure::Restart))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests;
