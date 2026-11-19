//! Receiver-owned, directional replication through bounded digest comparison.

use std::{sync::Arc, time::Duration};

use ankql::ast::{Resolved, Selection};
use ankurah_proto::{ComparisonFailure, EntityId, KnownState};
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
    state: Mut<ReplicationState>,
    cancel: CancelFlag,
    wake: Arc<tokio::sync::Notify>,
}

impl Drop for Inner {
    fn drop(&mut self) { self.cancel.cancel(); }
}

impl Replication {
    pub fn state(&self) -> Read<ReplicationState> { self.0.state.read() }

    /// Wait for a completed source capture, or return the current refusal.
    /// An existing CaughtUp value describes the last completed capture; call
    /// `retry` first to request a new sweep explicitly.
    pub async fn wait_caught_up(&self) -> Result<KnownState, RetrievalError> {
        self.0
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
    pub fn retry(&self) {
        let retry = self.0.state.update(|state| match state {
            ReplicationState::Failed(RetrievalError::NodeDropped(_) | RetrievalError::NodeHalted(_)) => false,
            _ => {
                *state = ReplicationState::Connecting;
                true
            }
        });
        if retry {
            self.0.wake.notify_one();
        }
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
        peer: EntityId,
        selection: Selection<Resolved>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Replication, RetrievalError> {
        if !self.durable {
            return Err(MutationError::NotDurable.into());
        }
        self.system.check_not_halted()?;
        if peer == self.id {
            return Err(RetrievalError::Other("replication source must be another node".into()));
        }
        let sessions = sessions.into();
        self.sessions.attach(&sessions);
        let state = Mut::new(ReplicationState::Connecting);
        let cancel = CancelFlag::default();
        let wake = Arc::new(tokio::sync::Notify::new());
        let handle = Replication(Arc::new(Inner { state: state.clone(), cancel: cancel.clone(), wake: wake.clone() }));
        let changed = wake.clone();
        let session_watch = sessions.subscribe(move |_| changed.notify_one());
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
                state.set(ReplicationState::Failed(error));
                return;
            }
            // A tree owner keeps continuation active between comparisons. Neither
            // it nor the waiting worker keeps the Node or public handle alive.
            let mut _tree = None;
            loop {
                let attempt = async {
                    let node = weak.upgrade().ok_or(NodeDropped)?;
                    node.check_ready()?;
                    let connection = node.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
                    if !connection.durable {
                        return Err(RetrievalError::Other("replication source must be durable".into()));
                    }
                    drop(connection);
                    let credentials = sessions.current();
                    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
                    let mut scoped = selection.clone();
                    scoped.predicate = policy.filter_predicate(scoped.predicate)?;
                    state.set(ReplicationState::Comparing);
                    node.reconcile_selection(peer, scoped, &credentials, None).await
                };
                let result = tokio::select! {
                    biased;
                    _ = cancel.wait_for_cancel() => return,
                    _ = run.wait_value(false) => {
                        state.set(ReplicationState::Failed(stopped()));
                        return;
                    },
                    _ = wake.notified() => continue,
                    result = attempt => result,
                };
                let retry = match result {
                    Ok((source, tree)) => {
                        _tree = Some(tree);
                        state.set(ReplicationState::CaughtUp(source));
                        true
                    }
                    Err(error) if retryable(&error) => {
                        state.set(ReplicationState::Connecting);
                        true
                    }
                    Err(error) => {
                        state.set(ReplicationState::Failed(error));
                        false
                    }
                };
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
                        state.set(ReplicationState::Failed(stopped()));
                        return;
                    },
                    _ = wake.notified() => {},
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
