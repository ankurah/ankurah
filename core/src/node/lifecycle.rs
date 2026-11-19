//! Admission and draining for a Node's terminal lifecycle.

use ankurah_signals::{Mut, Read, Wait};
use futures::future::BoxFuture;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

use super::{Node, NodeState};
use crate::error::{MutationError, NodeHaltReason};

#[derive(Clone)]
pub(crate) struct NodeLifecycle(Arc<Inner>);

struct Inner {
    state: Mut<NodeState>,
    admission: Mutex<Admission>,
    drained: Notify,
    reset: Mutex<Option<ResetAttempt>>,
}

#[derive(Default)]
struct Admission {
    halted: Option<NodeHaltReason>,
    active: usize,
}

struct ResetAttempt {
    result: Mut<Option<Result<(), Arc<MutationError>>>>,
}

/// Keeps reset behind admitted local work and its publication.
/// Engines separately fence native work that survives cancellation of its caller.
pub(crate) struct Operation(NodeLifecycle);

impl NodeLifecycle {
    pub(super) fn new() -> Self {
        Self(Arc::new(Inner {
            state: Mut::new(NodeState::Startup),
            admission: Mutex::new(Admission::default()),
            drained: Notify::new(),
            reset: Mutex::new(None),
        }))
    }

    pub(crate) fn state(&self) -> Read<NodeState> { self.0.state.read() }

    pub(crate) fn check_not_halted(&self) -> Result<(), NodeHaltReason> {
        self.0.admission.lock().unwrap().halted.clone().map_or(Ok(()), Err)
    }

    pub(crate) fn admit(&self) -> Result<Operation, NodeHaltReason> {
        let mut admission = self.0.admission.lock().unwrap();
        if let Some(reason) = &admission.halted {
            return Err(reason.clone());
        }
        admission.active += 1;
        Ok(Operation(self.clone()))
    }

    /// Cancel suspended work on halt, then drain its publication before resetting storage.
    /// A network wait never keeps reset waiting for the remote participant.
    pub(crate) fn while_running<'a, T: 'a, E: From<NodeHaltReason> + 'a>(
        &'a self,
        work: impl Future<Output = Result<T, E>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, E>> {
        // Box before constructing the wrapper: nested operations otherwise
        // embed their entire work future at every cancellation boundary.
        let work = Box::pin(work);
        Box::pin(async move {
            let _operation = self.admit()?;
            tokio::select! {
                biased;
                reason = self.wait_halted() => Err(reason.into()),
                result = work => result,
            }
        })
    }

    pub(crate) async fn wait_halted(&self) -> NodeHaltReason { self.state().wait_for(|state| state.halt_reason().cloned()).await }

    pub(crate) fn halt(&self, reason: NodeHaltReason) -> NodeHaltReason {
        let reason = {
            let mut admission = self.0.admission.lock().unwrap();
            admission.halted.get_or_insert(reason).clone()
        };
        self.0.state.set(NodeState::Halted(reason.clone()));
        reason
    }

    pub(super) fn mark_running(&self) {
        self.0.state.update(|state| {
            if matches!(state, NodeState::Startup) {
                *state = NodeState::Running;
            }
        });
    }

    async fn drain(&self) {
        loop {
            let drained = self.0.drained.notified();
            if self.0.admission.lock().unwrap().active == 0 {
                return;
            }
            drained.await;
        }
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        let mut admission = self.0 .0.admission.lock().unwrap();
        admission.active -= 1;
        if admission.active == 0 {
            self.0 .0.drained.notify_waiters();
        }
    }
}

impl<SE: crate::storage::StorageEngine + 'static, PA: crate::policy::PolicyAgent> Node<SE, PA> {
    /// Permanently halt this Node and empty its storage, including its persisted key and digest trees.
    /// Construct a new Node afterward. Coordinate any other Nodes or direct users of the same storage first.
    /// Concurrent callers share the attempt; dropping a caller does not cancel it. A failed wipe may be retried.
    /// The shared error retains the storage failure for every caller.
    pub async fn reset(&self) -> Result<(), Arc<MutationError>> {
        self.lifecycle.halt(NodeHaltReason::Reset);
        let result = {
            let mut attempt = self.lifecycle.0.reset.lock().unwrap();
            if attempt.as_ref().is_some_and(|attempt| attempt.result.with(|result| matches!(result, Some(Err(_))))) {
                *attempt = None;
            }
            let attempt = attempt.get_or_insert_with(|| {
                let attempt = ResetAttempt { result: Mut::new(None) };
                let node = self.clone();
                let result = attempt.result.clone();
                crate::task::spawn(async move {
                    // Closing connections wakes pending protocol work while admission is closed.
                    for (connection, _) in node.peer_connections.to_vec() {
                        node.deregister_peer(connection);
                    }
                    node.lifecycle.drain().await;
                    // A synchronous registration admitted just before halt may have finished during the drain.
                    for (connection, _) in node.peer_connections.to_vec() {
                        node.deregister_peer(connection);
                    }
                    node.reactor.close().await;
                    let _publication = node.commit_publication_lock.lock().await;
                    result.set(Some(node.storage.delete_all().await.map(|_| ()).map_err(Arc::new)));
                });
                attempt
            });
            attempt.result.read()
        };
        result.wait_for(Clone::clone).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test]
    async fn halt_closes_admission_and_drain_waits_for_existing_work() {
        let lifecycle = NodeLifecycle::new();
        let operation = lifecycle.admit().unwrap();
        lifecycle.halt(NodeHaltReason::Reset);
        assert!(matches!(lifecycle.admit(), Err(NodeHaltReason::Reset)));
        assert!(lifecycle.drain().now_or_never().is_none());
        drop(operation);
        lifecycle.drain().await;
        lifecycle.mark_running();
        assert_eq!(lifecycle.state().value(), NodeState::Halted(NodeHaltReason::Reset));
    }

    #[tokio::test]
    async fn halt_cancels_suspended_work_before_drain() {
        let lifecycle = NodeLifecycle::new();
        let work = lifecycle.while_running::<(), NodeHaltReason>(std::future::pending());
        tokio::pin!(work);
        assert!(work.as_mut().now_or_never().is_none());
        lifecycle.halt(NodeHaltReason::Reset);
        assert_eq!(work.await, Err(NodeHaltReason::Reset));
        lifecycle.drain().await;
    }
}

#[cfg(test)]
mod reset_tests {
    use super::*;
    use crate::{policy::PermissiveAgent, test_utils::TestStorage};
    use futures::FutureExt;
    use std::sync::atomic::Ordering;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn callers_share_reset_and_cancelling_a_waiter_does_not_cancel_the_wipe() {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
        node.wait_ready().await.unwrap();
        let old_id = node.id();
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *storage.hold_reset.lock().unwrap() = Some((entered, release_rx));
        let first = {
            let node = node.clone();
            tokio::spawn(async move { node.reset().await })
        };
        entered_rx.await.unwrap();
        let second = node.reset();
        tokio::pin!(second);
        assert!(second.as_mut().now_or_never().is_none());
        first.abort();
        release.send(()).unwrap();
        second.await.unwrap();
        let fresh = Node::new_durable(storage, PermissiveAgent::new());
        fresh.wait_ready().await.unwrap();
        assert_ne!(fresh.id(), old_id);
        node.reset().await.unwrap();
        // Idempotence includes not wiping a successor's newly stored key.
        assert_eq!(Node::new_durable(fresh.storage.clone(), PermissiveAgent::new()).wait_id().await.unwrap(), fresh.id().unwrap());
    }

    #[tokio::test]
    async fn failed_reset_keeps_the_node_terminal_and_can_be_retried() {
        let storage = Arc::new(TestStorage::default());
        let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
        node.wait_ready().await.unwrap();
        let old_id = node.id();
        storage.fail_reset.store(true, Ordering::SeqCst);
        assert!(matches!(*node.reset().await.unwrap_err(), MutationError::WriteConflict));
        assert_eq!(node.state().value(), NodeState::Halted(NodeHaltReason::Reset));
        node.reset().await.unwrap();
        assert_ne!(Some(Node::new_durable(storage, PermissiveAgent::new()).wait_id().await.unwrap()), old_id);
    }

    #[tokio::test]
    async fn reset_during_key_lookup_cancels_the_lookup_before_installing_a_key() {
        let storage = Arc::new(TestStorage::default());
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        *storage.hold_map_write.lock().unwrap() = Some((entered, release_rx));
        let node = Node::new(storage.clone(), PermissiveAgent::new());
        entered_rx.await.unwrap();
        node.reset().await.unwrap();
        assert_eq!(node.id(), None);
        assert!(release.send(()).is_err(), "reset cancelled the old lookup");
        assert_eq!(node.wait_id().await, Err(NodeHaltReason::Reset));
        Node::new_durable(storage, PermissiveAgent::new()).wait_ready().await.unwrap();
    }
}
