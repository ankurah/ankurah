use super::NodeState;
use crate::error::{MutationError, NodeHaltReason};
use crate::storage::StorageEngine;
use ankurah_signals::{Mut, Read, Wait};
use std::sync::Arc;

/// A node's run state from startup to terminal halt, and the reset that halts the node and wipes its storage.
pub(crate) struct NodeLifecycle<SE>(Arc<Inner<SE>>);
impl<SE> Clone for NodeLifecycle<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE> {
    storage: Arc<SE>,
    node_state: Mut<NodeState>,
}

impl<SE> NodeLifecycle<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(storage: Arc<SE>) -> Self { Self(Arc::new(Inner { storage, node_state: Mut::new(NodeState::Startup) })) }

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

    /// Halt with [`NodeHaltReason::Reset`], then delete everything in storage.
    /// The wipe runs in its own task, so it finishes even if the caller stops waiting.
    pub(crate) async fn reset(&self) -> Result<(), MutationError> {
        self.halt(NodeHaltReason::Reset);
        let storage = self.0.storage.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        crate::task::spawn(async move {
            let _ = finished.send(storage.delete_all().await);
        });
        // Cannot fail in practice: the task always sends before it ends, and it does not panic.
        completion.await.expect("storage wipe task ended without reporting")?;
        Ok(())
    }
}
