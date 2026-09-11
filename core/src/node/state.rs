use crate::error::{NodeHaltReason, NodeReadinessError};

/// A node's initialization and terminal lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// No system has been created, adopted, or restored yet; connections are allowed.
    Uninitialized,
    /// The system is established; the catalog is awaiting its initial durable answers.
    Startup,
    /// System and catalog initialization are complete; ongoing connectivity is independent.
    Running,
    /// Permanently stopped accepting new work. Discard this node and its contexts and queries.
    Halted(NodeHaltReason),
}

impl NodeState {
    pub(super) fn check_ready(&self) -> Result<(), NodeReadinessError> {
        match self {
            Self::Uninitialized | Self::Startup => Err(NodeReadinessError::NotReady),
            Self::Running => Ok(()),
            Self::Halted(reason) => Err(NodeReadinessError::Halted(reason.clone())),
        }
    }

    /// Why the node halted, or `None` while it can still accept work.
    pub fn halt_reason(&self) -> Option<&NodeHaltReason> {
        match self {
            Self::Uninitialized | Self::Startup | Self::Running => None,
            Self::Halted(reason) => Some(reason),
        }
    }
}
