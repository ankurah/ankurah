//! Internal error types.
//!
//! These are NOT exposed to FFI. They exist for internal Rust code structure
//! and are mapped to `Failure` at public API boundaries.

use std::collections::BTreeSet;

use ankurah_proto::{CollectionId, DecodeError, EntityId, EventId};
use thiserror::Error;

use crate::connector::SendError;
use crate::policy::AccessDenied;

#[derive(Debug)]
pub enum LineageError {
    Incomparable,
    PartiallyDescends { meet: Vec<EventId> },
    BudgetExceeded {
        original_budget: usize,
        subject_frontier: BTreeSet<EventId>,
        other_frontier: BTreeSet<EventId>,
    },
}

impl std::fmt::Display for LineageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineageError::Incomparable => write!(f, "incomparable"),
            LineageError::PartiallyDescends { meet } => {
                write!(f, "partially descends: [")?;
                let meets: Vec<_> = meet.iter().map(|id| id.to_base64_short()).collect();
                write!(f, "{}]", meets.join(", "))
            }
            LineageError::BudgetExceeded { original_budget, subject_frontier, other_frontier } => {
                let subject: Vec<_> = subject_frontier.iter().map(|id| id.to_base64_short()).collect();
                let other: Vec<_> = other_frontier.iter().map(|id| id.to_base64_short()).collect();
                write!(f, "budget exceeded ({}): subject[{}] other[{}]", original_budget, subject.join(", "), other.join(", "))
            }
        }
    }
}

impl std::error::Error for LineageError {}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("events not found: {0:?}")]
    EventsNotFound(Vec<EventId>),

    #[error("entity not found: {0:?}")]
    EntityNotFound(EntityId),

    #[error("collection not found: {0}")]
    CollectionNotFound(CollectionId),

    #[error("decode error: {0}")]
    DecodeError(DecodeError),

    #[error("serialization error: {0}")]
    SerializationError(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("backend error: {0}")]
    BackendError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<DecodeError> for StorageError {
    fn from(err: DecodeError) -> Self {
        StorageError::DecodeError(err)
    }
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("serialization error: {0}")]
    SerializationError(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("DDL error: {0}")]
    DDLError(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("DML error: {0}")]
    DMLError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<bincode::Error> for StateError {
    fn from(e: bincode::Error) -> Self {
        StateError::SerializationError(Box::new(e))
    }
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("peer not connected")]
    PeerNotConnected,

    #[error("connection lost")]
    ConnectionLost,

    #[error("server error: {0}")]
    ServerError(String),

    #[error("send error: {0}")]
    SendError(SendError),

    #[error("internal channel closed")]
    InternalChannelClosed,

    #[error("unexpected response: {0:?}")]
    UnexpectedResponse(ankurah_proto::NodeResponseBody),

    #[error("access denied: {0}")]
    AccessDenied(AccessDenied),
}

impl From<AccessDenied> for RequestError {
    fn from(err: AccessDenied) -> Self {
        RequestError::AccessDenied(err)
    }
}

impl From<SendError> for RequestError {
    fn from(err: SendError) -> Self {
        RequestError::SendError(err)
    }
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("predicate not found")]
    PredicateNotFound,

    #[error("already subscribed to predicate")]
    PredicateAlreadySubscribed,

    #[error("subscription not found")]
    SubscriptionNotFound,
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("deserialization error: {0}")]
    Deserialization(Box<dyn std::error::Error + Send + Sync + 'static>),

    #[error("validation failed: {0}")]
    ValidationFailed(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("rejected: {0}")]
    Rejected(&'static str),
}

/// Wrapper for anyhow::Error to make it compatible with error-stack
#[derive(Debug, Error)]
#[error("{0}")]
pub struct AnyhowWrapper(String);

impl From<anyhow::Error> for AnyhowWrapper {
    fn from(err: anyhow::Error) -> Self {
        AnyhowWrapper(format!("{err:#}"))
    }
}

#[derive(Debug)]
pub enum ApplyError {
    Items(Vec<ApplyErrorItem>),
    CollectionNotFound(CollectionId),
    Storage(StorageError),
    State(StateError),
    Lineage(LineageError),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::Items(errors) => {
                write!(f, "failed to apply {} delta(s)", errors.len())?;
                for (i, err) in errors.iter().enumerate() {
                    write!(f, "\n  [{}] {}", i + 1, err)?;
                }
                Ok(())
            }
            ApplyError::CollectionNotFound(id) => write!(f, "collection not found: {}", id),
            ApplyError::Storage(e) => write!(f, "storage error: {}", e),
            ApplyError::State(e) => write!(f, "state error: {}", e),
            ApplyError::Lineage(e) => write!(f, "lineage error: {}", e),
            ApplyError::Other(e) => write!(f, "error: {}", e),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApplyError::Storage(e) => Some(e),
            ApplyError::State(e) => Some(e),
            ApplyError::Lineage(e) => Some(e),
            ApplyError::Other(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ApplyErrorItem {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub cause: ApplyErrorCause,
}

impl std::fmt::Display for ApplyErrorItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to apply delta for entity {} in collection {}: {}",
            self.entity_id.to_base64_short(),
            self.collection,
            self.cause
        )
    }
}

/// Specific error causes when applying a delta or update to an entity.
#[derive(Debug, Error)]
pub enum ApplyErrorCause {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("state: {0}")]
    State(#[from] StateError),

    #[error("lineage: {0}")]
    Lineage(#[from] LineageError),

    #[error("access denied: {0}")]
    AccessDenied(#[from] AccessDenied),

    #[error("{0}")]
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}
