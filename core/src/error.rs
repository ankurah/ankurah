use std::{collections::BTreeSet, convert::Infallible};

use ankurah_proto::{CollectionId, DecodeError, EntityId, EventId};
use thiserror::Error;

use crate::{connector::SendError, policy::AccessDenied};

#[derive(Error, Debug)]
pub enum RetrievalError {
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("Parse error: {0}")]
    ParseError(ankql::error::ParseError),
    #[error("Entity not found: {0:?}")]
    EntityNotFound(EntityId),
    #[error("Event not found: {0:?}")]
    EventNotFound(EventId),
    #[error("Storage error: {0}")]
    StorageError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Collection not found: {0}")]
    CollectionNotFound(CollectionId),
    #[error("Update failed: {0}")]
    FailedUpdate(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Deserialization error: {0}")]
    DeserializationError(bincode::Error),
    #[error("No durable peers available for fetch operation")]
    NoDurablePeers,
    #[error("Other error: {0}")]
    Other(String),
    #[error("bucket name must only contain valid characters")]
    InvalidBucketName,
    #[error("ankql filter: {0}")]
    AnkqlFilter(crate::selection::filter::Error),
    #[error("Future join: {0}")]
    FutureJoin(tokio::task::JoinError),
    #[error("{0}")]
    Anyhow(anyhow::Error),
    #[error("Decode error: {0}")]
    DecodeError(DecodeError),
    #[error("State error: {0}")]
    StateError(StateError),
    #[error("Mutation error: {0}")]
    MutationError(Box<MutationError>),
    #[error("Property error: {0}")]
    PropertyError(Box<crate::property::PropertyError>),
    #[error("Request error: {0}")]
    RequestError(RequestError),
    #[error("Apply error: {0}")]
    ApplyError(ApplyError),
}

impl From<RequestError> for RetrievalError {
    fn from(err: RequestError) -> Self { RetrievalError::RequestError(err) }
}

impl From<crate::property::PropertyError> for RetrievalError {
    fn from(err: crate::property::PropertyError) -> Self { RetrievalError::PropertyError(Box::new(err)) }
}

impl From<tokio::task::JoinError> for RetrievalError {
    fn from(err: tokio::task::JoinError) -> Self { RetrievalError::FutureJoin(err) }
}

impl From<MutationError> for RetrievalError {
    fn from(err: MutationError) -> Self { RetrievalError::MutationError(Box::new(err)) }
}

impl RetrievalError {
    pub fn storage(err: impl std::error::Error + Send + Sync + 'static) -> Self { RetrievalError::StorageError(Box::new(err)) }
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self { RetrievalError::DeserializationError(e) }
}

impl From<crate::selection::filter::Error> for RetrievalError {
    fn from(err: crate::selection::filter::Error) -> Self { RetrievalError::AnkqlFilter(err) }
}

impl From<anyhow::Error> for RetrievalError {
    fn from(err: anyhow::Error) -> Self { RetrievalError::Anyhow(err) }
}

impl From<Infallible> for RetrievalError {
    fn from(_: Infallible) -> Self { unreachable!("Infallible can never be constructed") }
}

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Peer not connected")]
    PeerNotConnected,
    #[error("Connection lost")]
    ConnectionLost,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Send error: {0}")]
    SendError(SendError),
    #[error("Internal channel closed")]
    InternalChannelClosed,
    #[error("Unexpected response: {0:?}")]
    UnexpectedResponse(ankurah_proto::NodeResponseBody),
}

impl From<SendError> for RequestError {
    fn from(err: SendError) -> Self { RequestError::SendError(err) }
}

#[derive(Error, Debug)]
pub enum SubscriptionError {
    #[error("predicate not found")]
    PredicateNotFound,
    #[error("already subscribed to predicate")]
    PredicateAlreadySubscribed,
    #[error("subscription not found")]
    SubscriptionNotFound,
}

impl From<DecodeError> for RetrievalError {
    fn from(err: DecodeError) -> Self { RetrievalError::DecodeError(err) }
}

#[derive(Error, Debug)]
pub enum MutationError {
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("already exists")]
    AlreadyExists,
    #[error("retrieval error: {0}")]
    RetrievalError(RetrievalError),
    #[error("state error: {0}")]
    StateError(StateError),
    #[error("failed update: {0}")]
    UpdateFailed(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("failed step: {0}: {1}")]
    FailedStep(&'static str, String),
    #[error("failed to set property: {0}: {1}")]
    FailedToSetProperty(&'static str, String),
    #[error("general error: {0}")]
    General(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("no durable peers available")]
    NoDurablePeers,
    #[error("decode error: {0}")]
    DecodeError(DecodeError),
    #[error("lineage error: {0}")]
    LineageError(LineageError),
    #[error("peer rejected transaction")]
    PeerRejected,
    #[error("invalid event")]
    InvalidEvent,
    #[error("invalid update")]
    InvalidUpdate(&'static str),
    #[error("property error: {0}")]
    PropertyError(crate::property::PropertyError),
    #[error("future join: {0}")]
    FutureJoin(tokio::task::JoinError),
    #[error("anyhow error: {0}")]
    Anyhow(anyhow::Error),
    #[error("TOCTOU attempts exhausted")]
    TOCTOUAttemptsExhausted,
}

impl From<tokio::task::JoinError> for MutationError {
    fn from(err: tokio::task::JoinError) -> Self { MutationError::FutureJoin(err) }
}

impl From<anyhow::Error> for MutationError {
    fn from(err: anyhow::Error) -> Self { MutationError::Anyhow(err) }
}

#[derive(Debug)]
pub enum LineageError {
    Incomparable,
    PartiallyDescends { meet: Vec<EventId> },
    BudgetExceeded { original_budget: usize, subject_frontier: BTreeSet<EventId>, other_frontier: BTreeSet<EventId> },
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

impl From<LineageError> for MutationError {
    fn from(err: LineageError) -> Self { MutationError::LineageError(err) }
}

impl From<DecodeError> for MutationError {
    fn from(err: DecodeError) -> Self { MutationError::DecodeError(err) }
}

#[cfg(feature = "wasm")]
impl From<MutationError> for wasm_bindgen::JsValue {
    fn from(err: MutationError) -> Self { err.to_string().into() }
}
#[cfg(feature = "wasm")]
impl From<RetrievalError> for wasm_bindgen::JsValue {
    fn from(err: RetrievalError) -> Self { err.to_string().into() }
}

impl From<AccessDenied> for MutationError {
    fn from(err: AccessDenied) -> Self { MutationError::AccessDenied(err) }
}

impl From<bincode::Error> for MutationError {
    fn from(e: bincode::Error) -> Self { MutationError::StateError(StateError::SerializationError(e)) }
}

impl From<RetrievalError> for MutationError {
    fn from(err: RetrievalError) -> Self {
        match err {
            RetrievalError::AccessDenied(a) => MutationError::AccessDenied(a),
            _ => MutationError::RetrievalError(err),
        }
    }
}
impl From<AccessDenied> for RetrievalError {
    fn from(err: AccessDenied) -> Self { RetrievalError::AccessDenied(err) }
}

impl From<SubscriptionError> for RetrievalError {
    fn from(err: SubscriptionError) -> Self { RetrievalError::Anyhow(anyhow::anyhow!("Subscription error: {:?}", err)) }
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("serialization error: {0}")]
    SerializationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DDL error: {0}")]
    DDLError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("DMLError: {0}")]
    DMLError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<bincode::Error> for StateError {
    fn from(e: bincode::Error) -> Self { StateError::SerializationError(Box::new(e)) }
}

impl From<StateError> for MutationError {
    fn from(err: StateError) -> Self { MutationError::StateError(err) }
}

impl From<crate::property::PropertyError> for MutationError {
    fn from(err: crate::property::PropertyError) -> Self { MutationError::PropertyError(err) }
}

impl From<StateError> for RetrievalError {
    fn from(err: StateError) -> Self { RetrievalError::StateError(err) }
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Deserialization error: {0}")]
    Deserialization(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Validation failed: {0}")]
    ValidationFailed(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Rejected: {0}")]
    Rejected(&'static str),
}

/// Error type for NodeApplier operations (applying remote deltas)
#[derive(Debug)]
pub enum ApplyError {
    Items(Vec<ApplyErrorItem>),
    CollectionNotFound(CollectionId),
    RetrievalError(Box<RetrievalError>),
    MutationError(Box<MutationError>),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::Items(errors) => {
                write!(f, "Failed to apply {} delta(s)", errors.len())?;
                for (i, err) in errors.iter().enumerate() {
                    write!(f, "\n  [{}] {}", i + 1, err)?;
                }
                Ok(())
            }
            ApplyError::CollectionNotFound(id) => write!(f, "Collection not found: {}", id),
            ApplyError::RetrievalError(e) => write!(f, "Retrieval error: {}", e),
            ApplyError::MutationError(e) => write!(f, "Mutation error: {}", e),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApplyError::RetrievalError(e) => Some(e),
            ApplyError::MutationError(e) => Some(e),
            _ => None,
        }
    }
}

/// Error applying a specific delta
#[derive(Debug)]
pub struct ApplyErrorItem {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub cause: MutationError,
}

impl std::fmt::Display for ApplyErrorItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to apply delta for entity {} in collection {}: {}", self.entity_id.to_base64_short(), self.collection, self.cause)
    }
}

impl From<RetrievalError> for ApplyError {
    fn from(err: RetrievalError) -> Self { ApplyError::RetrievalError(Box::new(err)) }
}

impl From<MutationError> for ApplyError {
    fn from(err: MutationError) -> Self { ApplyError::MutationError(Box::new(err)) }
}

impl From<ApplyError> for RetrievalError {
    fn from(err: ApplyError) -> Self { RetrievalError::ApplyError(err) }
}
