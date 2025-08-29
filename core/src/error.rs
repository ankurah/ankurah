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
    AnkqlFilter(ankql::selection::filter::Error),
    #[error("Future join: {0}")]
    FutureJoin(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("{0}")]
    Anyhow(anyhow::Error),
    #[error("Decode error: {0}")]
    DecodeError(DecodeError),
    #[error("State error: {0}")]
    StateError(StateError),
    #[error("Mutation error: {0}")]
    MutationError(Box<MutationError>),
}

impl From<MutationError> for RetrievalError {
    fn from(err: MutationError) -> Self { RetrievalError::MutationError(Box::new(err)) }
}

impl RetrievalError {
    pub fn storage(err: impl std::error::Error + Send + Sync + 'static) -> Self { RetrievalError::StorageError(Box::new(err)) }

    pub fn future_join(err: impl std::error::Error + Send + Sync + 'static) -> Self { RetrievalError::FutureJoin(Box::new(err)) }
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self { RetrievalError::DeserializationError(e) }
}

impl From<ankql::selection::filter::Error> for RetrievalError {
    fn from(err: ankql::selection::filter::Error) -> Self { RetrievalError::AnkqlFilter(err) }
}

impl From<anyhow::Error> for RetrievalError {
    fn from(err: anyhow::Error) -> Self { RetrievalError::Anyhow(err) }
}

impl From<Infallible> for RetrievalError {
    fn from(_: Infallible) -> Self { unreachable!() }
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
}

#[derive(Error, Debug)]
pub enum LineageError {
    #[error("incomparable")]
    Incomparable,
    #[error("partially descends: {meet:?}")]
    PartiallyDescends { meet: Vec<EventId> },
    #[error("budget exceeded: {subject_frontier:?} {other_frontier:?}")]
    BudgetExceeded { original_budget: usize, subject_frontier: BTreeSet<EventId>, other_frontier: BTreeSet<EventId> },
}

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
