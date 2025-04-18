use std::convert::Infallible;

use ankurah_proto::{CollectionId, DecodeError, ID};
use thiserror::Error;

use crate::{connector::SendError, policy::AccessDenied};

#[derive(Error, Debug)]
pub enum RetrievalError {
    #[error("access denied")]
    AccessDenied(AccessDenied),
    #[error("Parse error: {0}")]
    ParseError(ankql::error::ParseError),
    #[error("ID {0:?} not found")]
    NotFound(ID),
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
    #[error("Send error: {0}")]
    SendError(SendError),
    #[error("Internal channel closed")]
    InternalChannelClosed,
}

impl From<SendError> for RequestError {
    fn from(err: SendError) -> Self { RequestError::SendError(err) }
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
    #[error("failed step: {0}")]
    FailedStep(&'static str),
    #[error("general error: {0}")]
    General(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("no durable peers available")]
    NoDurablePeers,
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
