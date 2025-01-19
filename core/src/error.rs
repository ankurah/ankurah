use std::convert::Infallible;

use ankurah_proto::{DecodeError, ID};
use thiserror::Error;

use crate::connector::SendError;

#[derive(Error, Debug)]
pub enum RetrievalError {
    #[error("Parse error: {0}")]
    ParseError(ankql::error::ParseError),
    #[error("ID {0:?} not found")]
    NotFound(ID),
    #[error("Storage error: {0}")]
    StorageError(Box<dyn std::error::Error + Send + Sync + 'static>),
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
