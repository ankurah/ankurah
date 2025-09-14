use ankurah_core::{
    error::{MutationError, RetrievalError},
    EntityId,
};
use ankurah_proto::EventId;
use thiserror::Error;

pub enum SledRetrievalError {
    StorageError(sled::Error),
    EntityNotFound(EntityId),
    EventNotFound(EventId),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sled::Error> for SledRetrievalError {
    fn from(val: sled::Error) -> Self { SledRetrievalError::StorageError(val) }
}

impl From<SledRetrievalError> for RetrievalError {
    fn from(err: SledRetrievalError) -> Self {
        match err {
            SledRetrievalError::StorageError(e) => RetrievalError::StorageError(Box::new(e)),
            SledRetrievalError::EntityNotFound(id) => RetrievalError::EntityNotFound(id),
            SledRetrievalError::EventNotFound(id) => RetrievalError::EventNotFound(id),
            SledRetrievalError::Other(e) => RetrievalError::StorageError(e),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Storage error: {0}")]
    StorageError(#[from] sled::Error),
    #[error("Bincode error: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("Invalid key length")]
    InvalidKeyLength,
    #[error("Property not found: {0}")]
    PropertyNotFound(String),
    #[error("UTF8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

impl From<IndexError> for RetrievalError {
    fn from(err: IndexError) -> Self { RetrievalError::StorageError(Box::new(err)) }
}

impl From<IndexError> for MutationError {
    fn from(err: IndexError) -> Self { MutationError::General(Box::new(err)) }
}

pub fn sled_error(err: sled::Error) -> RetrievalError { SledRetrievalError::StorageError(err).into() }
