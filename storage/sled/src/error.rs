use ankurah_core::value::ValueType;
use ankurah_core::{error::StorageError, EntityId};
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

impl From<SledRetrievalError> for StorageError {
    fn from(err: SledRetrievalError) -> Self {
        match err {
            SledRetrievalError::StorageError(e) => StorageError::BackendError(Box::new(e)),
            SledRetrievalError::EntityNotFound(id) => StorageError::EntityNotFound(id),
            SledRetrievalError::EventNotFound(id) => StorageError::EventsNotFound(vec![id]),
            SledRetrievalError::Other(e) => StorageError::BackendError(e),
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
    #[error("Type mismatch: expected {0:?}, got {1:?}")]
    TypeMismatch(ValueType, ValueType),
}

impl From<IndexError> for StorageError {
    fn from(err: IndexError) -> Self { StorageError::BackendError(Box::new(err)) }
}

pub fn sled_error(err: sled::Error) -> StorageError { SledRetrievalError::StorageError(err).into() }
