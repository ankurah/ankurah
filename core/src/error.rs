use crate::model::ID;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RetrievalError {
    #[error("ID {0:?} not found")]
    NotFound(ID),
    #[error("Storage error: {0}")]
    StorageError(Box<dyn std::error::Error>),
    #[error("Update failed: {0}")]
    FailedUpdate(Box<dyn std::error::Error>),
    #[error("Deserialization error: {0}")]
    DeserializationError(bincode::Error),
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self {
        RetrievalError::DeserializationError(e)
    }
}
